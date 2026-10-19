//! End-to-end test for the Noise_XX encrypted tunnel relay path.
//!
//! A provider node runs a tunnel listener with an echo server as its relay
//! target. A consumer performs a full Noise_XX handshake, pushes encrypted
//! payloads through the tunnel, verifies the provider relayed them to the
//! target and returned the response. It then verifies the provider's
//! settlement side effects: finalized connection history, peer reputation,
//! and a signed metering receipt.

use echo_daemon::availability::AvailabilityEngine;
use echo_daemon::identity::NodeIdentity;
use echo_daemon::meter::MeteringEngine;
use echo_daemon::sqlite_store::SqliteStore;
use echo_daemon::security::SecurityCounters;
use echo_daemon::tunnel::{RelayConfig, TunnelEvent, TunnelService};
use echo_daemon::MetricsBackend;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// snow default maximum Noise message size.
const NOISE_MAX_MSG_LEN: usize = 65535;

/// Find a free TCP port on loopback by binding with port 0, then releasing it.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Spawn a TCP echo server on loopback; returns its address.
async fn spawn_echo_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let (mut reader, mut writer) = stream.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
            });
        }
    });
    addr.to_string()
}

#[tokio::test]
async fn tunnel_handshake_relay_and_settlement_roundtrip() {
    let dir = TempDir::new().unwrap();

    // ── Identities (fresh, per-test) ──────────────────────────────
    let provider =
        NodeIdentity::load_or_generate(&dir.path().join("provider.json"), "us-test").unwrap();
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-test").unwrap();
    let provider_peer_id: libp2p::PeerId = provider.peer_id_str().parse().unwrap();
    let consumer_peer_id: libp2p::PeerId = consumer.peer_id_str().parse().unwrap();

    // ── Provider runtime state ────────────────────────────────────
    let db_path = format!("sqlite:{}", dir.path().join("tunnel.db").display());
    let backend = Arc::new(SqliteStore::new(&db_path).await.unwrap());
    let availability = Arc::new(RwLock::new(AvailabilityEngine::new(100.0, 100.0, 10)));

    // Metering configured to issue a settlement receipt on every packet so
    // the receipt path is exercised without needing a large transfer.
    let (metering, mut receipt_rx) = MeteringEngine::with_config(provider.clone(), 1, 0);

    // The provider's relay target: an echo server (exercises egress + ingress).
    let echo_addr = spawn_echo_server().await;

    // Provider tunnel service.
    let relay_config = RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: echo_addr.clone(),
        local_peer_id: provider.peer_id_str().to_string(),
        identity: None,
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
        policy: None,
        security: Arc::new(SecurityCounters::default()),
        handshake_timeout_ms: 10_000,
        enable_session_tickets: false,
    };
    let (provider_svc, mut provider_events) = TunnelService::new(
        provider_peer_id,
        metering.clone(),
        availability.clone(),
        relay_config,
        4,
        provider.noise_secret_key_bytes().to_vec(),
    );

    // Spawn the incoming tunnel listener (mirrors main.rs construction of
    // the accept-task RelayConfig from the service's own config).
    let tunnel_addr = format!("127.0.0.1:{}", free_port().await);
    let accept_config = RelayConfig {
        backend: provider_svc.relay_config().backend.clone(),
        metering: provider_svc.relay_config().metering.clone(),
        availability: provider_svc.relay_config().availability.clone(),
        target_addr: provider_svc.relay_config().target_addr.clone(),
        local_peer_id: provider_svc.relay_config().local_peer_id.clone(),
        identity: provider_svc.relay_config().identity.clone(),
        max_upload_mbps: provider_svc.relay_config().max_upload_mbps,
        max_download_mbps: provider_svc.relay_config().max_download_mbps,
        policy: None,
        security: Arc::new(SecurityCounters::default()),
        handshake_timeout_ms: 10_000,
        enable_session_tickets: false,
    };
    let listener_addr = tunnel_addr.clone();
    let listener_task = tokio::spawn(async move {
        TunnelService::accept_incoming(
            &listener_addr,
            provider_svc.event_tx().clone(),
            accept_config,
            provider_svc.conn_semaphore(),
            provider_svc.static_private_key(),
        )
        .await
        .expect("tunnel listener should bind");
    });

    // ── Consumer side: Noise_XX handshake ────────────────────────
    let consumer_metering = MeteringEngine::new(consumer.clone()).0;
    let consumer_avail = Arc::new(RwLock::new(AvailabilityEngine::new(0.0, 0.0, 4)));
    let consumer_config = RelayConfig {
        backend: backend.clone(),
        metering: consumer_metering.clone(),
        availability: consumer_avail.clone(),
        target_addr: "127.0.0.1:1".to_string(), // not used by this test path
        local_peer_id: consumer.peer_id_str().to_string(),
        identity: None,
        max_upload_mbps: 0.0,
        max_download_mbps: 0.0,
        policy: None,
        security: Arc::new(SecurityCounters::default()),
        handshake_timeout_ms: 10_000,
        enable_session_tickets: false,
    };
    let (consumer_svc, _consumer_events) = TunnelService::new(
        consumer_peer_id,
        consumer_metering,
        consumer_avail,
        consumer_config,
        4,
        consumer.noise_secret_key_bytes().to_vec(),
    );

    let session_id = "e2e-session-0001";
    let expected_provider_noise_pubkey = provider.noise_public_key_bytes().to_vec();

    // Retry a few times to absorb listener-bind races on loopback.
    let mut session = None;
    for attempt in 0..5 {
        match consumer_svc
            .connect_to_provider(&tunnel_addr, provider_peer_id, session_id, &expected_provider_noise_pubkey)
            .await
        {
            Ok(s) => {
                session = Some(s);
                break;
            }
            Err(e) => {
                assert!(attempt < 4, "handshake failed after 5 attempts: {e:?}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let mut session = session.expect("consumer and provider should complete the handshake");

    // ── Push an encrypted payload; provider relays to echo and back ──
    let payload = b"echo-node-tunnel-roundtrip-payload\n";
    let mut enc_buf = [0u8; NOISE_MAX_MSG_LEN];
    let enc_len = session
        .transport
        .lock()
        .await
        .write_message(payload, &mut enc_buf)
        .unwrap();
    session.stream.write_all(&enc_buf[..enc_len]).await.unwrap();

    // Read the relayed (echoed) response and decrypt it.
    let mut raw_buf = [0u8; NOISE_MAX_MSG_LEN];
    let mut plain_buf = [0u8; NOISE_MAX_MSG_LEN];
    let n = session.stream.read(&mut raw_buf).await.unwrap();
    assert!(n > 0, "provider should relay the echo response through the tunnel");
    let plain_len = session
        .transport
        .lock()
        .await
        .read_message(&raw_buf[..n], &mut plain_buf)
        .unwrap();
    assert_eq!(
        &plain_buf[..plain_len],
        &payload[..],
        "relayed response must match the original payload"
    );

    // ── Close the tunnel → provider-side teardown ─────────────────
    drop(session);

    // Provider must emit SessionClosed with the transfer accounting.
    match tokio::time::timeout(Duration::from_secs(10), provider_events.recv())
        .await
        .expect("provider should emit SessionClosed within 10s")
        .expect("provider event channel should stay open")
    {
        TunnelEvent::SessionClosed {
            session_id: sid, ..
        } => assert_eq!(sid, session_id),
        other => panic!("expected SessionClosed, got {other:?}"),
    }

    // ── Teardown side effect 1: connection_history is finalized ──
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (sent, recv, reason) = loop {
        let row: Option<(i64, i64, Option<String>)> = sqlx::query_as(
            "SELECT bytes_sent, bytes_received, exit_reason FROM connection_history \
             WHERE local_peer_id = ? ORDER BY id DESC LIMIT 1",
        )
        .bind(provider.peer_id_str())
        .fetch_optional(backend.pool())
        .await
        .unwrap();
        if let Some((sent, recv, Some(reason))) = row {
            break (sent, recv, reason);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "provider teardown did not finalize connection history in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(reason, "completed");
    assert!(
        sent >= payload.len() as i64,
        "bytes_sent ({sent}) should include the relayed payload"
    );
    assert!(
        recv >= payload.len() as i64,
        "bytes_received ({recv}) should include the relayed response"
    );

    // ── Teardown side effect 2: consumer reputation was recorded ──
    let rep = backend
        .get_peer_reputation(consumer.peer_id_str())
        .await
        .unwrap()
        .expect("provider should record reputation for the consumer peer");
    assert!(rep.successful_sessions >= 1);
    assert!(
        rep.total_bytes_relayed >= (sent + recv) as u64,
        "reputation should accumulate the relayed bytes"
    );

    // ── Settlement side effect: a signed receipt was issued ───────
    // With receipt_interval = 1 the provider issues interim receipts per
    // packet; the final settlement receipt comes from end_session and must
    // cover the full transfer. Keep reading until we find it.
    let total_bytes = (sent + recv) as u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut final_receipt = None;
    while std::time::Instant::now() < deadline {
        let receipt = match tokio::time::timeout(
            Duration::from_millis(500),
            receipt_rx.recv(),
        )
        .await
        {
            Ok(Some(r)) => r,
            Ok(None) => panic!("receipt channel closed unexpectedly"),
            Err(_) => break, // no more receipts in flight
        };
        assert_eq!(receipt.session_id, session_id);
        assert_eq!(receipt.signer_peer_id, provider.peer_id_str());
        let verified = receipt
            .verify_signature(provider.public_key_bytes.as_slice())
            .expect("signature verification should not error");
        assert!(verified, "receipt signature must verify against the provider key");
        if receipt.bytes_transferred >= total_bytes {
            final_receipt = Some(receipt);
            break;
        }
    }
    let final_receipt = final_receipt
        .expect("a final settlement receipt covering the full transfer should be issued");
    assert_eq!(final_receipt.bytes_transferred, total_bytes);

    // ── Settlement: the signed final receipt accumulates toward payout ──
    // Mirrors main.rs wiring: after insert_receipt succeeds, the verified
    // receipt is run through the SettlementEngine.
    use echo_daemon::settlement::SettlementEngine;
    let settlement = SettlementEngine::new(provider.clone(), backend.clone(), 0.50);
    let first = settlement
        .settle_receipt(final_receipt.clone())
        .await
        .unwrap();
    assert!(
        first.is_some(),
        "final receipt must settle toward payout the first time"
    );
    let replay = settlement.settle_receipt(final_receipt).await.unwrap();
    assert!(replay.is_none(), "replaying a settled receipt must not double-count");

    let summary = settlement.summary().await.unwrap();
    assert!(summary.receipts_settled >= 1, "payout summary must include the settled receipt");
    assert!(
        summary.total_bytes_settled >= total_bytes,
        "payout bytes must cover the full relayed transfer"
    );

    // Stop the tunnel listener.
    listener_task.abort();
}
#[tokio::test]
async fn stalled_handshake_is_timed_out_and_counted() {
    let dir = TempDir::new().unwrap();
    let provider =
        NodeIdentity::load_or_generate(&dir.path().join("provider.json"), "us-test").unwrap();
    let provider_peer_id: libp2p::PeerId = provider.peer_id_str().parse().unwrap();

    let db_path = format!("sqlite:{}", dir.path().join("slowloris.db").display());
    let backend = Arc::new(SqliteStore::new(&db_path).await.unwrap());
    let availability = Arc::new(RwLock::new(AvailabilityEngine::new(100.0, 100.0, 10)));
    let (metering, _receipt_rx) = MeteringEngine::with_config(provider.clone(), 1, 0);

    // Abuse guard: an aggressive 300ms handshake deadline for the test.
    let security = Arc::new(SecurityCounters::default());
    let relay_config = RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: "127.0.0.1:1".to_string(), // never reached
        local_peer_id: provider.peer_id_str().to_string(),
        identity: None,
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
        policy: None,
        security: security.clone(),
        handshake_timeout_ms: 300,
        enable_session_tickets: false,
    };
    let (provider_svc, _events) = TunnelService::new(
        provider_peer_id,
        metering.clone(),
        availability.clone(),
        relay_config,
        4,
        provider.noise_secret_key_bytes().to_vec(),
    );

    let tunnel_addr = format!("127.0.0.1:{}", free_port().await);
    let accept_config = RelayConfig {
        backend: provider_svc.relay_config().backend.clone(),
        metering: provider_svc.relay_config().metering.clone(),
        availability: provider_svc.relay_config().availability.clone(),
        target_addr: provider_svc.relay_config().target_addr.clone(),
        local_peer_id: provider_svc.relay_config().local_peer_id.clone(),
        identity: provider_svc.relay_config().identity.clone(),
        max_upload_mbps: provider_svc.relay_config().max_upload_mbps,
        max_download_mbps: provider_svc.relay_config().max_download_mbps,
        policy: None,
        security: provider_svc.relay_config().security.clone(),
        handshake_timeout_ms: provider_svc.relay_config().handshake_timeout_ms,
        enable_session_tickets: false,
    };
    let listener_addr = tunnel_addr.clone();
    let listener_task = tokio::spawn(async move {
        TunnelService::accept_incoming(
            &listener_addr,
            provider_svc.event_tx().clone(),
            accept_config,
            provider_svc.conn_semaphore(),
            provider_svc.static_private_key(),
        )
        .await
        .expect("tunnel listener should bind");
    });

    // Slowloris client: connect, send a truncated preamble (fewer bytes than
    // the 2-byte length header), then stall forever.
    let mut client = None;
    for attempt in 0..5 {
        match tokio::net::TcpStream::connect(&tunnel_addr).await {
            Ok(s) => {
                client = Some(s);
                break;
            }
            Err(_) => {
                assert!(attempt < 4, "listener never became reachable");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let mut client = client.expect("connect to tunnel listener");
    client.write_all(&[0x00]).await.unwrap();

    // The provider must drop the connection when the handshake deadline
    // hits — without the guard this read would hang indefinitely.
    let mut buf = [0u8; 16];
    let closed = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
        .await
        .expect("stalled handshake must be closed within the deadline");
    match closed {
        // Clean FIN or RST (unread bytes) — both are a closed connection.
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("unexpected data from provider: {n} bytes"),
    }

    // Give the accept task a moment to record the failure.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snap = security.snapshot();
    assert!(
        snap.handshake_failures >= 1,
        "stalled handshake must increment handshake_failures: {snap:?}"
    );
    let events = security.recent_events(8);
    assert!(
        events.iter().any(|e| e.kind == "handshake_failure"),
        "event ring should record the stall: {events:?}"
    );

    listener_task.abort();
}

/// Soft-slashing gate wiring: with `POLICY_MIN_ACCOUNTABILITY` active, an
/// unknown peer (fresh DB ⇒ composite score 0.0) must be denied **after**
/// the Noise handshake completes, counted as a policy rejection, and have a
/// per-peer violation attributed for future accountability scoring.
#[tokio::test]
async fn accountability_gate_denies_and_attributes_violation() {
    let dir = TempDir::new().unwrap();
    let provider =
        NodeIdentity::load_or_generate(&dir.path().join("provider.json"), "us-test").unwrap();
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-test").unwrap();
    let provider_peer_id: libp2p::PeerId = provider.peer_id_str().parse().unwrap();
    let consumer_peer_id: libp2p::PeerId = consumer.peer_id_str().parse().unwrap();

    let db_path = format!("sqlite:{}", dir.path().join("softslash.db").display());
    let backend = Arc::new(SqliteStore::new(&db_path).await.unwrap());
    let availability = Arc::new(RwLock::new(AvailabilityEngine::new(100.0, 100.0, 10)));
    let (metering, _receipt_rx) = MeteringEngine::with_config(provider.clone(), 1, 0);

    // One shared counter set (accept-side denial must be visible to us).
    let security = Arc::new(SecurityCounters::default());
    // Unreachable gate for an unknown peer: soft-slash score = 0.0 < 0.99.
    let policy = echo_daemon::policy::PolicyEngine::new(echo_daemon::policy::PolicyConfig {
        min_accountability: 0.99,
        ..Default::default()
    });

    let relay_config = RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: "127.0.0.1:1".to_string(), // never reached
        local_peer_id: provider.peer_id_str().to_string(),
        identity: None,
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
        policy: Some(policy),
        security: security.clone(),
        handshake_timeout_ms: 10_000,
        enable_session_tickets: false,
    };
    let (provider_svc, _events) = TunnelService::new(
        provider_peer_id,
        metering.clone(),
        availability.clone(),
        relay_config,
        4,
        provider.noise_secret_key_bytes().to_vec(),
    );

    let tunnel_addr = format!("127.0.0.1:{}", free_port().await);
    let accept_config = RelayConfig {
        backend: provider_svc.relay_config().backend.clone(),
        metering: provider_svc.relay_config().metering.clone(),
        availability: provider_svc.relay_config().availability.clone(),
        target_addr: provider_svc.relay_config().target_addr.clone(),
        local_peer_id: provider_svc.relay_config().local_peer_id.clone(),
        identity: provider_svc.relay_config().identity.clone(),
        max_upload_mbps: provider_svc.relay_config().max_upload_mbps,
        max_download_mbps: provider_svc.relay_config().max_download_mbps,
        policy: provider_svc.relay_config().policy.clone(),
        security: provider_svc.relay_config().security.clone(),
        handshake_timeout_ms: provider_svc.relay_config().handshake_timeout_ms,
        enable_session_tickets: false,
    };
    let listener_addr = tunnel_addr.clone();
    let listener_task = tokio::spawn(async move {
        TunnelService::accept_incoming(
            &listener_addr,
            provider_svc.event_tx().clone(),
            accept_config,
            provider_svc.conn_semaphore(),
            provider_svc.static_private_key(),
        )
        .await
        .expect("tunnel listener should bind");
    });

    // Consumer: full Noise handshake (the gate runs only after msg3).
    let consumer_metering = MeteringEngine::new(consumer.clone()).0;
    let consumer_avail = Arc::new(RwLock::new(AvailabilityEngine::new(0.0, 0.0, 4)));
    let consumer_config = RelayConfig {
        backend: backend.clone(),
        metering: consumer_metering.clone(),
        availability: consumer_avail.clone(),
        target_addr: "127.0.0.1:1".to_string(),
        local_peer_id: consumer.peer_id_str().to_string(),
        identity: None,
        max_upload_mbps: 0.0,
        max_download_mbps: 0.0,
        policy: None,
        security: Arc::new(SecurityCounters::default()),
        handshake_timeout_ms: 10_000,
        enable_session_tickets: false,
    };
    let (consumer_svc, _consumer_events) = TunnelService::new(
        consumer_peer_id,
        consumer_metering,
        consumer_avail,
        consumer_config,
        4,
        consumer.noise_secret_key_bytes().to_vec(),
    );

    let session_id = "softslash-session-0001";
    let expected_key = provider.noise_public_key_bytes().to_vec();
    let mut session = None;
    for attempt in 0..5 {
        match consumer_svc
            .connect_to_provider(&tunnel_addr, provider_peer_id, session_id, &expected_key)
            .await
        {
            Ok(s) => {
                session = Some(s);
                break;
            }
            Err(e) => {
                assert!(attempt < 4, "handshake failed after 5 attempts: {e:?}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let mut session = session.expect("handshake completes — the policy gate runs after msg3");

    // The provider applies the gate post-handshake and drops the connection:
    // the consumer must observe a closed tunnel, not a hang.
    let mut buf = [0u8; 16];
    let closed = tokio::time::timeout(Duration::from_secs(5), session.stream.read(&mut buf))
        .await
        .expect("provider must drop the policy-denied session promptly");
    match closed {
        Ok(0) | Err(_) => {} // clean FIN or RST — either is a close
        Ok(n) => panic!("unexpected data from provider after denial: {n} bytes"),
    }

    // Give the accept task a moment to record the denial.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snap = security.snapshot();
    assert!(
        snap.policy_rejections >= 1,
        "soft-slashing gate must count the denial: {snap:?}"
    );
    assert_eq!(
        security.peer_violation_count(consumer.peer_id_str()),
        1,
        "judgment-based denial must attribute a violation to the authenticated peer"
    );
    assert!(
        security
            .recent_events(8)
            .iter()
            .any(|e| e.kind == "policy_rejection"),
        "event ring should record the denial"
    );

    listener_task.abort();
}

/// Session ticket integration test:
///
/// 1. First connection: full Noise_XX handshake, provider issues ticket,
///    consumer receives and persists it to `data/tickets/`.
/// 2. Simulate provider restart: new TunnelService with same identity + DB.
/// 3. Reconnect: consumer presents stored ticket in preamble, provider
///    validates it, issues a new ticket, relay works.
#[tokio::test]
async fn session_ticket_issued_stored_and_resumed() {
    let dir = TempDir::new().unwrap();

    let provider =
        NodeIdentity::load_or_generate(&dir.path().join("provider.json"), "us-test").unwrap();
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-test").unwrap();
    let provider_peer_id: libp2p::PeerId = provider.peer_id_str().parse().unwrap();
    let consumer_peer_id: libp2p::PeerId = consumer.peer_id_str().parse().unwrap();
    let provider_noise_pubkey = provider.noise_public_key_bytes().to_vec();

    let db_path = format!("sqlite:{}", dir.path().join("ticket_test.db").display());
    let backend = Arc::new(SqliteStore::new(&db_path).await.unwrap());
    let echo_addr = spawn_echo_server().await;

    // ── First provider instance ───────────────────────────────────
    let port1 = free_port().await;
    let (listener1, tunnel_addr1) = Box::pin(spawn_provider_with_tickets(
        port1, provider.clone(), backend.clone(), echo_addr.clone(),
    )).await;

    // ── Build consumer service ────────────────────────────────────
    let consumer_avail = Arc::new(RwLock::new(AvailabilityEngine::new(0.0, 0.0, 4)));
    let (consumer_metering, _) = MeteringEngine::new(consumer.clone());
    let consumer_config = RelayConfig {
        backend: backend.clone(),
        metering: consumer_metering.clone(),
        availability: consumer_avail.clone(),
        target_addr: "127.0.0.1:1".to_string(),
        local_peer_id: consumer.peer_id_str().to_string(),
        identity: None,
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
        policy: None,
        security: Arc::new(SecurityCounters::default()),
        handshake_timeout_ms: 5_000,
        enable_session_tickets: false,
    };
    let (consumer_svc, _) = TunnelService::new(
        consumer_peer_id,
        consumer_metering,
        consumer_avail,
        consumer_config,
        4,
        consumer.noise_secret_key_bytes().to_vec(),
    );
    let consumer_svc = Arc::new(consumer_svc);

    // ── First connection: full Noise_XX ───────────────────────────
    let session1 = retry_connect(&consumer_svc, &tunnel_addr1, provider_peer_id, "ticket-sess-t1", &provider_noise_pubkey).await;

    // Quick relay to confirm the tunnel works.
    let mut s1 = session1;
    do_echo_roundtrip(&mut s1, b"first-connection-test\n").await;
    drop(s1);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Ticket must be persisted to disk by the consumer.
    let ticket_path = std::path::Path::new("data/tickets")
        .join(format!("{}.ticket", provider_peer_id));
    assert!(ticket_path.exists(), "consumer must persist ticket to {}", ticket_path.display());

    // Ticket must be in the DB on the provider side.
    let stored = backend.get_session_ticket("ticket-sess-t1").await.unwrap();
    assert!(stored.is_some(), "provider must store ticket in DB");

    // ── Simulated provider restart ────────────────────────────────
    listener1.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let port2 = free_port().await;
    let (listener2, tunnel_addr2) = Box::pin(spawn_provider_with_tickets(
        port2, provider.clone(), backend.clone(), echo_addr.clone(),
    )).await;

    // ── Second connection: consumer sends stored ticket ───────────
    let session2 = retry_connect(&consumer_svc, &tunnel_addr2, provider_peer_id, "ticket-sess-t1", &provider_noise_pubkey).await;

    let mut s2 = session2;
    do_echo_roundtrip(&mut s2, b"resumed-connection-test\n").await;
    drop(s2);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Clean up ticket so it doesn't affect other test runs.
    let _ = std::fs::remove_file(&ticket_path);

    listener2.abort();
}

/// Spawn a provider tunnel listener with session tickets enabled.
/// Returns (listener_task, tunnel_addr).
async fn spawn_provider_with_tickets(
    port: u16,
    provider: NodeIdentity,
    backend: Arc<SqliteStore>,
    echo_addr: String,
) -> (tokio::task::JoinHandle<()>, String) {
    let availability = Arc::new(RwLock::new(AvailabilityEngine::new(100.0, 100.0, 10)));
    let (metering, _rx) = MeteringEngine::with_config(provider.clone(), 1, 0);
    let provider_peer_id: libp2p::PeerId = provider.peer_id_str().parse().unwrap();
    let relay_config = RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: echo_addr,
        local_peer_id: provider.peer_id_str().to_string(),
        identity: Some(Arc::new(provider.clone())),
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
        policy: None,
        security: Arc::new(SecurityCounters::default()),
        handshake_timeout_ms: 5_000,
        enable_session_tickets: true,
    };
    let (svc, _events) = TunnelService::new(
        provider_peer_id,
        metering,
        availability,
        relay_config,
        4,
        provider.noise_secret_key_bytes().to_vec(),
    );
    let accept_config = RelayConfig {
        backend: backend.clone(),
        metering: svc.relay_config().metering.clone(),
        availability: svc.relay_config().availability.clone(),
        target_addr: svc.relay_config().target_addr.clone(),
        local_peer_id: svc.relay_config().local_peer_id.clone(),
        identity: svc.relay_config().identity.clone(),
        max_upload_mbps: svc.relay_config().max_upload_mbps,
        max_download_mbps: svc.relay_config().max_download_mbps,
        policy: None,
        security: svc.relay_config().security.clone(),
        handshake_timeout_ms: svc.relay_config().handshake_timeout_ms,
        enable_session_tickets: true,
    };
    let tunnel_addr = format!("127.0.0.1:{port}");
    let listen_addr = tunnel_addr.clone();
    let event_tx = svc.event_tx().clone();
    let semaphore = svc.conn_semaphore();
    let sk = svc.static_private_key();
    let task = tokio::spawn(async move {
        TunnelService::accept_incoming(&listen_addr, event_tx, accept_config, semaphore, sk)
            .await
            .ok();
    });
    (task, tunnel_addr)
}

/// Retry `connect_to_provider` up to 10 times with 50ms back-off.
async fn retry_connect(
    svc: &Arc<TunnelService>,
    addr: &str,
    peer_id: libp2p::PeerId,
    session_id: &str,
    noise_pubkey: &[u8],
) -> echo_daemon::tunnel::EncryptedTunnelSession {
    for attempt in 0..10 {
        match svc.connect_to_provider(addr, peer_id, session_id, noise_pubkey).await {
            Ok(s) => return s,
            Err(e) => {
                assert!(attempt < 9, "connect_to_provider failed after 10 attempts: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    unreachable!()
}

/// Send payload through tunnel, read echo back, assert roundtrip.
async fn do_echo_roundtrip(session: &mut echo_daemon::tunnel::EncryptedTunnelSession, payload: &[u8]) {
    let mut enc = [0u8; NOISE_MAX_MSG_LEN];
    let n = session.transport.lock().await.write_message(payload, &mut enc).unwrap();
    session.stream.write_all(&enc[..n]).await.unwrap();

    let mut raw = [0u8; NOISE_MAX_MSG_LEN];
    let mut plain = [0u8; NOISE_MAX_MSG_LEN];
    let rn = tokio::time::timeout(Duration::from_secs(5), session.stream.read(&mut raw))
        .await
        .expect("echo should return within 5s")
        .unwrap();
    assert!(rn > 0, "echo server must return data");
    let pn = session.transport.lock().await.read_message(&raw[..rn], &mut plain).unwrap();
    assert_eq!(&plain[..pn], payload, "echo roundtrip payload mismatch");
}
