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
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
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
        max_upload_mbps: provider_svc.relay_config().max_upload_mbps,
        max_download_mbps: provider_svc.relay_config().max_download_mbps,
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
        max_upload_mbps: 0.0,
        max_download_mbps: 0.0,
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