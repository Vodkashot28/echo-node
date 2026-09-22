//! End-to-end test for consumer mode: the `run_consumer_listener` forward
//! listener dials a provider node, and local application traffic is relayed
//! through a Noise_XX tunnel to the provider's target (an echo server).
//!
//! Verifies:
//! - The full path local app → consumer listener → encrypted tunnel →
//!   provider relay → echo target → back to the local app.
//! - The consumer's availability slot is acquired during
//!   `connect_to_provider` and released exactly once on teardown.
//! - The provider finalizes connection history and issues a signed receipt
//!   for the consumer session.
//! - The consumer does NOT persist its own (non-authoritative) receipts.

use echo_daemon::availability::AvailabilityEngine;
use echo_daemon::consumer::{run_consumer_listener, ConsumerConfig};
use echo_daemon::identity::NodeIdentity;
use echo_daemon::meter::MeteringEngine;
use echo_daemon::sqlite_store::SqliteStore;
use echo_daemon::tunnel::{RelayConfig, TunnelService};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

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

/// Read from the local app stream until we have `expected` bytes, with a
/// hard timeout to keep the test from hanging on a broken relay.
async fn read_until(stream: &mut tokio::net::TcpStream, expected: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(expected);
    let mut chunk = [0u8; 1024];
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while buf.len() < expected {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for relayed data (got {} of {} bytes)",
            buf.len(),
            expected
        );
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .expect("read should not stall")
            .expect("stream read should succeed");
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    buf
}

#[tokio::test]
async fn consumer_listener_relays_local_app_through_provider_tunnel() {
    let dir = TempDir::new().unwrap();

    // ── Identities (fresh, per-test) ──────────────────────────────
    let provider =
        NodeIdentity::load_or_generate(&dir.path().join("provider.json"), "us-test").unwrap();
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-test").unwrap();
    let provider_peer_id: libp2p::PeerId = provider.peer_id_str().parse().unwrap();

    // ── Provider runtime state ────────────────────────────────────
    let provider_db = format!("sqlite:{}", dir.path().join("provider.db").display());
    let backend = Arc::new(SqliteStore::new(&provider_db).await.unwrap());
    let provider_avail = Arc::new(RwLock::new(AvailabilityEngine::new(100.0, 100.0, 10)));

    // Metering configured to issue a receipt on every packet; the final
    // settlement receipt comes from end_session.
    let (metering, mut receipt_rx) = MeteringEngine::with_config(provider.clone(), 1, 0);

    // The provider's relay target: an echo server.
    let echo_addr = spawn_echo_server().await;

    let relay_config = RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: provider_avail.clone(),
        target_addr: echo_addr.clone(),
        local_peer_id: provider.peer_id_str().to_string(),
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
    };
    let (provider_svc, _provider_events) = TunnelService::new(
        provider_peer_id,
        metering.clone(),
        provider_avail.clone(),
        relay_config,
        4,
        provider.noise_secret_key_bytes().to_vec(),
    );

    // Spawn the provider's incoming tunnel listener.
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

    // ── Consumer runtime state ────────────────────────────────────
    let consumer_db = format!("sqlite:{}", dir.path().join("consumer.db").display());
    let consumer_backend = Arc::new(SqliteStore::new(&consumer_db).await.unwrap());
    // max_sessions=4 so the single test session fits; we assert slots are
    // released back to 0 after the relay closes.
    let consumer_avail = Arc::new(RwLock::new(AvailabilityEngine::new(10.0, 10.0, 4)));
    let consumer_metering = MeteringEngine::new(consumer.clone()).0;
    let consumer_config = RelayConfig {
        backend: consumer_backend.clone(),
        metering: consumer_metering.clone(),
        availability: consumer_avail.clone(),
        target_addr: "127.0.0.1:1".to_string(), // unused: local app stream is the target
        local_peer_id: consumer.peer_id_str().to_string(),
        max_upload_mbps: 10.0,
        max_download_mbps: 10.0,
    };
    let (consumer_svc, _consumer_events) = TunnelService::new(
        consumer.peer_id_str().parse::<libp2p::PeerId>().unwrap(),
        consumer_metering,
        consumer_avail.clone(),
        consumer_config,
        4,
        consumer.noise_secret_key_bytes().to_vec(),
    );
    let consumer_svc = Arc::new(consumer_svc);

    // Spawn the consumer forward listener, pointing at the provider.
    let consumer_listen_addr = format!("127.0.0.1:{}", free_port().await);
    let listener_cfg = ConsumerConfig {
        listen_addr: consumer_listen_addr.clone(),
        provider_addr: tunnel_addr.clone(),
        provider_peer_id,
        provider_noise_pubkey: provider.noise_public_key_bytes().to_vec(),
    };
    let consumer_task = tokio::spawn(async move {
        run_consumer_listener(consumer_svc, listener_cfg)
            .await
            .expect("consumer listener should bind and run");
    });

    // ── Local application connects to the consumer listener ───────
    let mut app_stream = None;
    for attempt in 0..5 {
        match tokio::net::TcpStream::connect(&consumer_listen_addr).await {
            Ok(s) => {
                app_stream = Some(s);
                break;
            }
            Err(_) => {
                assert!(attempt < 4, "app connect failed after 5 attempts");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let mut app_stream = app_stream.expect("local app should connect to the consumer listener");

    // The tunnel handshake (`connect_to_provider` → availability acquire)
    // runs concurrently with the app connect; wait until the slot is held.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let slot_held = loop {
        let active = consumer_avail.read().await.active_sessions();
        if active == 1 {
            break true;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "consumer should hold one availability slot during the relay (active={active})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(slot_held, "consumer should hold one availability slot during the relay");

    let payload = b"consumer-mode-echo-roundtrip-payload\n";
    app_stream.write_all(payload).await.unwrap();

    // Read the echo response through the full tunnel path.
    let response = read_until(&mut app_stream, payload.len()).await;
    assert_eq!(&response[..], &payload[..], "relayed echo must match the original payload");

    // Disconnect the local app → relay ends → teardown runs.
    drop(app_stream);
    // Shut down the forward listener so no stray connections are accepted.
    consumer_task.abort();

    // ── Invariant: consumer availability slot released exactly once ──
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let slots_released = loop {
        let active = consumer_avail.read().await.active_sessions();
        if active == 0 {
            break true;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "consumer availability slot was not released after teardown (active={active})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(slots_released, "consumer availability should return to 0");

    // ── Provider side effects ─────────────────────────────────────
    // Connection history finalized with the consumer-session byte counts.
    let row: (i64, i64, String) = sqlx::query_as(
        "SELECT bytes_sent, bytes_received, exit_reason FROM connection_history \
         WHERE local_peer_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(provider.peer_id_str())
    .fetch_one(backend.pool())
    .await
    .unwrap();
    assert_eq!(row.2, "completed");
    assert!(
        row.0 >= payload.len() as i64,
        "provider bytes_sent ({}) should include the relayed payload",
        row.0
    );
    assert!(
        row.1 >= payload.len() as i64,
        "provider bytes_received ({}) should include the relayed response",
        row.1
    );

    // The provider issues a signed final receipt for the consumer session.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut final_receipt = None;
    while std::time::Instant::now() < deadline {
        let receipt = match tokio::time::timeout(Duration::from_millis(500), receipt_rx.recv()).await
        {
            Ok(Some(r)) => r,
            Ok(None) => panic!("receipt channel closed unexpectedly"),
            Err(_) => break,
        };
        assert!(
            receipt.session_id.starts_with("consumer-"),
            "provider should see the consumer-generated session id, got {}",
            receipt.session_id
        );
        assert_eq!(receipt.signer_peer_id, provider.peer_id_str());
        assert!(
            receipt
                .verify_signature(provider.public_key_bytes.as_slice())
                .expect("signature verification should not error"),
            "provider receipt signature must verify"
        );
        if receipt.bytes_transferred >= (row.0 + row.1) as u64 {
            final_receipt = Some(receipt);
            break;
        }
    }
    let final_receipt =
        final_receipt.expect("a final provider receipt covering the full transfer should be issued");
    assert_eq!(final_receipt.bytes_transferred, (row.0 + row.1) as u64);

    // ── Consumer side effects ─────────────────────────────────────
    // The consumer relays without metering and never persists its own
    // (non-authoritative) receipts into state_receipts.
    let consumer_receipt_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM state_receipts")
        .fetch_one(consumer_backend.pool())
        .await
        .unwrap();
    assert_eq!(
        consumer_receipt_count, 0,
        "consumer must not persist its own forwarded-traffic receipts"
    );

    // Stop the provider tunnel listener.
    listener_task.abort();
}