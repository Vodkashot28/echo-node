// Integration tests for P0 critical fixes:
// - Session ticket persistence and recovery
// - Auction state persistence and crash recovery
// - Relay peer configuration parsing

use echo_daemon::identity::NodeIdentity;
use echo_daemon::session_ticket::{SessionState, SessionTicket};
use echo_daemon::{MetricsBackend, SqliteStore};
use tempfile::NamedTempFile;

fn temp_identity_path() -> NamedTempFile {
    NamedTempFile::new().unwrap()
}

fn test_identity() -> NodeIdentity {
    let temp = temp_identity_path();
    NodeIdentity::load_or_generate(temp.path(), "test-region").unwrap()
}

// ══════════════════════════════════════════════════════════════
// Session Ticket Persistence Tests
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn session_ticket_store_and_retrieve() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let identity = test_identity();
    let session_id = "session-123";
    let consumer_peer_id = "12D3KooWConsumer";

    let state = SessionState {
        encryption_key: vec![1u8; 32],
        decryption_key: vec![2u8; 32],
        send_sequence: 42,
        recv_sequence: 43,
        bytes_sent: 1024,
        bytes_received: 2048,
    };

    // Issue and store ticket
    let ticket = SessionTicket::issue(
        &identity,
        session_id.to_string(),
        consumer_peer_id.to_string(),
        state.clone(),
    )
    .unwrap();

    let ticket_data = ticket.serialize().unwrap();
    backend
        .store_session_ticket(
            session_id,
            consumer_peer_id,
            &ticket_data,
            ticket.expires_at as i64,
        )
        .await
        .unwrap();

    // Retrieve ticket
    let retrieved = backend.get_session_ticket(session_id).await.unwrap();
    assert!(retrieved.is_some(), "ticket should be retrievable");

    let retrieved_data = retrieved.unwrap();
    let retrieved_ticket = SessionTicket::deserialize(&retrieved_data).unwrap();

    assert_eq!(retrieved_ticket.session_id, session_id);
    assert_eq!(retrieved_ticket.consumer_peer_id, consumer_peer_id);

    // Verify and decrypt
    let decrypted = retrieved_ticket
        .verify_and_decrypt(&identity, consumer_peer_id)
        .unwrap();

    assert_eq!(decrypted.send_sequence, 42);
    assert_eq!(decrypted.recv_sequence, 43);
    assert_eq!(decrypted.bytes_sent, 1024);
    assert_eq!(decrypted.bytes_received, 2048);
}

#[tokio::test]
async fn session_ticket_cleanup_expired() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let now = chrono::Utc::now().timestamp();

    // Store expired ticket
    backend
        .store_session_ticket("expired-session", "peer1", b"data1", now - 3600)
        .await
        .unwrap();

    // Store valid ticket
    backend
        .store_session_ticket("valid-session", "peer2", b"data2", now + 3600)
        .await
        .unwrap();

    // Cleanup
    let cleaned = backend.cleanup_expired_tickets().await.unwrap();
    assert_eq!(cleaned, 1, "should clean up 1 expired ticket");

    // Verify expired ticket is gone
    let expired = backend.get_session_ticket("expired-session").await.unwrap();
    assert!(expired.is_none(), "expired ticket should be removed");

    // Verify valid ticket remains
    let valid = backend.get_session_ticket("valid-session").await.unwrap();
    assert!(valid.is_some(), "valid ticket should remain");
}

#[tokio::test]
async fn session_ticket_upsert_overwrites() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let session_id = "session-456";
    let consumer = "peer-consumer";
    let now = chrono::Utc::now().timestamp();

    // Store first ticket
    backend
        .store_session_ticket(session_id, consumer, b"data-v1", now + 1000)
        .await
        .unwrap();

    // Overwrite with new ticket
    backend
        .store_session_ticket(session_id, consumer, b"data-v2", now + 2000)
        .await
        .unwrap();

    // Verify only latest ticket exists
    let retrieved = backend
        .get_session_ticket(session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&retrieved, b"data-v2", "should have latest ticket data");
}

// ══════════════════════════════════════════════════════════════
// Auction Persistence Tests
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn auction_upsert_and_retrieve() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let auction_id = "auction-123";
    let now = chrono::Utc::now().timestamp();

    // Create auction
    backend
        .upsert_auction(
            auction_id,
            "requester-peer-id",
            "us-west",
            100.0,
            0.50,
            30,
            now,
            now + 30,
            "open",
        )
        .await
        .unwrap();

    // Retrieve auction
    let auction = backend.get_auction(auction_id).await.unwrap();
    assert!(auction.is_some(), "auction should exist");

    let auction = auction.unwrap();
    assert_eq!(auction["auction_id"], auction_id);
    assert_eq!(auction["region"], "us-west");
    assert_eq!(auction["bandwidth_mbps"], 100.0);
    assert_eq!(auction["budget_usd_per_gb"], 0.50);
    assert_eq!(auction["status"], "open");
}

#[tokio::test]
async fn auction_list_by_status() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let now = chrono::Utc::now().timestamp();

    // Create multiple auctions
    for i in 0..5 {
        backend
            .upsert_auction(
                &format!("auction-{}", i),
                "requester",
                "us-east",
                50.0,
                0.30,
                60,
                now + i,
                now + 60 + i,
                if i < 3 { "open" } else { "cancelled" },
            )
            .await
            .unwrap();
    }

    // List open auctions
    let open = backend.list_auctions("open", 100).await.unwrap();
    assert_eq!(open.len(), 3, "should have 3 open auctions");

    // List cancelled auctions
    let closed = backend.list_auctions("cancelled", 100).await.unwrap();
    assert_eq!(closed.len(), 2, "should have 2 cancelled auctions");
}

#[tokio::test]
async fn auction_update_status_with_winner() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let auction_id = "auction-789";
    let now = chrono::Utc::now().timestamp();

    // Create open auction
    backend
        .upsert_auction(
            auction_id,
            "requester",
            "eu-west",
            200.0,
            0.60,
            45,
            now,
            now + 45,
            "open",
        )
        .await
        .unwrap();

    // Close with winner
    backend
        .update_auction_status(
            auction_id,
            "awarded",
            Some("winner-peer-id"),
            Some(0.55),
            Some(now + 45),
        )
        .await
        .unwrap();

    // Verify status updated
    let auction = backend.get_auction(auction_id).await.unwrap().unwrap();
    assert_eq!(auction["status"], "awarded");
    assert_eq!(auction["winning_peer_id"], "winner-peer-id");
    assert_eq!(auction["winning_price"], 0.55);
}

#[tokio::test]
async fn auction_bids_insert_and_retrieve() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let auction_id = "auction-with-bids";
    let now = chrono::Utc::now().timestamp();

    // Create auction first
    backend
        .upsert_auction(
            auction_id,
            "requester",
            "ap-south",
            150.0,
            0.70,
            30,
            now,
            now + 30,
            "open",
        )
        .await
        .unwrap();

    // Insert multiple bids
    for i in 0..3 {
        backend
            .insert_bid(
                &format!("bid-{}", i),
                auction_id,
                &format!("provider-{}", i),
                0.50 - (i as f64 * 0.05),
                0.8,
                50.0,
                now + i,
            )
            .await
            .unwrap();
    }

    // Retrieve bids
    let bids = backend.get_bids_for_auction(auction_id).await.unwrap();
    assert_eq!(bids.len(), 3, "should have 3 bids");

    // Verify bids are ordered by submitted_at
    assert_eq!(bids[0]["provider_peer_id"], "provider-0");
    assert_eq!(bids[1]["provider_peer_id"], "provider-1");
    assert_eq!(bids[2]["provider_peer_id"], "provider-2");

    // Verify prices
    assert_eq!(bids[0]["price_usd_per_gb"], 0.50);
    assert_eq!(bids[1]["price_usd_per_gb"], 0.45);
    assert_eq!(bids[2]["price_usd_per_gb"], 0.40);
}

#[tokio::test]
async fn auction_bid_upsert_per_provider() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let auction_id = "auction-upsert";
    let provider_id = "provider-x";
    let now = chrono::Utc::now().timestamp();

    // Create auction
    backend
        .upsert_auction(
            auction_id,
            "req",
            "region",
            100.0,
            1.0,
            30,
            now,
            now + 30,
            "open",
        )
        .await
        .unwrap();

    // First bid
    backend
        .insert_bid("bid-1", auction_id, provider_id, 0.80, 0.9, 40.0, now)
        .await
        .unwrap();

    // Second bid from same provider (should update)
    backend
        .insert_bid("bid-2", auction_id, provider_id, 0.70, 0.9, 40.0, now + 10)
        .await
        .unwrap();

    // Verify only one bid exists with latest price
    let bids = backend.get_bids_for_auction(auction_id).await.unwrap();
    assert_eq!(bids.len(), 1, "should have only 1 bid per provider");
    assert_eq!(
        bids[0]["price_usd_per_gb"], 0.70,
        "should have latest price"
    );
}

#[tokio::test]
async fn auction_cleanup_old_closed() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());
    let backend = SqliteStore::new(&db_url).await.unwrap();

    let now = chrono::Utc::now().timestamp();
    let old = now - 86400 * 10; // 10 days ago

    // Create old awarded auction (should be cleaned)
    backend
        .upsert_auction(
            "old-awarded",
            "req",
            "region",
            100.0,
            1.0,
            30,
            old,
            old + 30,
            "open",
        )
        .await
        .unwrap();
    backend
        .update_auction_status(
            "old-awarded",
            "awarded",
            Some("provider-1"),
            Some(0.5),
            Some(old + 25),
        )
        .await
        .unwrap();

    // Create old cancelled auction (should be cleaned)
    backend
        .upsert_auction(
            "old-cancelled",
            "req",
            "region",
            100.0,
            1.0,
            30,
            old,
            old + 30,
            "open",
        )
        .await
        .unwrap();
    backend
        .update_auction_status("old-cancelled", "cancelled", None, None, None)
        .await
        .unwrap();

    // Create recent awarded auction (should not be cleaned)
    backend
        .upsert_auction(
            "recent-awarded",
            "req",
            "region",
            100.0,
            1.0,
            30,
            now - 100,
            now - 70,
            "open",
        )
        .await
        .unwrap();
    backend
        .update_auction_status(
            "recent-awarded",
            "awarded",
            Some("provider-1"),
            Some(0.5),
            Some(now - 75),
        )
        .await
        .unwrap();

    // Create old open auction (should not be cleaned — still active)
    backend
        .upsert_auction(
            "old-open",
            "req",
            "region",
            100.0,
            1.0,
            30,
            old,
            old + 30,
            "open",
        )
        .await
        .unwrap();

    // Cleanup auctions older than 7 days
    let threshold = now - (86400 * 7);
    let cleaned = backend.cleanup_old_auctions(threshold).await.unwrap();
    assert_eq!(
        cleaned, 2,
        "should clean up 2 old terminal auctions (awarded + cancelled)"
    );

    // Verify old awarded is gone
    let old_awarded = backend.get_auction("old-awarded").await.unwrap();
    assert!(
        old_awarded.is_none(),
        "old awarded auction should be removed"
    );

    // Verify old cancelled is gone
    let old_cancelled = backend.get_auction("old-cancelled").await.unwrap();
    assert!(
        old_cancelled.is_none(),
        "old cancelled auction should be removed"
    );

    // Verify recent awarded remains
    let recent = backend.get_auction("recent-awarded").await.unwrap();
    assert!(recent.is_some(), "recent awarded auction should remain");

    // Verify old open remains (not cleaned based on status)
    let old_open = backend.get_auction("old-open").await.unwrap();
    assert!(old_open.is_some(), "old open auction should remain");
}

#[tokio::test]
async fn auction_crash_recovery_scenario() {
    let db_file = NamedTempFile::new().unwrap();
    let db_url = format!("sqlite:{}", db_file.path().display());

    let auction_id = "crash-test-auction";
    let now = chrono::Utc::now().timestamp();

    // Simulate pre-crash: create auction with bids
    {
        let backend = SqliteStore::new(&db_url).await.unwrap();

        backend
            .upsert_auction(
                auction_id,
                "requester",
                "us-east",
                100.0,
                0.50,
                30,
                now,
                now + 30,
                "open",
            )
            .await
            .unwrap();

        backend
            .insert_bid("bid-1", auction_id, "provider-a", 0.45, 0.9, 30.0, now + 5)
            .await
            .unwrap();

        backend
            .insert_bid("bid-2", auction_id, "provider-b", 0.48, 0.8, 40.0, now + 10)
            .await
            .unwrap();

        // backend drops here, simulating crash
    }

    // Simulate post-crash: new instance loads from DB
    {
        let backend = SqliteStore::new(&db_url).await.unwrap();

        // Verify auction recovered
        let auction = backend.get_auction(auction_id).await.unwrap();
        assert!(auction.is_some(), "auction should survive crash");

        let auction = auction.unwrap();
        assert_eq!(auction["status"], "open");
        assert_eq!(auction["bandwidth_mbps"], 100.0);

        // Verify bids recovered
        let bids = backend.get_bids_for_auction(auction_id).await.unwrap();
        assert_eq!(bids.len(), 2, "bids should survive crash");

        // Verify bid details
        let provider_a_bid = bids
            .iter()
            .find(|b| b["provider_peer_id"] == "provider-a")
            .unwrap();
        assert_eq!(provider_a_bid["price_usd_per_gb"], 0.45);

        let provider_b_bid = bids
            .iter()
            .find(|b| b["provider_peer_id"] == "provider-b")
            .unwrap();
        assert_eq!(provider_b_bid["price_usd_per_gb"], 0.48);
    }
}

// ══════════════════════════════════════════════════════════════
// Discovery/Relay Configuration Tests
// ══════════════════════════════════════════════════════════════

#[test]
fn relay_mode_parsing_from_env() {
    use echo_daemon::discovery::{DiscoveryConfig, RelayMode};

    // Test server mode
    std::env::set_var("DHT_RELAY_MODE", "server");
    let config = DiscoveryConfig::from_env();
    assert_eq!(config.relay_mode, RelayMode::Server);

    // Test client mode
    std::env::set_var("DHT_RELAY_MODE", "client");
    let config = DiscoveryConfig::from_env();
    assert_eq!(config.relay_mode, RelayMode::Client);

    // Test auto mode (default)
    std::env::remove_var("DHT_RELAY_MODE");
    let config = DiscoveryConfig::from_env();
    assert_eq!(config.relay_mode, RelayMode::Auto);

    // Cleanup
    std::env::remove_var("DHT_RELAY_MODE");
}

#[test]
fn relay_peers_parsing() {
    use echo_daemon::discovery::DiscoveryConfig;
    use libp2p::PeerId;

    // Real PeerIds: parse_bootstrap_peer rejects any /p2p/ component that
    // isn't a decodable peer id, so fabricated strings ("12D3KooWRelay1")
    // would be silently skipped.
    let peer_a = PeerId::random();
    let peer_b = PeerId::random();
    let relay_peers =
        format!("/ip4/10.0.0.1/tcp/4001/p2p/{peer_a},/ip4/10.0.0.2/tcp/4001/p2p/{peer_b}");
    std::env::set_var("DHT_RELAY_PEERS", &relay_peers);

    let config = DiscoveryConfig::from_env();
    assert_eq!(config.relay_peers.len(), 2, "should parse 2 relay peers");

    assert_eq!(config.relay_peers[0].0, peer_a);
    assert_eq!(config.relay_peers[1].0, peer_b);
    assert_eq!(
        config.relay_peers[0].1.to_string(),
        "/ip4/10.0.0.1/tcp/4001"
    );
    assert_eq!(
        config.relay_peers[1].1.to_string(),
        "/ip4/10.0.0.2/tcp/4001"
    );

    // Cleanup
    std::env::remove_var("DHT_RELAY_PEERS");
}
