//! Integration tests for the Echo Node daemon.
//!
//! These tests exercise multiple components together without requiring
//! network access or external services.

use echo_daemon::identity::NodeIdentity;
use echo_daemon::migrator::{run_sqlite, Migration};
use echo_daemon::models::{
    CapabilityDescriptor, PeerReputation, SettlementRecord, StateReceipt,
};
use echo_daemon::sqlite_store::SqliteStore;
use echo_daemon::{base64_encode, MetricsBackend};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: create a temporary identity file path.
fn temp_identity_path(dir: &std::path::Path) -> PathBuf {
    dir.join("identity.json")
}

// ──────────────────────────────────────────────────────────────
// SQLite Backend Integration
// ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn sqlite_backend_initialization_and_migration() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());

    let store = SqliteStore::new(&db_path).await.unwrap();

    // Verify schema_migrations table was created and migrations ran
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM schema_migrations")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(count.0 >= 4, "expected at least 4 migrations, got {}", count.0);

    // Verify all expected tables exist
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    let table_names: Vec<&str> = tables.iter().map(|(n,)| n.as_str()).collect();
    assert!(table_names.contains(&"nodes"));
    assert!(table_names.contains(&"node_metrics"));
    assert!(table_names.contains(&"network_metrics"));
    assert!(table_names.contains(&"node_intelligence"));
    assert!(table_names.contains(&"peer_reputation"));
    assert!(table_names.contains(&"connection_history"));
    assert!(table_names.contains(&"state_receipts"));
    assert!(table_names.contains(&"capability_descriptors"));
    assert!(table_names.contains(&"settlements"));
    assert!(table_names.contains(&"schema_migrations"));
}

#[tokio::test]
async fn sqlite_idempotent_migration() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());

    // Initialize twice — second run should skip all migrations
    let _store1 = SqliteStore::new(&db_path).await.unwrap();
    let store2 = SqliteStore::new(&db_path).await.unwrap();

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM schema_migrations")
        .fetch_one(store2.pool())
        .await
        .unwrap();
    assert!(count.0 >= 4, "migrations should not re-run");
}

#[tokio::test]
async fn sqlite_failed_migration_not_recorded_and_retried() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_path)
        .unwrap()
        .create_if_missing(true);
    let pool = sqlx::SqlitePool::connect_with(options).await.unwrap();

    // Craft a broken migration appended after the real ones.
    let mut migrations = echo_daemon::migrator::sqlite_migrations();
    migrations.push(Migration {
        version: 99,
        name: "broken_migration",
        sql: "CREATE TABLE nope_broken (id INT); THIS IS NOT SQL;",
    });

    let err = run_sqlite(&pool, &migrations).await;
    assert!(err.is_err(), "a genuine migration failure must abort the run");

    // The failed migration must NOT be recorded as applied...
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM schema_migrations WHERE version = 99")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count.0, 0, "failed migration must not be marked applied");

    // ...and the statements that ran before the failure must be rolled back.
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='nope_broken'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        tables.is_empty(),
        "failed migration must be rolled back atomically"
    );

    // A retry with fixed SQL succeeds and records the migration.
    let mut fixed = echo_daemon::migrator::sqlite_migrations();
    fixed.push(Migration {
        version: 99,
        name: "broken_migration_fixed",
        sql: "CREATE TABLE nope_fixed (id INT);",
    });
    run_sqlite(&pool, &fixed).await.unwrap();

    let tables2: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='nope_fixed'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(tables2.len(), 1, "retried migration should apply");
}

#[tokio::test]
async fn sqlite_node_upsert_and_retrieve() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = SqliteStore::new(&db_path).await.unwrap();

    let row = echo_daemon::NodeRow {
        id: "node-1".to_string(),
        name: "TestNode".to_string(),
        user_id: "user-1".to_string(),
        peer_id: Some("12D3KooWTest".to_string()),
        public_key: None,
        ip_address: Some("127.0.0.1".to_string()),
        region: Some("us-west".to_string()),
        status: Some("online".to_string()),
        last_seen_at: None,
        uptime: Some(0.5),
        uptime_pct: Some(50.0),
        avg_latency_ms: Some(10.5),
        bandwidth_down_mbps: Some(100.0),
        bandwidth_up_mbps: Some(50.0),
        packet_loss_pct: Some(0.1),
        quality_score: Some(0.95),
        trust_score: Some(0.9),
        sessions_count: Some(3),
        earnings_usd: Some(0.001),
        reported_upload_cap_mbps: Some(80.0),
        reported_download_cap_mbps: Some(90.0),
        supported_encryption: Some("noise-xx".to_string()),
        services: None,
    };

    store.upsert_node(&row).await.unwrap();

    // Update the same node
    let mut updated = row.clone();
    updated.uptime = Some(0.75);
    updated.bandwidth_down_mbps = Some(120.0);
    store.upsert_node(&updated).await.unwrap();

    // Verify only one row exists
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM nodes WHERE id = 'node-1'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn sqlite_capability_upsert_and_query() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = SqliteStore::new(&db_path).await.unwrap();

    let cap = CapabilityDescriptor {
        peer_id: "peer-1".to_string(),
        public_key: vec![1, 2, 3, 4],
        region: "us-west".to_string(),
        upload_cap_mbps: 100.0,
        download_cap_mbps: 200.0,
        avg_latency_ms: 5.0,
        reputation_score: 0.9,
        supported_encryption: vec!["noise-xx".to_string()],
        max_sessions: 10,
        active_sessions: 2,
        last_updated: 1700000000,
        noise_public_key: vec![10, 20, 30],
    };

    store.upsert_capability(&cap).await.unwrap();

    // A second provider with MORE bandwidth but a WORSE reputation score —
    // reputation must dominate the ordering so trusted providers rank first.
    let high_bw_low_rep = CapabilityDescriptor {
        peer_id: "peer-2".to_string(),
        public_key: vec![5, 6, 7, 8],
        region: "us-west".to_string(),
        upload_cap_mbps: 500.0,
        download_cap_mbps: 400.0,
        avg_latency_ms: 8.0,
        reputation_score: 0.4,
        supported_encryption: vec!["noise-xx".to_string()],
        max_sessions: 10,
        active_sessions: 0,
        last_updated: 1700000001,
        noise_public_key: vec![40, 50, 60],
    };
    store.upsert_capability(&high_bw_low_rep).await.unwrap();

    let results = store
        .get_capabilities_in_region("us-west", 50.0, 100.0, 10)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].peer_id, "peer-1", "reputation must rank above bandwidth");
    assert_eq!(results[1].peer_id, "peer-2");
    assert_eq!(results[0].upload_cap_mbps, 100.0);
    assert_eq!(results[0].noise_public_key, vec![10, 20, 30]);

    // Limit is respected
    let results = store
        .get_capabilities_in_region("us-west", 50.0, 100.0, 1)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].peer_id, "peer-1");

    // Query with requirements above every provider should return nothing
    let results = store
        .get_capabilities_in_region("us-west", 600.0, 100.0, 10)
        .await
        .unwrap();
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn sqlite_receipt_insert_and_retrieve() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = SqliteStore::new(&db_path).await.unwrap();

    let receipt = StateReceipt {
        receipt_id: "receipt-1".to_string(),
        session_id: "session-abc".to_string(),
        signer_peer_id: "provider-peer".to_string(),
        counterparty_peer_id: "consumer-peer".to_string(),
        bytes_transferred: 5000,
        direction: "bidirectional".to_string(),
        sequence_number: 1,
        timestamp_secs: 1700000000,
        signature: "abcdef1234567890".to_string(),
    };

    store.insert_receipt(&receipt).await.unwrap();

    let receipts = store.get_receipts_for_session("session-abc").await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].receipt_id, "receipt-1");
    assert_eq!(receipts[0].bytes_transferred, 5000);
}

// ──────────────────────────────────────────────────────────────
// Settlement Engine
// ──────────────────────────────────────────────────────────────

/// Helper: create a signed receipt issued by `identity`.
fn signed_receipt(identity: &NodeIdentity, id: &str, bytes: u64) -> StateReceipt {
    use ed25519_dalek::{Signer, SigningKey};

    let signing_key = SigningKey::from_bytes(identity.keypair_bytes.as_slice().try_into().unwrap());
    let mut receipt = StateReceipt {
        receipt_id: id.to_string(),
        session_id: "session-settle".to_string(),
        signer_peer_id: identity.peer_id_str().to_string(),
        counterparty_peer_id: "consumer-peer".to_string(),
        bytes_transferred: bytes,
        direction: "bidirectional".to_string(),
        sequence_number: 1,
        timestamp_secs: 1700000000,
        signature: String::new(),
    };
    let msg = receipt.signed_message().into_bytes();
    let sig = signing_key.sign(&msg);
    receipt.signature = base64_encode(&sig.to_bytes());
    receipt
}

#[tokio::test]
async fn sqlite_settlement_record_idempotent_and_summary() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = SqliteStore::new(&db_path).await.unwrap();

    // Record two distinct receipts, then replay the first receipt_id.
    let r1 = SettlementRecord {
        receipt_id: "receipt-1".to_string(),
        signer_peer_id: "provider".to_string(),
        counterparty_peer_id: "consumer-a".to_string(),
        bytes_settled: 1_000_000_000,
        earnings_usd: 0.50,
        settled_at: "2026-01-01T00:00:00Z".to_string(),
    };
    let r2 = SettlementRecord {
        receipt_id: "receipt-2".to_string(),
        signer_peer_id: "provider".to_string(),
        counterparty_peer_id: "consumer-b".to_string(),
        bytes_settled: 2_000_000_000,
        earnings_usd: 1.00,
        settled_at: "2026-01-02T00:00:00Z".to_string(),
    };
    store.record_settlement(&r1).await.unwrap();
    store.record_settlement(&r2).await.unwrap();

    // Replay receipt-1 — must be a no-op, not double-counted.
    store.record_settlement(&r1).await.unwrap();

    let settlements = store.get_settlements(100).await.unwrap();
    assert_eq!(settlements.len(), 2, "replayed receipt must not double-count");

    let summary = store.settlement_summary().await.unwrap();
    assert_eq!(summary.receipts_settled, 2);
    assert_eq!(summary.total_bytes_settled, 3_000_000_000);
    assert!((summary.total_earnings_usd - 1.50).abs() < f64::EPSILON);

    // Limit is respected
    let limited = store.get_settlements(1).await.unwrap();
    assert_eq!(limited.len(), 1);
}

#[tokio::test]
async fn settlement_engine_accumulates_verified_receipts() {
    use echo_daemon::settlement::SettlementEngine;

    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = Arc::new(SqliteStore::new(&db_path).await.unwrap());
    let identity = NodeIdentity::load_or_generate(&temp_identity_path(dir.path()), "us-west").unwrap();

    let engine = SettlementEngine::new(identity.clone(), store.clone(), 0.50);

    // 2 GiB relayed → exactly $1.00 at $0.50/GB.
    let receipt = signed_receipt(&identity, "receipt-signed-1", 2_000_000_000);

    let settled = engine.settle_receipt(receipt.clone()).await.unwrap();
    let record = settled.expect("valid own-signed receipt must settle");
    assert_eq!(record.receipt_id, "receipt-signed-1");
    assert!((record.earnings_usd - 1.00).abs() < f64::EPSILON);

    // Replaying the same receipt is idempotent.
    let replayed = engine.settle_receipt(receipt).await.unwrap();
    assert!(replayed.is_none(), "already-settled receipt must not settle twice");

    let summary = engine.summary().await.unwrap();
    assert_eq!(summary.receipts_settled, 1);
    assert!(summary.total_earnings_usd > 0.0);
}

#[tokio::test]
async fn settlement_engine_rejects_foreign_or_tampered_receipts() {
    use echo_daemon::settlement::SettlementEngine;

    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = Arc::new(SqliteStore::new(&db_path).await.unwrap());
    let identity = NodeIdentity::load_or_generate(&temp_identity_path(dir.path()), "us-west").unwrap();

    let engine = SettlementEngine::new(identity.clone(), store.clone(), 0.50);

    // Signed by someone else entirely → rejected on signature check.
    let foreign =
        NodeIdentity::load_or_generate(&dir.path().join("foreign.json"), "eu").unwrap();
    let foreign_receipt = signed_receipt(&foreign, "receipt-foreign", 1_000);
    let result = engine.settle_receipt(foreign_receipt.clone()).await.unwrap();
    assert!(result.is_none(), "foreign receipt must not settle");

    // Our own receipt, but tampered with after signing → rejected.
    let mut tampered = signed_receipt(&identity, "receipt-tampered", 1_000);
    tampered.bytes_transferred = 999_999_999;
    let result = engine.settle_receipt(tampered).await.unwrap();
    assert!(result.is_none(), "tampered receipt must not settle");

    // Nothing was accumulated.
    let summary = engine.summary().await.unwrap();
    assert_eq!(summary.receipts_settled, 0);
}

#[tokio::test]
async fn sqlite_peer_reputation_accumulates() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = SqliteStore::new(&db_path).await.unwrap();

    let rep = PeerReputation {
        peer_id: "peer-1".to_string(),
        reputation_score: 0.8,
        total_bytes_relayed: 1000,
        successful_sessions: 5,
        failed_sessions: 1,
        avg_latency_ms: 10.0,
        last_active_at: None,
        recorded_at: None,
    };

    store.upsert_peer_reputation(&rep).await.unwrap();

    // Update with more bytes — should accumulate
    let mut rep2 = rep.clone();
    rep2.total_bytes_relayed = 2000;
    rep2.successful_sessions = 3;
    store.upsert_peer_reputation(&rep2).await.unwrap();

    let fetched = store.get_peer_reputation("peer-1").await.unwrap().unwrap();
    assert_eq!(fetched.total_bytes_relayed, 3000, "bytes should accumulate");
    assert_eq!(fetched.successful_sessions, 8, "sessions should accumulate");
}

// ──────────────────────────────────────────────────────────────
// Identity & Receipt Verification
// ──────────────────────────────────────────────────────────────

#[test]
fn identity_generation_and_persistence() {
    let dir = TempDir::new().unwrap();
    let path = temp_identity_path(dir.path());

    // Generate new identity
    let identity = NodeIdentity::load_or_generate(&path, "us-west").unwrap();
    assert!(!identity.peer_id.is_empty());
    assert_eq!(identity.region, "us-west");
    assert_eq!(identity.noise_secret_key.len(), 32);
    assert_eq!(identity.noise_public_key.len(), 32);
    assert!(path.exists());

    // Load existing identity
    let loaded = NodeIdentity::load_or_generate(&path, "eu-central").unwrap();
    assert_eq!(loaded.peer_id, identity.peer_id);
    assert_eq!(loaded.noise_secret_key, identity.noise_secret_key);
    // Region should not change on reload
    assert_eq!(loaded.region, "us-west");
}

#[test]
fn identity_legacy_migration_adds_noise_keys() {
    let dir = TempDir::new().unwrap();
    let path = temp_identity_path(dir.path());

    // Create a legacy identity (no noise keys)
    let legacy = serde_json::json!({
        "peer_id": "12D3KooWLegacy",
        "keypair_bytes": vec![0u8; 32],
        "public_key_bytes": vec![1u8; 32],
        "region": "us-west",
        "noise_secret_key": [],
        "noise_public_key": [],
    });
    std::fs::write(&path, serde_json::to_string_pretty(&legacy).unwrap()).unwrap();

    // Load should auto-migrate
    let identity = NodeIdentity::load_or_generate(&path, "us-west").unwrap();
    assert_eq!(identity.peer_id, "12D3KooWLegacy");
    assert_eq!(identity.noise_secret_key.len(), 32);
    assert_eq!(identity.noise_public_key.len(), 32);

    // Verify the file was updated
    let reloaded = NodeIdentity::load_or_generate(&path, "us-west").unwrap();
    assert_eq!(reloaded.noise_secret_key, identity.noise_secret_key);
}

#[test]
fn receipt_sign_and_verify_roundtrip() {
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();

    let mut receipt = StateReceipt {
        receipt_id: "r1".to_string(),
        session_id: "session-abc".to_string(),
        signer_peer_id: "provider".to_string(),
        counterparty_peer_id: "consumer".to_string(),
        bytes_transferred: 10240,
        direction: "bidirectional".to_string(),
        sequence_number: 7,
        timestamp_secs: 1700000000,
        signature: String::new(),
    };

    // Sign
    let msg = receipt.signed_message().into_bytes();
    let sig = signing_key.sign(&msg);
    receipt.signature = base64_encode(&sig.to_bytes());

    // Verify with correct key
    assert!(receipt.verify_signature(&verifying_key.to_bytes()).unwrap());

    // Verify with wrong key
    let other_key = SigningKey::generate(&mut OsRng).verifying_key();
    assert!(!receipt.verify_signature(&other_key.to_bytes()).unwrap());
}

// ──────────────────────────────────────────────────────────────
// Concurrent Access
// ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn sqlite_concurrent_upserts() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = Arc::new(SqliteStore::new(&db_path).await.unwrap());

    let mut handles = vec![];

    // Spawn 10 concurrent upserts to the same node
    for i in 0..10 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let row = echo_daemon::NodeRow {
                id: "node-concurrent".to_string(),
                name: format!("Node-{}", i),
                user_id: "user-1".to_string(),
                peer_id: None,
                public_key: None,
                ip_address: None,
                region: None,
                status: Some("online".to_string()),
                last_seen_at: None,
                uptime: Some(i as f64),
                uptime_pct: None,
                avg_latency_ms: Some(0.0),
                bandwidth_down_mbps: Some(0.0),
                bandwidth_up_mbps: Some(0.0),
                packet_loss_pct: Some(0.0),
                quality_score: Some(0.0),
                trust_score: Some(0.0),
                sessions_count: None,
                earnings_usd: Some(0.0),
                reported_upload_cap_mbps: Some(0.0),
                reported_download_cap_mbps: Some(0.0),
                supported_encryption: None,
                services: None,
            };
            store.upsert_node(&row).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Only one row should exist
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM nodes WHERE id = 'node-concurrent'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

// ──────────────────────────────────────────────────────────────
// Cleanup
// ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn sqlite_cleanup_old_metrics() {
    let dir = TempDir::new().unwrap();
    let db_path = format!("sqlite:{}", dir.path().join("test.db").display());
    let store = SqliteStore::new(&db_path).await.unwrap();

    // Insert a metric with a timestamp 60 days ago
    let old_ts = (chrono::Utc::now() - chrono::Duration::days(60))
        .to_rfc3339();
    sqlx::query(
        "INSERT INTO node_metrics (node_id, user_id, recorded_at) VALUES ('n1', 'u1', ?)",
    )
    .bind(&old_ts)
    .execute(store.pool())
    .await
    .unwrap();

    // Insert a recent metric
    let store2 = SqliteStore::new(&db_path).await.unwrap();
    let row = echo_daemon::NodeMetricsRow {
        node_id: "n1".to_string(),
        user_id: "u1".to_string(),
        latency_ms: None,
        bandwidth_down_mbps: None,
        bandwidth_up_mbps: None,
        packet_loss_pct: None,
        quality_score: None,
        earnings_usd: None,
        recorded_at: None, // will use default (now)
    };
    store2.insert_node_metrics(&row).await.unwrap();

    // Cleanup should remove only old metrics
    let deleted = store.cleanup_old_metrics(30).await.unwrap();
    assert!(deleted >= 1, "old metric should be deleted");

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM node_metrics WHERE node_id = 'n1'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count.0, 1, "recent metric should remain");
}
