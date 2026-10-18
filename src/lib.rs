pub mod accountability;
pub mod auth;
pub mod availability;
pub mod bidding;
pub mod consumer;
pub mod discovery;
pub mod identity;
pub mod ledger;
pub mod meter;
pub mod migrator;
pub mod models;
pub mod neon;
pub mod policy;
pub mod security;
pub mod session_ticket;
pub mod settlement;
pub mod sqlite_store;
pub mod telemetry;
pub mod tunnel;

pub use models::*;
pub use neon::NeonStore;
pub use sqlite_store::SqliteStore;

use anyhow::Result;
use async_trait::async_trait;

/// Noise_XX cipher suite shared across identity and tunnel modules.
pub const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

// ──────────────────────────────────────────────────────────────
// Shared base64 utilities (avoids adding another dependency)
// ──────────────────────────────────────────────────────────────

/// Encode bytes to base64 string.
pub fn base64_encode(bytes: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Decode a base64 string to bytes.
pub fn base64_decode(input: &str) -> Vec<u8> {
    const TABLE: [u8; 128] = {
        let mut t = [0u8; 128];
        let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < chars.len() {
            t[chars[i] as usize] = i as u8;
            i += 1;
        }
        t
    };

    let input = input.trim_end_matches('=');
    let input = input.as_bytes();

    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    for chunk in input.chunks(4) {
        let b0 = TABLE[chunk[0] as usize] as u32;
        let b1 = if chunk.len() > 1 { TABLE[chunk[1] as usize] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { TABLE[chunk[2] as usize] as u32 } else { 0 };
        let b3 = if chunk.len() > 3 { TABLE[chunk[3] as usize] as u32 } else { 0 };

        let triple = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;
        output.push(((triple >> 16) & 0xFF) as u8);
        if chunk.len() > 2 {
            output.push(((triple >> 8) & 0xFF) as u8);
        }
        if chunk.len() > 3 {
            output.push((triple & 0xFF) as u8);
        }
    }
    output
}

/// Constant-time byte-string comparison to prevent timing side-channel attacks.
/// Returns `true` if `a` and `b` have the same length and every byte is equal,
/// without early-exiting on the first mismatch.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[async_trait]
pub trait MetricsBackend: Send + Sync {
    async fn upsert_node(&self, row: &NodeRow) -> Result<()>;
    async fn insert_node_metrics(&self, row: &NodeMetricsRow) -> Result<()>;
    async fn insert_network_metrics(&self, row: &NetworkMetricsRow) -> Result<()>;
    async fn insert_node_intelligence(&self, row: &NodeIntelligenceRow) -> Result<()>;
    async fn cleanup_old_metrics(&self, days: i64) -> Result<u64>;

    async fn upsert_peer_reputation(&self, row: &PeerReputation) -> Result<()>;
    async fn get_peer_reputation(&self, peer_id: &str) -> Result<Option<PeerReputation>>;
    async fn insert_connection(&self, record: &ConnectionRecord) -> Result<()>;
    #[allow(clippy::too_many_arguments)]
    async fn update_connection_end(
        &self,
        local_peer_id: &str,
        remote_peer_id: &str,
        bytes_sent: u64,
        bytes_received: u64,
        duration_secs: f64,
        avg_latency_ms: f64,
        exit_reason: &str,
    ) -> Result<()>;
    async fn insert_receipt(&self, receipt: &StateReceipt) -> Result<()>;
    async fn get_receipts_for_session(&self, session_id: &str) -> Result<Vec<StateReceipt>>;
    async fn record_settlement(&self, record: &SettlementRecord) -> Result<bool>;
    async fn get_settlements(&self, limit: i64) -> Result<Vec<SettlementRecord>>;
    async fn settlement_summary(&self) -> Result<SettlementSummary>;
    async fn upsert_capability(&self, desc: &CapabilityDescriptor) -> Result<()>;
    async fn get_capabilities_in_region(
        &self,
        region: &str,
        min_upload_mbps: f64,
        min_download_mbps: f64,
        limit: i64,
    ) -> Result<Vec<CapabilityDescriptor>>;
    async fn get_capability_for_peer(&self, peer_id: &str) -> Result<Option<CapabilityDescriptor>>;

    /// Append one canonical ledger event (strictly append-only — the
    /// hash chain assumes rows are never updated or deleted).
    async fn append_ledger_event(&self, row: &LedgerEventRow) -> Result<()>;

    /// Load the per-node hash-chain state (highest sequence + its hash,
    /// plus the genesis hash) so the ledger writer can resume the chain
    /// after a restart. `None` when the node has no events yet.
    async fn load_ledger_chain_state(&self, node_id: &str) -> Result<Option<LedgerChainState>>;

    /// Store a session ticket for resumption (provider side)
    async fn store_session_ticket(
        &self,
        session_id: &str,
        consumer_peer_id: &str,
        ticket_data: &[u8],
        expires_at: i64,
    ) -> Result<()>;

    /// Retrieve a session ticket by session_id (provider side)
    async fn get_session_ticket(&self, session_id: &str) -> Result<Option<Vec<u8>>>;

    /// Delete expired session tickets (cleanup task)
    async fn cleanup_expired_tickets(&self) -> Result<u64>;

    // Auction persistence methods
    /// Save or update an auction
    #[allow(clippy::too_many_arguments)]
    async fn upsert_auction(
        &self,
        auction_id: &str,
        requester_peer_id: &str,
        region: &str,
        bandwidth_mbps: f64,
        budget_usd_per_gb: f64,
        window_secs: i64,
        created_at: i64,
        closes_at: i64,
        status: &str,
    ) -> Result<()>;

    /// Get an auction by ID
    async fn get_auction(&self, auction_id: &str) -> Result<Option<serde_json::Value>>;

    /// List all auctions matching a status
    async fn list_auctions(&self, status: &str, limit: i64) -> Result<Vec<serde_json::Value>>;

    /// Update auction status (open -> closed/cancelled)
    async fn update_auction_status(
        &self,
        auction_id: &str,
        status: &str,
        winning_peer_id: Option<&str>,
        winning_price: Option<f64>,
        awarded_at: Option<i64>,
    ) -> Result<()>;

    /// Store a bid for an auction
    #[allow(clippy::too_many_arguments)]
    async fn insert_bid(
        &self,
        bid_id: &str,
        auction_id: &str,
        provider_peer_id: &str,
        price_usd_per_gb: f64,
        reputation_score: f64,
        avg_latency_ms: f64,
        submitted_at: i64,
    ) -> Result<()>;

    /// Get all bids for an auction
    async fn get_bids_for_auction(&self, auction_id: &str) -> Result<Vec<serde_json::Value>>;

    /// Delete auctions older than specified timestamp (cleanup)
    async fn cleanup_old_auctions(&self, older_than: i64) -> Result<u64>;
}
