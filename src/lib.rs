pub mod availability;
pub mod consumer;
pub mod discovery;
pub mod identity;
pub mod meter;
pub mod migrator;
pub mod models;
pub mod neon;
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
}
