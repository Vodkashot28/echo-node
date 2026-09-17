pub mod availability;
pub mod discovery;
pub mod identity;
pub mod meter;
pub mod models;
pub mod neon;
pub mod sqlite_store;
pub mod tunnel;

pub use models::*;
pub use neon::NeonStore;
pub use sqlite_store::SqliteStore;

use anyhow::Result;
use async_trait::async_trait;

/// Noise_XX cipher suite shared across identity and tunnel modules.
pub const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

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
    async fn upsert_capability(&self, desc: &CapabilityDescriptor) -> Result<()>;
    async fn get_capabilities_in_region(
        &self,
        region: &str,
        min_upload_mbps: f64,
        min_download_mbps: f64,
        limit: i64,
    ) -> Result<Vec<CapabilityDescriptor>>;
}
