use serde::{Deserialize, Serialize};

// ──────────────────────────────────────────────────────────────
// Step 1: Repurposed models — peer reputation, connection
// history, state receipts, capability descriptors
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRow {
    pub id: String,
    pub name: String,
    pub user_id: String,
    pub peer_id: Option<String>,
    pub public_key: Option<String>,
    pub ip_address: Option<String>,
    pub region: Option<String>,
    pub status: Option<String>,
    pub last_seen_at: Option<String>,
    pub uptime: Option<f64>,
    pub uptime_pct: Option<f64>,
    pub avg_latency_ms: Option<f64>,
    pub bandwidth_down_mbps: Option<f64>,
    pub bandwidth_up_mbps: Option<f64>,
    pub packet_loss_pct: Option<f64>,
    pub quality_score: Option<f64>,
    pub trust_score: Option<f64>,
    pub sessions_count: Option<i64>,
    pub earnings_usd: Option<f64>,
    pub reported_upload_cap_mbps: Option<f64>,
    pub reported_download_cap_mbps: Option<f64>,
    pub supported_encryption: Option<String>,
    /// JSON object of active services advertised by this node (e.g. `{"relay": true}`).
    /// Defaults to an empty object when not provided.
    #[serde(default)]
    pub services: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetricsRow {
    pub node_id: String,
    pub user_id: String,
    pub latency_ms: Option<f64>,
    pub bandwidth_down_mbps: Option<f64>,
    pub bandwidth_up_mbps: Option<f64>,
    pub packet_loss_pct: Option<f64>,
    pub quality_score: Option<f64>,
    pub earnings_usd: Option<f64>,
    pub recorded_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetricsRow {
    pub user_id: String,
    pub active_nodes: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub bandwidth_egress_mb: Option<f64>,
    pub bandwidth_ingress_mb: Option<f64>,
    pub packet_loss_pct: Option<f64>,
    pub uptime_pct: Option<f64>,
    pub earnings_usd: Option<f64>,
    pub recorded_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIntelligenceRow {
    pub node_id: String,
    pub user_id: String,
    pub quality_score: Option<f64>,
    pub trust_score: Option<f64>,
    pub anomaly_score: Option<f64>,
    pub is_anomalous: Option<bool>,
    pub cluster_id: Option<i64>,
    pub feature_vector: Option<serde_json::Value>,
    pub recorded_at: Option<String>,
}

// ──────────────────────────────────────────────────────────────
// New: Peer reputation tracking
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReputation {
    pub peer_id: String,
    pub reputation_score: f64,
    pub total_bytes_relayed: u64,
    pub successful_sessions: u64,
    pub failed_sessions: u64,
    pub avg_latency_ms: f64,
    pub last_active_at: Option<String>,
    pub recorded_at: Option<String>,
}

// ──────────────────────────────────────────────────────────────
// New: Connection history
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionRecord {
    pub id: Option<i64>,
    pub local_peer_id: String,
    pub remote_peer_id: String,
    pub remote_ip: Option<String>,
    pub remote_port: Option<u16>,
    pub direction: String,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub duration_secs: f64,
    pub avg_latency_ms: f64,
    pub exit_reason: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
}

// ──────────────────────────────────────────────────────────────
// New: Cryptographic state receipts (metering)
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateReceipt {
    pub receipt_id: String,
    pub session_id: String,
    pub signer_peer_id: String,
    pub counterparty_peer_id: String,
    pub bytes_transferred: u64,
    pub direction: String,
    pub sequence_number: u64,
    pub timestamp_secs: u64,
    pub signature: String,
}

// ──────────────────────────────────────────────────────────────
// New: Capability descriptor (published to DHT)
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityDescriptor {
    pub peer_id: String,
    pub public_key: Vec<u8>,
    pub region: String,
    pub upload_cap_mbps: f64,
    pub download_cap_mbps: f64,
    pub avg_latency_ms: f64,
    pub reputation_score: f64,
    pub supported_encryption: Vec<String>,
    pub max_sessions: u32,
    pub active_sessions: u32,
    pub last_updated: u64,
    /// X25519 static public key for Noise_XX tunnel encryption.
    #[serde(default)]
    pub noise_public_key: Vec<u8>,
}

// ──────────────────────────────────────────────────────────────
// System metrics snapshot (produced by availability engine)
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    pub cpu_pct: f64,
    pub memory_pct: f64,
    pub disk_usage_pct: f64,
    pub active_sessions: u32,
    pub current_usage_rx_mbps: f64,
    pub current_usage_tx_mbps: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}
