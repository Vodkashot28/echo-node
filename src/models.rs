use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::base64_decode;

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

impl StateReceipt {
    /// Reconstruct the signed message from receipt fields.
    ///
    /// Format: `"{session_id}:{bytes_transferred}:{sequence_number}:{timestamp_secs}:{signer_peer_id}"`
    pub fn signed_message(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.session_id,
            self.bytes_transferred,
            self.sequence_number,
            self.timestamp_secs,
            self.signer_peer_id
        )
    }

    /// Verify the ed25519 signature against the signer's public key.
    ///
    /// Returns `Ok(true)` if valid, `Ok(false)` if invalid, `Err` on
    /// decode failure (malformed signature or key).
    pub fn verify_signature(&self, public_key_bytes: &[u8]) -> Result<bool, anyhow::Error> {
        let key_array: [u8; 32] = public_key_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid public key length: expected 32 bytes"))?;

        let vk = VerifyingKey::from_bytes(&key_array)
            .map_err(|e| anyhow::anyhow!("invalid ed25519 public key: {}", e))?;

        let sig_bytes: [u8; 64] = base64_decode(&self.signature)
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid signature length: expected 64 bytes"))?;

        let sig = Signature::from_bytes(&sig_bytes);
        let msg = self.signed_message().into_bytes();

        Ok(vk.verify_strict(&msg, &sig).is_ok())
    }
}

// ──────────────────────────────────────────────────────────────
// New: Settlement record (payout accumulation)
// ──────────────────────────────────────────────────────────────

/// An accumulated settlement entry: one row per signed receipt that has
/// been verified and marked for payout. Keyed by `receipt_id` so identical
/// receipts are naturally idempotent (never double-counted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementRecord {
    pub receipt_id: String,
    pub signer_peer_id: String,
    pub counterparty_peer_id: String,
    /// Bytes transferred for this settlement (from the signed receipt).
    pub bytes_settled: u64,
    /// Payout value in USD (bytes * conversion rate).
    pub earnings_usd: f64,
    /// When this settlement entry was recorded (RFC3339).
    pub settled_at: String,
}

/// Aggregated payout totals derived from the `settlements` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementSummary {
    pub receipts_settled: u64,
    pub total_bytes_settled: u64,
    pub total_earnings_usd: f64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base64_encode;

    #[test]
    fn base64_decode_empty() {
        assert!(base64_decode("").is_empty());
    }

    #[test]
    fn base64_decode_known_values() {
        assert_eq!(base64_decode("QQ=="), b"A");
        assert_eq!(base64_decode("QUI="), b"AB");
        assert_eq!(base64_decode("QUJD"), b"ABC");
        assert_eq!(
            base64_decode("SGVsbG8sIFdvcmxkIQ=="),
            b"Hello, World!"
        );
    }

    #[test]
    fn signed_message_format() {
        let receipt = StateReceipt {
            receipt_id: "r1".to_string(),
            session_id: "s1".to_string(),
            signer_peer_id: "peer_a".to_string(),
            counterparty_peer_id: "peer_b".to_string(),
            bytes_transferred: 1024,
            direction: "bidirectional".to_string(),
            sequence_number: 5,
            timestamp_secs: 1700000000,
            signature: "".to_string(),
        };
        assert_eq!(
            receipt.signed_message(),
            "s1:1024:5:1700000000:peer_a"
        );
    }

    #[test]
    fn verify_signature_bad_key_length() {
        let receipt = StateReceipt {
            receipt_id: "r1".to_string(),
            session_id: "s1".to_string(),
            signer_peer_id: "peer_a".to_string(),
            counterparty_peer_id: "peer_b".to_string(),
            bytes_transferred: 1024,
            direction: "bidirectional".to_string(),
            sequence_number: 5,
            timestamp_secs: 1700000000,
            signature: base64_encode(&[0u8; 64]),
        };
        assert!(receipt.verify_signature(&[0u8; 16]).is_err());
    }

    #[test]
    fn verify_signature_bad_signature_length() {
        let receipt = StateReceipt {
            receipt_id: "r1".to_string(),
            session_id: "s1".to_string(),
            signer_peer_id: "peer_a".to_string(),
            counterparty_peer_id: "peer_b".to_string(),
            bytes_transferred: 1024,
            direction: "bidirectional".to_string(),
            sequence_number: 5,
            timestamp_secs: 1700000000,
            signature: base64_encode(&[0u8; 32]), // only 32 bytes, need 64
        };
        assert!(receipt.verify_signature(&[0u8; 32]).is_err());
    }

    #[test]
    fn verify_signature_wrong_key() {
        use ed25519_dalek::{SigningKey, Signer};
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let other_key = SigningKey::generate(&mut OsRng).verifying_key();

        let mut receipt = StateReceipt {
            receipt_id: "r1".to_string(),
            session_id: "s1".to_string(),
            signer_peer_id: "peer_a".to_string(),
            counterparty_peer_id: "peer_b".to_string(),
            bytes_transferred: 1024,
            direction: "bidirectional".to_string(),
            sequence_number: 5,
            timestamp_secs: 1700000000,
            signature: String::new(),
        };

        // Sign with one key, verify with a different key
        let msg = receipt.signed_message().into_bytes();
        let sig = signing_key.sign(&msg);
        receipt.signature = base64_encode(&sig.to_bytes());

        let result = receipt.verify_signature(&other_key.to_bytes());
        assert!(result.is_ok());
        assert!(!result.unwrap(), "verification should have failed with wrong key");
    }
}
