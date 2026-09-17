use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

use crate::identity::NodeIdentity;
use crate::models::StateReceipt;

// ──────────────────────────────────────────────────────────────
// Token Bucket Rate Limiter
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_rate: f64, // tokens per second
    last_refill: std::time::Instant,
}

impl TokenBucket {
    pub fn new(capacity_mbps: f64, refill_rate_mbps: f64) -> Self {
        Self {
            capacity: capacity_mbps,
            tokens: capacity_mbps,
            refill_rate: refill_rate_mbps,
            last_refill: std::time::Instant::now(),
        }
    }

    /// Try to consume tokens. Returns actual amount consumed.
    pub fn consume(&mut self, requested_mbps: f64) -> f64 {
        self.refill();
        let available = self.tokens.min(requested_mbps);
        self.tokens -= available;
        available
    }

    /// Check if tokens are available without consuming.
    pub fn available(&mut self) -> f64 {
        self.refill();
        self.tokens
    }

    /// Get the refill rate (tokens/seconds) for computing sleep durations.
    pub fn refill_rate(&self) -> f64 {
        self.refill_rate
    }

    fn refill(&mut self) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }
}

// ──────────────────────────────────────────────────────────────
// Metering Engine
// ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MeteredSession {
    pub session_id: String,
    pub local_peer_id: String,
    pub remote_peer_id: String,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub receipts_issued: u64,
    pub sequence_counter: u64,
}

#[derive(Clone)]
pub struct MeteringEngine {
    identity: NodeIdentity,
    sessions: Arc<RwLock<HashMap<String, MeteredSession>>>,
    receipt_tx: mpsc::UnboundedSender<StateReceipt>,
}

impl MeteringEngine {
    pub fn new(
        identity: NodeIdentity,
    ) -> (Self, mpsc::UnboundedReceiver<StateReceipt>) {
        let (receipt_tx, receipt_rx) = mpsc::unbounded_channel();
        (
            Self {
                identity,
                sessions: Arc::new(RwLock::new(HashMap::new())),
                receipt_tx,
            },
            receipt_rx,
        )
    }

    /// Register a new metered session
    pub async fn start_session(&self, session_id: &str, remote_peer_id: &str) {
        let session = MeteredSession {
            session_id: session_id.to_string(),
            local_peer_id: self.identity.peer_id_str().to_string(),
            remote_peer_id: remote_peer_id.to_string(),
            bytes_sent: 0,
            bytes_received: 0,
            receipts_issued: 0,
            sequence_counter: 0,
        };
        self.sessions
            .write()
            .await
            .insert(session_id.to_string(), session);
        info!(session = session_id, remote = remote_peer_id, "metered session started");
    }

    /// Record bytes transferred and potentially issue a receipt
    pub async fn record_transfer(
        &self,
        session_id: &str,
        direction: &str,
        bytes: u64,
    ) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .context("session not found")?;

        match direction {
            "egress" => session.bytes_sent += bytes,
            "ingress" => session.bytes_received += bytes,
            _ => return Err(anyhow::anyhow!("invalid direction: {}", direction)),
        }

        session.sequence_counter += 1;

        // Issue receipt at configured intervals
        if session.sequence_counter % 100 == 0 || bytes > 1_000_000 {
            let receipt = self.create_receipt(session).await?;
            session.receipts_issued += 1;
            let _ = self.receipt_tx.send(receipt);
        }

        Ok(())
    }

    /// Create a cryptographically signed state receipt
    async fn create_receipt(&self, session: &MeteredSession) -> Result<StateReceipt> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        // Message to sign: session_id + bytes_transferred + sequence + timestamp + signer_peer_id
        let bytes_transferred = session.bytes_sent + session.bytes_received;
        let message = format!(
            "{}:{}:{}:{}:{}",
            session.session_id,
            bytes_transferred,
            session.sequence_counter,
            timestamp,
            session.local_peer_id
        );

        let signature = self
            .identity
            .sign(message.as_bytes())
            .context("failed to sign receipt")?;

        let receipt = StateReceipt {
            receipt_id: format!("{}-{}", session.session_id, session.sequence_counter),
            session_id: session.session_id.clone(),
            signer_peer_id: session.local_peer_id.clone(),
            counterparty_peer_id: session.remote_peer_id.clone(),
            bytes_transferred: session.bytes_sent + session.bytes_received,
            direction: "bidirectional".to_string(),
            sequence_number: session.sequence_counter,
            timestamp_secs: timestamp,
            signature: base64_encode(&signature),
        };

        debug!(
            session = session.session_id,
            seq = session.sequence_counter,
            "receipt issued"
        );

        Ok(receipt)
    }

    /// Finalize a session and issue final receipt
    pub async fn end_session(&self, session_id: &str) -> Result<StateReceipt> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .remove(session_id)
            .context("session not found")?;

        let receipt = self.create_receipt(&session).await?;
        let _ = self.receipt_tx.send(receipt.clone());

        info!(
            session = session_id,
            bytes_sent = session.bytes_sent,
            bytes_received = session.bytes_received,
            receipts = session.receipts_issued,
            "metered session ended"
        );

        Ok(receipt)
    }

}

// Simple base64 helpers (avoids adding another dependency)
fn base64_encode(bytes: &[u8]) -> String {
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

// ──────────────────────────────────────────────────────────────
// Wire format: ed25519 signatures are 64 bytes = 86 chars base64.
// base64_encode is used by create_receipt(); base64_decode was only
// used by the now-removed verify_receipt(), so it is deleted.
// ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_refill_rate() {
        let bucket = TokenBucket::new(10.0, 5.0);
        assert_eq!(bucket.refill_rate(), 5.0);
    }

    #[test]
    fn token_bucket_initial_tokens_equal_capacity() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        assert_eq!(bucket.available(), 10.0);
    }

    #[test]
    fn token_bucket_consume_within_capacity() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        let granted = bucket.consume(5.0);
        assert_eq!(granted, 5.0);
        // ~0 tokens remain (minus tiny elapsed time)
        let remaining = bucket.available();
        assert!(remaining < 5.5, "expected < 5.5, got {}", remaining);
    }

    #[test]
    fn token_bucket_consume_exceeds_capacity() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        let granted = bucket.consume(20.0);
        // Should only grant what's available (capacity)
        assert!(granted <= 10.1, "expected <= 10.1, got {}", granted);
    }

    #[test]
    fn token_bucket_consume_zero() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        let granted = bucket.consume(0.0);
        assert_eq!(granted, 0.0);
    }

    #[test]
    fn base64_encode_empty() {
        assert_eq!(base64_encode(b""), "");
    }

    #[test]
    fn base64_encode_single_byte() {
        assert_eq!(base64_encode(b"A"), "QQ==");
    }

    #[test]
    fn base64_encode_two_bytes() {
        assert_eq!(base64_encode(b"AB"), "QUI=");
    }

    #[test]
    fn base64_encode_three_bytes() {
        assert_eq!(base64_encode(b"ABC"), "QUJD");
    }

    #[test]
    fn base64_encode_known_value() {
        // "Hello, World!" in base64 is "SGVsbG8sIFdvcmxkIQ=="
        assert_eq!(base64_encode(b"Hello, World!"), "SGVsbG8sIFdvcmxkIQ==");
    }

    #[test]
    fn base64_encode_64_bytes() {
        let input = vec![0u8; 64];
        let encoded = base64_encode(&input);
        // 64 bytes = 85 chars base64 (64/3 * 4 rounded up)
        assert_eq!(encoded.len(), 88); // 64/3 = 21.33 -> 22 groups * 4 = 88
        // All chars should be valid base64
        for c in encoded.chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=',
                "invalid base64 char: {}",
                c
            );
        }
    }
}
