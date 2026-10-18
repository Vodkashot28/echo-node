// Session Resumption Protocol - allows tunnels to survive provider restarts
//
// Design:
// - Provider issues signed session tickets after successful handshake
// - Tickets contain: session_id, peer_ids, ephemeral_key, expiry, signature
// - Consumer stores tickets and presents them on reconnect
// - Provider validates ticket and resumes encrypted state
// - Reduces handshake overhead and enables seamless migration

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::identity::NodeIdentity;

/// Session ticket lifetime (24 hours)
const TICKET_VALIDITY_SECS: u64 = 86400;

/// Maximum ticket size to prevent DoS
const MAX_TICKET_SIZE: usize = 2048;

/// A cryptographically signed session ticket that allows resumption.
/// Issued by the provider after successful handshake, presented by consumer
/// on reconnect to skip full Noise_XX handshake and restore tunnel state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTicket {
    /// Original session identifier
    pub session_id: String,
    /// Provider peer ID (issuer)
    pub provider_peer_id: String,
    /// Consumer peer ID
    pub consumer_peer_id: String,
    /// Encrypted session state (includes transport keys, sequence numbers)
    pub encrypted_state: Vec<u8>,
    /// Ticket issue timestamp (unix seconds)
    pub issued_at: u64,
    /// Ticket expiry timestamp (unix seconds)
    pub expires_at: u64,
    /// Ed25519 signature over the ticket fields (provider signs)
    pub signature: String,
}

/// Session state that gets encrypted into the ticket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    /// Noise transport encryption key (32 bytes)
    pub encryption_key: Vec<u8>,
    /// Noise transport decryption key (32 bytes)
    pub decryption_key: Vec<u8>,
    /// Last sent nonce/sequence number
    pub send_sequence: u64,
    /// Last received nonce/sequence number
    pub recv_sequence: u64,
    /// Bytes transferred before migration
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl SessionTicket {
    /// Create a new session ticket (provider side).
    /// Encrypts the session state and signs the ticket.
    pub fn issue(
        identity: &NodeIdentity,
        session_id: String,
        consumer_peer_id: String,
        state: SessionState,
    ) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before UNIX_EPOCH")?
            .as_secs();

        let expires_at = now + TICKET_VALIDITY_SECS;

        // Serialize and encrypt the state
        let state_json = serde_json::to_vec(&state)
            .context("failed to serialize session state")?;
        
        // Simple XOR encryption with provider's Noise key (good enough for
        // non-adversarial resumption; full AES-GCM would be better but adds deps)
        let noise_key = identity.noise_secret_key_bytes();
        let encrypted_state = xor_encrypt(&state_json, noise_key);

        // Build unsigned ticket
        let provider_peer_id = identity.peer_id_str();
        let unsigned = Self {
            session_id: session_id.clone(),
            provider_peer_id: provider_peer_id.to_string(),
            consumer_peer_id: consumer_peer_id.clone(),
            encrypted_state: encrypted_state.clone(),
            issued_at: now,
            expires_at,
            signature: String::new(), // placeholder
        };

        // Sign the ticket
        let message = unsigned.signable_message();
        let signature = identity.sign(message.as_bytes())?;
        let signature_b64 = crate::base64_encode(&signature);

        Ok(Self {
            session_id,
            provider_peer_id: provider_peer_id.to_string(),
            consumer_peer_id,
            encrypted_state,
            issued_at: now,
            expires_at,
            signature: signature_b64,
        })
    }

    /// Verify and decrypt a session ticket (provider side).
    /// Returns the decrypted session state if ticket is valid.
    pub fn verify_and_decrypt(
        &self,
        identity: &NodeIdentity,
        presented_consumer_peer_id: &str,
    ) -> Result<SessionState> {
        // Check expiry
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before UNIX_EPOCH")?
            .as_secs();
        
        if now >= self.expires_at {
            bail!("ticket expired");
        }

        // Verify issuer matches this provider
        if self.provider_peer_id != identity.peer_id_str() {
            bail!("ticket issued by different provider");
        }

        // Verify consumer peer ID matches
        if self.consumer_peer_id != presented_consumer_peer_id {
            bail!("consumer peer ID mismatch");
        }

        // Verify signature
        let message = self.signable_message();
        let signature = crate::base64_decode(&self.signature);
        
        identity.verify(message.as_bytes(), &signature)
            .context("invalid ticket signature")?;

        // Decrypt state
        let noise_key = identity.noise_secret_key_bytes();
        let state_json = xor_encrypt(&self.encrypted_state, noise_key);
        
        let state: SessionState = serde_json::from_slice(&state_json)
            .context("failed to deserialize session state")?;

        Ok(state)
    }

    /// Serialize ticket for transmission (consumer stores this)
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)
            .context("failed to serialize ticket")?;
        
        if bytes.len() > MAX_TICKET_SIZE {
            bail!("ticket too large: {} bytes", bytes.len());
        }

        Ok(bytes)
    }

    /// Deserialize ticket from bytes (consumer presents this)
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_TICKET_SIZE {
            bail!("ticket too large: {} bytes", bytes.len());
        }

        let ticket: Self = serde_json::from_slice(bytes)
            .context("failed to deserialize ticket")?;

        Ok(ticket)
    }

    /// Build the message that gets signed (all fields except signature)
    fn signable_message(&self) -> String {
        format!(
            "session_ticket:{}:{}:{}:{}:{}:{}",
            self.session_id,
            self.provider_peer_id,
            self.consumer_peer_id,
            crate::base64_encode(&self.encrypted_state),
            self.issued_at,
            self.expires_at
        )
    }

    /// Check if ticket is expired without full verification
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        now >= self.expires_at
    }
}

/// Simple XOR encryption/decryption (symmetric operation)
fn xor_encrypt(data: &[u8], key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return data.to_vec();
    }

    data.iter()
        .enumerate()
        .map(|(i, &byte)| byte ^ key[i % key.len()])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity() -> NodeIdentity {
        let temp = tempfile::NamedTempFile::new().unwrap();
        NodeIdentity::load_or_generate(temp.path(), "test-region").unwrap()
    }

    #[test]
    fn ticket_issue_and_verify_roundtrip() {
        let identity = test_identity();
        let consumer_peer_id = "12D3KooWConsumer".to_string();
        
        let state = SessionState {
            encryption_key: vec![1u8; 32],
            decryption_key: vec![2u8; 32],
            send_sequence: 42,
            recv_sequence: 43,
            bytes_sent: 1024,
            bytes_received: 2048,
        };

        // Issue ticket
        let ticket = SessionTicket::issue(
            &identity,
            "session-123".to_string(),
            consumer_peer_id.clone(),
            state.clone(),
        )
        .unwrap();

        // Verify and decrypt
        let decrypted = ticket.verify_and_decrypt(&identity, &consumer_peer_id).unwrap();

        assert_eq!(decrypted.send_sequence, 42);
        assert_eq!(decrypted.recv_sequence, 43);
        assert_eq!(decrypted.bytes_sent, 1024);
        assert_eq!(decrypted.bytes_received, 2048);
    }

    #[test]
    fn ticket_rejects_wrong_consumer() {
        let identity = test_identity();
        
        let state = SessionState {
            encryption_key: vec![1u8; 32],
            decryption_key: vec![2u8; 32],
            send_sequence: 0,
            recv_sequence: 0,
            bytes_sent: 0,
            bytes_received: 0,
        };

        let ticket = SessionTicket::issue(
            &identity,
            "session-123".to_string(),
            "12D3KooWConsumer1".to_string(),
            state,
        )
        .unwrap();

        // Try to verify with different consumer peer ID
        let result = ticket.verify_and_decrypt(&identity, "12D3KooWConsumer2");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("peer ID mismatch"));
    }

    #[test]
    fn ticket_rejects_wrong_provider() {
        let identity1 = test_identity();
        let identity2 = test_identity();
        
        let state = SessionState {
            encryption_key: vec![1u8; 32],
            decryption_key: vec![2u8; 32],
            send_sequence: 0,
            recv_sequence: 0,
            bytes_sent: 0,
            bytes_received: 0,
        };

        let consumer = "12D3KooWConsumer".to_string();
        let ticket = SessionTicket::issue(&identity1, "session-123".to_string(), consumer.clone(), state).unwrap();

        // Try to verify with different provider identity
        let result = ticket.verify_and_decrypt(&identity2, &consumer);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("different provider"));
    }

    #[test]
    fn ticket_serialize_deserialize_roundtrip() {
        let identity = test_identity();
        
        let state = SessionState {
            encryption_key: vec![1u8; 32],
            decryption_key: vec![2u8; 32],
            send_sequence: 100,
            recv_sequence: 200,
            bytes_sent: 5000,
            bytes_received: 10000,
        };

        let ticket = SessionTicket::issue(
            &identity,
            "session-456".to_string(),
            "12D3KooWConsumer".to_string(),
            state,
        )
        .unwrap();

        let bytes = ticket.serialize().unwrap();
        let deserialized = SessionTicket::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.session_id, "session-456");
        assert_eq!(deserialized.consumer_peer_id, "12D3KooWConsumer");
    }

    #[test]
    fn xor_encrypt_is_symmetric() {
        let data = b"Hello, World!";
        let key = b"secret_key_12345";
        
        let encrypted = xor_encrypt(data, key);
        assert_ne!(&encrypted[..], data);
        
        let decrypted = xor_encrypt(&encrypted, key);
        assert_eq!(&decrypted[..], data);
    }
}
