//! Canonical ledger events — the append-only, hash-chained audit log.
//!
//! Every marketplace, settlement, reputation, security, and tunnel action
//! emits one [`LedgerEvent`]: a stable, signed, hashable envelope that
//! outlives any particular marketplace, settlement system, or UI.
//!
//! Design invariants (see the EchoMesh ledger spec):
//!
//! - **Immutable + append-only** — events land in `ledger_events` (migration
//!   005) and are never updated or deleted; the hash chain assumes it.
//! - **Hashable** — `hash = "sha256:<hex>"` over the canonical JSON of the
//!   envelope fields + payload + provenance (hash and signature excluded).
//! - **Signed** — the node's ed25519 key signs the raw 32-byte digest.
//!   `signature.public_key` makes events self-verifying.
//! - **Versioned** — `event_version` (per-type definition) evolves
//!   independently from `schema_version` (this envelope).
//! - **Chainable** — `provenance.prev_hash` links to the previous event
//!   hash (Git-commit style); `provenance.root_hash` pins the chain root.
//!   Sequence numbers are per-node, monotonic, and resume across restarts.
//! - **IPLD-ready** — the canonical JSON + sha256 digest is exactly the
//!   input a future CID pipeline needs; no envelope change required.
//!
//! Emission is fire-and-forget: [`LedgerWriter::try_emit`] never blocks the
//! caller (tunnel hot path, HTTP handlers). A single background writer task
//! assigns sequence numbers, maintains the hash chain, signs, and appends —
//! strictly in channel order. A failed append leaves the chain state
//! unchanged (the next successful event simply references the last
//! *persisted* hash, so the stored chain stays contiguous).

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use ed25519_dalek::{Signature, Signer, VerifyingKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::identity::NodeIdentity;
use crate::{base64_decode, base64_encode, LedgerEventRow, MetricsBackend};

/// Envelope schema version — bumps only on envelope-breaking changes.
pub const LEDGER_SCHEMA_VERSION: u16 = 1;
/// Baseline event-definition version (types evolve independently).
pub const LEDGER_EVENT_VERSION: u16 = 1;
/// Default `network` tag; override with `ECHO_NETWORK`.
pub const DEFAULT_NETWORK: &str = "echomesh-devnet";
/// Bounded emit queue. Overflow drops the event (never blocks callers);
/// real event rates are far below this.
pub const LEDGER_CHANNEL_CAP: usize = 512;

/// Canonical event-type identifiers. Stable strings — serialized as-is.
pub mod event_type {
    pub const NODE_REGISTERED: &str = "NodeRegistered";
    pub const PEER_DISCOVERED: &str = "PeerDiscovered";
    pub const AUCTION_CREATED: &str = "AuctionCreated";
    pub const BID_SUBMITTED: &str = "BidSubmitted";
    pub const AUCTION_AWARDED: &str = "AuctionAwarded";
    pub const TUNNEL_ESTABLISHED: &str = "TunnelEstablished";
    pub const TUNNEL_TERMINATED: &str = "TunnelTerminated";
    pub const USAGE_SNAPSHOT: &str = "UsageSnapshot";
    pub const SETTLEMENT_CREATED: &str = "SettlementCreated";
    pub const SETTLEMENT_COMPLETED: &str = "SettlementCompleted";
    pub const REPUTATION_CHANGED: &str = "ReputationChanged";
    pub const SECURITY_ALERT_RAISED: &str = "SecurityAlertRaised";
}

/// Provenance block — source of the event and its hash-chain links.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    /// Emitting software family (always `echo-node` here).
    pub source: String,
    /// Emitting subsystem (`marketplace`, `settlement`, `tunnel`, …).
    pub component: String,
    /// Best-effort hostname of the emitting node.
    pub hostname: String,
    /// Hash of the previous event in this node's chain (`None` on genesis).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    /// Hash of the first event ever persisted for this node (`None` on
    /// the genesis event itself, whose hash cannot be known in advance).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_hash: Option<String>,
}

/// Detached signature block — self-verifying via the embedded public key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventSignature {
    /// Signature algorithm (always `ed25519`).
    pub algorithm: String,
    /// Signer's ed25519 public key, base64 (32 bytes).
    pub public_key: String,
    /// Signature over the raw sha256 digest bytes, base64 (64 bytes).
    pub signature: String,
}

/// The canonical event envelope. `T` defaults to [`serde_json::Value`] —
/// payloads are domain-specific JSON objects; the envelope never changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEvent<T = serde_json::Value> {
    /// Globally unique id: `evt_` + ULID (sortable, time-ordered).
    pub event_id: String,
    /// Version of this event-type definition.
    pub event_version: u16,
    /// Version of the envelope schema.
    pub schema_version: u16,
    /// Canonical event-type string (see [`event_type`]).
    pub event_type: String,
    /// RFC3339 UTC timestamp, second precision, always `Z`.
    pub ts: String,
    /// Network tag (`echomesh-devnet` / `-testnet` / `-mainnet`).
    pub network: String,
    /// Technical origin: `peer:<libp2p-peer-id>`.
    pub node_id: String,
    /// Acting principal (`system:marketplace`, `provider:<peer>`, …).
    pub actor_id: String,
    /// Monotonic per-node sequence (resume-safe across restarts).
    pub sequence: u64,
    /// Optional direct parent event id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<String>,
    /// Groups the events of one business flow (auction id, session id…).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    /// Domain-specific payload — only this differs between event types.
    pub payload: T,
    /// Hash-chain + source metadata.
    pub provenance: Provenance,
    /// Detached ed25519 signature over the digest.
    pub signature: EventSignature,
    /// Canonical digest: `sha256:<hex>`.
    pub hash: String,
}

/// The subset of an event covered by its hash, in fixed field order.
/// (Serde serializes structs in declaration order, so this is the
/// canonical byte string for hashing.)
#[derive(Serialize)]
struct HashableEvent<'a, T> {
    event_id: &'a str,
    event_version: u16,
    schema_version: u16,
    event_type: &'a str,
    ts: &'a str,
    network: &'a str,
    node_id: &'a str,
    actor_id: &'a str,
    sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_event_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<&'a str>,
    payload: &'a T,
    provenance: &'a Provenance,
}

/// Compute the canonical `sha256:<hex>` digest over the hashable subset.
///
/// Takes the envelope fields positionally on purpose: it mirrors
/// `HashableEvent`'s declaration order exactly, which is what makes the
/// digest canonical. Grouping them into a config struct would decouple the
/// hash from the serialized form, so the many-arg signature is intentional.
#[allow(clippy::too_many_arguments)]
fn compute_hash<T: Serialize>(
    event_id: &str,
    event_version: u16,
    schema_version: u16,
    event_type: &str,
    ts: &str,
    network: &str,
    node_id: &str,
    actor_id: &str,
    sequence: u64,
    parent_event_id: Option<&str>,
    correlation_id: Option<&str>,
    payload: &T,
    provenance: &Provenance,
) -> String {
    let view = HashableEvent {
        event_id,
        event_version,
        schema_version,
        event_type,
        ts,
        network,
        node_id,
        actor_id,
        sequence,
        parent_event_id,
        correlation_id,
        payload,
        provenance,
    };
    let bytes = serde_json::to_vec(&view).expect("hashable event serializes");
    let digest = Sha256::digest(&bytes);
    let mut hex = String::with_capacity(64);
    for b in digest {
        hex.push_str(&format!("{b:02x}"));
    }
    format!("sha256:{hex}")
}

/// Decode a `sha256:<hex>` digest back to its 32 raw bytes.
fn digest_bytes(hash: &str) -> Option<[u8; 32]> {
    let hex = hash.strip_prefix("sha256:")?;
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        out[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(out)
}

/// Recompute the canonical hash of a (possibly deserialized) event and
/// verify its ed25519 signature. Returns `Ok(true)` only when both the
/// stored hash and the signature check out.
pub fn verify_event<T: Serialize + DeserializeOwned>(event: &LedgerEvent<T>) -> Result<bool> {
    let recomputed = compute_hash(
        &event.event_id,
        event.event_version,
        event.schema_version,
        &event.event_type,
        &event.ts,
        &event.network,
        &event.node_id,
        &event.actor_id,
        event.sequence,
        event.parent_event_id.as_deref(),
        event.correlation_id.as_deref(),
        &event.payload,
        &event.provenance,
    );
    if recomputed != event.hash {
        return Ok(false);
    }
    let digest = digest_bytes(&event.hash).context("malformed digest")?;
    if event.signature.algorithm != "ed25519" {
        return Ok(false);
    }
    let pk = base64_decode(&event.signature.public_key);
    let pk: [u8; 32] = pk.try_into().map_err(|_| anyhow::anyhow!("bad public key length"))?;
    let vk = VerifyingKey::from_bytes(&pk).context("invalid public key")?;
    let sig = base64_decode(&event.signature.signature);
    let sig = Signature::from_slice(&sig).context("invalid signature length")?;
    Ok(vk.verify_strict(&digest, &sig).is_ok())
}

// ──────────────────────────────────────────────────────────────
// ULID (Crockford base32: 48-bit ms timestamp + 80-bit randomness)
// ──────────────────────────────────────────────────────────────

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

fn encode_crockford_128(value: u128) -> String {
    let mut out = String::with_capacity(26);
    for i in (0..26).rev() {
        let digit = ((value >> (i * 5)) & 0x1f) as usize;
        out.push(CROCKFORD[digit] as char);
    }
    out
}

fn increment_bytes(bytes: &[u8; 10]) -> [u8; 10] {
    let mut out = *bytes;
    for slot in out.iter_mut().rev() {
        if *slot == 0xff {
            *slot = 0;
        } else {
            *slot += 1;
            break;
        }
    }
    out
}

fn random_10_bytes() -> [u8; 10] {
    use rand::RngCore;
    let mut bytes = [0u8; 10];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Monotonic ULID generator: same-millisecond calls increment the random
/// component so ids stay unique and time-sortable within the process.
#[derive(Default)]
struct UlidGenerator {
    last_ms: u64,
    last_rand: [u8; 10],
    initialized: bool,
}

impl UlidGenerator {
    fn next(&mut self) -> String {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.next_at(now_ms, &random_10_bytes())
    }

    /// Deterministic core (tests inject the clock + randomness).
    fn next_at(&mut self, now_ms: u64, rand: &[u8; 10]) -> String {
        let mut ms = now_ms;
        let mut rand = *rand;
        if self.initialized && ms <= self.last_ms {
            ms = self.last_ms;
            rand = increment_bytes(&self.last_rand);
        }
        self.last_ms = ms;
        self.last_rand = rand;
        self.initialized = true;
        // Layout: 48-bit ms in the high bits, 80-bit randomness in the low
        // bits. Build the two halves separately — folding bytes into the
        // whole value with `<<` would shift the timestamp out of u128.
        let mut rand_v: u128 = 0;
        for b in rand {
            rand_v = (rand_v << 8) | u128::from(b);
        }
        let value = ((ms as u128) << 80) | rand_v;
        format!("evt_{}", encode_crockford_128(value))
    }
}

// ──────────────────────────────────────────────────────────────
// Writer: one task, strict FIFO, chain maintained under append
// ──────────────────────────────────────────────────────────────

/// A fully-specified event request handed to the ledger writer.
#[derive(Debug, Clone)]
pub struct LedgerDraft {
    pub event_type: &'static str,
    /// Emitting subsystem (`marketplace`, `settlement`, …).
    pub component: &'static str,
    /// Acting principal (`system:marketplace`, `provider:<peer>`, …).
    pub actor_id: String,
    pub correlation_id: Option<String>,
    pub parent_event_id: Option<String>,
    pub payload: serde_json::Value,
}

impl LedgerDraft {
    pub fn new(
        event_type: &'static str,
        component: &'static str,
        actor_id: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event_type,
            component,
            actor_id: actor_id.into(),
            correlation_id: None,
            parent_event_id: None,
            payload,
        }
    }

    pub fn with_correlation(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub fn with_parent(mut self, id: impl Into<String>) -> Self {
        self.parent_event_id = Some(id.into());
        self
    }
}

/// Fire-and-forget handle to the ledger writer. `try_emit` never blocks
/// and never fails the caller — the invariant is "best-effort durable,
/// never on the critical path".
#[derive(Clone)]
pub struct LedgerWriter {
    tx: mpsc::Sender<LedgerDraft>,
}

impl LedgerWriter {
    /// Queue an event for persistence. Drops (with a warning) when the
    /// queue is full or the writer has stopped.
    pub fn try_emit(&self, draft: LedgerDraft) {
        if let Err(e) = self.tx.try_send(draft) {
            warn!(error = %e, "ledger event dropped (queue full or writer stopped)");
        }
    }

    /// Read `ECHO_NETWORK` (default `echomesh-devnet`).
    pub fn network_from_env() -> String {
        std::env::var("ECHO_NETWORK")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_NETWORK.to_string())
    }
}

/// Spawn the background writer. Returns the cheap cloneable handle plus
/// the task handle (awaited at shutdown so the final events flush).
pub fn spawn_ledger_writer(
    backend: Arc<dyn MetricsBackend>,
    identity: NodeIdentity,
    node_id: String,
    network: String,
) -> (LedgerWriter, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(LEDGER_CHANNEL_CAP);
    let handle = tokio::spawn(run_ledger_writer(rx, backend, identity, node_id, network));
    (LedgerWriter { tx }, handle)
}

/// The single-writer loop: assign sequence, chain, sign, append.
async fn run_ledger_writer(
    mut rx: mpsc::Receiver<LedgerDraft>,
    backend: Arc<dyn MetricsBackend>,
    identity: NodeIdentity,
    node_id: String,
    network: String,
) {
    let signing_key = match identity.signing_key() {
        Ok(k) => k,
        Err(e) => {
            error!(error = %e, "ledger writer: identity has no signing key; disabled");
            return;
        }
    };
    let public_key_b64 = base64_encode(&identity.public_key_bytes);
    let hostname = std::env::var("HOSTNAME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    // Resume the per-node chain. A load failure is fatal for the writer:
    // appending without knowing the true last hash would fork the chain.
    let (mut next_seq, mut prev_hash, mut root_hash) =
        match backend.load_ledger_chain_state(&node_id).await {
            Ok(Some(chain)) => (
                chain.last_sequence.saturating_add(1),
                Some(chain.last_hash),
                Some(chain.genesis_hash),
            ),
            Ok(None) => (1, None, None),
            Err(e) => {
                error!(error = %e, "ledger writer: failed to load chain state; disabled");
                return;
            }
        };
    info!(
        node_id = %node_id,
        network = %network,
        next_sequence = next_seq,
        "ledger writer started"
    );

    let mut ulid = UlidGenerator::default();
    while let Some(draft) = rx.recv().await {
        let event_id = ulid.next();
        let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let provenance = Provenance {
            source: "echo-node".to_string(),
            component: draft.component.to_string(),
            hostname: hostname.clone(),
            prev_hash: prev_hash.clone(),
            root_hash: root_hash.clone(),
        };
        let hash = compute_hash(
            &event_id,
            LEDGER_EVENT_VERSION,
            LEDGER_SCHEMA_VERSION,
            draft.event_type,
            &ts,
            &network,
            &node_id,
            &draft.actor_id,
            next_seq,
            draft.parent_event_id.as_deref(),
            draft.correlation_id.as_deref(),
            &draft.payload,
            &provenance,
        );
        let digest = match digest_bytes(&hash) {
            Some(d) => d,
            None => {
                error!(event_id, "ledger writer: computed digest malformed; dropping");
                continue;
            }
        };
        let signature = signing_key.sign(&digest);
        let event: LedgerEvent = LedgerEvent {
            event_id: event_id.clone(),
            event_version: LEDGER_EVENT_VERSION,
            schema_version: LEDGER_SCHEMA_VERSION,
            event_type: draft.event_type.to_string(),
            ts,
            network: network.clone(),
            node_id: node_id.clone(),
            actor_id: draft.actor_id.clone(),
            sequence: next_seq,
            parent_event_id: draft.parent_event_id.clone(),
            correlation_id: draft.correlation_id.clone(),
            payload: draft.payload.clone(),
            provenance,
            signature: EventSignature {
                algorithm: "ed25519".to_string(),
                public_key: public_key_b64.clone(),
                signature: base64_encode(&signature.to_bytes()),
            },
            hash: hash.clone(),
        };
        let row = LedgerEventRow {
            event_id: event.event_id.clone(),
            sequence: event.sequence,
            event_type: event.event_type.clone(),
            event_version: event.event_version,
            schema_version: event.schema_version,
            ts: event.ts.clone(),
            network: event.network.clone(),
            node_id: event.node_id.clone(),
            actor_id: event.actor_id.clone(),
            correlation_id: event.correlation_id.clone(),
            parent_event_id: event.parent_event_id.clone(),
            hash: event.hash.clone(),
            prev_hash: event.provenance.prev_hash.clone(),
            event_json: serde_json::to_string(&event).expect("ledger event serializes"),
        };
        match backend.append_ledger_event(&row).await {
            Ok(()) => {
                debug!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    sequence = event.sequence,
                    hash = %event.hash,
                    "ledger event appended"
                );
                if root_hash.is_none() {
                    root_hash = Some(hash.clone());
                }
                prev_hash = Some(hash);
                next_seq = next_seq.saturating_add(1);
            }
            Err(e) => {
                // Chain state deliberately unchanged: the next successful
                // append references the last *persisted* hash, keeping the
                // stored chain contiguous even if this event is lost.
                error!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    error = %e,
                    "ledger append failed; chain state unchanged, event dropped"
                );
            }
        }
    }
    info!("ledger writer channel closed; stopped");
}

// ──────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn test_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn build(
        key: &SigningKey,
        event_id: &str,
        sequence: u64,
        prev_hash: Option<&str>,
        root_hash: Option<&str>,
        payload: serde_json::Value,
    ) -> LedgerEvent {
        let ts = "2026-10-16T13:05:46Z";
        let provenance = Provenance {
            source: "echo-node".into(),
            component: "marketplace".into(),
            hostname: "test-host".into(),
            prev_hash: prev_hash.map(str::to_string),
            root_hash: root_hash.map(str::to_string),
        };
        let hash = compute_hash(
            event_id,
            LEDGER_EVENT_VERSION,
            LEDGER_SCHEMA_VERSION,
            event_type::AUCTION_AWARDED,
            ts,
            "echomesh-devnet",
            "peer:12D3KooWTest",
            "system:marketplace",
            sequence,
            None,
            Some("auction_1"),
            &payload,
            &provenance,
        );
        let digest = digest_bytes(&hash).expect("digest");
        let sig = key.sign(&digest);
        LedgerEvent {
            event_id: event_id.to_string(),
            event_version: LEDGER_EVENT_VERSION,
            schema_version: LEDGER_SCHEMA_VERSION,
            event_type: event_type::AUCTION_AWARDED.to_string(),
            ts: ts.to_string(),
            network: "echomesh-devnet".to_string(),
            node_id: "peer:12D3KooWTest".to_string(),
            actor_id: "system:marketplace".to_string(),
            sequence,
            parent_event_id: None,
            correlation_id: Some("auction_1".to_string()),
            payload,
            provenance,
            signature: EventSignature {
                algorithm: "ed25519".to_string(),
                public_key: base64_encode(&key.verifying_key().to_bytes()),
                signature: base64_encode(&sig.to_bytes()),
            },
            hash,
        }
    }

    #[test]
    fn ulid_has_prefix_length_and_charset() {
        let mut gen = UlidGenerator::default();
        let id = gen.next_at(1_700_000_000_000, &[0x01; 10]);
        assert!(id.starts_with("evt_"));
        let body = &id[4..];
        assert_eq!(body.len(), 26);
        assert!(body
            .chars()
            .all(|c| CROCKFORD.contains(&(c as u8))));
    }

    #[test]
    fn ulid_is_monotonic_within_the_same_millisecond() {
        let mut gen = UlidGenerator::default();
        let a = gen.next_at(1_700_000_000_000, &[0x00; 10]);
        let b = gen.next_at(1_700_000_000_000, &[0x00; 10]);
        let c = gen.next_at(1_700_000_000_000, &[0x00; 10]);
        assert!(a < b, "same ms must produce increasing ids: {a} !< {b}");
        assert!(b < c);
        // A later millisecond always sorts after an earlier one.
        let d = gen.next_at(1_700_000_000_001, &[0x00; 10]);
        assert!(c < d);
    }

    #[test]
    fn hash_is_deterministic_and_payload_sensitive() {
        let key = test_key(1);
        let a = build(
            &key,
            "evt_A",
            1,
            None,
            None,
            serde_json::json!({"auction_id": "a1", "price": 0.42}),
        );
        let b = build(
            &key,
            "evt_A",
            1,
            None,
            None,
            serde_json::json!({"auction_id": "a1", "price": 0.42}),
        );
        assert_eq!(a.hash, b.hash, "identical inputs must hash identically");
        let c = build(
            &key,
            "evt_A",
            1,
            None,
            None,
            serde_json::json!({"auction_id": "a1", "price": 0.43}),
        );
        assert_ne!(a.hash, c.hash, "payload change must change the hash");
        assert!(a.hash.starts_with("sha256:"));
        assert_eq!(a.hash.len(), "sha256:".len() + 64);
    }

    #[test]
    fn hash_excludes_hash_and_signature_fields() {
        // Tampering with the signature must not change the *recomputed*
        // hash — but verification must then fail.
        let key = test_key(2);
        let mut ev = build(
            &key,
            "evt_B",
            7,
            None,
            None,
            serde_json::json!({"x": 1}),
        );
        let original = ev.hash.clone();
        ev.signature.signature = base64_encode(&[0u8; 64]);
        assert!(
            !verify_event(&ev).expect("verify runs"),
            "tampered signature must not verify"
        );
        assert_eq!(ev.hash, original, "signature is outside the hash");
    }

    #[test]
    fn signature_verifies_and_tampering_breaks_it() {
        let key = test_key(3);
        let ev = build(
            &key,
            "evt_C",
            3,
            None,
            None,
            serde_json::json!({"winner": "providerA"}),
        );
        assert!(verify_event(&ev).expect("verify runs"), "valid event failed verification");

        let mut tampered = ev.clone();
        tampered.payload = serde_json::json!({"winner": "providerB"});
        assert!(
            !verify_event(&tampered).expect("verify runs"),
            "tampered payload must fail"
        );

        let mut wrong_key = ev.clone();
        wrong_key.signature.public_key = base64_encode(&test_key(99).verifying_key().to_bytes());
        assert!(
            !verify_event(&wrong_key).expect("verify runs"),
            "signature from another key must fail"
        );
    }

    #[test]
    fn serde_roundtrip_preserves_hash_and_verifies() {
        let key = test_key(4);
        let ev = build(
            &key,
            "evt_D",
            9,
            None,
            None,
            serde_json::json!({"bytes": 4096}),
        );
        let json = serde_json::to_string(&ev).expect("serialize");
        let back: LedgerEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, ev);
        assert!(verify_event(&back).expect("verify runs"));
    }

    #[test]
    fn chained_events_link_prev_and_root_hashes() {
        let key = test_key(5);
        let genesis = build(
            &key,
            "evt_G",
            1,
            None,
            None,
            serde_json::json!({"step": "first"}),
        );
        assert!(genesis.provenance.prev_hash.is_none());
        assert!(genesis.provenance.root_hash.is_none());

        let second = build(
            &key,
            "evt_S",
            2,
            Some(&genesis.hash),
            Some(&genesis.hash),
            serde_json::json!({"step": "second"}),
        );
        assert_eq!(second.provenance.prev_hash.as_deref(), Some(genesis.hash.as_str()));
        assert_eq!(second.provenance.root_hash.as_deref(), Some(genesis.hash.as_str()));
        assert!(verify_event(&second).expect("verify runs"));

        // Mutating the chain link invalidates the hash.
        let mut broken = second.clone();
        broken.provenance.prev_hash = Some("sha256:".to_string() + &"00".repeat(32));
        assert!(!verify_event(&broken).expect("verify runs"));
    }

    #[test]
    fn draft_helpers_attach_correlation_and_parent() {
        let d = LedgerDraft::new(
            event_type::BID_SUBMITTED,
            "marketplace",
            "provider:p1",
            serde_json::json!({"price": 0.4}),
        )
        .with_correlation("auction_9")
        .with_parent("evt_PARENT");
        assert_eq!(d.correlation_id.as_deref(), Some("auction_9"));
        assert_eq!(d.parent_event_id.as_deref(), Some("evt_PARENT"));
        assert_eq!(d.event_type, event_type::BID_SUBMITTED);
    }

    #[test]
    fn network_from_env_reads_blank_and_defaults() {
        // Consolidated env test for ECHO_NETWORK (this is the only test
        // that touches the variable).
        std::env::remove_var("ECHO_NETWORK");
        assert_eq!(LedgerWriter::network_from_env(), DEFAULT_NETWORK);
        std::env::set_var("ECHO_NETWORK", "echomesh-testnet");
        assert_eq!(LedgerWriter::network_from_env(), "echomesh-testnet");
        std::env::set_var("ECHO_NETWORK", "   ");
        assert_eq!(LedgerWriter::network_from_env(), DEFAULT_NETWORK);
        std::env::remove_var("ECHO_NETWORK");
    }
}
