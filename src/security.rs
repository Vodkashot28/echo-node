//! Process-wide security and health counters.
//!
//! Failure sites across the daemon (handshake errors, static-key pinning
//! rejections, Noise transport decrypt failures, replay-signal rejects,
//! API auth failures, policy denials, settlement anomalies) increment
//! counters here; `/status` exposes a snapshot so operators can watch
//! attack/anomaly pressure without grepping logs.
//!
//! All counters are lock-free atomics (`Relaxed` — they are pure metrics,
//! never used for synchronization).

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared security counter registry. One instance per daemon, held behind
/// `Arc` by the tunnel service, auth middleware, settlement task, and HTTP state.
#[derive(Debug, Default)]
pub struct SecurityCounters {
    handshake_failures: AtomicU64,
    invalid_key_rejections: AtomicU64,
    replay_attempts: AtomicU64,
    frame_decrypt_failures: AtomicU64,
    api_auth_failures: AtomicU64,
    policy_rejections: AtomicU64,
    settlement_anomalies: AtomicU64,
}

/// Point-in-time view of the counters (serialized into `/status`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecuritySnapshot {
    /// Noise handshake failures (malformed prelude, transport-mode errors, I/O).
    pub handshake_failures: u64,
    /// Static-key pinning rejections (MITM / wrong discovery record).
    pub invalid_key_rejections: u64,
    /// Noise handshake messages that failed Noise authentication (includes
    /// replayed/garbled handshake frames — snow cannot distinguish them).
    pub replay_attempts: u64,
    /// Noise transport-mode AEAD rejects (corruption, tampering, or replay
    /// of tunnel frames — snow collapses these into one decrypt error).
    pub frame_decrypt_failures: u64,
    /// HTTP control-plane authentication/authorization failures.
    pub api_auth_failures: u64,
    /// Sessions denied by the provider policy engine.
    pub policy_rejections: u64,
    /// Receipts rejected during settlement (bad signature / malformed).
    pub settlement_anomalies: u64,
}

impl SecurityCounters {
    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_handshake_failure(&self) {
        Self::inc(&self.handshake_failures);
    }

    pub fn inc_invalid_key_rejection(&self) {
        Self::inc(&self.invalid_key_rejections);
    }

    pub fn inc_replay_attempt(&self) {
        Self::inc(&self.replay_attempts);
    }

    pub fn inc_frame_decrypt_failure(&self) {
        Self::inc(&self.frame_decrypt_failures);
    }

    pub fn inc_api_auth_failure(&self) {
        Self::inc(&self.api_auth_failures);
    }

    pub fn inc_policy_rejection(&self) {
        Self::inc(&self.policy_rejections);
    }

    pub fn inc_settlement_anomaly(&self) {
        Self::inc(&self.settlement_anomalies);
    }

    /// Snapshot all counters. Cheap (seven relaxed loads) — safe to call on
    /// every `/status` poll.
    pub fn snapshot(&self) -> SecuritySnapshot {
        SecuritySnapshot {
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
            invalid_key_rejections: self.invalid_key_rejections.load(Ordering::Relaxed),
            replay_attempts: self.replay_attempts.load(Ordering::Relaxed),
            frame_decrypt_failures: self.frame_decrypt_failures.load(Ordering::Relaxed),
            api_auth_failures: self.api_auth_failures.load(Ordering::Relaxed),
            policy_rejections: self.policy_rejections.load(Ordering::Relaxed),
            settlement_anomalies: self.settlement_anomalies.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_defaults_to_zero() {
        let s = SecurityCounters::default();
        assert_eq!(s.snapshot(), SecuritySnapshot::default());
    }

    #[test]
    fn increments_are_visible_in_snapshot() {
        let s = SecurityCounters::default();
        s.inc_handshake_failure();
        s.inc_handshake_failure();
        s.inc_invalid_key_rejection();
        s.inc_replay_attempt();
        s.inc_frame_decrypt_failure();
        s.inc_api_auth_failure();
        s.inc_policy_rejection();
        s.inc_settlement_anomaly();
        let snap = s.snapshot();
        assert_eq!(snap.handshake_failures, 2);
        assert_eq!(snap.invalid_key_rejections, 1);
        assert_eq!(snap.replay_attempts, 1);
        assert_eq!(snap.frame_decrypt_failures, 1);
        assert_eq!(snap.api_auth_failures, 1);
        assert_eq!(snap.policy_rejections, 1);
        assert_eq!(snap.settlement_anomalies, 1);
    }

    #[test]
    fn snapshot_serializes_snake_case() {
        let snap = SecurityCounters::default().snapshot();
        let json = serde_json::to_value(snap).unwrap();
        assert_eq!(json["handshake_failures"], 0);
        assert_eq!(json["settlement_anomalies"], 0);
    }
}
