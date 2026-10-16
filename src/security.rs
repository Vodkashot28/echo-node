//! Process-wide security and health counters.
//!
//! Failure sites across the daemon (handshake errors, static-key pinning
//! rejections, Noise transport decrypt failures, replay-signal rejects,
//! API auth failures, policy denials, settlement anomalies) increment
//! counters here; `/status` exposes a snapshot so operators can watch
//! attack/anomaly pressure without grepping logs.
//!
//! Beyond raw counters this module provides **admin observability**:
//!
//! - every increment is also recorded as a [`SecurityEvent`] (bounded ring
//!   buffer, newest first) with a context string — surfaced by `GET /security`
//! - a sliding-window burst detector raises a **security alert** when
//!   `threshold` events occur within `window_secs` (`error!` log once per
//!   burst, `info!` when it clears, `alert_active` in every snapshot)
//!
//! Counters themselves are lock-free atomics (`Relaxed` — pure metrics,
//! never used for synchronization); only the event ring and alert window use
//! a short-held `std::sync::Mutex` (never across `.await` points).

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{error, info};

/// Cap on recorded events (oldest dropped first).
const MAX_RECORDED_EVENTS: usize = 64;

/// Cap on tracked peer ids for per-peer violation attribution (soft-slashing
/// input). At the cap, already-tracked peers keep counting; new peer ids are
/// ignored until entries clear (restarted process) — a bound beats an
/// unbounded map fed by attacker-chosen ids.
const MAX_TRACKED_PEERS: usize = 4096;

/// One recorded security anomaly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecurityEvent {
    /// Unix seconds.
    pub ts_secs: u64,
    /// Stable kind tag (e.g. `"handshake_failure"`).
    pub kind: &'static str,
    /// Human context: addr, session id, rule name, receipt id, …
    pub detail: String,
}

/// Shared security counter registry. One instance per daemon, held behind
/// `Arc` by the tunnel service, auth middleware, settlement task, and HTTP state.
#[derive(Debug)]
pub struct SecurityCounters {
    handshake_failures: AtomicU64,
    invalid_key_rejections: AtomicU64,
    replay_attempts: AtomicU64,
    frame_decrypt_failures: AtomicU64,
    api_auth_failures: AtomicU64,
    policy_rejections: AtomicU64,
    settlement_anomalies: AtomicU64,
    /// Bounded ring of recent events (newest at the back).
    events: Mutex<VecDeque<SecurityEvent>>,
    /// Timestamps of events inside the alert window.
    window: Mutex<VecDeque<u64>>,
    /// Burst threshold: events within `alert_window_secs` that trip the alert.
    /// `0` disables alerting entirely.
    alert_threshold: u64,
    alert_window_secs: u64,
    /// Latching flag: has the current burst alerted (log-once semantics)?
    alert_fired: AtomicBool,
    /// True while a burst is active (mirrors `alert_fired`, cheap load).
    alert_active: AtomicBool,
    /// Per-peer violation counts for soft-slashing (`accountability.rs`):
    /// incremented only from **post-authentication** attribution sites so an
    /// attacker can't frame another peer by claiming its id pre-handshake.
    peer_violations: Mutex<HashMap<String, u64>>,
}

/// One peer's attributed violation count (`GET /security` payload).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerViolation {
    /// Authenticated libp2p peer id the violations were attributed to.
    pub peer_id: String,
    /// Cumulative count for this process lifetime.
    pub violations: u64,
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
    /// A burst of security events is active right now (see `GET /security`).
    #[serde(default)]
    pub alert_active: bool,
}

impl Default for SecurityCounters {
    fn default() -> Self {
        // Counting only: alerting disabled (threshold 0). Production daemons
        // use `from_env()`, which defaults the burst detector ON.
        Self::with_alerts(0, 60)
    }
}

impl SecurityCounters {
    /// Construct with an explicit burst-alert configuration.
    /// `threshold == 0` disables alerting; window is clamped to ≥ 1 s.
    pub fn with_alerts(threshold: u64, window_secs: u64) -> Self {
        Self {
            handshake_failures: AtomicU64::new(0),
            invalid_key_rejections: AtomicU64::new(0),
            replay_attempts: AtomicU64::new(0),
            frame_decrypt_failures: AtomicU64::new(0),
            api_auth_failures: AtomicU64::new(0),
            policy_rejections: AtomicU64::new(0),
            settlement_anomalies: AtomicU64::new(0),
            events: Mutex::new(VecDeque::with_capacity(MAX_RECORDED_EVENTS)),
            window: Mutex::new(VecDeque::new()),
            alert_threshold: threshold,
            alert_window_secs: window_secs.max(1),
            alert_fired: AtomicBool::new(false),
            alert_active: AtomicBool::new(false),
            peer_violations: Mutex::new(HashMap::new()),
        }
    }

    /// Build from `SECURITY_ALERT_*` env vars.
    ///
    /// - `SECURITY_ALERT_THRESHOLD` (default `10`; `0` disables alerts)
    /// - `SECURITY_ALERT_WINDOW_SECS` (default `60`)
    pub fn from_env() -> Self {
        let threshold = std::env::var("SECURITY_ALERT_THRESHOLD")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(10);
        let window_secs = std::env::var("SECURITY_ALERT_WINDOW_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(60);
        Self::with_alerts(threshold, window_secs)
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Millisecond clock for the alert window — whole-second stamps would
    /// make short windows nondeterministic at second boundaries.
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Increment a counter and record an event + alert evaluation.
    fn record(&self, counter: &AtomicU64, kind: &'static str, detail: &str) {
        counter.fetch_add(1, Ordering::Relaxed);

        // Ring-buffer the event (bounded: oldest dropped first).
        if let Ok(mut events) = self.events.lock() {
            if events.len() >= MAX_RECORDED_EVENTS {
                events.pop_front();
            }
            events.push_back(SecurityEvent {
                ts_secs: Self::now_secs(),
                kind,
                detail: detail.to_string(),
            });
        }

        // Record the event in the burst window; `evaluate_alert` prunes.
        if self.alert_threshold > 0 {
            if let Ok(mut window) = self.window.lock() {
                window.push_back(Self::now_ms());
            }
            self.evaluate_alert();
        }
    }

    /// Prune the expired window, then re-evaluate the burst state; log on
    /// changes. Called from both `record` and `snapshot` so a burst that
    /// quietly drains clears without needing another event.
    fn evaluate_alert(&self) {
        if self.alert_threshold == 0 {
            return;
        }
        let count = {
            let Ok(mut window) = self.window.lock() else {
                return;
            };
            let now = Self::now_ms();
            let expires_at = self.alert_window_secs.saturating_mul(1_000);
            while window.front().is_some_and(|&ts| ts + expires_at <= now) {
                window.pop_front();
            }
            window.len()
        };
        let should_fire = count as u64 >= self.alert_threshold;
        let was_fired = self.alert_fired.load(Ordering::Relaxed);
        if should_fire && !was_fired {
            self.alert_fired.store(true, Ordering::Relaxed);
            self.alert_active.store(true, Ordering::Relaxed);
            error!(
                events_in_window = count,
                threshold = self.alert_threshold,
                window_secs = self.alert_window_secs,
                "SECURITY ALERT: burst of security events detected (invalid handshakes / abuse)"
            );
        } else if !should_fire && was_fired {
            self.alert_fired.store(false, Ordering::Relaxed);
            self.alert_active.store(false, Ordering::Relaxed);
            info!(
                events_in_window = count,
                threshold = self.alert_threshold,
                "security alert cleared"
            );
        }
    }

    pub fn inc_handshake_failure(&self, detail: &str) {
        self.record(&self.handshake_failures, "handshake_failure", detail);
    }

    pub fn inc_invalid_key_rejection(&self, detail: &str) {
        self.record(
            &self.invalid_key_rejections,
            "invalid_key_rejection",
            detail,
        );
    }

    pub fn inc_replay_attempt(&self, detail: &str) {
        self.record(&self.replay_attempts, "replay_attempt", detail);
    }

    pub fn inc_frame_decrypt_failure(&self, detail: &str) {
        self.record(
            &self.frame_decrypt_failures,
            "frame_decrypt_failure",
            detail,
        );
    }

    pub fn inc_api_auth_failure(&self, detail: &str) {
        self.record(&self.api_auth_failures, "api_auth_failure", detail);
    }

    pub fn inc_policy_rejection(&self, detail: &str) {
        self.record(&self.policy_rejections, "policy_rejection", detail);
    }

    pub fn inc_settlement_anomaly(&self, detail: &str) {
        self.record(
            &self.settlement_anomalies,
            "settlement_anomaly",
            detail,
        );
    }

    /// Snapshot all counters. Cheap (seven relaxed loads) — safe to call on
    /// every `/status` poll. Also re-evaluates the alert window so a burst
    /// that quietly drained shows as cleared without waiting for a new event.
    pub fn snapshot(&self) -> SecuritySnapshot {
        self.evaluate_alert();
        SecuritySnapshot {
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
            invalid_key_rejections: self.invalid_key_rejections.load(Ordering::Relaxed),
            replay_attempts: self.replay_attempts.load(Ordering::Relaxed),
            frame_decrypt_failures: self.frame_decrypt_failures.load(Ordering::Relaxed),
            api_auth_failures: self.api_auth_failures.load(Ordering::Relaxed),
            policy_rejections: self.policy_rejections.load(Ordering::Relaxed),
            settlement_anomalies: self.settlement_anomalies.load(Ordering::Relaxed),
            alert_active: self.alert_active.load(Ordering::Relaxed),
        }
    }

    /// Most recent events, newest first (for `GET /security`).
    pub fn recent_events(&self, limit: usize) -> Vec<SecurityEvent> {
        self.events
            .lock()
            .map(|events| events.iter().rev().take(limit).cloned().collect())
            .unwrap_or_default()
    }

    /// Is a burst alert currently active?
    pub fn alert_active(&self) -> bool {
        self.alert_active.load(Ordering::Relaxed)
    }

    /// Attribute one security violation to an **authenticated** peer
    /// (soft-slashing input). Call only where the remote peer id has been
    /// proven *and* the rule broken is the peer's own doing — post-Noise
    /// judgment-based policy denials (blacklist / reputation /
    /// accountability). Never from pre-handshake sites (the peer id is
    /// attacker-claimable ⇒ slander vector) and never for the dialed peer on
    /// a MITM mismatch (that id is the spoofed *victim*, not the attacker).
    ///
    /// Bounded by [`MAX_TRACKED_PEERS`]: at capacity, unknown peer ids are
    /// dropped (existing entries keep counting).
    pub fn note_peer_violation(&self, peer_id: &str) {
        if peer_id.is_empty() {
            return;
        }
        let Ok(mut map) = self.peer_violations.lock() else {
            return;
        };
        // Two-step (NLL can't hold `get_mut`'s borrow across `map.len()`).
        if map.contains_key(peer_id) {
            if let Some(count) = map.get_mut(peer_id) {
                *count += 1;
            }
        } else if map.len() < MAX_TRACKED_PEERS {
            map.insert(peer_id.to_string(), 1);
        }
        // At capacity with an unknown id: dropped rather than growing the map.
    }

    /// Violations attributed to `peer_id` in this process lifetime (`0` if
    /// clean or unknown).
    pub fn peer_violation_count(&self, peer_id: &str) -> u64 {
        self.peer_violations
            .lock()
            .ok()
            .and_then(|map| map.get(peer_id).copied())
            .unwrap_or(0)
    }

    /// The `limit` dirtiest peers by attributed violations, highest first
    /// (ties broken by peer id for stable output). For `GET /security`.
    pub fn top_peer_violations(&self, limit: usize) -> Vec<PeerViolation> {
        let Ok(map) = self.peer_violations.lock() else {
            return Vec::new();
        };
        let mut peers: Vec<(&String, u64)> =
            map.iter().map(|(peer, count)| (peer, *count)).collect();
        peers.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        peers
            .into_iter()
            .take(limit)
            .map(|(peer_id, violations)| PeerViolation {
                peer_id: peer_id.clone(),
                violations,
            })
            .collect()
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
        s.inc_handshake_failure("msg1 read");
        s.inc_handshake_failure("preamble read");
        s.inc_invalid_key_rejection("provider key mismatch");
        s.inc_replay_attempt("session abc replay");
        s.inc_frame_decrypt_failure("egress frame 7");
        s.inc_api_auth_failure("bad token");
        s.inc_policy_rejection("blocked peer");
        s.inc_settlement_anomaly("receipt r-1");
        let snap = s.snapshot();
        assert_eq!(snap.handshake_failures, 2);
        assert_eq!(snap.invalid_key_rejections, 1);
        assert_eq!(snap.replay_attempts, 1);
        assert_eq!(snap.frame_decrypt_failures, 1);
        assert_eq!(snap.api_auth_failures, 1);
        assert_eq!(snap.policy_rejections, 1);
        assert_eq!(snap.settlement_anomalies, 1);
        assert!(!snap.alert_active, "default construction has alerts off");
    }

    #[test]
    fn snapshot_serializes_snake_case() {
        let snap = SecurityCounters::default().snapshot();
        let json = serde_json::to_value(snap).unwrap();
        assert_eq!(json["handshake_failures"], 0);
        assert_eq!(json["settlement_anomalies"], 0);
        assert_eq!(json["alert_active"], false);
    }

    #[test]
    fn events_ring_is_bounded_and_newest_first() {
        let s = SecurityCounters::default();
        for i in 0..100 {
            s.inc_handshake_failure(&format!("event {i}"));
        }
        let recent = s.recent_events(10);
        assert_eq!(recent.len(), 10);
        assert_eq!(recent[0].detail, "event 99", "newest first");
        assert_eq!(recent[9].detail, "event 90");
        assert_eq!(
            s.recent_events(10_000).len(),
            MAX_RECORDED_EVENTS,
            "ring never exceeds its cap"
        );
        assert_eq!(recent[0].kind, "handshake_failure");
        assert!(recent[0].ts_secs > 0);
    }

    #[test]
    fn alert_fires_at_threshold_and_clears_when_window_drains() {
        let s = SecurityCounters::with_alerts(3, 1);
        s.inc_handshake_failure("a");
        assert!(!s.alert_active(), "1 < threshold");
        s.inc_handshake_failure("b");
        assert!(!s.alert_active(), "2 < threshold");
        s.inc_replay_attempt("c");
        assert!(s.alert_active(), "3 >= threshold fires");

        // Window is 1s: after it drains, the next snapshot/record clears.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(
            !s.snapshot().alert_active,
            "drained window clears the alert"
        );
    }

    #[test]
    fn alert_disabled_with_zero_threshold() {
        let s = SecurityCounters::with_alerts(0, 60);
        for _ in 0..50 {
            s.inc_handshake_failure("x");
        }
        assert!(!s.alert_active());
        assert!(!s.snapshot().alert_active);
    }

    #[test]
    fn from_env_parses_threshold_and_window() {
        // Consolidated env test: `from_env` reads SECURITY_ALERT_* — set them
        // here only (no other test calls from_env concurrently).
        std::env::set_var("SECURITY_ALERT_THRESHOLD", "4");
        std::env::set_var("SECURITY_ALERT_WINDOW_SECS", "30");
        let s = SecurityCounters::from_env();
        for _ in 0..4 {
            s.inc_api_auth_failure("x");
        }
        assert!(s.alert_active(), "env-configured threshold (4) fires");
        std::env::remove_var("SECURITY_ALERT_THRESHOLD");
        std::env::remove_var("SECURITY_ALERT_WINDOW_SECS");
    }

    #[test]
    fn peer_violations_track_and_count() {
        let s = SecurityCounters::default();
        assert_eq!(s.peer_violation_count("peer-a"), 0, "clean peer starts at 0");
        s.note_peer_violation("peer-a");
        s.note_peer_violation("peer-a");
        s.note_peer_violation("peer-b");
        s.note_peer_violation(""); // empty id ignored
        assert_eq!(s.peer_violation_count("peer-a"), 2);
        assert_eq!(s.peer_violation_count("peer-b"), 1);
        assert_eq!(s.peer_violation_count("peer-c"), 0);
        // Attribution is orthogonal to the global counters (no double-count).
        assert_eq!(s.snapshot().policy_rejections, 0);
    }

    #[test]
    fn top_peer_violations_sorts_desc() {
        let s = SecurityCounters::default();
        for _ in 0..3 {
            s.note_peer_violation("mid");
        }
        for _ in 0..5 {
            s.note_peer_violation("worst");
        }
        s.note_peer_violation("clean-once");
        let top = s.top_peer_violations(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].peer_id, "worst");
        assert_eq!(top[0].violations, 5);
        assert_eq!(top[1].peer_id, "mid");
        assert_eq!(top[1].violations, 3);
        // Full list keeps stable tie ordering (peer id asc at equal counts).
        let all = s.top_peer_violations(10);
        assert_eq!(
            all.iter().map(|p| p.peer_id.as_str()).collect::<Vec<_>>(),
            vec!["worst", "mid", "clean-once"]
        );
    }

    #[test]
    fn peer_violation_map_respects_capacity() {
        let s = SecurityCounters::default();
        for i in 0..MAX_TRACKED_PEERS {
            s.note_peer_violation(&format!("peer-{i}"));
        }
        // At capacity: tracked peers keep counting, unknown ids are dropped.
        s.note_peer_violation("peer-0");
        s.note_peer_violation("peer-new");
        assert_eq!(s.peer_violation_count("peer-0"), 2, "existing entry keeps counting");
        assert_eq!(s.peer_violation_count("peer-new"), 0, "unknown id dropped at cap");
        assert_eq!(
            s.top_peer_violations(usize::MAX).len(),
            MAX_TRACKED_PEERS,
            "map never exceeds its bound"
        );
    }
}
