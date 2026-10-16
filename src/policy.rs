//! Provider-side session admission policy.
//!
//! After the Noise_XX handshake completes and the remote peer id is known,
//! the provider consults the [`PolicyEngine`] before spending a session slot:
//!
//! - **blacklisted peers** — hard deny (`POLICY_BLOCKED_PEERS`)
//! - **min reputation** — from `peer_reputation`; unknown peers score 0.0
//!   (`POLICY_MIN_REPUTATION`, `0` disables)
//! - **min accountability (soft-slashing)** — composite of decayed
//!   reputation × per-peer security violations × recorded session
//!   availability (see [`crate::accountability`]); unknown peers score 0.0
//!   (`POLICY_MIN_ACCOUNTABILITY`, `0` disables)
//! - **max latency** — peer's recorded `avg_latency_ms` (`POLICY_MAX_LATENCY_MS`,
//!   unset disables; unrecorded/zero samples are not enforced)
//! - **per-peer session quota** (`POLICY_MAX_SESSIONS_PER_PEER`, `0` = unlimited)
//! - **per-region session cap** (`POLICY_MAX_SESSIONS_PER_REGION`, `0` = unlimited);
//!   the peer's advertised region comes from its capability record, falling
//!   back to the `"unknown"` bucket
//!
//! Quota slots are held by a [`PolicyPermit`] (RAII): dropping the permit
//! releases the peer and region counts exactly once, so early returns and
//! panics can never leak quota. The engine is intentionally synchronous
//! internally (a short-held `std::sync::Mutex` over two count maps) and is
//! never held across `.await` points.

use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::warn;

use crate::accountability::soft_slash_score;

/// Static policy configuration, parsed once from the environment.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PolicyConfig {
    /// Minimum peer reputation required for admission. `0.0` disables.
    pub min_reputation: f64,
    /// Maximum recorded average RTT (ms). `None` disables.
    pub max_latency_ms: Option<f64>,
    /// Max concurrent sessions per peer. `0` = unlimited.
    pub max_sessions_per_peer: u32,
    /// Max concurrent sessions per advertised region. `0` = unlimited.
    pub max_sessions_per_region: u32,
    /// Peer ids that are never admitted.
    pub blocked_peers: HashSet<String>,
    /// Idle-time half-life (days) applied to reputation scores before the
    /// min-reputation gate: `score * 0.5^(idle/half_life)`. `0` disables
    /// decay (raw stored score is gated). Default `30`.
    pub reputation_half_life_days: f64,
    /// Minimum soft-slashing score required for admission (composite of
    /// decayed reputation × security-violation discount × availability
    /// history — see [`crate::accountability`]). `0.0` disables.
    pub min_accountability: f64,
}

impl PolicyConfig {
    /// Read the `POLICY_*` env vars. Every knob defaults to "disabled".
    pub fn from_env() -> Self {
        let max_latency_raw = std::env::var("POLICY_MAX_LATENCY_MS")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let max_latency_ms = match max_latency_raw {
            Some(raw) => match raw.parse::<f64>() {
                Ok(v) if v.is_finite() && v > 0.0 => Some(v),
                Ok(v) => {
                    warn!(value = %v, "POLICY_MAX_LATENCY_MS must be > 0; latency gate disabled");
                    None
                }
                Err(_) => {
                    warn!(value = %raw, "unparseable POLICY_MAX_LATENCY_MS; latency gate disabled");
                    None
                }
            },
            None => None,
        };

        let half_life_raw = std::env::var("REPUTATION_DECAY_HALF_LIFE_DAYS")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let reputation_half_life_days = match half_life_raw {
            Some(raw) => match raw.parse::<f64>() {
                Ok(v) if v.is_finite() && v >= 0.0 => v,
                Ok(v) => {
                    warn!(value = %v, "REPUTATION_DECAY_HALF_LIFE_DAYS must be >= 0; using default 30");
                    30.0
                }
                Err(_) => {
                    warn!(value = %raw, "unparseable REPUTATION_DECAY_HALF_LIFE_DAYS; using default 30");
                    30.0
                }
            },
            None => 30.0,
        };

        Self {
            min_reputation: env_f64_clamped("POLICY_MIN_REPUTATION", 0.0),
            max_latency_ms,
            max_sessions_per_peer: env_u32("POLICY_MAX_SESSIONS_PER_PEER", 0),
            max_sessions_per_region: env_u32("POLICY_MAX_SESSIONS_PER_REGION", 0),
            blocked_peers: std::env::var("POLICY_BLOCKED_PEERS")
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            reputation_half_life_days,
            min_accountability: env_f64_clamped("POLICY_MIN_ACCOUNTABILITY", 0.0),
        }
    }

    /// True when every gate is off — admission always allows.
    pub fn is_open(&self) -> bool {
        self.min_reputation <= 0.0
            && self.min_accountability <= 0.0
            && self.max_latency_ms.is_none()
            && self.max_sessions_per_peer == 0
            && self.max_sessions_per_region == 0
            && self.blocked_peers.is_empty()
    }
}

/// Reason a session was denied (serialized into logs / future admin APIs).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum PolicyViolation {
    BlockedPeer,
    LowReputation { score: f64, min: f64 },
    LowAccountability { score: f64, min: f64 },
    HighLatency { latency_ms: f64, max_ms: f64 },
    PeerQuotaExceeded { peer: String, in_use: u32, max: u32 },
    RegionCapExceeded { region: String, in_use: u32, max: u32 },
}

impl std::fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockedPeer => write!(f, "peer is blacklisted"),
            Self::LowReputation { score, min } => {
                write!(f, "reputation {score:.2} below minimum {min:.2}")
            }
            Self::LowAccountability { score, min } => {
                write!(
                    f,
                    "soft-slashing score {score:.2} below minimum {min:.2}"
                )
            }
            Self::HighLatency { latency_ms, max_ms } => {
                write!(f, "recorded latency {latency_ms:.1}ms above maximum {max_ms:.1}ms")
            }
            Self::PeerQuotaExceeded { peer, in_use, max } => {
                write!(f, "peer {peer} already holds {in_use}/{max} sessions")
            }
            Self::RegionCapExceeded { region, in_use, max } => {
                write!(f, "region {region} already holds {in_use}/{max} sessions")
            }
        }
    }
}

#[derive(Debug, Default)]
struct PolicyState {
    peer_sessions: HashMap<String, u32>,
    region_sessions: HashMap<String, u32>,
}

/// Admission engine. One instance per daemon, shared behind `Arc`.
pub struct PolicyEngine {
    config: PolicyConfig,
    state: Mutex<PolicyState>,
}

/// RAII quota permit: releases the peer + region counts on drop.
pub struct PolicyPermit {
    engine: Arc<PolicyEngine>,
    peer: String,
    region: String,
}

impl std::fmt::Debug for PolicyPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Skip the engine: `PolicyEngine` is deliberately not Debug (holds a
        // Mutex over live session counts).
        f.debug_struct("PolicyPermit")
            .field("peer", &self.peer)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl Drop for PolicyPermit {
    fn drop(&mut self) {
        self.engine.release(&self.peer, &self.region);
    }
}

/// Reputation + latency facts about a peer, looked up from the backend
/// before admission (see `handle_incoming_connection`).
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerFacts {
    /// `None` = never seen before (score treated as 0.0 when gating).
    pub reputation_score: Option<f64>,
    /// Recorded average RTT; `None`/`0.0` = not recorded (latency gate skipped).
    pub avg_latency_ms: Option<f64>,
    /// Seconds since the peer was last active (`last_active_at`); `None` =
    /// unknown recency (reputation treated as fresh for decay purposes).
    pub idle_secs: Option<u64>,
    /// Security violations attributed to this peer (soft-slashing input;
    /// `0` = clean / process-fresh).
    pub violations: u64,
    /// Provider-recorded successful sessions with this peer.
    pub successful_sessions: u64,
    /// Provider-recorded failed sessions with this peer.
    pub failed_sessions: u64,
}

/// Time-decay a reputation score: `score * 0.5^(idle / half_life)`.
///
/// Trust erodes while a peer is idle — a good score from months ago should
/// count for less than a good score from yesterday. `half_life_days <= 0`
/// disables decay; unknown idle time is treated as fresh (no decay).
/// Result is rounded to 4 decimals to keep gate comparisons stable.
pub fn decay_reputation(score: f64, idle_secs: Option<u64>, half_life_days: f64) -> f64 {
    if half_life_days.is_nan() || half_life_days <= 0.0 || !score.is_finite() {
        return score;
    }
    let Some(idle) = idle_secs else {
        return score;
    };
    if idle == 0 {
        return score;
    }
    let idle_days = idle as f64 / 86_400.0;
    let decayed = score * 0.5f64.powf(idle_days / half_life_days);
    (decayed * 10_000.0).round() / 10_000.0
}

impl PolicyEngine {
    pub fn new(config: PolicyConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::new(PolicyState::default()),
        })
    }

    pub fn config(&self) -> &PolicyConfig {
        &self.config
    }

    /// Decide whether `peer_id` may establish a session.
    ///
    /// On `Ok`, the returned permit holds one peer slot and one region slot
    /// until dropped. On `Err`, nothing is reserved. `region` is the peer's
    /// advertised region (empty → `"unknown"` bucket).
    pub fn admit(
        self: &Arc<Self>,
        peer_id: &str,
        facts: &PeerFacts,
        region: &str,
    ) -> Result<PolicyPermit, PolicyViolation> {
        let cfg = &self.config;

        if cfg.blocked_peers.contains(peer_id) {
            return Err(PolicyViolation::BlockedPeer);
        }

        if cfg.min_reputation > 0.0 {
            // Gate against the decayed score: idle trust erodes with a
            // configurable half-life (`REPUTATION_DECAY_HALF_LIFE_DAYS`).
            let raw = facts.reputation_score.unwrap_or(0.0);
            let score = decay_reputation(raw, facts.idle_secs, cfg.reputation_half_life_days);
            if score < cfg.min_reputation {
                return Err(PolicyViolation::LowReputation {
                    score,
                    min: cfg.min_reputation,
                });
            }
        }

        if cfg.min_accountability > 0.0 {
            // Soft-slashing gate: composite of trust-decayed reputation,
            // per-peer security violations, and recorded session outcomes
            // (see `crate::accountability`). Unknown peers score 0.0.
            let score = soft_slash_score(
                facts.reputation_score.unwrap_or(0.0),
                facts.idle_secs,
                cfg.reputation_half_life_days,
                facts.violations,
                facts.successful_sessions,
                facts.failed_sessions,
            );
            if score < cfg.min_accountability {
                return Err(PolicyViolation::LowAccountability {
                    score,
                    min: cfg.min_accountability,
                });
            }
        }

        if let (Some(max_ms), Some(latency)) = (cfg.max_latency_ms, facts.avg_latency_ms) {
            // Only enforce against a real recorded sample (tunnel bookkeeping
            // writes 0.0 placeholders; don't reject on "no data").
            if latency > 0.0 && latency > max_ms {
                return Err(PolicyViolation::HighLatency {
                    latency_ms: latency,
                    max_ms,
                });
            }
        }

        let region = if region.trim().is_empty() { "unknown" } else { region };

        let mut state = self.lock_state();

        if cfg.max_sessions_per_peer > 0 {
            let in_use = state.peer_sessions.get(peer_id).copied().unwrap_or(0);
            if in_use >= cfg.max_sessions_per_peer {
                return Err(PolicyViolation::PeerQuotaExceeded {
                    peer: peer_id.to_string(),
                    in_use,
                    max: cfg.max_sessions_per_peer,
                });
            }
        }

        if cfg.max_sessions_per_region > 0 {
            let in_use = state.region_sessions.get(region).copied().unwrap_or(0);
            if in_use >= cfg.max_sessions_per_region {
                return Err(PolicyViolation::RegionCapExceeded {
                    region: region.to_string(),
                    in_use,
                    max: cfg.max_sessions_per_region,
                });
            }
        }

        *state.peer_sessions.entry(peer_id.to_string()).or_insert(0) += 1;
        *state.region_sessions.entry(region.to_string()).or_insert(0) += 1;

        Ok(PolicyPermit {
            engine: Arc::clone(self),
            peer: peer_id.to_string(),
            region: region.to_string(),
        })
    }

    /// Currently tracked sessions (sum of region buckets).
    pub fn tracked_sessions(&self) -> u32 {
        self.lock_state().region_sessions.values().sum()
    }

    /// Sessions currently held by one peer.
    pub fn peer_sessions_in_use(&self, peer_id: &str) -> u32 {
        self.lock_state()
            .peer_sessions
            .get(peer_id)
            .copied()
            .unwrap_or(0)
    }

    /// Sessions currently held in one region bucket.
    pub fn region_sessions_in_use(&self, region: &str) -> u32 {
        self.lock_state()
            .region_sessions
            .get(if region.is_empty() { "unknown" } else { region })
            .copied()
            .unwrap_or(0)
    }

    fn release(&self, peer: &str, region: &str) {
        let mut state = self.lock_state();

        let mut peer_zero = false;
        if let Some(count) = state.peer_sessions.get_mut(peer) {
            *count = count.saturating_sub(1);
            peer_zero = *count == 0;
        }
        if peer_zero {
            state.peer_sessions.remove(peer);
        }

        let mut region_zero = false;
        if let Some(count) = state.region_sessions.get_mut(region) {
            *count = count.saturating_sub(1);
            region_zero = *count == 0;
        }
        if region_zero {
            state.region_sessions.remove(region);
        }
    }

    /// Lock the state map, recovering from poisoning (permit `Drop` must
    /// never panic during unwinding).
    fn lock_state(&self) -> MutexGuard<'_, PolicyState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn env_f64_clamped(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match raw.parse::<f64>() {
            Ok(v) if v.is_finite() && (0.0..=1.0).contains(&v) => v,
            Ok(v) => {
                warn!(key, value = %v, "policy min-score knob must be in [0,1]; using default");
                default
            }
            Err(_) => {
                warn!(key, value = %raw, "unparseable policy min-score knob; using default");
                default
            }
        },
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_config() -> PolicyConfig {
        PolicyConfig::default()
    }

    fn facts(reputation: Option<f64>, latency: Option<f64>) -> PeerFacts {
        PeerFacts {
            reputation_score: reputation,
            avg_latency_ms: latency,
            idle_secs: None,
            violations: 0,
            successful_sessions: 0,
            failed_sessions: 0,
        }
    }

    fn facts_idle(reputation: Option<f64>, idle_secs: Option<u64>) -> PeerFacts {
        PeerFacts {
            reputation_score: reputation,
            avg_latency_ms: None,
            idle_secs,
            violations: 0,
            successful_sessions: 0,
            failed_sessions: 0,
        }
    }

    #[test]
    fn open_config_allows_everything() {
        let engine = PolicyEngine::new(open_config());
        assert!(engine.config().is_open());
        let permit = engine.admit("peerA", &facts(None, None), "us-west").unwrap();
        assert_eq!(engine.tracked_sessions(), 1);
        drop(permit);
        assert_eq!(engine.tracked_sessions(), 0);
    }

    #[test]
    fn blocked_peer_is_denied() {
        let config = PolicyConfig {
            blocked_peers: ["bad-actor".to_string()].into_iter().collect(),
            ..open_config()
        };
        let engine = PolicyEngine::new(config);
        let err = engine.admit("bad-actor", &facts(Some(1.0), None), "us-west").unwrap_err();
        assert_eq!(err, PolicyViolation::BlockedPeer);
        // Other peers unaffected.
        assert!(engine.admit("good", &facts(Some(1.0), None), "us-west").is_ok());
    }

    #[test]
    fn min_reputation_gates_unknown_and_low_peers() {
        let config = PolicyConfig {
            min_reputation: 0.5,
            ..open_config()
        };
        let engine = PolicyEngine::new(config);

        // Unknown peer → score 0.0 → denied.
        let err = engine.admit("newcomer", &facts(None, None), "eu").unwrap_err();
        assert!(matches!(err, PolicyViolation::LowReputation { score, min } if score == 0.0 && min == 0.5));

        // Below threshold → denied.
        let err = engine
            .admit("meh", &facts(Some(0.4), None), "eu")
            .unwrap_err();
        assert!(matches!(err, PolicyViolation::LowReputation { .. }));

        // Exactly at threshold → allowed (>= semantics).
        assert!(engine.admit("ok", &facts(Some(0.5), None), "eu").is_ok());
    }

    #[test]
    fn max_latency_gates_recorded_samples_only() {
        let config = PolicyConfig {
            max_latency_ms: Some(100.0),
            ..open_config()
        };
        let engine = PolicyEngine::new(config);

        // Unrecorded / zero placeholder → gate skipped.
        assert!(engine.admit("a", &facts(None, None), "eu").is_ok());
        assert!(engine.admit("b", &facts(None, Some(0.0)), "eu").is_ok());

        // Over the ceiling → denied.
        let err = engine
            .admit("slow", &facts(None, Some(150.0)), "eu")
            .unwrap_err();
        assert!(matches!(err, PolicyViolation::HighLatency { latency_ms, max_ms }
            if latency_ms == 150.0 && max_ms == 100.0));

        // Under the ceiling → allowed.
        assert!(engine.admit("fast", &facts(None, Some(40.0)), "eu").is_ok());
    }

    #[test]
    fn peer_quota_enforced_and_released_on_drop() {
        let config = PolicyConfig {
            max_sessions_per_peer: 2,
            ..open_config()
        };
        let engine = PolicyEngine::new(config);

        let p1 = engine.admit("peerA", &facts(None, None), "eu").unwrap();
        let p2 = engine.admit("peerA", &facts(None, None), "eu").unwrap();
        assert_eq!(engine.peer_sessions_in_use("peerA"), 2);

        let err = engine.admit("peerA", &facts(None, None), "eu").unwrap_err();
        assert!(matches!(err, PolicyViolation::PeerQuotaExceeded { in_use: 2, max: 2, .. }));

        // Other peers have their own quota.
        assert!(engine.admit("peerB", &facts(None, None), "eu").is_ok());

        // Dropping a permit frees the slot (RAII).
        drop(p1);
        assert_eq!(engine.peer_sessions_in_use("peerA"), 1);
        assert!(engine.admit("peerA", &facts(None, None), "eu").is_ok());
        drop(p2);
    }

    #[test]
    fn region_cap_enforced_across_peers() {
        let config = PolicyConfig {
            max_sessions_per_region: 2,
            ..open_config()
        };
        let engine = PolicyEngine::new(config);

        let a = engine.admit("peerA", &facts(None, None), "us-west").unwrap();
        let b = engine.admit("peerB", &facts(None, None), "us-west").unwrap();

        // Same region, third session → denied (different peer, same bucket).
        let err = engine
            .admit("peerC", &facts(None, None), "us-west")
            .unwrap_err();
        assert!(matches!(err, PolicyViolation::RegionCapExceeded { in_use: 2, max: 2, .. }));

        // Different region unaffected.
        assert!(engine.admit("peerD", &facts(None, None), "eu-central").is_ok());

        drop(a);
        assert_eq!(engine.region_sessions_in_use("us-west"), 1);
        assert!(engine.admit("peerC", &facts(None, None), "us-west").is_ok());
        drop(b);
    }

    #[test]
    fn empty_region_buckets_as_unknown() {
        let config = PolicyConfig {
            max_sessions_per_region: 1,
            ..open_config()
        };
        let engine = PolicyEngine::new(config);
        let _held = engine.admit("peerA", &facts(None, None), "").unwrap();
        assert_eq!(engine.region_sessions_in_use("unknown"), 1);
        // Empty string and "unknown" share the bucket.
        let err = engine.admit("peerB", &facts(None, None), "unknown").unwrap_err();
        assert!(matches!(err, PolicyViolation::RegionCapExceeded { .. }));
    }

    #[test]
    fn permit_drop_after_move_still_releases() {
        let engine = PolicyEngine::new(PolicyConfig {
            max_sessions_per_peer: 1,
            ..open_config()
        });
        let permit = engine.admit("peerA", &facts(None, None), "eu").unwrap();
        {
            let moved = permit; // moved into inner scope
            assert_eq!(engine.peer_sessions_in_use("peerA"), 1);
            drop(moved);
        }
        assert_eq!(engine.peer_sessions_in_use("peerA"), 0);
    }

    #[test]
    fn violation_display_is_human_readable() {
        assert_eq!(
            PolicyViolation::BlockedPeer.to_string(),
            "peer is blacklisted"
        );
        assert!(PolicyViolation::LowReputation { score: 0.1, min: 0.5 }
            .to_string()
            .contains("0.10"));
    }

    // ── Trust decay ────────────────────────────────────────────────

    #[test]
    fn decay_halves_score_after_one_half_life() {
        assert_eq!(decay_reputation(0.8, Some(30 * 86_400), 30.0), 0.4);
        assert_eq!(decay_reputation(1.0, Some(60 * 86_400), 30.0), 0.25);
    }

    #[test]
    fn decay_disabled_or_fresh_returns_raw_score() {
        assert_eq!(decay_reputation(0.9, Some(365 * 86_400), 0.0), 0.9, "half-life 0 = off");
        assert_eq!(decay_reputation(0.9, Some(0), 30.0), 0.9, "no idle = no decay");
        assert_eq!(decay_reputation(0.9, None, 30.0), 0.9, "unknown idle = treated fresh");
        assert_eq!(decay_reputation(0.9, Some(10_000_000), f64::NAN), 0.9, "NaN half-life = off");
    }

    #[test]
    fn decay_is_monotonic_and_clamped_non_negative() {
        let d1 = decay_reputation(1.0, Some(86_400), 30.0);
        let d2 = decay_reputation(1.0, Some(90 * 86_400), 30.0);
        let d3 = decay_reputation(1.0, Some(10 * 365 * 86_400), 30.0);
        assert!(d1 > d2 && d2 > d3, "decay must be monotonic: {d1} {d2} {d3}");
        assert!(d3 >= 0.0);
        assert!(decay_reputation(0.0, Some(86_400), 30.0) == 0.0);
    }

    #[test]
    fn admission_gates_on_decayed_score_not_raw() {
        // Peer scored 0.9 last month — with a 7-day half-life the gate at
        // 0.5 must reject it (0.9 * 0.5^(30/7) ≈ 0.17), even though the
        // raw stored score clears 0.5.
        let engine = PolicyEngine::new(PolicyConfig {
            min_reputation: 0.5,
            reputation_half_life_days: 7.0,
            ..open_config()
        });
        let facts = facts_idle(Some(0.9), Some(30 * 86_400));
        let err = engine
            .admit("idle-peer", &facts, "eu")
            .expect_err("decayed score must gate");
        match err {
            PolicyViolation::LowReputation { score, min } => {
                assert!(score < 0.5, "score {score} should be decayed below {min}");
                assert_eq!(min, 0.5);
            }
            other => panic!("unexpected violation: {other:?}"),
        }

        // Same peer active yesterday: 0.9 * 0.5^(1/7) ≈ 0.83 > 0.5 → allowed.
        let fresh = facts_idle(Some(0.9), Some(86_400));
        let permit = engine
            .admit("idle-peer", &fresh, "eu")
            .expect("recent good reputation passes");
        drop(permit);
    }

    #[test]
    fn from_env_parses_policy_and_decay_knobs() {
        // Consolidated env test (tests run in parallel threads).
        std::env::set_var("POLICY_MIN_REPUTATION", "0.25");
        std::env::set_var("POLICY_MIN_ACCOUNTABILITY", "0.6");
        std::env::set_var("POLICY_MAX_SESSIONS_PER_PEER", "3");
        std::env::set_var("POLICY_BLOCKED_PEERS", "peerX, peerY ,");
        std::env::set_var("REPUTATION_DECAY_HALF_LIFE_DAYS", "14");
        let cfg = PolicyConfig::from_env();
        assert_eq!(cfg.min_reputation, 0.25);
        assert_eq!(cfg.min_accountability, 0.6);
        assert_eq!(cfg.max_sessions_per_peer, 3);
        assert!(cfg.blocked_peers.contains("peerX"));
        assert!(cfg.blocked_peers.contains("peerY"));
        assert!(!cfg.blocked_peers.contains(""));
        assert_eq!(cfg.reputation_half_life_days, 14.0);
        assert!(!cfg.is_open(), "configured gates ⇒ engine active");
        std::env::remove_var("POLICY_MIN_REPUTATION");
        std::env::remove_var("POLICY_MIN_ACCOUNTABILITY");
        std::env::remove_var("POLICY_MAX_SESSIONS_PER_PEER");
        std::env::remove_var("POLICY_BLOCKED_PEERS");
        std::env::remove_var("REPUTATION_DECAY_HALF_LIFE_DAYS");
    }

    /// Soft-slashing gate: violations and failed history push the composite
    /// score below `min_accountability`; a clean peer with history passes.
    #[test]
    fn min_accountability_gates_on_composite_score() {
        let engine = PolicyEngine::new(PolicyConfig {
            min_accountability: 0.6,
            ..open_config()
        });

        // Clean + full reputation + no history ⇒ 1.0 ⇒ allowed.
        let clean = PeerFacts {
            reputation_score: Some(1.0),
            ..facts(None, None)
        };
        assert!(engine.admit("good", &clean, "eu").is_ok());

        // 4 attributed violations ⇒ 1.0 × 0.5 × 1.0 = 0.5 < 0.6 ⇒ denied.
        let viol = PeerFacts {
            reputation_score: Some(1.0),
            violations: 4,
            ..facts(None, None)
        };
        let err = engine
            .admit("abuser", &viol, "eu")
            .expect_err("violations must discount below the gate");
        match err {
            PolicyViolation::LowAccountability { score, min } => {
                assert!(score < min && min == 0.6, "score {score} vs min {min}");
            }
            other => panic!("unexpected violation: {other:?}"),
        }

        // 100 % failed history ⇒ availability factor 0.5 ⇒ denied.
        let failed = PeerFacts {
            reputation_score: Some(1.0),
            successful_sessions: 0,
            failed_sessions: 5,
            ..facts(None, None)
        };
        assert_eq!(
            engine.admit("flaky", &failed, "eu").unwrap_err(),
            PolicyViolation::LowAccountability { score: 0.5, min: 0.6 }
        );

        // Same peer with mixed history (5/8 success ⇒ factor ≈ 0.8125) passes.
        let mixed = PeerFacts {
            reputation_score: Some(1.0),
            successful_sessions: 5,
            failed_sessions: 3,
            ..facts(None, None)
        };
        assert!(engine.admit("mixed", &mixed, "eu").is_ok());

        // Unknown peer (no reputation row) scores 0 ⇒ denied.
        assert_eq!(
            engine.admit("stranger", &facts(None, None), "eu").unwrap_err(),
            PolicyViolation::LowAccountability { score: 0.0, min: 0.6 }
        );
    }
}
