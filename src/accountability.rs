//! Soft-slashing: economic accountability without on-chain stake.
//!
//! Real provider staking/slashing needs a market and a settlement layer that
//! don't exist yet. Until then the four signals the daemon *already
//! records* — reputation, security violations, availability history, and
//! idle-time trust decay — combine into one composite score that plays the
//! same role economically: **a peer that misbehaves loses admission rights,
//! and a peer that stays healthy keeps them.**
//!
//! [`soft_slash_score`] folds the inputs into a single `0.0..=1.0` score:
//!
//! ```text
//! score = decayed_reputation  ×  violation_discount  ×  availability_factor
//! ```
//!
//! - **reputation × trust decay** — the stored `peer_reputation` score,
//!   decayed by [`crate::policy::decay_reputation`] so stale trust erodes
//!   (`REPUTATION_DECAY_HALF_LIFE_DAYS`).
//! - **security violations** — per-peer denials/rejections attributed by
//!   [`crate::security::SecurityCounters::note_peer_violation`]. Each
//!   violation applies a multiplicative discount `1 / (1 + 0.25·v)` —
//!   halved by ~3 violations, never reaching zero (soft, not permanent).
//!   Only *post-authentication* events are attributed: pre-handshake peer
//!   ids are attacker-claimable, so counting them would let an attacker
//!   slander an innocent peer by using its id.
//! - **availability history** — the provider's recorded session outcomes
//!   with that peer (`successful_sessions / (successful + failed)`), mapped
//!   to a factor in `[0.5, 1.0]`. No history = neutral `1.0`.
//!
//! The score gates admission via `POLICY_MIN_ACCOUNTABILITY` (in
//! [`crate::policy`]) and is admin-visible through `GET /security`. No
//! tokens are locked, nothing is destroyed — "slashing" here is denial of
//! future earning capacity, which is exactly what a reputation system does.
//! Revisit real staking only when a market makes stake worth slashing.

use crate::policy::decay_reputation;

/// Multiplicative penalty weight per attributed security violation
/// (`discount = 1 / (1 + 0.25·v)`): 1 violation ≈ −20 %, 3 ≈ −50 %.
pub const VIOLATION_PENALTY: f64 = 0.25;

/// Availability factor floor when a peer's recorded history is *all*
/// failures. The factor never zeroes the score outright — an all-failed
/// peer is already heavily penalized by reputation (`failed_sessions`
/// feeds the score that `end_session` writes back).
const MIN_AVAILABILITY_FACTOR: f64 = 0.5;

/// Compute the soft-slashing score for a peer.
///
/// | argument | source | effect |
/// |----------|--------|--------|
/// | `reputation` | `peer_reputation.reputation_score` (raw, `0..=1`) | base of the score |
/// | `idle_secs` | seconds since `last_active_at` | feeds trust decay |
/// | `half_life_days` | `REPUTATION_DECAY_HALF_LIFE_DAYS` | decay half-life (`0` = off) |
/// | `violations` | per-peer security violation count | multiplicative discount |
/// | `successful_sessions` / `failed_sessions` | provider's history with the peer | availability factor in `[0.5, 1.0]` |
///
/// Result is rounded to 4 decimals (stable gate comparisons) and clamped
/// to `0.0..=1.0`. Non-finite reputation is treated as `0.0`.
pub fn soft_slash_score(
    reputation: f64,
    idle_secs: Option<u64>,
    half_life_days: f64,
    violations: u64,
    successful_sessions: u64,
    failed_sessions: u64,
) -> f64 {
    let raw = if reputation.is_finite() {
        reputation.clamp(0.0, 1.0)
    } else {
        0.0
    };

    // Trust decay: idle good standing counts for less over time.
    let decayed = decay_reputation(raw, idle_secs, half_life_days);

    // Security violations: multiplicative discount, never quite zero.
    let violation_discount = 1.0 / (1.0 + VIOLATION_PENALTY * violations as f64);

    // Availability history: 100 % success → 1.0, no history → 1.0 (neutral),
    // 100 % failures → the 0.5 floor.
    let total = successful_sessions + failed_sessions;
    let availability = if total == 0 {
        1.0
    } else {
        successful_sessions as f64 / total as f64
    };
    let availability_factor = MIN_AVAILABILITY_FACTOR + (1.0 - MIN_AVAILABILITY_FACTOR) * availability;

    let score = (decayed * violation_discount * availability_factor).clamp(0.0, 1.0);
    (score * 10_000.0).round() / 10_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Perfect peer: full fresh reputation, no violations, no history.
    #[test]
    fn perfect_peer_scores_one() {
        assert_eq!(soft_slash_score(1.0, None, 30.0, 0, 0, 0), 1.0);
    }

    /// Violations discount monotonically: 1 → 0.8, 2 → 0.6667, 4 → 0.5.
    #[test]
    fn violations_discount_monotonically() {
        let s1 = soft_slash_score(1.0, None, 30.0, 1, 0, 0);
        let s2 = soft_slash_score(1.0, None, 30.0, 2, 0, 0);
        let s4 = soft_slash_score(1.0, None, 30.0, 4, 0, 0);
        assert!((s1 - 0.8).abs() < 1e-4, "1 violation ⇒ ~0.8, got {s1}");
        assert!((s2 - 0.6667).abs() < 1e-3, "2 violations ⇒ ~0.6667, got {s2}");
        assert!((s4 - 0.5).abs() < 1e-4, "4 violations ⇒ ~0.5, got {s4}");
        assert!(s4 < s2 && s2 < s1, "score must fall as violations rise");
    }

    /// Availability history: all-success neutral, all-failure floors at 0.5,
    /// missing history is neutral (not penalized).
    #[test]
    fn availability_history_factor() {
        let all_good = soft_slash_score(1.0, None, 30.0, 0, 10, 0);
        let all_bad = soft_slash_score(1.0, None, 30.0, 0, 0, 10);
        let no_history = soft_slash_score(1.0, None, 30.0, 0, 0, 0);
        assert_eq!(all_good, 1.0);
        assert!((all_bad - 0.5).abs() < 1e-4, "all-failure ⇒ 0.5, got {all_bad}");
        assert_eq!(no_history, 1.0, "no history must stay neutral");
        // Mixed: 1/4 success → factor 0.5 + 0.5·0.25 = 0.625.
        let mixed = soft_slash_score(1.0, None, 30.0, 0, 1, 3);
        assert!((mixed - 0.625).abs() < 1e-4, "1/4 success ⇒ 0.625, got {mixed}");
    }

    /// Trust decay composes with the other factors.
    #[test]
    fn trust_decay_composes() {
        // Fresh: 1.0; idle 30 days @ 30-day half-life: 0.5.
        let fresh = soft_slash_score(1.0, Some(0), 30.0, 0, 0, 0);
        let idle = soft_slash_score(1.0, Some(30 * 86_400), 30.0, 0, 0, 0);
        assert_eq!(fresh, 1.0);
        assert!((idle - 0.5).abs() < 1e-4, "one half-life ⇒ 0.5, got {idle}");
        // Combined with a violation: 0.5 × 0.8 = 0.4.
        let combo = soft_slash_score(1.0, Some(30 * 86_400), 30.0, 1, 0, 0);
        assert!((combo - 0.4).abs() < 1e-4, "decay × violation ⇒ 0.4, got {combo}");
    }

    /// The composite is always within `0..=1` and well-formed inputs never
    /// panic; garbage reputation collapses to 0 rather than propagating NaN.
    #[test]
    fn score_is_bounded_and_nan_safe() {
        for (rep, v, ok, bad) in [
            (f64::NAN, 0, 0, 0),
            (f64::INFINITY, 0, 0, 0),
            (-5.0, 100, 0, 1_000),
            (2.0, 0, u64::MAX / 2, u64::MAX / 2),
            (0.7, 7, 3, 4),
        ] {
            let s = soft_slash_score(rep, Some(u64::MAX), 30.0, v, ok, bad);
            assert!(s.is_finite() && (0.0..=1.0).contains(&s), "score {s} out of range");
        }
        assert_eq!(soft_slash_score(f64::NAN, None, 30.0, 0, 0, 0), 0.0);
    }

    /// `half_life_days <= 0` disables decay (mirrors `decay_reputation`).
    #[test]
    fn zero_half_life_disables_decay() {
        let s = soft_slash_score(1.0, Some(365 * 86_400), 0.0, 0, 0, 0);
        assert_eq!(s, 1.0, "decay off ⇒ full score regardless of idle time");
    }
}
