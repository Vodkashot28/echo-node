use crate::models::{CapabilityDescriptor, SystemStats};
use std::collections::VecDeque;
use sysinfo::{Components, CpuRefreshKind, Disks, Networks, RefreshKind, System};
use tracing::info;

/// Rolling window size for latency samples used in average computation.
const LATENCY_WINDOW: usize = 60;

/// Default smoothing factor for the availability estimator: 0.3 keeps the
/// advertised capacity responsive without letting one noisy sysinfo sample
/// bounce the DHT record. `1.0` disables smoothing (raw passthrough).
pub const DEFAULT_EWMA_ALPHA: f64 = 0.3;

/// The Availability Engine computes how much bandwidth this node can
/// safely advertise to the network based on current system load.
///
/// When system resources are under pressure, the node reduces its
/// reported throughput capacity to avoid saturation.
pub struct AvailabilityEngine {
    system: System,
    networks: Networks,
    components: Components,
    disks: Disks,
    /// Physical link capacity (configured max)
    max_upload_mbps: f64,
    max_download_mbps: f64,
    /// Maximum concurrent sessions this node supports
    max_sessions: u32,
    /// Current active sessions
    active_sessions: u32,
    /// Current bandwidth in use by local processes (bytes/sec)
    current_usage_rx_bps: f64,
    current_usage_tx_bps: f64,
    /// Cumulative network byte counters (total since start)
    total_network_rx: u64,
    total_network_tx: u64,
    /// Network sample buffer for rate calculation (O(1) front removal)
    rx_samples: VecDeque<(u64, std::time::Instant)>,
    tx_samples: VecDeque<(u64, std::time::Instant)>,
    /// Latency samples for computing avg_latency_ms in capability descriptors.
    latency_samples: VecDeque<f64>,
    /// EWMA smoothing factor for advertised availability (0 < alpha <= 1).
    ewma_alpha: f64,
    /// Smoothed upload/download availability estimates (None until first sample).
    ewma_upload: Option<f64>,
    ewma_download: Option<f64>,
}

/// One exponentially-weighted step of the availability estimator.
///
/// - first sample (`prev == None`) → raw value (bootstraps the estimate)
/// - raw `<= 0` → snap to `0.0` (a saturated node must advertise zero, not
///   an asymptotically-decaying leftover)
/// - otherwise `prev + alpha * (raw - prev)`, rounded to 2 decimals
fn ewma_step(prev: Option<f64>, raw: f64, alpha: f64) -> f64 {
    let est = match prev {
        None => raw,
        Some(_) if raw <= 0.0 => 0.0,
        Some(p) => p + alpha.clamp(0.0, 1.0) * (raw - p),
    };
    (est * 100.0).round() / 100.0
}

impl AvailabilityEngine {
    pub fn new(
        max_upload_mbps: f64,
        max_download_mbps: f64,
        max_sessions: u32,
    ) -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(sysinfo::MemoryRefreshKind::everything()),
        );
        system.refresh_all();

        let networks = Networks::new_with_refreshed_list();

        Self {
            system,
            networks,
            components: Components::new(),
            disks: Disks::new(),
            max_upload_mbps,
            max_download_mbps,
            max_sessions,
            active_sessions: 0,
            current_usage_rx_bps: 0.0,
            current_usage_tx_bps: 0.0,
            total_network_rx: 0,
            total_network_tx: 0,
            rx_samples: VecDeque::with_capacity(11),
            tx_samples: VecDeque::with_capacity(11),
            latency_samples: VecDeque::with_capacity(LATENCY_WINDOW + 1),
            ewma_alpha: DEFAULT_EWMA_ALPHA,
            ewma_upload: None,
            ewma_download: None,
        }
    }

    /// Override the availability EWMA smoothing factor (`AVAIL_EWMA_ALPHA`).
    /// Values outside `(0, 1]` are clamped: `1.0` = no smoothing.
    pub fn with_ewma_alpha(mut self, alpha: f64) -> Self {
        if !alpha.is_finite() || alpha <= 0.0 || alpha > 1.0 {
            tracing::warn!(
                value = %alpha,
                "AVAIL_EWMA_ALPHA must be in (0, 1]; using default {DEFAULT_EWMA_ALPHA}"
            );
            self.ewma_alpha = DEFAULT_EWMA_ALPHA;
        } else {
            self.ewma_alpha = alpha;
        }
        self
    }

    /// Current smoothing factor (observability/tests).
    pub fn ewma_alpha(&self) -> f64 {
        self.ewma_alpha
    }

    /// Current smoothed availability estimates (`upload`, `download`);
    /// `None` until the first compute.
    pub fn ewma_estimates(&self) -> (Option<f64>, Option<f64>) {
        (self.ewma_upload, self.ewma_download)
    }

    /// Refresh system metrics and recalculate local bandwidth usage.
    pub fn refresh(&mut self) {
        self.system.refresh_all();
        self.networks.refresh();
        self.components.refresh();
        self.disks.refresh();

        // Track cumulative network counters for rate calculation
        let mut total_rx: u64 = 0;
        let mut total_tx: u64 = 0;
        for data in self.networks.values() {
            total_rx += data.total_received();
            total_tx += data.total_transmitted();
        }

        let now = std::time::Instant::now();
        self.total_network_rx = total_rx;
        self.total_network_tx = total_tx;
        self.rx_samples.push_back((total_rx, now));
        self.tx_samples.push_back((total_tx, now));

        // Keep only last 10 samples (VecDeque::pop_front is O(1))
        if self.rx_samples.len() > 10 {
            self.rx_samples.pop_front();
        }
        if self.tx_samples.len() > 10 {
            self.tx_samples.pop_front();
        }

        // Compute current usage rate
        if self.rx_samples.len() >= 2 {
            let first = &self.rx_samples[0];
            let last = self.rx_samples.back().unwrap();
            let dt = last.1.duration_since(first.1).as_secs_f64();
            if dt > 0.0 {
                self.current_usage_rx_bps =
                    ((last.0.saturating_sub(first.0)) as f64 / dt) * 8.0;
                self.current_usage_tx_bps = {
                    let t_first = &self.tx_samples[0];
                    let t_last = self.tx_samples.back().unwrap();
                    ((t_last.0.saturating_sub(t_first.0)) as f64 / dt) * 8.0
                };
            }
        }
    }

    /// Build a CapabilityDescriptor for DHT publishing.
    pub fn build_capability_descriptor(
        &mut self,
        peer_id: &str,
        public_key: Vec<u8>,
        region: &str,
        reputation_score: f64,
        noise_public_key: Vec<u8>,
    ) -> CapabilityDescriptor {
        let (_, upload_cap, download_cap) = self.refresh_and_compute();
        let avg_latency = self.compute_avg_latency();

        CapabilityDescriptor {
            peer_id: peer_id.to_string(),
            public_key,
            region: region.to_string(),
            upload_cap_mbps: upload_cap,
            download_cap_mbps: download_cap,
            avg_latency_ms: avg_latency,
            reputation_score,
            supported_encryption: vec!["noise-xx".to_string()],
            max_sessions: self.max_sessions,
            active_sessions: self.active_sessions,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            noise_public_key,
        }
    }

    /// Try to acquire a session slot. Returns Err if at capacity.
    pub fn try_acquire_session(&mut self) -> Result<(), &'static str> {
        if self.active_sessions >= self.max_sessions {
            return Err("max session limit reached");
        }
        self.active_sessions += 1;
        info!(active = self.active_sessions, max = self.max_sessions, "session acquired");
        Ok(())
    }

    /// Release a session slot, freeing capacity for new connections.
    pub fn release_session(&mut self) {
        if self.active_sessions > 0 {
            self.active_sessions -= 1;
            info!(active = self.active_sessions, max = self.max_sessions, "session released");
        }
    }

    /// Current number of active sessions (for observation/testing).
    pub fn active_sessions(&self) -> u32 {
        self.active_sessions
    }

    /// Push a new latency sample into the rolling window.
    pub fn record_latency(&mut self, latency_ms: f64) {
        self.latency_samples.push_back(latency_ms);
        if self.latency_samples.len() > LATENCY_WINDOW {
            self.latency_samples.pop_front();
        }
    }

    /// Compute average latency from the rolling window of ping samples.
    /// Returns 0.0 if no samples are available.
    fn compute_avg_latency(&self) -> f64 {
        if self.latency_samples.is_empty() {
            0.0
        } else {
            let sum: f64 = self.latency_samples.iter().sum();
            (sum / self.latency_samples.len() as f64 * 100.0).round() / 100.0
        }
    }

    /// Read CPU temperature from sysinfo components (best-effort).
    /// Returns `None` if no temperature sensor is available.
    pub fn cpu_temp_c(&self) -> Option<f64> {
        self.components
            .iter()
            .find(|c| {
                let label = c.label().to_lowercase();
                label.contains("cpu") || label.contains("core") || label.contains("processor")
            })
            .map(|c| c.temperature() as f64)
    }

    /// Combined refresh: refresh sysinfo once, then compute both
    /// system stats and available capacity in a single pass.
    /// Returns (SystemStats, upload_mbps, download_mbps).
    pub fn refresh_and_compute(&mut self) -> (SystemStats, f64, f64) {
        self.refresh();

        // Build stats from the fresh data
        let cpu_pct = self.system.global_cpu_info().cpu_usage() as f64;
        let total_mem = self.system.total_memory();
        let used_mem = self.system.used_memory();
        let mem_pct = if total_mem > 0 {
            (used_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        let total_disk: u64 = self.disks.iter().map(|d| d.total_space()).sum();
        let available_disk: u64 = self.disks.iter().map(|d| d.available_space()).sum();
        let disk_pct = if total_disk > 0 {
            ((total_disk - available_disk) as f64 / total_disk as f64) * 100.0
        } else {
            0.0
        };

        let stats = SystemStats {
            cpu_pct,
            memory_pct: mem_pct,
            disk_usage_pct: disk_pct,
            active_sessions: self.active_sessions,
            current_usage_rx_mbps: self.current_usage_rx_bps / 1_000_000.0,
            current_usage_tx_mbps: self.current_usage_tx_bps / 1_000_000.0,
            memory_used_bytes: used_mem,
            memory_total_bytes: total_mem,
            network_rx_bytes: self.total_network_rx,
            network_tx_bytes: self.total_network_tx,
        };

        // Compute available capacity from the same fresh data
        let mut avail_up = self.max_upload_mbps;
        let mut avail_down = self.max_download_mbps;

        let usage_up_mbps = self.current_usage_tx_bps / 1_000_000.0;
        let usage_down_mbps = self.current_usage_rx_bps / 1_000_000.0;
        avail_up = (avail_up - usage_up_mbps).max(0.0);
        avail_down = (avail_down - usage_down_mbps).max(0.0);

        let cpu_factor = (1.0 - cpu_pct / 100.0).max(0.1);
        avail_up *= cpu_factor;
        avail_down *= cpu_factor;

        let mem_factor = (1.0 - mem_pct / 100.0).max(0.1);
        avail_up *= mem_factor;
        avail_down *= mem_factor;

        let session_factor = if self.active_sessions >= self.max_sessions {
            0.0
        } else {
            1.0 - (self.active_sessions as f64 / self.max_sessions as f64 * 0.3)
        };
        avail_up *= session_factor;
        avail_down *= session_factor;

        let avail_up = (avail_up * 100.0).round() / 100.0;
        let avail_down = (avail_down * 100.0).round() / 100.0;

        // Availability estimator: smooth the advertised capacity so a single
        // noisy CPU/memory sample doesn't bounce the DHT record. Saturated
        // (0.0) results snap to zero inside `ewma_step`.
        let est_up = ewma_step(self.ewma_upload, avail_up, self.ewma_alpha);
        let est_down = ewma_step(self.ewma_download, avail_down, self.ewma_alpha);
        self.ewma_upload = Some(est_up);
        self.ewma_download = Some(est_down);

        (stats, est_up, est_down)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enormous physical capacity so machine-local usage can never zero it
    /// out — lets tests assert capacity sign/magnitude deterministically.
    const HUGE_MBPS: f64 = 1.0e9;

    #[test]
    fn session_slot_acquire_up_to_max_then_reject() {
        let mut engine = AvailabilityEngine::new(100.0, 100.0, 3);
        assert_eq!(engine.active_sessions(), 0);

        engine.try_acquire_session().unwrap();
        engine.try_acquire_session().unwrap();
        engine.try_acquire_session().unwrap();
        assert_eq!(engine.active_sessions(), 3);

        let err = engine
            .try_acquire_session()
            .expect_err("4th session must be rejected at max_sessions=3");
        assert!(err.contains("max session limit"));
        assert_eq!(engine.active_sessions(), 3, "failed acquire must not leak a slot");
    }

    #[test]
    fn session_slot_release_is_idempotent_and_never_underflows() {
        let mut engine = AvailabilityEngine::new(100.0, 100.0, 2);
        // Releasing with zero active sessions must not wrap around.
        engine.release_session();
        assert_eq!(engine.active_sessions(), 0);

        engine.try_acquire_session().unwrap();
        engine.release_session();
        engine.release_session(); // double-release (bug guard)
        assert_eq!(engine.active_sessions(), 0);
    }

    #[test]
    fn avg_latency_uses_empty_until_samples_arrive() {
        let engine = AvailabilityEngine::new(100.0, 100.0, 1);
        assert_eq!(engine.compute_avg_latency(), 0.0);
    }

    #[test]
    fn avg_latency_averages_and_rounds_samples() {
        let mut engine = AvailabilityEngine::new(100.0, 100.0, 1);
        engine.record_latency(10.0);
        engine.record_latency(20.5);
        engine.record_latency(30.0);
        // (10 + 20.5 + 30) / 3 = 20.1666... → 20.17
        assert_eq!(engine.compute_avg_latency(), 20.17);
    }

    #[test]
    fn avg_latency_window_keeps_only_last_60_samples() {
        let mut engine = AvailabilityEngine::new(100.0, 100.0, 1);
        for _ in 0..LATENCY_WINDOW {
            engine.record_latency(10.0);
        }
        // Window is now full of 10.0 samples; 100 more of 30.0 push them out.
        for _ in 0..100 {
            engine.record_latency(30.0);
        }
        assert_eq!(engine.compute_avg_latency(), 30.0);
    }

    #[test]
    fn capacity_positive_when_sessions_available() {
        let mut engine = AvailabilityEngine::new(HUGE_MBPS, HUGE_MBPS, 10);
        let (_, up, down) = engine.refresh_and_compute();
        assert!(up > 0.0, "expected positive upload capacity, got {up}");
        assert!(down > 0.0, "expected positive download capacity, got {down}");
    }

    #[test]
    fn capacity_zero_when_session_slots_saturated() {
        let mut engine = AvailabilityEngine::new(HUGE_MBPS, HUGE_MBPS, 2);
        engine.try_acquire_session().unwrap();
        engine.try_acquire_session().unwrap();

        let (stats, up, down) = engine.refresh_and_compute();
        assert_eq!(up, 0.0, "saturated sessions must advertise zero upload");
        assert_eq!(down, 0.0, "saturated sessions must advertise zero download");
        assert_eq!(stats.active_sessions, 2);
    }

    #[test]
    fn capability_descriptor_reports_session_state() {
        let mut engine = AvailabilityEngine::new(HUGE_MBPS, HUGE_MBPS, 2);
        engine.record_latency(42.0);
        engine.try_acquire_session().unwrap();

        let descriptor = engine.build_capability_descriptor(
            "peer_test",
            vec![1, 2, 3],
            "us-west",
            0.9,
            vec![4, 5, 6],
        );
        assert_eq!(descriptor.peer_id, "peer_test");
        assert_eq!(descriptor.region, "us-west");
        assert_eq!(descriptor.max_sessions, 2);
        assert_eq!(descriptor.active_sessions, 1);
        assert_eq!(descriptor.avg_latency_ms, 42.0);
        assert!(descriptor.upload_cap_mbps > 0.0);
        assert_eq!(descriptor.supported_encryption, vec!["noise-xx"]);
    }

    #[test]
    fn zero_max_sessions_advertises_zero_without_dividing_by_zero() {
        // max_sessions = 0 → session_factor guard must short-circuit to 0.0
        // instead of dividing by zero.
        let mut engine = AvailabilityEngine::new(HUGE_MBPS, HUGE_MBPS, 0);
        let (_, up, down) = engine.refresh_and_compute();
        assert_eq!(up, 0.0);
        assert_eq!(down, 0.0);
        assert!(engine.try_acquire_session().is_err());
    }

    // ── Availability estimator (EWMA) ──────────────────────────────

    #[test]
    fn ewma_first_sample_is_raw() {
        assert_eq!(ewma_step(None, 42.37, 0.3), 42.37);
        assert_eq!(ewma_step(None, 0.0, 0.3), 0.0);
        assert_eq!(ewma_step(None, 100.0, 1.0), 100.0);
    }

    #[test]
    fn ewma_blends_and_rounds() {
        // 10 + 0.5 * (20 - 10) = 15
        assert_eq!(ewma_step(Some(10.0), 20.0, 0.5), 15.0);
        // rounding to 2 decimals: 10.005 → 10.01 (float-safe values only)
        assert_eq!(ewma_step(Some(10.0), 10.004, 1.0), 10.0);
        // alpha 1.0 = raw passthrough
        assert_eq!(ewma_step(Some(10.0), 7.5, 1.0), 7.5);
        // alpha 0.0 = hold previous (clamped range allows 0 inside helper)
        assert_eq!(ewma_step(Some(10.0), 7.5, 0.0), 10.0);
        // alpha out of range is clamped
        assert_eq!(ewma_step(Some(10.0), 20.0, 99.0), 20.0);
        assert_eq!(ewma_step(Some(10.0), 20.0, -1.0), 10.0);
    }

    #[test]
    fn ewma_snaps_to_zero_when_raw_saturated() {
        // A saturated node must advertise exactly 0, not a decaying residue.
        assert_eq!(ewma_step(Some(35.7), 0.0, 0.3), 0.0);
        assert_eq!(ewma_step(Some(35.7), -1.0, 0.3), 0.0);
        // …and recovers from zero toward the next positive raw sample.
        assert_eq!(ewma_step(Some(0.0), 10.0, 0.3), 3.0);
    }

    #[test]
    fn ewma_converges_toward_raw_over_repeated_samples() {
        let mut est = None;
        for _ in 0..64 {
            est = Some(ewma_step(est, 100.0, 0.3));
        }
        assert!((est.unwrap() - 100.0).abs() < 0.5, "should converge, got {est:?}");
    }

    #[test]
    fn engine_estimator_bootstraps_on_first_compute() {
        let mut engine = AvailabilityEngine::new(HUGE_MBPS, HUGE_MBPS, 10);
        assert_eq!(engine.ewma_estimates(), (None, None));
        let (_, up, _down) = engine.refresh_and_compute();
        let (eu, ed) = engine.ewma_estimates();
        assert_eq!(eu, Some(up), "first estimate == first raw sample");
        assert!(ed.is_some());
        assert!((engine.ewma_alpha() - DEFAULT_EWMA_ALPHA).abs() < f64::EPSILON);
    }

    #[test]
    fn ewma_alpha_builder_clamps_invalid_values() {
        let e = AvailabilityEngine::new(100.0, 100.0, 1).with_ewma_alpha(0.5);
        assert_eq!(e.ewma_alpha(), 0.5);
        let e = AvailabilityEngine::new(100.0, 100.0, 1).with_ewma_alpha(7.0);
        assert_eq!(e.ewma_alpha(), DEFAULT_EWMA_ALPHA, "out-of-range → default");
        let e = AvailabilityEngine::new(100.0, 100.0, 1).with_ewma_alpha(f64::NAN);
        assert_eq!(e.ewma_alpha(), DEFAULT_EWMA_ALPHA, "NaN → default");
    }
}
