use crate::models::{CapabilityDescriptor, SystemStats};
use std::collections::VecDeque;
use sysinfo::{Components, CpuRefreshKind, Disks, Networks, RefreshKind, System};
use tracing::info;

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
        }
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

    /// Compute average latency from ping history (placeholder — real impl uses stored history)
    fn compute_avg_latency(&self) -> f64 {
        // In the full implementation, this pulls from the MetricsState latency_history.
        // For now, derive from sysinfo or return a default.
        0.0
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

        (stats, avail_up, avail_down)
    }
}
