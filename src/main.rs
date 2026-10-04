use echo_daemon::availability::AvailabilityEngine;
use echo_daemon::consumer::run_consumer_listener;
use echo_daemon::discovery::DiscoveryService;
use echo_daemon::identity::NodeIdentity;
use echo_daemon::meter::MeteringEngine;
use echo_daemon::neon;
use echo_daemon::settlement::SettlementEngine;
use echo_daemon::sqlite_store;
use echo_daemon::tunnel::TunnelService;
use echo_daemon::{
    telemetry::{self, DashboardConfig, DashboardReporter, DashboardTelemetry, FeatureInputs},
    MetricsBackend, NetworkMetricsRow, NodeIntelligenceRow, NodeMetricsRow,
    NodeRow,
};

use anyhow::{Context, Result};
use axum::{
    extract::{Json, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use chrono::Utc;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{signal, sync::RwLock, time::sleep};
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, error, info, warn};

/// Detect the node's public IP address by querying a public endpoint.
/// Falls back to `None` on failure (no panics, no blocking the runtime).
async fn detect_public_ip() -> Option<String> {
    // Try multiple sources for resilience
    let urls = [
        "https://ifconfig.me/ip",
        "https://api.ipify.org",
        "https://icanhazip.com",
    ];
    for url in &urls {
        match reqwest::get(*url).await {
            Ok(resp) => {
                if let Ok(text) = resp.text().await {
                    let ip = text.trim().to_string();
                    // Basic validation: must look like an IP address
                    if !ip.is_empty() && ip.len() <= 45 {
                        return Some(ip);
                    }
                }
            }
            Err(_) => continue,
        }
    }
    None
}

const DEFAULT_HEARTBEAT_SECS: u64 = 10;
const DEFAULT_METRICS_WINDOW: usize = 60;
const DEFAULT_PING_TARGETS: &[&str] = &["1.1.1.1", "8.8.8.8", "208.67.222.222"];
const DEFAULT_RU_PER_SECOND: f64 = 1.0;
const DEFAULT_CAPABILITY_PUBLISH_INTERVAL_SECS: u64 = 60;
const DEFAULT_RETENTION_DAYS: i64 = 30;
const DEFAULT_EARNINGS_PER_RU: f64 = 0.0001;
const DEFAULT_SETTLEMENT_RATE_USD_PER_GB: f64 = 0.50;
const DEFAULT_UNHEALTHY_THRESHOLD_SECS: u64 = 30;
const DEFAULT_CONSUMER_LISTEN_ADDR: &str = "127.0.0.1:3003";
const NOISE_PUBKEY_LEN: usize = 32;

/// Runtime configuration parsed from environment variables.
struct Config {
    node_id: String,
    user_id: String,
    node_name: String,
    region: String,
    listen_addr: String,
    db_path: String,
    tunnel_listen_addr: String,
    relay_target: String,
    ping_targets: Vec<String>,
    max_upload_mbps: f64,
    max_download_mbps: f64,
    max_sessions: u32,
    heartbeat_secs: u64,
    metrics_window: usize,
    ru_per_second: f64,
    capability_publish_interval_secs: u64,
    retention_days: i64,
    earnings_per_ru: f64,
    unhealthy_threshold_secs: u64,
    receipt_interval_packets: u64,
    receipt_min_bytes: u64,
    settlement_rate_usd_per_gb: f64,
    identity_path: PathBuf,
    database_url: Option<String>,
    api_key: Option<String>,
    cors_origins: Option<String>,
    neon_max_connections: u32,
    neon_acquire_timeout_secs: u64,
    neon_idle_timeout_secs: u64,
    /// "provider" (advertise + serve tunnels) or "consumer" (dial a provider).
    node_mode: String,
    /// Local address for the consumer forward listener.
    consumer_listen_addr: String,
    /// Provider tunnel address required in consumer mode.
    provider_addr: Option<String>,
    /// Provider libp2p PeerId (string form) required in consumer mode.
    provider_peer_id: Option<String>,
    /// Provider Noise static public key (base64) required in consumer mode.
    provider_noise_pubkey: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            node_id: std::env::var("NODE_ID").unwrap_or_else(|_| {
                format!("node-{}", now_epoch_secs())
            }),
            user_id: std::env::var("USER_ID").unwrap_or_else(|_| "anonymous".to_string()),
            node_name: std::env::var("NODE_NAME").unwrap_or_else(|_| "EchoNode".to_string()),
            region: std::env::var("NODE_REGION").unwrap_or_else(|_| "auto".to_string()),
            listen_addr: std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3001".to_string()),
            db_path: std::env::var("SQLITE_DB").unwrap_or_else(|_| "sqlite:echo_node.db".to_string()),
            tunnel_listen_addr: std::env::var("TUNNEL_ADDR").unwrap_or_else(|_| "0.0.0.0:3002".to_string()),
            relay_target: std::env::var("RELAY_TARGET").unwrap_or_else(|_| "127.0.0.1:80".to_string()),
            ping_targets: std::env::var("PING_TARGETS")
                .unwrap_or_else(|_| DEFAULT_PING_TARGETS.join(","))
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            max_upload_mbps: env_f64("MAX_UPLOAD_MBPS", 100.0),
            max_download_mbps: env_f64("MAX_DOWNLOAD_MBPS", 100.0),
            max_sessions: env_u32("MAX_SESSIONS", 10),
            heartbeat_secs: env_u64("HEARTBEAT_SECS", DEFAULT_HEARTBEAT_SECS),
            metrics_window: env_usize("METRICS_WINDOW", DEFAULT_METRICS_WINDOW),
            ru_per_second: env_f64("RU_PER_SECOND", DEFAULT_RU_PER_SECOND),
            capability_publish_interval_secs: env_u64("CAPABILITY_PUBLISH_INTERVAL_SECS", DEFAULT_CAPABILITY_PUBLISH_INTERVAL_SECS),
            retention_days: env_i64("RETENTION_DAYS", DEFAULT_RETENTION_DAYS),
            earnings_per_ru: env_f64("EARNINGS_PER_RU", DEFAULT_EARNINGS_PER_RU),
            unhealthy_threshold_secs: env_u64("UNHEALTHY_THRESHOLD_SECS", DEFAULT_UNHEALTHY_THRESHOLD_SECS),
            receipt_interval_packets: env_u64("RECEIPT_INTERVAL_PACKETS", 100),
            receipt_min_bytes: env_u64("RECEIPT_MIN_BYTES", 1_000_000),
            settlement_rate_usd_per_gb: env_f64("SETTLEMENT_RATE_USD_PER_GB", DEFAULT_SETTLEMENT_RATE_USD_PER_GB),
            identity_path: PathBuf::from(
                std::env::var("IDENTITY_PATH").unwrap_or_else(|_| "data/identity.json".to_string()),
            ),
            database_url: std::env::var("DATABASE_URL").ok().filter(|v| !v.is_empty()),
            api_key: std::env::var("API_KEY").ok().filter(|v| !v.is_empty()),
            cors_origins: std::env::var("CORS_ORIGINS").ok().filter(|v| !v.is_empty()),
            neon_max_connections: env_u32("NEON_MAX_CONNECTIONS", 5),
            neon_acquire_timeout_secs: env_u64("NEON_ACQUIRE_TIMEOUT_SECS", 10),
            neon_idle_timeout_secs: env_u64("NEON_IDLE_TIMEOUT_SECS", 240),
            node_mode: std::env::var("NODE_MODE")
                .unwrap_or_else(|_| "provider".to_string())
                .to_ascii_lowercase(),
            consumer_listen_addr: std::env::var("CONSUMER_LISTEN_ADDR")
                .unwrap_or_else(|_| DEFAULT_CONSUMER_LISTEN_ADDR.to_string()),
            provider_addr: std::env::var("PROVIDER_ADDR").ok().filter(|v| !v.is_empty()),
            provider_peer_id: std::env::var("PROVIDER_PEER_ID").ok().filter(|v| !v.is_empty()),
            provider_noise_pubkey: std::env::var("PROVIDER_NOISE_PUBKEY")
                .ok()
                .filter(|v| !v.is_empty()),
        }
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Constant-time byte-string comparison to prevent timing side-channel attacks.
/// Returns `true` if `a` and `b` have the same length and every byte is equal,
/// without early-exiting on the first mismatch.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonMetrics {
    pub timestamp: u64,
    pub uptime_secs: u64,
    pub cpu_pct: f64,
    pub cpu_temp_c: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_pct: f64,
    pub disk_usage_pct: f64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub network_rx_rate_bps: f64,
    pub network_tx_rate_bps: f64,
    pub latency_ms: f64,
    pub packet_loss_pct: f64,
    pub bandwidth_down_mbps: f64,
    pub bandwidth_up_mbps: f64,
    pub quality_score: f64,
    pub trust_score: f64,
    pub ru_total: f64,
    pub earnings_usd: f64,
    pub available_upload_mbps: f64,
    pub available_download_mbps: f64,
    pub active_sessions: u32,
    pub peer_id: String,
    /// Region tag from `NODE_REGION` (echo-sync/dashboard display + matching).
    pub region: String,
    /// Public IP detected at startup; omitted from JSON until detection lands
    /// (so downstream zod `.optional()` consumers never see `null`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonNode {
    pub id: String,
    pub name: String,
    pub region: String,
    pub status: String,
    pub peer_id: String,
    pub ip_address: Option<String>,
    pub last_seen: u64,
    pub uptime_secs: u64,
    pub cpu_pct: f64,
    pub memory_pct: f64,
    pub latency_ms: f64,
    pub bandwidth_down_mbps: f64,
    pub bandwidth_up_mbps: f64,
    pub packet_loss_pct: f64,
    pub quality_score: f64,
    pub trust_score: f64,
    pub available_upload_mbps: f64,
    pub available_download_mbps: f64,
    pub active_sessions: u32,
    pub ru_total: f64,
    pub earnings_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub running: bool,
    pub node_id: String,
    pub peer_id: String,
    pub uptime_secs: u64,
    pub heartbeat_count: u64,
    pub last_heartbeat: Option<u64>,
    pub db_connected: bool,
    pub active_sessions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlRequest {
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    pub success: bool,
    pub message: String,
    pub status: DaemonStatus,
}

#[derive(Debug, Clone, Serialize)]
struct HealthResponse {
    status: String,
    database: String,
    last_heartbeat_age_secs: u64,
    uptime_secs: u64,
    heartbeat_count: u64,
    active_sessions: u32,
    available_upload_mbps: f64,
    available_download_mbps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchRequest {
    pub region: String,
    pub min_upload_mbps: f64,
    pub min_download_mbps: f64,
}

#[derive(Clone)]
struct ApiKeyAuth {
    key: Option<String>,
}

impl ApiKeyAuth {
    fn new(key: Option<String>) -> Self {
        Self { key }
    }

    /// Middleware: verify `Authorization: Bearer <key>` header.
    /// If no API_KEY is configured, all requests are allowed through.
    /// Uses constant-time comparison to prevent timing side-channel attacks.
    async fn verify(&self, req: axum::http::Request<axum::body::Body>, next: Next) -> Response {
        match &self.key {
            None => next.run(req).await,
            Some(expected) => {
                let authorized = req
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
                    .map(|token| constant_time_eq(token.as_bytes(), expected.as_bytes()))
                    .unwrap_or(false);

                if authorized {
                    next.run(req).await
                } else {
                    (StatusCode::UNAUTHORIZED, "missing or invalid API key").into_response()
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResponse {
    pub providers: Vec<echo_daemon::CapabilityDescriptor>,
    pub count: usize,
}

/// Response for GET /settlements: payout totals plus the most recent
/// per-receipt settlement entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementView {
    pub summary: echo_daemon::SettlementSummary,
    pub settlements: Vec<echo_daemon::SettlementRecord>,
}

struct MetricsState {
    running: bool,
    node_id: String,
    user_id: String,
    node_name: String,
    region: String,
    start_time: Instant,
    heartbeat_count: u64,
    last_heartbeat_ts: Option<u64>,
    db_connected: bool,
    system_stats: echo_daemon::SystemStats,
    metrics_history: VecDeque<DaemonMetrics>,
    latency_history: VecDeque<f64>,
    packet_loss_history: VecDeque<f64>,
    ru_accrued: f64,
    earnings_usd: f64,
    ping_targets: Vec<String>,
    peer_id: String,
    /// Public IP address of this node (detected on startup).
    ip_address: Option<String>,
    /// Maximum concurrent tunnel sessions (from `MAX_SESSIONS`).
    max_sessions: u32,
}

/// Shared application state available to all HTTP handlers.
#[derive(Clone)]
struct AppState {
    metrics: Arc<RwLock<MetricsState>>,
    backend: Arc<dyn MetricsBackend>,
    settlement: SettlementEngine,
    unhealthy_threshold_secs: u64,
    provider_match_limit: i64,
}

impl MetricsState {
    #[allow(clippy::too_many_arguments)] // constructor: identity, region, ping, peer, window, max_sessions
    fn new(
        node_id: &str,
        user_id: &str,
        node_name: &str,
        region: &str,
        ping_targets: Vec<String>,
        peer_id: &str,
        metrics_window: usize,
        max_sessions: u32,
    ) -> Self {
        Self {
            running: true,
            node_id: node_id.to_string(),
            user_id: user_id.to_string(),
            node_name: node_name.to_string(),
            region: region.to_string(),
            start_time: Instant::now(),
            heartbeat_count: 0,
            last_heartbeat_ts: None,
            db_connected: false,
            system_stats: echo_daemon::SystemStats {
                cpu_pct: 0.0,
                memory_pct: 0.0,
                disk_usage_pct: 0.0,
                active_sessions: 0,
                current_usage_rx_mbps: 0.0,
                current_usage_tx_mbps: 0.0,
                memory_used_bytes: 0,
                memory_total_bytes: 0,
                network_rx_bytes: 0,
                network_tx_bytes: 0,
            },
            metrics_history: VecDeque::with_capacity(metrics_window),
            latency_history: VecDeque::with_capacity(metrics_window),
            packet_loss_history: VecDeque::with_capacity(metrics_window),
            ru_accrued: 0.0,
            earnings_usd: 0.0,
            ping_targets,
            peer_id: peer_id.to_string(),
            ip_address: None,
            max_sessions,
        }
    }
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Extract the RTT reported by `ping` itself (`time=39.2 ms` / `time<1 ms`).
///
/// We deliberately prefer the tool's own RTT over wall-clock
/// `Instant::now()`-around-`Command::output()`: process spawn/reap on
/// constrained/containerized hosts can exceed the real network RTT by
/// 10–50× (observed: 400ms+ process overhead on a 40ms link), which
/// poisoned `latency_ms` → `quality_score` → dashboard status.
/// Returns `None` if no `time=` field is found (caller falls back to wall-clock).
fn parse_ping_rtt_ms(stdout: &str) -> Option<f64> {
    for line in stdout.lines() {
        if let Some(pos) = line.find("time=") {
            let rest = &line[pos + 5..];
            let end = rest
                .find(|c: char| !c.is_ascii_digit() && c != '.')
                .unwrap_or(rest.len());
            if end > 0 {
                if let Ok(v) = rest[..end].parse::<f64>() {
                    return Some(v);
                }
            }
        } else if line.contains("time<") {
            // iputils prints `time<1 ms` for sub-millisecond replies
            return Some(0.5);
        }
    }
    None
}

fn measure_latency_and_loss_sync(targets: &[String]) -> (f64, f64) {
    let mut successes = 0u32;
    let mut total_ms = 0.0;
    let total = targets.len() as f64;

    for target in targets {
        let start = Instant::now();
        // Platform-specific ping flags:
        //   Linux:  -c 1 -W 2  (count=1, wait=2s)
        //   macOS:  -c 1 -t 2  (count=1, timeout=2s)
        #[cfg(target_os = "macos")]
        let result = std::process::Command::new("ping")
            .args(["-c", "1", "-t", "2", target])
            .output();
        #[cfg(not(target_os = "macos"))]
        let result = std::process::Command::new("ping")
            .args(["-c", "1", "-W", "2", target])
            .output();
        let elapsed = start.elapsed().as_millis() as f64;

        if let Ok(output) = result {
            if output.status.success() {
                successes += 1;
                // Prefer ping's own RTT; wall-clock is only a fallback for
                // platforms whose output we can't parse.
                let stdout = String::from_utf8_lossy(&output.stdout);
                total_ms += parse_ping_rtt_ms(&stdout).unwrap_or(elapsed);
            }
        }
    }

    let loss_pct = if total > 0.0 {
        ((total - successes as f64) / total) * 100.0
    } else {
        100.0
    };
    let avg_latency = if successes > 0 {
        total_ms / successes as f64
    } else {
        999.0
    };

    (avg_latency, loss_pct)
}

fn compute_quality_score(
    latency_ms: f64,
    packet_loss_pct: f64,
    cpu_pct: f64,
    memory_pct: f64,
    uptime_secs: u64,
) -> f64 {
    let latency_score = (1.0 - (latency_ms / 500.0).min(1.0)).max(0.0);
    let loss_score = (1.0 - (packet_loss_pct / 100.0)).max(0.0);
    let cpu_score = (1.0 - (cpu_pct / 100.0)).max(0.0);
    let mem_score = (1.0 - (memory_pct / 100.0)).max(0.0);
    let uptime_bonus = (uptime_secs as f64 / 86400.0).min(1.0);

    let raw = latency_score * 0.3
        + loss_score * 0.3
        + cpu_score * 0.2
        + mem_score * 0.1
        + uptime_bonus * 0.1;
    (raw * 100.0).round() / 100.0
}

fn compute_trust_score(
    quality_score: f64,
    uptime_secs: u64,
    heartbeat_count: u64,
    packet_loss_pct: f64,
) -> f64 {
    let quality_factor = quality_score;
    let uptime_factor = (uptime_secs as f64 / 86400.0).min(1.0);
    let consistency_factor = if heartbeat_count > 0 {
        (heartbeat_count as f64 / 600.0).min(1.0)
    } else {
        0.0
    };
    let loss_penalty = (packet_loss_pct / 100.0) * 0.5;

    let raw = (quality_factor * 0.4 + uptime_factor * 0.3 + consistency_factor * 0.3)
        - loss_penalty;
    (raw.clamp(0.0, 1.0) * 100.0).round() / 100.0
}

fn bps_to_mbps(bps: f64) -> f64 {
    (bps / 1_000_000.0 * 100.0).round() / 100.0
}

#[allow(clippy::too_many_arguments)] // heartbeat context: state, backend, availability, timers, reporter
async fn heartbeat(
    state: Arc<RwLock<MetricsState>>,
    backend: Arc<dyn MetricsBackend>,
    availability: Arc<RwLock<AvailabilityEngine>>,
    heartbeat_secs: u64,
    metrics_window: usize,
    ru_per_second: f64,
    earnings_per_ru: f64,
    reporter: Option<DashboardReporter>,
) {
    loop {
        sleep(Duration::from_secs(heartbeat_secs)).await;

        // Phase 1: Collect system stats and available capacity from the
        //          availability engine *before* touching the state lock.
        //          This eliminates the nested-lock hazard where `state` was
        //          held while waiting on `availability`.
        //          Uses refresh_and_compute() for a single sysinfo refresh
        //          instead of two separate refreshes.
        let (stats, avail_upload, avail_download, cpu_temp_c) = {
            let mut avail = availability.write().await;
            let (stats, up, down) = avail.refresh_and_compute();
            let temp = avail.cpu_temp_c();
            (stats, up, down, temp)
        };

        let cpu_pct = stats.cpu_pct;
        let mem_pct = stats.memory_pct;
        let disk_usage_pct = stats.disk_usage_pct;
        let rx_rate_bps = stats.current_usage_rx_mbps * 1_000_000.0;
        let tx_rate_bps = stats.current_usage_tx_mbps * 1_000_000.0;

        // Phase 2: Check whether we should run, then clone the ping
        //          targets *without* holding the lock across the blocking
        //          ping syscall.
        {
            let st = state.read().await;
            if !st.running {
                continue;
            }
        }

        let ping_targets = {
            let st = state.read().await;
            st.ping_targets.clone()
        };

        let (latency_ms, packet_loss_pct) = tokio::task::spawn_blocking(move || {
            measure_latency_and_loss_sync(&ping_targets)
        })
        .await
        .unwrap_or((999.0, 100.0));

        // Feed the latest latency sample into the availability engine so
        // the capability descriptor reports a real avg_latency_ms.
        {
            let mut avail = availability.write().await;
            avail.record_latency(latency_ms);
        }

        // Phase 3: Single state-lock acquisition for all bookkeeping and
        //          DB writes.  No other lock is acquired inside this block.
        let mut st = state.write().await;
        if !st.running {
            continue;
        }

        st.system_stats = stats.clone();

        st.latency_history.push_back(latency_ms);
        st.packet_loss_history.push_back(packet_loss_pct);
        if st.latency_history.len() > metrics_window {
            st.latency_history.pop_front();
        }
        if st.packet_loss_history.len() > metrics_window {
            st.packet_loss_history.pop_front();
        }

        let avg_latency: f64 = if st.latency_history.is_empty() {
            0.0
        } else {
            st.latency_history.iter().sum::<f64>() / st.latency_history.len() as f64
        };

        let uptime_secs = st.start_time.elapsed().as_secs();

        let quality_score = compute_quality_score(
            avg_latency,
            packet_loss_pct,
            cpu_pct,
            mem_pct,
            uptime_secs,
        );
        let trust_score = compute_trust_score(
            quality_score,
            uptime_secs,
            st.heartbeat_count,
            packet_loss_pct,
        );

        let ru = uptime_secs as f64 * ru_per_second;
        st.ru_accrued = ru;
        st.earnings_usd = ru * earnings_per_ru;

        let metrics = DaemonMetrics {
            timestamp: now_epoch_secs(),
            uptime_secs,
            cpu_pct: (cpu_pct * 100.0).round() / 100.0,
            cpu_temp_c: cpu_temp_c.unwrap_or(0.0),
            memory_used_bytes: stats.memory_used_bytes,
            memory_total_bytes: stats.memory_total_bytes,
            memory_pct: (mem_pct * 100.0).round() / 100.0,
            disk_usage_pct: (disk_usage_pct * 100.0).round() / 100.0,
            network_rx_bytes: stats.network_rx_bytes,
            network_tx_bytes: stats.network_tx_bytes,
            network_rx_rate_bps: (rx_rate_bps * 100.0).round() / 100.0,
            network_tx_rate_bps: (tx_rate_bps * 100.0).round() / 100.0,
            latency_ms: (avg_latency * 100.0).round() / 100.0,
            packet_loss_pct: (packet_loss_pct * 100.0).round() / 100.0,
            bandwidth_down_mbps: bps_to_mbps(rx_rate_bps),
            bandwidth_up_mbps: bps_to_mbps(tx_rate_bps),
            quality_score,
            trust_score,
            ru_total: (ru * 100.0).round() / 100.0,
            earnings_usd: (st.earnings_usd * 10000.0).round() / 10000.0,
            available_upload_mbps: avail_upload,
            available_download_mbps: avail_download,
            active_sessions: st.system_stats.active_sessions,
            peer_id: st.peer_id.clone(),
            region: st.region.clone(),
            ip_address: st.ip_address.clone(),
        };

        st.metrics_history.push_back(metrics.clone());
        if st.metrics_history.len() > metrics_window {
            st.metrics_history.pop_front();
        }

        st.heartbeat_count += 1;
        st.last_heartbeat_ts = Some(now_epoch_secs());

        let node_row = NodeRow {
            id: st.node_id.clone(),
            name: st.node_name.clone(),
            user_id: st.user_id.clone(),
            peer_id: Some(st.peer_id.clone()),
            public_key: None,
            ip_address: st.ip_address.clone(),
            region: Some(st.region.clone()),
            status: Some("online".to_string()),
            last_seen_at: Some(Utc::now().to_rfc3339()),
            uptime: Some(((uptime_secs as f64 / 86400.0).min(1.0) * 100.0).round() / 100.0),
            uptime_pct: Some(((uptime_secs as f64 / 86400.0).min(1.0) * 100.0).round() / 100.0),
            avg_latency_ms: Some(metrics.latency_ms),
            bandwidth_down_mbps: Some(metrics.bandwidth_down_mbps),
            bandwidth_up_mbps: Some(metrics.bandwidth_up_mbps),
            packet_loss_pct: Some(metrics.packet_loss_pct),
            quality_score: Some(metrics.quality_score),
            trust_score: Some(metrics.trust_score),
            sessions_count: Some(stats.active_sessions as i64),
            earnings_usd: Some(metrics.earnings_usd),
            reported_upload_cap_mbps: Some(avail_upload),
            reported_download_cap_mbps: Some(avail_download),
            supported_encryption: Some("noise-xx".to_string()),
            services: None,
        };

        let node_metrics = NodeMetricsRow {
            node_id: st.node_id.clone(),
            user_id: st.user_id.clone(),
            latency_ms: Some(metrics.latency_ms),
            bandwidth_down_mbps: Some(metrics.bandwidth_down_mbps),
            bandwidth_up_mbps: Some(metrics.bandwidth_up_mbps),
            packet_loss_pct: Some(metrics.packet_loss_pct),
            quality_score: Some(metrics.quality_score),
            earnings_usd: Some(metrics.earnings_usd),
            recorded_at: Some(Utc::now().to_rfc3339()),
        };

        let network_metrics = NetworkMetricsRow {
            user_id: st.user_id.clone(),
            active_nodes: Some((stats.active_sessions as i64).max(1)),
            avg_latency_ms: Some(metrics.latency_ms),
            bandwidth_egress_mb: Some(metrics.bandwidth_up_mbps),
            bandwidth_ingress_mb: Some(metrics.bandwidth_down_mbps),
            packet_loss_pct: Some(metrics.packet_loss_pct),
            uptime_pct: Some(((uptime_secs as f64 / 86400.0).min(1.0) * 100.0).round() / 100.0),
            earnings_usd: Some(metrics.earnings_usd),
            recorded_at: Some(Utc::now().to_rfc3339()),
        };

        // 32-dimension feature vector consumed by the echomesh dashboard.
        // Dimension order is fixed by the dashboard's FEATURE_LABELS /
        // node_intelligence migration — see telemetry::build_feature_vector.
        let feature_series: Vec<FeatureInputs> = st
            .metrics_history
            .iter()
            .map(|m| FeatureInputs {
                cpu_pct: m.cpu_pct,
                memory_pct: m.memory_pct,
                disk_usage_pct: m.disk_usage_pct,
                latency_ms: m.latency_ms,
                packet_loss_pct: m.packet_loss_pct,
                bandwidth_up_mbps: m.bandwidth_up_mbps,
                bandwidth_down_mbps: m.bandwidth_down_mbps,
                uptime_secs: m.uptime_secs,
                earnings_usd: m.earnings_usd,
                available_upload_mbps: m.available_upload_mbps,
                available_download_mbps: m.available_download_mbps,
                active_sessions: m.active_sessions,
                max_sessions: st.max_sessions,
            })
            .collect();
        let feature_vector = serde_json::json!(telemetry::build_feature_vector(&feature_series));

        // Multi-signal anomaly detection:
        //   - High packet loss (>10%)
        //   - High latency (>500ms)
        //   - High CPU (>95%)
        //   - Low quality score (<0.3)
        //   - High disk usage (>95%)
        let loss_anomaly = metrics.packet_loss_pct / 100.0;
        let latency_anomaly = (metrics.latency_ms / 1000.0).min(1.0);
        let cpu_anomaly = if metrics.cpu_pct > 95.0 { 0.3 } else { 0.0 };
        let quality_anomaly = if metrics.quality_score < 0.3 {
            0.3
        } else {
            0.0
        };
        let disk_anomaly = if metrics.disk_usage_pct > 95.0 {
            0.1
        } else {
            0.0
        };
        let anomaly_score =
            (loss_anomaly * 0.3 + latency_anomaly * 0.3 + cpu_anomaly + quality_anomaly + disk_anomaly)
                .min(1.0);

        let intel = NodeIntelligenceRow {
            node_id: st.node_id.clone(),
            user_id: st.user_id.clone(),
            quality_score: Some(metrics.quality_score),
            trust_score: Some(metrics.trust_score),
            anomaly_score: Some(anomaly_score),
            is_anomalous: Some(anomaly_score > 0.5),
            cluster_id: Some(0),
            feature_vector: Some(feature_vector),
            recorded_at: Some(Utc::now().to_rfc3339()),
        };

        if let Err(e) = backend.upsert_node(&node_row).await {
            error!(error = ?e, "node upsert failed");
            st.db_connected = false;
        } else {
            st.db_connected = true;
        }
        if let Err(e) = backend.insert_node_metrics(&node_metrics).await {
            error!(error = %e, "metrics insert failed");
        }
        if let Err(e) = backend.insert_network_metrics(&network_metrics).await {
            error!(error = %e, "network metrics insert failed");
        }
        if let Err(e) = backend.insert_node_intelligence(&intel).await {
            error!(error = %e, "intelligence insert failed");
        }

        // Push this heartbeat to the dashboard control plane (opt-in, never
        // blocks the heartbeat — the reporter task does the HTTP POST).
        if let Some(reporter) = &reporter {
            let frame = DashboardTelemetry {
                version: telemetry::TELEMETRY_SCHEMA_VERSION,
                timestamp: Utc::now().to_rfc3339(),
                node_id: reporter.node_id().to_string(),
                user_id: reporter.user_id().to_string(),
                node: node_row.clone(),
                node_metrics: node_metrics.clone(),
                network_metrics: network_metrics.clone(),
                intelligence: intel.clone(),
            };
            reporter.try_push(frame);
        }

        debug!(
            heartbeat = st.heartbeat_count,
            cpu_pct = metrics.cpu_pct,
            latency_ms = metrics.latency_ms,
            avail_up = metrics.available_upload_mbps,
            avail_down = metrics.available_download_mbps,
            "heartbeat complete"
        );
    }
}

async fn handle_metrics(
    State(app): State<AppState>,
) -> Result<Json<DaemonMetrics>, StatusCode> {
    let st = app.metrics.read().await;
    st.metrics_history
        .back()
        .cloned()
        .map(Json)
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)
}

async fn handle_nodes(
    State(app): State<AppState>,
) -> Json<Vec<DaemonNode>> {
    let st = app.metrics.read().await;
    let uptime = st.start_time.elapsed().as_secs();
    let metrics = st.metrics_history.back().cloned();

    let node = DaemonNode {
        id: st.node_id.clone(),
        name: st.node_name.clone(),
        region: st.region.clone(),
        status: if st.running { "online" } else { "offline" }.to_string(),
        peer_id: st.peer_id.clone(),
        ip_address: st.ip_address.clone(),
        last_seen: now_epoch_secs(),
        uptime_secs: uptime,
        cpu_pct: metrics.as_ref().map_or(0.0, |m| m.cpu_pct),
        memory_pct: metrics.as_ref().map_or(0.0, |m| m.memory_pct),
        latency_ms: metrics.as_ref().map_or(0.0, |m| m.latency_ms),
        bandwidth_down_mbps: metrics
            .as_ref()
            .map_or(0.0, |m| m.bandwidth_down_mbps),
        bandwidth_up_mbps: metrics.as_ref().map_or(0.0, |m| m.bandwidth_up_mbps),
        packet_loss_pct: metrics.as_ref().map_or(0.0, |m| m.packet_loss_pct),
        quality_score: metrics.as_ref().map_or(0.0, |m| m.quality_score),
        trust_score: metrics.as_ref().map_or(0.0, |m| m.trust_score),
        available_upload_mbps: metrics.as_ref().map_or(0.0, |m| m.available_upload_mbps),
        available_download_mbps: metrics.as_ref().map_or(0.0, |m| m.available_download_mbps),
        active_sessions: metrics.as_ref().map_or(0, |m| m.active_sessions),
        ru_total: metrics.as_ref().map_or(0.0, |m| m.ru_total),
        earnings_usd: metrics.as_ref().map_or(0.0, |m| m.earnings_usd),
    };

    Json(vec![node])
}

async fn handle_history(
    State(app): State<AppState>,
) -> Json<Vec<DaemonMetrics>> {
    let st = app.metrics.read().await;
    Json(st.metrics_history.iter().cloned().collect())
}

async fn handle_status(
    State(app): State<AppState>,
) -> Json<DaemonStatus> {
    let st = app.metrics.read().await;
    Json(DaemonStatus {
        running: st.running,
        node_id: st.node_id.clone(),
        peer_id: st.peer_id.clone(),
        uptime_secs: st.start_time.elapsed().as_secs(),
        heartbeat_count: st.heartbeat_count,
        last_heartbeat: st.last_heartbeat_ts,
        db_connected: st.db_connected,
        active_sessions: st.system_stats.active_sessions,
    })
}

async fn handle_control(
    State(app): State<AppState>,
    Json(req): Json<ControlRequest>,
) -> Result<Json<ControlResponse>, StatusCode> {
    let mut st = app.metrics.write().await;
    let response = match req.action.as_str() {
        "start" => {
            st.running = true;
            ControlResponse {
                success: true,
                message: "Daemon started".to_string(),
                status: DaemonStatus {
                    running: true,
                    node_id: st.node_id.clone(),
                    peer_id: st.peer_id.clone(),
                    uptime_secs: st.start_time.elapsed().as_secs(),
                    heartbeat_count: st.heartbeat_count,
                    last_heartbeat: st.last_heartbeat_ts,
                    db_connected: st.db_connected,
                    active_sessions: st.system_stats.active_sessions,
                },
            }
        }
        "stop" => {
            st.running = false;
            ControlResponse {
                success: true,
                message: "Daemon stopped".to_string(),
                status: DaemonStatus {
                    running: false,
                    node_id: st.node_id.clone(),
                    peer_id: st.peer_id.clone(),
                    uptime_secs: st.start_time.elapsed().as_secs(),
                    heartbeat_count: st.heartbeat_count,
                    last_heartbeat: st.last_heartbeat_ts,
                    db_connected: st.db_connected,
                    active_sessions: 0,
                },
            }
        }
        "restart" => {
            st.running = true;
            st.heartbeat_count = 0;
            st.start_time = Instant::now();
            st.metrics_history.clear();
            st.latency_history.clear();
            st.packet_loss_history.clear();
            ControlResponse {
                success: true,
                message: "Daemon restarted".to_string(),
                status: DaemonStatus {
                    running: true,
                    node_id: st.node_id.clone(),
                    peer_id: st.peer_id.clone(),
                    uptime_secs: 0,
                    heartbeat_count: 0,
                    last_heartbeat: None,
                    db_connected: st.db_connected,
                    active_sessions: 0,
                },
            }
        }
        _ => {
            return Err(StatusCode::BAD_REQUEST);
        }
    };
    Ok(Json(response))
}

async fn handle_health(
    State(app): State<AppState>,
) -> Json<HealthResponse> {
    let st = app.metrics.read().await;
    let db_status = if st.db_connected { "ok" } else { "degraded" };

    let age = st
        .last_heartbeat_ts
        .map(|ts| now_epoch_secs() - ts)
        .unwrap_or(999);

    let overall = if age > app.unhealthy_threshold_secs {
        "unhealthy"
    } else if !st.db_connected {
        "degraded"
    } else {
        "healthy"
    };

    let metrics = st.metrics_history.back();

    Json(HealthResponse {
        status: overall.to_string(),
        database: db_status.to_string(),
        last_heartbeat_age_secs: age,
        uptime_secs: st.start_time.elapsed().as_secs(),
        heartbeat_count: st.heartbeat_count,
        active_sessions: st.system_stats.active_sessions,
        available_upload_mbps: metrics.map_or(0.0, |m| m.available_upload_mbps),
        available_download_mbps: metrics.map_or(0.0, |m| m.available_download_mbps),
    })
}

async fn handle_settlements(
    State(app): State<AppState>,
) -> Result<Json<SettlementView>, StatusCode> {
    let summary = app
        .settlement
        .summary()
        .await
        .map_err(|e| {
            error!(error = %e, "failed to load settlement summary");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let receipts = app
        .settlement
        .settlements(100)
        .await
        .map_err(|e| {
            error!(error = %e, "failed to load settlements");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(SettlementView {
        summary,
        settlements: receipts,
    }))
}

async fn handle_match_providers(
    State(app): State<AppState>,
    Json(req): Json<MatchRequest>,
) -> Result<Json<MatchResponse>, StatusCode> {
    info!(
        region = %req.region,
        min_upload = req.min_upload_mbps,
        min_download = req.min_download_mbps,
        "provider match request"
    );

    match app
        .backend
        .get_capabilities_in_region(
            &req.region,
            req.min_upload_mbps,
            req.min_download_mbps,
            app.provider_match_limit,
        )
        .await
    {
        Ok(providers) => {
            let count = providers.len();
            info!(count, "provider match completed");
            Ok(Json(MatchResponse { providers, count }))
        }
        Err(e) => {
            error!(error = %e, "provider match query failed");
            // Fall back to empty result rather than 500 — the caller
            // can retry or treat as "no providers found".
            Ok(Json(MatchResponse {
                providers: vec![],
                count: 0,
            }))
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            error!(error = %e, "failed to listen for Ctrl+C");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => { sig.recv().await; }
            Err(e) => {
                error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, draining...");
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "echo_daemon=info".into()),
        )
        .init();

    let config = Config::from_env();

    // Validate the operating mode before touching anything else.
    if !matches!(config.node_mode.as_str(), "provider" | "consumer") {
        anyhow::bail!(
            "NODE_MODE must be 'provider' or 'consumer', got '{}'",
            config.node_mode
        );
    }
    if config.node_mode == "consumer" {
        if config.provider_addr.is_none() {
            anyhow::bail!("consumer mode requires PROVIDER_ADDR (the provider's TUNNEL_ADDR)");
        }
        if config.provider_peer_id.is_none() {
            anyhow::bail!("consumer mode requires PROVIDER_PEER_ID (the provider's libp2p PeerId)");
        }
        if config.provider_noise_pubkey.is_none() {
            anyhow::bail!(
                "consumer mode requires PROVIDER_NOISE_PUBKEY (the provider's base64 X25519 key)"
            );
        }
    }

    // Step 1: Load or generate node identity
    let identity = NodeIdentity::load_or_generate(&config.identity_path, &config.region)?;
    info!(
        peer_id = identity.peer_id_str(),
        "node identity loaded"
    );

    // Step 2: Initialize database backend
    let backend: Arc<dyn MetricsBackend> = if let Some(url) = &config.database_url {
        info!(backend = "neon", url = %url, "database backend initialized");
        Arc::new(neon::NeonStore::with_config(
            url,
            config.neon_max_connections,
            config.neon_acquire_timeout_secs,
            config.neon_idle_timeout_secs,
        ).await?)
    } else {
        info!(backend = "sqlite", reason = "DATABASE_URL not set", "database backend initialized");
        Arc::new(sqlite_store::SqliteStore::new(&config.db_path).await?)
    };

    // Step 3: Initialize availability engine
    let availability = Arc::new(RwLock::new(AvailabilityEngine::new(
        config.max_upload_mbps,
        config.max_download_mbps,
        config.max_sessions,
    )));

    // Step 4: Initialize metering engine
    let (metering, receipt_rx) = MeteringEngine::with_config(
        identity.clone(),
        config.receipt_interval_packets,
        config.receipt_min_bytes,
    );

    // Step 4.5: Initialize tunnel service (holds metering engine alive)
    let local_peer_id: libp2p::PeerId = identity
        .peer_id_str()
        .parse()
        .context("failed to parse peer_id")?;
    let relay_config = echo_daemon::tunnel::RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: config.relay_target.clone(),
        local_peer_id: identity.peer_id_str().to_string(),
        max_upload_mbps: config.max_upload_mbps,
        max_download_mbps: config.max_download_mbps,
    };
    let (tunnel_service, mut tunnel_rx) = TunnelService::new(
        local_peer_id,
        metering,
        availability.clone(),
        relay_config,
        config.max_sessions as usize,
        identity.noise_secret_key_bytes().to_vec(),
    );
    // Shared so both the listener and (in consumer mode) the forward
    // listener can reference the same service.
    let tunnel_service = Arc::new(tunnel_service);

    // Step 4.75: Dashboard telemetry bridge (opt-in via DASHBOARD_* env vars).
    // Created before the background tasks so both the heartbeat (metrics) and
    // the tunnel event handler (session federation) can push real data to the
    // echomesh control plane.
    let reporter = match DashboardConfig::from_env() {
        Some(cfg) => {
            info!(
                url = %cfg.url,
                node_id = %cfg.node_id,
                user_id = %cfg.user_id,
                "dashboard telemetry reporting enabled"
            );
            Some(DashboardReporter::spawn(cfg))
        }
        None => {
            debug!("dashboard telemetry reporting disabled (set DASHBOARD_TELEMETRY_URL, DASHBOARD_TELEMETRY_TOKEN, DASHBOARD_USER_ID)");
            None
        }
    };

    // Spawn tunnel event handler
    // NOTE: end_session is called inside handle_incoming_connection (provider side)
    // and should NOT be called here to avoid the double-end-session bug.
    let event_reporter = reporter.clone();
    let settlement_rate = config.settlement_rate_usd_per_gb;
    tokio::spawn(async move {
        while let Some(event) = tunnel_rx.recv().await {
            match event {
                echo_daemon::tunnel::TunnelEvent::SessionEstablished {
                    session_id,
                    remote_peer_id,
                } => {
                    info!(
                        session = %session_id,
                        peer = %remote_peer_id,
                        "tunnel session established"
                    );
                }
                echo_daemon::tunnel::TunnelEvent::SessionClosed {
                    session_id,
                    bytes_sent,
                    bytes_received,
                    details,
                } => {
                    info!(
                        session = %session_id,
                        bytes_sent,
                        bytes_received,
                        "tunnel session closed"
                    );
                    // Do NOT call end_session here — it's already handled by
                    // handle_incoming_connection on the provider side.
                    //
                    // Slice 2 session federation: provider-side closes carry
                    // full accounting; push them to the dashboard so sessions
                    // show real relays instead of seeded rows (see GAPS.md).
                    if let (Some(reporter), Some(details)) = (&event_reporter, &details) {
                        let session_event = telemetry::SessionEvent::from_close(
                            reporter.node_id(),
                            reporter.user_id(),
                            &session_id,
                            bytes_sent,
                            bytes_received,
                            settlement_rate,
                            details,
                        );
                        reporter.try_push_session(session_event);
                    }
                }
            }
        }
    });

    // Spawn incoming tunnel listener (provider side) — or, in consumer mode,
    // the consumer forward listener that relays local apps to a provider.
    if config.node_mode == "provider" {
        let tunnel_event_tx = tunnel_service.event_tx().clone();
        let relay_config = echo_daemon::tunnel::RelayConfig {
            backend: tunnel_service.relay_config().backend.clone(),
            metering: tunnel_service.relay_config().metering.clone(),
            availability: tunnel_service.relay_config().availability.clone(),
            target_addr: tunnel_service.relay_config().target_addr.clone(),
            local_peer_id: tunnel_service.relay_config().local_peer_id.clone(),
            max_upload_mbps: tunnel_service.relay_config().max_upload_mbps,
            max_download_mbps: tunnel_service.relay_config().max_download_mbps,
        };
        let conn_semaphore = tunnel_service.conn_semaphore();
        let static_private_key = tunnel_service.static_private_key();
        let tunnel_addr = config.tunnel_listen_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = TunnelService::accept_incoming(
                &tunnel_addr,
                tunnel_event_tx,
                relay_config,
                conn_semaphore,
                static_private_key,
            )
            .await
            {
                error!(error = %e, "tunnel listener failed");
            }
        });
    } else {
        // Consumer mode: parse the provider identity (validated above).
        let provider_peer_id: libp2p::PeerId = config
            .provider_peer_id
            .as_deref()
            .expect("validated above")
            .parse()
            .context("PROVIDER_PEER_ID is not a valid libp2p PeerId")?;
        let provider_noise_pubkey = echo_daemon::base64_decode(
            config.provider_noise_pubkey.as_deref().expect("validated above"),
        );
        if provider_noise_pubkey.len() != NOISE_PUBKEY_LEN {
            anyhow::bail!(
                "PROVIDER_NOISE_PUBKEY decodes to {} bytes, expected {} (X25519)",
                provider_noise_pubkey.len(),
                NOISE_PUBKEY_LEN
            );
        }

        let consumer_service = tunnel_service.clone();
        let consumer_config = echo_daemon::consumer::ConsumerConfig {
            listen_addr: config.consumer_listen_addr.clone(),
            provider_addr: config.provider_addr.clone().expect("validated above"),
            provider_peer_id,
            provider_noise_pubkey,
        };
        info!(
            listen_addr = %consumer_config.listen_addr,
            provider = %consumer_config.provider_addr,
            "consumer mode enabled; forwarding local connections to provider"
        );
        tokio::spawn(async move {
            if let Err(e) = run_consumer_listener(consumer_service, consumer_config).await {
                error!(error = %e, "consumer listener failed");
            }
        });
    }

    // Step 5: Initialize discovery (libp2p swarm)
    let discovery_listen: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse()?;
    let (mut discovery, mut discovery_rx) = DiscoveryService::new(
        &identity,
        discovery_listen,
        vec![],
    )
    .await
    .map_err(|e| {
        error!(error = %e, "discovery init failed");
        anyhow::anyhow!("discovery init failed: {}", e)
    })?;

    info!(peer_id = %identity.peer_id_str(), "discovery initialized");

    // Spawn discovery event loop
    tokio::spawn(async move {
        discovery.run().await;
    });

    // Spawn discovery event handler
    tokio::spawn(async move {
        while let Some(event) = discovery_rx.recv().await {
            match event {
                echo_daemon::discovery::DiscoveryEvent::PeerFound(peer_id, addr) => {
                    info!(peer = %peer_id, addr = %addr, "peer discovered via DHT");
                }
                echo_daemon::discovery::DiscoveryEvent::PeerDisconnected(peer_id) => {
                    info!(peer = %peer_id, "peer disconnected");
                }
                echo_daemon::discovery::DiscoveryEvent::CapabilityPublished => {
                    debug!("capability published to DHT");
                }
            }
        }
    });

    // Step 6: Spawn heartbeat
    let heartbeat_secs = config.heartbeat_secs;
    let metrics_window = config.metrics_window;
    let ru_per_second = config.ru_per_second;
    let earnings_per_ru = config.earnings_per_ru;
    let unhealthy_threshold_secs = config.unhealthy_threshold_secs;
    let state = Arc::new(RwLock::new(MetricsState::new(
        &config.node_id,
        &config.user_id,
        &config.node_name,
        &config.region,
        config.ping_targets.clone(),
        identity.peer_id_str(),
        metrics_window,
        config.max_sessions,
    )));

    // Step 6.5 (moved earlier): Dashboard telemetry bridge is created right
    // after the tunnel service so both the heartbeat and the tunnel event
    // handler can push real data. `reporter` is already in scope above.

    // Detect public IP address (non-blocking, best-effort)
    let state_for_ip = state.clone();
    tokio::spawn(async move {
        match detect_public_ip().await {
            Some(ip) => {
                info!(ip = %ip, "public IP detected");
                let mut st = state_for_ip.write().await;
                st.ip_address = Some(ip);
            }
            None => {
                warn!("failed to detect public IP address (will be unknown)");
            }
        }
    });

    let state_clone = state.clone();
    let backend_clone = backend.clone();
    let avail_clone = availability.clone();
    let reporter_clone = reporter.clone();
    tokio::spawn(async move {
        heartbeat(state_clone, backend_clone, avail_clone, heartbeat_secs, metrics_window, ru_per_second, earnings_per_ru, reporter_clone).await;
    });

    // Step 7: Spawn data retention cleanup (daily)
    let cleanup_backend = backend.clone();
    let retention_days = config.retention_days;
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(86400)).await;
            match cleanup_backend.cleanup_old_metrics(retention_days).await {
                Ok(rows) => {
                    if rows > 0 {
                        info!(rows_deleted = rows, "data retention cleanup completed");
                    }
                }
                Err(e) => {
                    error!(error = %e, "data retention cleanup failed");
                }
            }
        }
    });

    // Step 8: Spawn capability publisher (periodic DHT republish)
    // Only providers advertise capacity — a consumer has nothing to publish.
    if config.node_mode == "provider" {
        let identity_clone = identity.clone();
        let backend_clone2 = backend.clone();
        let cap_publish_interval = config.capability_publish_interval_secs;
        let cap_region = config.region.clone();
        let cap_state = state.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(cap_publish_interval)).await;
                // Compute reputation from current quality/trust scores
                let reputation = {
                    let st = cap_state.read().await;
                    st.metrics_history
                        .back()
                        .map(|m| {
                            // Weighted average: 60% quality + 40% trust
                            m.quality_score * 0.6 + m.trust_score * 0.4
                        })
                        .unwrap_or(0.5)
                };
                // Build and store capability descriptor
                let mut avail = availability.write().await;
                let cap = avail.build_capability_descriptor(
                    identity_clone.peer_id_str(),
                    identity_clone.public_key_bytes.clone(),
                    &cap_region,
                    reputation,
                    identity_clone.noise_public_key_bytes().to_vec(),
                );
                drop(avail);

                if let Err(e) = backend_clone2.upsert_capability(&cap).await {
                    error!(error = %e, "capability publish failed");
                } else {
                    debug!("capability descriptor published");
                }
            }
        });
    }

    // Step 9: Spawn receipt settlement task
    // The metering engine is kept alive inside TunnelService.
    // Use a separate channel for shutdown signaling.
    let settlement_engine = SettlementEngine::new(
        identity.clone(),
        backend.clone(),
        config.settlement_rate_usd_per_gb,
    );

    // Providers alone settle receipts: a consumer never issues authoritative
    // receipts for traffic it merely forwards, so there is nothing to settle
    // in consumer mode (and no stray consumer-signed rows to persist).
    let shutdown_tx = if config.node_mode == "provider" {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let mut receipt_rx_task = receipt_rx;
        let receipt_backend = backend.clone();
        let receipt_signer_key = identity.public_key_bytes.clone();
        let receipt_settlement = settlement_engine.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        // Drain any remaining receipts before shutdown
                        let mut count = 0u64;
                        while let Ok(receipt) = receipt_rx_task.try_recv() {
                            match receipt.verify_signature(&receipt_signer_key) {
                                Ok(true) => {
                                    if let Err(e) = receipt_backend.insert_receipt(&receipt).await {
                                        error!(error = %e, "receipt flush failed during shutdown");
                                    } else {
                                        count += 1;
                                        if let Err(e) = receipt_settlement.settle_receipt(receipt).await {
                                            error!(error = %e, "receipt settlement flush failed during shutdown");
                                        }
                                    }
                                }
                                Ok(false) => {
                                    warn!(
                                        receipt = %receipt.receipt_id,
                                        session = %receipt.session_id,
                                        "receipt signature verification failed during shutdown flush, skipping"
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        receipt = %receipt.receipt_id,
                                        error = %e,
                                        "receipt malformed during shutdown flush, skipping"
                                    );
                                }
                            }
                        }
                        if count > 0 {
                            info!(count, "receipts flushed during shutdown drain");
                        }
                        break;
                    }
                    receipt = receipt_rx_task.recv() => {
                        match receipt {
                            Some(receipt) => {
                                // Verify the receipt signature before persistence
                                match receipt.verify_signature(&receipt_signer_key) {
                                    Ok(true) => {
                                        if let Err(e) = receipt_backend.insert_receipt(&receipt).await {
                                            error!(error = %e, "receipt settlement failed");
                                        } else {
                                            debug!(
                                                session = %receipt.session_id,
                                                seq = receipt.sequence_number,
                                                "receipt settled"
                                            );
                                            // Accumulate the verified receipt toward payout.
                                            if let Err(e) = receipt_settlement.settle_receipt(receipt).await {
                                                error!(error = %e, "payout accumulation failed");
                                            }
                                        }
                                    }
                                    Ok(false) => {
                                        warn!(
                                            receipt = %receipt.receipt_id,
                                            session = %receipt.session_id,
                                            seq = receipt.sequence_number,
                                            "receipt signature verification failed, discarding"
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            receipt = %receipt.receipt_id,
                                            error = %e,
                                            "receipt malformed, discarding"
                                        );
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Some(shutdown_tx)
    } else {
        None
    };

    // Step 10: Set up HTTP API
    // CORS: restrict in production via CORS_ORIGINS env var (comma-separated).
    // Defaults to allow-all for development convenience.
    let cors = if let Some(origins_str) = &config.cors_origins {
        let origins: Vec<_> = origins_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::OPTIONS,
            ])
            .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::AUTHORIZATION])
    } else {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    // API key for /control endpoint (optional — if unset, auth is disabled)
    if config.api_key.is_some() {
        info!("API key authentication enabled for /control");
    } else {
        info!("API key authentication disabled (set API_KEY env var to enable)");
    }
    let auth = ApiKeyAuth::new(config.api_key.clone());

    // /control and /settlements are protected by API key middleware
    let control_routes = Router::new()
        .route("/control", post(handle_control))
        .route("/settlements", get(handle_settlements))
        .layer(middleware::from_fn(move |req, next| {
            let auth = auth.clone();
            async move { auth.verify(req, next).await }
        }));

    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/nodes", get(handle_nodes))
        .route("/history", get(handle_history))
        .route("/status", get(handle_status))
        .route("/health", get(handle_health))
        .route("/match", post(handle_match_providers))
        .merge(control_routes)
        .layer(cors)
        .with_state(AppState {
            metrics: state,
            backend: backend.clone(),
            settlement: settlement_engine,
            unhealthy_threshold_secs,
            provider_match_limit: 10,
        });

    let addr: SocketAddr = config.listen_addr.parse()?;
    info!(addr = %addr, "server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Signal shutdown to receipt settlement task (flush remaining receipts)
    if let Some(shutdown_tx) = shutdown_tx {
        let _ = shutdown_tx.send(true);
        // Give the drain a moment to complete
        sleep(Duration::from_secs(1)).await;
    }

    info!("shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_same_bytes() {
        assert!(constant_time_eq(b"hello", b"hello"));
    }

    #[test]
    fn constant_time_eq_different_bytes() {
        assert!(!constant_time_eq(b"hello", b"world"));
    }

    #[test]
    fn constant_time_eq_different_lengths() {
        assert!(!constant_time_eq(b"hello", b"hello!"));
    }

    #[test]
    fn constant_time_eq_empty() {
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn constant_time_eq_single_byte_match() {
        assert!(constant_time_eq(&[0x42], &[0x42]));
    }

    #[test]
    fn constant_time_eq_single_byte_mismatch() {
        assert!(!constant_time_eq(&[0x42], &[0x43]));
    }

    #[test]
    fn bps_to_mbps_conversion() {
        assert_eq!(bps_to_mbps(1_000_000.0), 1.0);
        assert_eq!(bps_to_mbps(10_000_000.0), 10.0);
        assert_eq!(bps_to_mbps(500_000.0), 0.5);
    }

    #[test]
    fn compute_quality_score_all_zero() {
        let score = compute_quality_score(0.0, 0.0, 0.0, 0.0, 0);
        // With zero latency, zero loss, zero CPU, zero memory, zero uptime:
        // latency_score = 1.0, loss_score = 1.0, cpu_score = 1.0, mem_score = 1.0, uptime_bonus = 0.0
        // raw = 1.0*0.3 + 1.0*0.3 + 1.0*0.2 + 1.0*0.1 + 0.0*0.1 = 0.9
        assert!((score - 0.9).abs() < 0.01, "expected ~0.9, got {}", score);
    }

    #[test]
    fn compute_quality_score_all_bad() {
        let score = compute_quality_score(999.0, 100.0, 100.0, 100.0, 0);
        // latency_score = 0.0, loss_score = 0.0, cpu_score = 0.0, mem_score = 0.0, uptime_bonus = 0.0
        // raw = 0.0
        assert!((score - 0.0).abs() < 0.01, "expected ~0.0, got {}", score);
    }

    #[test]
    fn compute_trust_score_perfect() {
        // quality=1.0, uptime=86400 (1 day), 600 heartbeats, 0% loss
        let score = compute_trust_score(1.0, 86400, 600, 0.0);
        // quality_factor=1.0, uptime_factor=1.0, consistency_factor=1.0, loss_penalty=0.0
        // raw = 1.0*0.4 + 1.0*0.3 + 1.0*0.3 = 1.0
        assert!((score - 1.0).abs() < 0.01, "expected ~1.0, got {}", score);
    }

    #[test]
    fn compute_trust_score_with_loss() {
        let score = compute_trust_score(1.0, 86400, 600, 50.0);
        // loss_penalty = 0.5 * 0.5 = 0.25
        // raw = 1.0 - 0.25 = 0.75
        assert!((score - 0.75).abs() < 0.01, "expected ~0.75, got {}", score);
    }

    #[test]
    fn compute_trust_score_clamped_to_zero() {
        let score = compute_trust_score(0.0, 0, 0, 100.0);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn now_epoch_secs_returns_reasonable_value() {
        let ts = now_epoch_secs();
        // Should be after 2020 (1577836800) and before 2100 (4102444800)
        assert!(ts > 1_577_836_800, "timestamp too old: {}", ts);
        assert!(ts < 4_102_444_800, "timestamp too far in future: {}", ts);
    }

    #[test]
    fn parse_ping_rtt_iputils_format() {
        let out = "64 bytes from 1.1.1.1: icmp_seq=1 ttl=57 time=39.2 ms\n";
        assert_eq!(parse_ping_rtt_ms(out), Some(39.2));
    }

    #[test]
    fn parse_ping_rtt_busybox_and_subms() {
        // busybox/toybox: integer-only variant
        let out = "64 bytes from 8.8.8.8: seq=0 ttl=117 time=42 ms\n";
        assert_eq!(parse_ping_rtt_ms(out), Some(42.0));
        // iputils sub-millisecond form
        let out = "64 bytes from 127.0.0.1: icmp_seq=1 ttl=64 time<1 ms\n";
        assert_eq!(parse_ping_rtt_ms(out), Some(0.5));
        // no rtt fields at all (error output) → fallback signal
        assert_eq!(parse_ping_rtt_ms("ping: unknown host foo\n"), None);
        // empty output
        assert_eq!(parse_ping_rtt_ms(""), None);
    }

    #[test]
    fn parse_ping_rtt_ignores_non_numeric_time() {
        // garbled value must not panic; falls through to later lines/None
        let out = "weird time=abc ms\n64 bytes from 1.1.1.1: icmp_seq=2 time=55.5 ms\n";
        assert_eq!(parse_ping_rtt_ms(out), Some(55.5));
    }
}
