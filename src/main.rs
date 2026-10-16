use echo_daemon::auth::{require_scope, ApiKeyRegistry, AuthGate, Scope};
use echo_daemon::availability::AvailabilityEngine;
use echo_daemon::bidding::{
    award_key, auction_key, decode_auction_index, encode_auction_index, host_index_key,
    region_providers_key, sign_bid, verify_signed_bid, Auction, AuctionBook, AuctionRecord, Award,
    Bid, BidConfig, BidEngine, BidError, CreateAuction, SubmitBid, MAX_AUCTION_INDEX_IDS,
    MAX_AUCTION_WINDOW_SECS, MAX_PRICE_USD_PER_GB, MIN_PRICE_USD_PER_GB,
};
use echo_daemon::consumer::run_consumer_listener;
use echo_daemon::discovery::{DiscoveryCommand, DiscoveryConfig, DiscoveryService};
use echo_daemon::identity::NodeIdentity;
use echo_daemon::meter::MeteringEngine;
use echo_daemon::neon;
use echo_daemon::policy::{PolicyConfig, PolicyEngine};
use echo_daemon::security::{PeerViolation, SecurityCounters, SecurityEvent, SecuritySnapshot};
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
    extract::{Json, Path, State},
    http::StatusCode,
    middleware,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
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

// ── G6 marketplace DHT sync — wire-traffic bounds (not env-tunable) ──
/// Lifetime of `auction/*` and `award/*` value records on the DHT.
const DHT_MARKET_RECORD_TTL_SECS: u64 = 45;
/// Republish open auctions + the host index while we host any.
const DHT_AUCTION_PUBLISH_INTERVAL_SECS: u64 = 10;
/// Sweep for newly finalized awards to publish.
const DHT_AWARD_PUBLISH_INTERVAL_SECS: u64 = 2;
/// Hosts fetched per region provider lookup (bounds chained queries).
const MAX_DHT_PROVIDERS_PER_QUERY: usize = 16;
/// Per-counterparty cooldown between DHT award lookups from settlement.
const AWARD_LOOKUP_COOLDOWN_SECS: u64 = 120;
/// Cap on the award-lookup cooldown map.
const MAX_AWARD_LOOKUPS: usize = 512;
/// Awards republished from the log after a restart (bounded boot burst).
const MAX_AWARD_PUBLISH_BATCH: usize = 128;
/// HTTP timeout for signed remote bids.
const REMOTE_BID_TIMEOUT_SECS: u64 = 3;
/// Auto-bid remote-dedupe map cap (entries age out with auction windows).
const MAX_BID_TRACK: usize = 4096;

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
            max_upload_mbps: env_bandwidth("MAX_UPLOAD_MBPS", 100.0),
            max_download_mbps: env_bandwidth("MAX_DOWNLOAD_MBPS", 100.0),
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

/// Parse a bandwidth/capacity env var (Mbps). Unset → `default` (silent).
/// A value that parses but is zero, negative, or non-finite falls back to
/// `default` with a warning: the token-bucket rate limiter divides by the
/// refill rate when computing backpressure sleeps, and a zero capacity
/// would stall or disable the relay in surprising ways.
fn env_bandwidth(key: &str, default: f64) -> f64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match raw.parse::<f64>() {
            Ok(v) if v.is_finite() && v > 0.0 => v,
            Ok(v) => {
                warn!(key, value = %v, default, "invalid bandwidth setting (must be > 0); using default");
                default
            }
            Err(_) => {
                warn!(key, value = %raw, default, "unparseable bandwidth setting; using default");
                default
            }
        },
    }
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
    /// Security/health counters (handshake failures, policy denials, …).
    pub security: SecuritySnapshot,
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
    /// Security/health counters (see `/status` for field semantics).
    security: SecuritySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchRequest {
    pub region: String,
    pub min_upload_mbps: f64,
    pub min_download_mbps: f64,
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
    security: Arc<SecurityCounters>,
    /// This node's peer id — never advertise itself as a match candidate.
    local_peer_id: String,
    /// `POLICY_BLOCKED_PEERS` — also filtered from `/match` results so
    /// consumers never get pointed at blacklisted providers.
    blocked_peers: Arc<HashSet<String>>,
    /// Drop capability records older than this many seconds in `/match`
    /// (`0` disables). Stale rows advertise capacity the provider may no
    /// longer have.
    match_max_stale_secs: u64,
    /// Marketplace bidding engine (sealed-bid auctions, price discovery).
    bid_engine: Arc<BidEngine>,
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
        security: app.security.snapshot(),
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
                    security: app.security.snapshot(),
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
                    security: app.security.snapshot(),
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
                    security: app.security.snapshot(),
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
        security: app.security.snapshot(),
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

/// Maximum accepted `region` length for `/match` (bytes).
const MAX_REGION_LEN: usize = 64;

/// Validate a `/match` request before it hits the database.
/// Rejects empty/oversized/control-char regions and non-finite, negative,
/// or absurd bandwidth floors.
fn validate_match_request(req: &MatchRequest) -> Result<(), (StatusCode, String)> {
    let region = req.region.trim();
    if region.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "region must not be empty".to_string(),
        ));
    }
    if region.len() > MAX_REGION_LEN {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("region must be at most {MAX_REGION_LEN} bytes"),
        ));
    }
    if region.chars().any(|c| c.is_control()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "region must not contain control characters".to_string(),
        ));
    }
    for (name, value) in [
        ("min_upload_mbps", req.min_upload_mbps),
        ("min_download_mbps", req.min_download_mbps),
    ] {
        if !value.is_finite() || !(0.0..=1_000_000.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("{name} must be finite and within 0..=1000000"),
            ));
        }
    }
    Ok(())
}

/// Drop unusable/stale/blacklisted/self capability rows from a match result.
///
/// A provider is kept only when it is:
/// - not this node (self-matches are useless and loop-prone),
/// - not in `POLICY_BLOCKED_PEERS`,
/// - not at session capacity (`active >= max`, `max > 0`),
/// - capable of consumer-mode dialing (non-empty Noise static key for pinning),
/// - freshly published (age ≤ `max_stale_secs`, `0` disables the check).
fn filter_match_providers(
    providers: Vec<echo_daemon::CapabilityDescriptor>,
    local_peer_id: &str,
    blocked_peers: &HashSet<String>,
    now_secs: u64,
    max_stale_secs: u64,
) -> Vec<echo_daemon::CapabilityDescriptor> {
    providers
        .into_iter()
        .filter(|p| {
            if p.peer_id == local_peer_id {
                return false;
            }
            if blocked_peers.contains(&p.peer_id) {
                return false;
            }
            if p.max_sessions > 0 && p.active_sessions >= p.max_sessions {
                return false;
            }
            if p.noise_public_key.is_empty() {
                return false;
            }
            if max_stale_secs > 0 && p.last_updated > 0 {
                let age = now_secs.saturating_sub(p.last_updated);
                if age > max_stale_secs {
                    return false;
                }
            }
            true
        })
        .collect()
}

async fn handle_match_providers(
    State(app): State<AppState>,
    Json(req): Json<MatchRequest>,
) -> Result<Json<MatchResponse>, (StatusCode, String)> {
    validate_match_request(&req)?;
    let region = req.region.trim().to_string();
    info!(
        region = %region,
        min_upload = req.min_upload_mbps,
        min_download = req.min_download_mbps,
        "provider match request"
    );

    match app
        .backend
        .get_capabilities_in_region(
            &region,
            req.min_upload_mbps,
            req.min_download_mbps,
            app.provider_match_limit,
        )
        .await
    {
        Ok(providers) => {
            let filtered = filter_match_providers(
                providers,
                &app.local_peer_id,
                &app.blocked_peers,
                now_epoch_secs(),
                app.match_max_stale_secs,
            );
            let count = filtered.len();
            info!(count, "provider match completed");
            Ok(Json(MatchResponse {
                providers: filtered,
                count,
            }))
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

// ──────────────────────────────────────────────────────────────
// Marketplace bidding endpoints (price discovery)
// ──────────────────────────────────────────────────────────────

/// Map engine rejections to HTTP status codes.
fn bid_err_to_http(err: BidError) -> (StatusCode, String) {
    let status = match &err {
        BidError::Invalid(_) | BidError::BudgetExceeded { .. } => StatusCode::BAD_REQUEST,
        BidError::NotFound(_) => StatusCode::NOT_FOUND,
        BidError::Closed(_) | BidError::DuplicateBid | BidError::TooManyBids => StatusCode::CONFLICT,
        BidError::NotRequester | BidError::Blocked => StatusCode::FORBIDDEN,
    };
    (status, err.to_string())
}

#[derive(Debug, Serialize)]
struct AuctionListResponse {
    auctions: Vec<Auction>,
    count: usize,
}

#[derive(Debug, Serialize)]
struct AwardListResponse {
    awards: Vec<Award>,
    count: usize,
}

/// `POST /bid` — open a new auction (the consumer's bandwidth request).
async fn handle_create_auction(
    State(app): State<AppState>,
    Json(req): Json<CreateAuction>,
) -> Result<Json<Auction>, (StatusCode, String)> {
    let auction = app
        .bid_engine
        .create_auction(req, &app.local_peer_id, now_epoch_secs())
        .map_err(bid_err_to_http)?;
    Ok(Json(auction))
}

/// `GET /auctions` — every open auction (providers poll this to find work).
async fn handle_list_auctions(State(app): State<AppState>) -> Json<AuctionListResponse> {
    let mut auctions = app.bid_engine.open_auctions();
    auctions.sort_by_key(|a| std::cmp::Reverse(a.created_at));
    let count = auctions.len();
    Json(AuctionListResponse { auctions, count })
}

/// `GET /auctions/:id` — full auction state: bids (revealed) + award.
async fn handle_get_auction(
    State(app): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Auction>, (StatusCode, String)> {
    app.bid_engine
        .get_auction(&id)
        .map(Json)
        .ok_or_else(|| bid_err_to_http(BidError::NotFound(id)))
}

/// `POST /auctions/:id/bid` — submit a sealed bid. Local (unsigned) bids
/// are attributed to this node exactly as before (pre-G6); a signed
/// payload proves a remote provider's ed25519 identity, so the host can
/// attribute — and reputation-score — a bidder it has never met. Snapshots
/// come from *this host's* records for the verified peer, keeping scoring
/// deterministic per bid.
async fn handle_submit_bid(
    State(app): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<SubmitBid>,
) -> Result<Json<Bid>, (StatusCode, String)> {
    let signed = [
        req.provider_peer_id.is_some(),
        req.public_key.is_some(),
        req.signature.is_some(),
    ];
    let (peer, reputation, latency_ms) = if signed.iter().any(|&p| p) {
        // Remote signed bid: all three fields or none.
        if !signed.iter().all(|&p| p) {
            return Err(bid_err_to_http(BidError::Invalid(
                "signed bid requires provider_peer_id, public_key and signature together"
                    .into(),
            )));
        }
        let peer = verify_signed_bid(
            &id,
            req.price_usd_per_gb,
            req.provider_peer_id.as_deref().unwrap_or_default(),
            req.public_key.as_deref().unwrap_or_default(),
            req.signature.as_deref().unwrap_or_default(),
        )
        .map_err(bid_err_to_http)?;
        // DB-sourced snapshots for the *verified* peer; a stranger with
        // no history scores as unknown (rep 0, neutral latency) rather
        // than best-case.
        let reputation = app
            .backend
            .get_peer_reputation(&peer)
            .await
            .ok()
            .flatten()
            .map(|r| r.reputation_score)
            .unwrap_or(0.0);
        let latency_ms = app
            .backend
            .get_capability_for_peer(&peer)
            .await
            .ok()
            .flatten()
            .map(|c| c.avg_latency_ms)
            .unwrap_or_else(|| app.bid_engine.config().ref_latency_ms);
        (peer, reputation, latency_ms)
    } else {
        // Legacy local attribution (pre-G6 behavior).
        let peer = app.local_peer_id.clone();
        let reputation = app
            .backend
            .get_peer_reputation(&peer)
            .await
            .ok()
            .flatten()
            .map(|r| r.reputation_score)
            .unwrap_or(0.0);
        let latency_ms = app
            .backend
            .get_capability_for_peer(&peer)
            .await
            .ok()
            .flatten()
            .map(|c| c.avg_latency_ms)
            .unwrap_or(0.0);
        (peer, reputation, latency_ms)
    };
    let bid = app
        .bid_engine
        .submit_bid(&id, &peer, req.price_usd_per_gb, reputation, latency_ms, now_epoch_secs())
        .map_err(bid_err_to_http)?;
    Ok(Json(bid))
}

/// `POST /auctions/:id/close` — finalize early and award the winner
/// (requester only).
async fn handle_close_auction(
    State(app): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Auction>, (StatusCode, String)> {
    let auction = app
        .bid_engine
        .close_auction(&id, &app.local_peer_id, now_epoch_secs())
        .map_err(bid_err_to_http)?;
    Ok(Json(auction))
}

/// `POST /auctions/:id/cancel` — abort an open auction (requester only).
async fn handle_cancel_auction(
    State(app): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Auction>, (StatusCode, String)> {
    let auction = app
        .bid_engine
        .cancel_auction(&id, &app.local_peer_id)
        .map_err(bid_err_to_http)?;
    Ok(Json(auction))
}

/// `GET /awards` — recent auction outcomes (locked market prices).
async fn handle_list_awards(State(app): State<AppState>) -> Json<AwardListResponse> {
    let awards = app.bid_engine.recent_awards(32);
    let count = awards.len();
    Json(AwardListResponse { awards, count })
}

/// Admin observability: full security snapshot + the most recent security
/// events (newest first) + the dirtiest peers by attributed violations
/// (soft-slashing input, `accountability.rs`). Gated behind the `control`
/// scope.
#[derive(Debug, Serialize)]
struct SecurityReport {
    snapshot: SecuritySnapshot,
    recent_events: Vec<SecurityEvent>,
    peer_violations: Vec<PeerViolation>,
}

async fn handle_security(State(app): State<AppState>) -> Json<SecurityReport> {
    Json(SecurityReport {
        snapshot: app.security.snapshot(),
        recent_events: app.security.recent_events(32),
        peer_violations: app.security.top_peer_violations(32),
    })
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
    // AVAIL_EWMA_ALPHA smooths advertised capacity (0.3 default; 1.0 = raw).
    let availability = Arc::new(RwLock::new(
        AvailabilityEngine::new(
            config.max_upload_mbps,
            config.max_download_mbps,
            config.max_sessions,
        )
        .with_ewma_alpha(env_f64(
            "AVAIL_EWMA_ALPHA",
            echo_daemon::availability::DEFAULT_EWMA_ALPHA,
        )),
    ));

    // Step 3.5: Security counters (handshake failures, policy denials, …)
    // shared by the tunnel service, auth middleware, and HTTP state.
    // `from_env` arms the burst detector (SECURITY_ALERT_* knobs).
    let security = Arc::new(SecurityCounters::from_env());

    // Step 3.6: Provider admission policy (blacklist / reputation / latency /
    // quotas). An entirely-open policy keeps the hot path free of DB lookups
    // (policy stays None); a single constraint activates the engine.
    let policy_config = PolicyConfig::from_env();
    // The blocked list doubles as a `/match` filter (consumers must never be
    // pointed at a blacklisted provider) — clone it before the config is
    // moved into the engine.
    let blocked_peers = Arc::new(policy_config.blocked_peers.clone());
    let policy = if policy_config.is_open() {
        info!("provider policy engine disabled (no POLICY_* constraints set)");
        None
    } else {
        info!(
            min_reputation = policy_config.min_reputation,
            max_latency_ms = ?policy_config.max_latency_ms,
            max_sessions_per_peer = policy_config.max_sessions_per_peer,
            max_sessions_per_region = policy_config.max_sessions_per_region,
            blocked_peers = policy_config.blocked_peers.len(),
            "provider policy engine enabled"
        );
        Some(PolicyEngine::new(policy_config))
    };

    // Step 3.7: Marketplace bidding engine (sealed-bid auctions with
    // quality-adjusted winner selection + Vickrey pricing). Shares the
    // policy engine's blocked-peer list so blacklisted peers can neither
    // open auctions nor bid on them.
    let bid_engine = Arc::new(BidEngine::new(BidConfig::from_env_with_blocked(
        blocked_peers.as_ref().clone(),
    )));
    {
        let cfg = bid_engine.config();
        info!(
            default_window_secs = cfg.default_window_secs,
            w_reputation = cfg.w_reputation,
            w_latency = cfg.w_latency,
            pricing = ?cfg.pricing,
            "marketplace bidding engine ready"
        );
    }

    // Step 3.8: Auction sweeper — finalizes auctions past their deadline
    // (award winner or expire). Runs in both node modes: consumers open
    // auctions, providers bid on them; either may need finalization.
    {
        let engine = bid_engine.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                for auction in engine.expire_stale(now_epoch_secs()) {
                    match auction.status {
                        echo_daemon::bidding::AuctionStatus::Awarded => {
                            let award = auction.award.expect("awarded auction has award");
                            info!(
                                auction = %auction.id,
                                provider = %award.provider_peer_id,
                                price = award.price_usd_per_gb,
                                bids = auction.bids.len(),
                                "auction closed at deadline"
                            );
                        }
                        echo_daemon::bidding::AuctionStatus::Expired => {
                            debug!(auction = %auction.id, "auction expired with no bids");
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    // Step 3.85: Foreign auction book — auctions discovered over the DHT
    // (G6 cross-node marketplace). Kept apart from the local sealed-bid
    // registry: we bid on these remotely over signed HTTP.
    let auction_book = Arc::new(AuctionBook::new());

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
    // Handshake deadline (ms) — slowloris / stalled-handshake guard for both
    // the provider accept path and the consumer dial path. 0 disables.
    let handshake_timeout_ms = env_u64("TUNNEL_HANDSHAKE_TIMEOUT_MS", 10_000);

    let relay_config = echo_daemon::tunnel::RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: config.relay_target.clone(),
        local_peer_id: identity.peer_id_str().to_string(),
        max_upload_mbps: config.max_upload_mbps,
        max_download_mbps: config.max_download_mbps,
        policy: policy.clone(),
        security: security.clone(),
        handshake_timeout_ms,
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
            policy: tunnel_service.relay_config().policy.clone(),
            security: tunnel_service.relay_config().security.clone(),
            handshake_timeout_ms: tunnel_service.relay_config().handshake_timeout_ms,
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

    // Step 5: Initialize discovery (libp2p swarm) — DHT_* env config:
    // bootstrap peers, network id namespace, optional local-only dev mode.
    let discovery_config = DiscoveryConfig::from_env();
    let (mut discovery, discovery_cmd, mut discovery_rx) = DiscoveryService::new(
        &identity,
        &discovery_config,
    )
    .await
    .map_err(|e| {
        error!(error = %e, "discovery init failed");
        anyhow::anyhow!("discovery init failed: {}", e)
    })?;

    info!(
        peer_id = %identity.peer_id_str(),
        network_id = %discovery_config.network_id,
        local_only = discovery_config.local_only,
        bootstrap_peers = discovery_config.bootstrap.len(),
        "discovery initialized"
    );

    // Spawn discovery event loop
    tokio::spawn(async move {
        discovery.run().await;
    });

    // Spawn discovery event handler — routes market records (G6) into the
    // foreign auction book / award log and chains follow-up DHT lookups.
    // Kademlia has no prefix queries, so discovery walks explicit keys:
    //   providers(auctions/<region>) → index(auctions_by_host/<host>)
    //     → records(auction/<id>)
    {
        let cmd = discovery_cmd.clone();
        let engine = bid_engine.clone();
        let book = auction_book.clone();
        let self_peer = identity.peer_id_str().to_string();
        tokio::spawn(async move {
            while let Some(event) = discovery_rx.recv().await {
                match event {
                    echo_daemon::discovery::DiscoveryEvent::PeerFound(peer_id, addr) => {
                        info!(peer = %peer_id, addr = %addr, "peer discovered via DHT");
                    }
                    echo_daemon::discovery::DiscoveryEvent::PeerDisconnected(peer_id) => {
                        info!(peer = %peer_id, "peer disconnected");
                    }
                    echo_daemon::discovery::DiscoveryEvent::RecordPublished { key } => {
                        debug!(key, "record published to DHT");
                    }
                    echo_daemon::discovery::DiscoveryEvent::RecordFound { key, value } => {
                        if let Some(id) = key.strip_prefix("auction/") {
                            // An open auction published by its consumer host.
                            match AuctionRecord::from_bytes(&value) {
                                Ok(rec) => {
                                    let now = now_epoch_secs();
                                    if rec.requester_peer_id == self_peer {
                                        debug!(auction = id, "skipped our own DHT auction record");
                                    } else if book.upsert(rec, &self_peer, now) {
                                        info!(auction = id, "foreign auction imported from DHT");
                                    }
                                }
                                Err(e) => debug!(auction = id, error = %e, "bad auction record"),
                            }
                        } else if let Some(host) = key.strip_prefix("auctions_by_host/") {
                            // A host's index of its open auction ids →
                            // fetch each record we don't have yet.
                            match decode_auction_index(&value) {
                                Ok(ids) => {
                                    let now = now_epoch_secs();
                                    for id in ids {
                                        if !book.contains(&id) && !book.recently_rejected(&id, now)
                                        {
                                            let _ = cmd.send(DiscoveryCommand::GetRecord {
                                                key: auction_key(&id),
                                            });
                                        }
                                    }
                                }
                                Err(e) => debug!(host, error = %e, "bad auction index record"),
                            }
                        } else if key.strip_prefix("award/").is_some() {
                            // A finalized award we (the provider) won —
                            // unlocks locked-price settlement for that
                            // consumer's receipts.
                            if value.len() > 1024 {
                                debug!(key, "award record too large; ignored");
                            } else {
                                match serde_json::from_slice::<Award>(&value) {
                                    Ok(award) => {
                                        if engine.import_foreign_award(award, &self_peer) {
                                            debug!(key, "award imported from DHT");
                                        }
                                    }
                                    Err(e) => debug!(key, error = %e, "bad award record"),
                                }
                            }
                        } else {
                            debug!(key, "unrecognized DHT record key");
                        }
                    }
                    echo_daemon::discovery::DiscoveryEvent::RecordNotFound { key } => {
                        debug!(key, "no DHT record for key");
                    }
                    echo_daemon::discovery::DiscoveryEvent::ProvidersFound { key, providers } => {
                        // Region host lookup → chain one index fetch per host.
                        if key.strip_prefix("auctions/").is_some() {
                            for peer in providers
                                .into_iter()
                                .take(MAX_DHT_PROVIDERS_PER_QUERY)
                            {
                                let _ = cmd.send(DiscoveryCommand::GetRecord {
                                    key: host_index_key(&peer.to_string()),
                                });
                            }
                        } else {
                            debug!(key, count = providers.len(), "providers found via DHT");
                        }
                    }
                }
            }
        });
    }

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
        let security_clone = security.clone();
        // Market-discovered prices: receipts whose counterparty (the
        // consumer) won a marketplace auction here settle at the award's
        // locked USD/GB price instead of the flat SETTLEMENT_RATE_USD_PER_GB.
        let bid_pricing = bid_engine.clone();
        // G6: a counterparty's first receipt triggers a DHT lookup for the
        // award record (published by its consumer host). Cooldown-bounded;
        // an imported award reprices *subsequent* settlements.
        let award_lookup_cmd = discovery_cmd.clone();
        let award_lookup_peer = identity.peer_id_str().to_string();
        let award_cooldown = Arc::new(std::sync::Mutex::new(
            std::collections::HashMap::<String, u64>::new(),
        ));
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
                                        let market_price =
                                            bid_pricing.awarded_price_for(&receipt.counterparty_peer_id);
                                        if let Err(e) = receipt_settlement
                                            .settle_receipt_at_price(receipt, market_price)
                                            .await
                                        {
                                            error!(error = %e, "receipt settlement flush failed during shutdown");
                                        }
                                    }
                                }
                                Ok(false) => {
                                    security_clone.inc_settlement_anomaly(&format!("receipt {}", receipt.receipt_id));
                                    warn!(
                                        receipt = %receipt.receipt_id,
                                        session = %receipt.session_id,
                                        "receipt signature verification failed during shutdown flush, skipping"
                                    );
                                }
                                Err(e) => {
                                    security_clone.inc_settlement_anomaly(&format!("receipt {}", receipt.receipt_id));
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
                                            // G6: cooldown-bounded DHT award
                                            // lookup for this counterparty.
                                            {
                                                let mut map = award_cooldown
                                                    .lock()
                                                    .expect("award cooldown lock poisoned");
                                                let now = now_epoch_secs();
                                                let cooled = map
                                                    .get(&receipt.counterparty_peer_id)
                                                    .is_none_or(|t| {
                                                        now.saturating_sub(*t)
                                                            >= AWARD_LOOKUP_COOLDOWN_SECS
                                                    });
                                                if cooled {
                                                    if map.len() >= MAX_AWARD_LOOKUPS {
                                                        map.clear();
                                                    }
                                                    map.insert(
                                                        receipt.counterparty_peer_id.clone(),
                                                        now,
                                                    );
                                                    let _ = award_lookup_cmd
                                                        .send(DiscoveryCommand::GetRecord {
                                                            key: award_key(
                                                                &receipt.counterparty_peer_id,
                                                                &award_lookup_peer,
                                                            ),
                                                        });
                                                }
                                            }
                                            // Accumulate the verified receipt toward payout
                                            // at the market price when this counterparty
                                            // won an auction, else the flat rate.
                                            let market_price = bid_pricing
                                                .awarded_price_for(&receipt.counterparty_peer_id);
                                            if let Err(e) = receipt_settlement
                                                .settle_receipt_at_price(receipt, market_price)
                                                .await
                                            {
                                                error!(error = %e, "payout accumulation failed");
                                            }
                                        }
                                    }
                                    Ok(false) => {
                                        security_clone.inc_settlement_anomaly(&format!("receipt {}", receipt.receipt_id));
                                        warn!(
                                            receipt = %receipt.receipt_id,
                                            session = %receipt.session_id,
                                            seq = receipt.sequence_number,
                                            "receipt signature verification failed, discarding"
                                        );
                                    }
                                    Err(e) => {
                                        security_clone.inc_settlement_anomaly(&format!("receipt {}", receipt.receipt_id));
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

    // Step 9.45: Marketplace DHT sync (G6) — publish what only we can:
    // our open auctions + host/region indexes (10 s refresh against a
    // 45 s record TTL) and newly finalized awards we requested (2 s
    // catch-up cursor). Auction *discovery* lives in the auto-bid task.
    {
        let engine = bid_engine.clone();
        let cmd = discovery_cmd.clone();
        let self_peer = identity.peer_id_str().to_string();
        let listen_port = config
            .listen_addr
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u16>().ok());
        let advertise_override = std::env::var("API_ADVERTISE_ADDR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let state_ip = state.clone();
        tokio::spawn(async move {
            let mut award_cursor = 0usize;
            let mut award_tick =
                tokio::time::interval(Duration::from_secs(DHT_AWARD_PUBLISH_INTERVAL_SECS));
            let mut publish_tick =
                tokio::time::interval(Duration::from_secs(DHT_AUCTION_PUBLISH_INTERVAL_SECS));
            loop {
                tokio::select! {
                    _ = award_tick.tick() => {
                        let (fresh, next) = engine.awards_from(award_cursor);
                        award_cursor = next;
                        // A restart begins at cursor 0 — cap the boot burst
                        // so a long award log can't flood the DHT.
                        let start = fresh.len().saturating_sub(MAX_AWARD_PUBLISH_BATCH);
                        for award in &fresh[start..] {
                            if award.requester_peer_id != self_peer {
                                continue; // only publish awards we requested
                            }
                            match serde_json::to_vec(award) {
                                Ok(value) => {
                                    let _ = cmd.send(DiscoveryCommand::PutRecord {
                                        key: award_key(
                                            &award.requester_peer_id,
                                            &award.provider_peer_id,
                                        ),
                                        value,
                                        ttl: Some(Duration::from_secs(
                                            DHT_MARKET_RECORD_TTL_SECS,
                                        )),
                                    });
                                    debug!(auction = %award.auction_id, "award published to DHT");
                                }
                                Err(e) => warn!(error = %e, "award encode failed"),
                            }
                        }
                    }
                    _ = publish_tick.tick() => {
                        let open = engine.open_auctions();
                        if open.is_empty() {
                            continue;
                        }
                        // Host REST address for remote bidders: explicit
                        // override, else detected IP + listen port. Deferred
                        // until both are known.
                        let host_api_addr = if let Some(addr) = advertise_override.clone() {
                            if addr.starts_with("http://") || addr.starts_with("https://") {
                                Some(addr)
                            } else {
                                Some(format!("http://{addr}"))
                            }
                        } else {
                            let ip = state_ip.read().await.ip_address.clone();
                            match (ip, listen_port) {
                                (Some(ip), Some(port)) => Some(format!("http://{ip}:{port}")),
                                _ => None,
                            }
                        };
                        let Some(host_api_addr) = host_api_addr else {
                            debug!(
                                "auction DHT publish deferred: public address unknown \
                                 (set API_ADVERTISE_ADDR to publish from behind NAT)"
                            );
                            continue;
                        };
                        // Index of our open auction ids (capped, deduped).
                        let mut ids: Vec<String> = Vec::new();
                        for a in &open {
                            if ids.len() >= MAX_AUCTION_INDEX_IDS {
                                break;
                            }
                            if !ids.contains(&a.id) {
                                ids.push(a.id.clone());
                            }
                        }
                        if let Ok(value) = encode_auction_index(&ids) {
                            let _ = cmd.send(DiscoveryCommand::PutRecord {
                                key: host_index_key(&self_peer),
                                value,
                                ttl: Some(Duration::from_secs(DHT_MARKET_RECORD_TTL_SECS)),
                            });
                        }
                        // Announce ourselves as a host per distinct region —
                        // providers discover hosts through this key.
                        let mut regions: std::collections::BTreeSet<&str> = Default::default();
                        for a in &open {
                            regions.insert(a.region.as_str());
                        }
                        for region in regions {
                            let _ = cmd.send(DiscoveryCommand::StartProviding {
                                key: region_providers_key(region),
                            });
                        }
                        // The indexed auction records themselves.
                        for id in &ids {
                            let Some(a) = open.iter().find(|a| &a.id == id) else {
                                continue;
                            };
                            let rec = AuctionRecord::from_auction(a, host_api_addr.clone());
                            match rec.to_bytes() {
                                Ok(value) => {
                                    let _ = cmd.send(DiscoveryCommand::PutRecord {
                                        key: auction_key(id),
                                        value,
                                        ttl: Some(Duration::from_secs(
                                            DHT_MARKET_RECORD_TTL_SECS,
                                        )),
                                    });
                                }
                                Err(e) => {
                                    warn!(auction = %id, error = %e, "auction record encode failed")
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // Step 9.5: Marketplace auto-bid (provider mode, `BID_AUTO=true`).
    // Polls open auctions and submits a sealed bid at this provider's ask
    // price whenever it fits the auction's budget cap and capacity floor —
    // this is what turns isolated nodes into a live bidding market.
    let bid_auto = std::env::var("BID_AUTO")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if config.node_mode == "provider" && bid_auto {
        let ask_raw = env_f64("BID_PRICE_USD_PER_GB", config.settlement_rate_usd_per_gb);
        let ask_price = if ask_raw.is_finite() {
            ask_raw.clamp(MIN_PRICE_USD_PER_GB, MAX_PRICE_USD_PER_GB)
        } else {
            config.settlement_rate_usd_per_gb
        };
        let poll_secs = env_u64("BID_POLL_SECS", 3).max(1);
        let engine = bid_engine.clone();
        let bid_backend = backend.clone();
        let bid_peer_id = identity.peer_id_str().to_string();
        // G6: signed remote bidding on foreign auctions discovered over
        // the DHT (same budget/capacity gates as local bids).
        let disc_cmd = discovery_cmd.clone();
        let book = auction_book.clone();
        let bid_region = config.region.clone();
        let remote_pk = echo_daemon::base64_encode(&identity.public_key_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            identity
                .keypair_bytes
                .as_slice()
                .try_into()
                .expect("identity keypair is 32 bytes"),
        );
        let http = reqwest::Client::new();
        let mut bid_track: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        info!(ask_price, poll_secs, "marketplace auto-bid enabled");
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(poll_secs));
            loop {
                tick.tick().await;
                let now = now_epoch_secs();
                // Snapshot this provider's reputation + capability once per
                // tick; both feed bid quality and the capacity floor check.
                let (reputation, latency_ms, capability) = {
                    let rep = bid_backend
                        .get_peer_reputation(&bid_peer_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|r| r.reputation_score)
                        .unwrap_or(0.0);
                    let cap = bid_backend
                        .get_capability_for_peer(&bid_peer_id)
                        .await
                        .ok()
                        .flatten();
                    let lat = cap.as_ref().map(|c| c.avg_latency_ms).unwrap_or(0.0);
                    (rep, lat, cap)
                };
                for auction in engine.open_auctions() {
                    // Never bid on our own auction; skip ones we cannot
                    // afford or serve.
                    if auction.requester_peer_id == bid_peer_id {
                        continue;
                    }
                    if ask_price > auction.max_price_usd_per_gb {
                        continue;
                    }
                    if let Some(cap) = &capability {
                        if cap.upload_cap_mbps < auction.min_upload_mbps
                            || cap.download_cap_mbps < auction.min_download_mbps
                        {
                            continue;
                        }
                    }
                    let price = ask_price.min(auction.max_price_usd_per_gb);
                    match engine.submit_bid(
                        &auction.id,
                        &bid_peer_id,
                        price,
                        reputation,
                        latency_ms,
                        now,
                    ) {
                        Ok(bid) => {
                            info!(auction = %auction.id, price = bid.price_usd_per_gb, "auto-bid submitted");
                        }
                        Err(BidError::DuplicateBid) => {} // already in — normal
                        Err(e) => debug!(auction = %auction.id, error = %e, "auto-bid skipped"),
                    }
                }
                // G6: trigger the region host lookup (the event handler
                // chains index + auction-record fetches) and bid on every
                // foreign auction already in the book, signed so the host
                // can verify our identity without an auth token.
                let _ = disc_cmd.send(DiscoveryCommand::GetProviders {
                    key: region_providers_key(&bid_region),
                });
                for rec in book.open(now) {
                    if rec.requester_peer_id == bid_peer_id
                        || rec.is_expired(now)
                        || bid_track.contains_key(&rec.id)
                    {
                        continue;
                    }
                    if ask_price > rec.max_price_usd_per_gb {
                        continue;
                    }
                    if let Some(cap) = &capability {
                        if cap.upload_cap_mbps < rec.min_upload_mbps
                            || cap.download_cap_mbps < rec.min_download_mbps
                        {
                            continue;
                        }
                    }
                    let price = ask_price.min(rec.max_price_usd_per_gb);
                    let body = SubmitBid {
                        price_usd_per_gb: price,
                        provider_peer_id: Some(bid_peer_id.clone()),
                        public_key: Some(remote_pk.clone()),
                        signature: Some(sign_bid(&signing_key, &rec.id, price)),
                    };
                    let url = format!(
                        "{}/auctions/{}/bid",
                        rec.host_api_addr.trim_end_matches('/'),
                        rec.id
                    );
                    match http
                        .post(&url)
                        .json(&body)
                        .timeout(Duration::from_secs(REMOTE_BID_TIMEOUT_SECS))
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => {
                            info!(
                                auction = %rec.id,
                                host = %rec.host_api_addr,
                                price,
                                "remote auto-bid accepted"
                            );
                            if bid_track.len() >= MAX_BID_TRACK {
                                bid_track.clear();
                            }
                            bid_track.insert(rec.id.clone(), now);
                        }
                        Ok(resp) => debug!(
                            auction = %rec.id,
                            status = %resp.status(),
                            "remote auto-bid rejected"
                        ),
                        Err(e) => debug!(auction = %rec.id, error = %e, "remote auto-bid failed"),
                    }
                }
                // Age out dedupe entries as their auctions expire.
                bid_track.retain(|_, t| now.saturating_sub(*t) <= MAX_AUCTION_WINDOW_SECS);
            }
        });
    }

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

    // Scoped API keys (API_KEYS JSON + legacy API_KEY). Empty registry →
    // auth disabled (open control plane, the historical default).
    let api_registry = Arc::new(ApiKeyRegistry::from_env());
    if api_registry.is_empty() {
        info!("API key authentication disabled (set API_KEY or API_KEYS env var to enable)");
    }

    // /control and /settlements are protected by scoped API key middleware.
    // `.layer` applies to routes registered before it, so each route group
    // gets its own scope gate.
    let control_routes = Router::new()
        .route("/control", post(handle_control))
        .route("/security", get(handle_security))
        .layer(middleware::from_fn_with_state(
            AuthGate {
                registry: api_registry.clone(),
                required: Scope::Control,
                security: security.clone(),
            },
            require_scope,
        ))
        .route("/settlements", get(handle_settlements))
        .layer(middleware::from_fn_with_state(
            AuthGate {
                registry: api_registry.clone(),
                required: Scope::Settlements,
                security: security.clone(),
            },
            require_scope,
        ));

    // Public-API hardening: cap request bodies (all control-plane JSON
    // payloads are tiny; /match is a few hundred bytes at most).
    let body_limit = axum::extract::DefaultBodyLimit::max(16 * 1024);

    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/nodes", get(handle_nodes))
        .route("/history", get(handle_history))
        .route("/status", get(handle_status))
        .route("/health", get(handle_health))
        .route("/match", post(handle_match_providers))
        // Marketplace bidding (price discovery): open auctions, bid,
        // close/cancel, and the award log. Public like /match — the
        // control plane stays open unless API_KEY(S) are set.
        .route("/bid", post(handle_create_auction))
        .route("/auctions", get(handle_list_auctions))
        .route("/auctions/:id", get(handle_get_auction))
        .route("/auctions/:id/bid", post(handle_submit_bid))
        .route("/auctions/:id/close", post(handle_close_auction))
        .route("/auctions/:id/cancel", post(handle_cancel_auction))
        .route("/awards", get(handle_list_awards))
        .merge(control_routes)
        .layer(cors)
        .layer(body_limit)
        .with_state(AppState {
            metrics: state,
            backend: backend.clone(),
            settlement: settlement_engine,
            unhealthy_threshold_secs,
            provider_match_limit: 10,
            security,
            local_peer_id: identity.peer_id_str().to_string(),
            blocked_peers,
            match_max_stale_secs: env_u64("MATCH_MAX_STALE_SECS", 300),
            bid_engine,
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

    // Time-boxed final dashboard telemetry flush; unsent frames are spooled
    // to disk and replayed on the next boot.
    if let Some(reporter) = &reporter {
        reporter.shutdown();
        sleep(Duration::from_secs(1)).await;
    }

    info!("shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use echo_daemon::constant_time_eq;

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

    #[test]
    fn env_bandwidth_sanitizes_invalid_values() {
        // Unique key so this test never touches real daemon config.
        let key = "ECHO_TEST_BANDWIDTH_ENV";

        // Unset → default, silently.
        std::env::remove_var(key);
        assert_eq!(env_bandwidth(key, 100.0), 100.0);

        // Valid values pass through.
        std::env::set_var(key, "42.5");
        assert_eq!(env_bandwidth(key, 100.0), 42.5);

        // Zero / negative / non-finite / unparseable → default.
        std::env::set_var(key, "0");
        assert_eq!(env_bandwidth(key, 100.0), 100.0);
        std::env::set_var(key, "-3.0");
        assert_eq!(env_bandwidth(key, 100.0), 100.0);
        std::env::set_var(key, "nan");
        assert_eq!(env_bandwidth(key, 100.0), 100.0);
        std::env::set_var(key, "inf");
        assert_eq!(env_bandwidth(key, 100.0), 100.0);
        std::env::set_var(key, "not-a-number");
        assert_eq!(env_bandwidth(key, 100.0), 100.0);

        std::env::remove_var(key);
    }

    // ── Phase 1: public API hardening / provider filtering ────────

    fn req(region: &str, up: f64, down: f64) -> MatchRequest {
        MatchRequest {
            region: region.to_string(),
            min_upload_mbps: up,
            min_download_mbps: down,
        }
    }

    #[test]
    fn match_request_validation_hardens_input() {
        // Valid input passes (incl. surrounding whitespace).
        assert!(validate_match_request(&req(" us-west ", 10.0, 50.0)).is_ok());
        assert!(validate_match_request(&req("a", 0.0, 0.0)).is_ok());

        // Region: empty / oversized / control characters → 400.
        assert!(validate_match_request(&req("   ", 0.0, 0.0)).is_err());
        assert!(validate_match_request(&req(&"x".repeat(65), 0.0, 0.0)).is_err());
        assert!(validate_match_request(&req("west\u{0007}region", 0.0, 0.0)).is_err());

        // Bandwidth floors: negative / non-finite / absurd → 400.
        assert!(validate_match_request(&req("eu", -1.0, 0.0)).is_err());
        assert!(validate_match_request(&req("eu", 0.0, -0.5)).is_err());
        assert!(validate_match_request(&req("eu", 1_000_001.0, 0.0)).is_err());
        assert!(validate_match_request(&req("eu", f64::INFINITY, 0.0)).is_err());

        // Rejections carry a 400 status.
        let (status, msg) = validate_match_request(&req("", 0.0, 0.0)).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!msg.is_empty());
    }

    fn cap(
        peer_id: &str,
        last_updated: u64,
        active_sessions: u32,
        max_sessions: u32,
        noise_key: Vec<u8>,
    ) -> echo_daemon::CapabilityDescriptor {
        echo_daemon::CapabilityDescriptor {
            peer_id: peer_id.to_string(),
            public_key: vec![1, 2, 3],
            region: "us-west".to_string(),
            upload_cap_mbps: 100.0,
            download_cap_mbps: 100.0,
            avg_latency_ms: 20.0,
            reputation_score: 0.8,
            supported_encryption: vec!["noise-xx".to_string()],
            max_sessions,
            active_sessions,
            last_updated,
            noise_public_key: noise_key,
        }
    }

    #[test]
    fn match_filter_drops_self_blocked_full_stale_and_keyless_providers() {
        let now = 1_790_000_000u64;
        let blocked: HashSet<String> = ["bad-peer".to_string()].into_iter().collect();
        let providers = vec![
            cap("self-peer", now, 0, 10, vec![9; 32]),
            cap("bad-peer", now, 0, 10, vec![9; 32]),
            cap("full-peer", now, 5, 5, vec![9; 32]),
            cap("stale-peer", now - 400, 0, 10, vec![9; 32]),
            cap("keyless-peer", now, 0, 10, vec![]),
            cap("good-peer", now, 3, 10, vec![9; 32]),
        ];

        let out =
            filter_match_providers(providers, "self-peer", &blocked, now, 300);
        let ids: Vec<_> = out.iter().map(|p| p.peer_id.as_str()).collect();
        assert_eq!(ids, vec!["good-peer"], "only a fresh, dialable, unblocked, non-full non-self provider survives");
    }

    #[test]
    fn match_filter_rules_can_be_disabled_or_relaxed() {
        let now = 1_790_000_000u64;
        let empty: HashSet<String> = HashSet::new();

        // Stale check disabled (0) → stale provider kept.
        let out = filter_match_providers(
            vec![cap("stale-peer", now - 999_999, 0, 10, vec![9; 32])],
            "self-peer",
            &empty,
            now,
            0,
        );
        assert_eq!(out.len(), 1);

        // max_sessions = 0 → treated as "unlimited", never "full".
        let out = filter_match_providers(
            vec![cap("zero-max", now, 7, 0, vec![9; 32])],
            "self-peer",
            &empty,
            now,
            300,
        );
        assert_eq!(out.len(), 1);

        // last_updated = 0 (unset) → not judged stale.
        let out = filter_match_providers(
            vec![cap("never-set", 0, 0, 10, vec![9; 32])],
            "self-peer",
            &empty,
            now,
            300,
        );
        assert_eq!(out.len(), 1);

        // Future timestamp (clock skew) → age saturates to 0 → kept.
        let out = filter_match_providers(
            vec![cap("clock-skew", now + 60, 0, 10, vec![9; 32])],
            "self-peer",
            &empty,
            now,
            300,
        );
        assert_eq!(out.len(), 1);

        // Empty input stays empty.
        let out = filter_match_providers(vec![], "self-peer", &empty, now, 300);
        assert!(out.is_empty());
    }
}
