use echo_daemon::availability::AvailabilityEngine;
use echo_daemon::discovery::DiscoveryService;
use echo_daemon::identity::NodeIdentity;
use echo_daemon::meter::MeteringEngine;
use echo_daemon::neon;
use echo_daemon::sqlite_store;
use echo_daemon::tunnel::TunnelService;
use echo_daemon::{
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
use tracing::{debug, error, info};

const HEARTBEAT_SECS: u64 = 10;
const METRICS_WINDOW: usize = 60;
const DEFAULT_PING_TARGETS: &[&str] = &["1.1.1.1", "8.8.8.8", "208.67.222.222"];
const RU_PER_SECOND: f64 = 1.0;
const CAPABILITY_PUBLISH_INTERVAL_SECS: u64 = 60;

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
}

/// Shared application state available to all HTTP handlers.
#[derive(Clone)]
struct AppState {
    metrics: Arc<RwLock<MetricsState>>,
    backend: Arc<dyn MetricsBackend>,
}

impl MetricsState {
    fn new(
        node_id: &str,
        user_id: &str,
        node_name: &str,
        region: &str,
        ping_targets: Vec<String>,
        peer_id: &str,
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
            metrics_history: VecDeque::with_capacity(METRICS_WINDOW),
            latency_history: VecDeque::with_capacity(METRICS_WINDOW),
            packet_loss_history: VecDeque::with_capacity(METRICS_WINDOW),
            ru_accrued: 0.0,
            earnings_usd: 0.0,
            ping_targets,
            peer_id: peer_id.to_string(),
        }
    }
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
                total_ms += elapsed;
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

async fn heartbeat(
    state: Arc<RwLock<MetricsState>>,
    backend: Arc<dyn MetricsBackend>,
    availability: Arc<RwLock<AvailabilityEngine>>,
) {
    loop {
        sleep(Duration::from_secs(HEARTBEAT_SECS)).await;

        // Phase 1: Collect system stats and available capacity from the
        //          availability engine *before* touching the state lock.
        //          This eliminates the nested-lock hazard where `state` was
        //          held while waiting on `availability`.
        //          Uses refresh_and_compute() for a single sysinfo refresh
        //          instead of two separate refreshes.
        let (stats, avail_upload, avail_download) = {
            let mut avail = availability.write().await;
            avail.refresh_and_compute()
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

        // Phase 3: Single state-lock acquisition for all bookkeeping and
        //          DB writes.  No other lock is acquired inside this block.
        let mut st = state.write().await;
        if !st.running {
            continue;
        }

        st.system_stats = stats.clone();

        st.latency_history.push_back(latency_ms);
        st.packet_loss_history.push_back(packet_loss_pct);
        if st.latency_history.len() > METRICS_WINDOW {
            st.latency_history.pop_front();
        }
        if st.packet_loss_history.len() > METRICS_WINDOW {
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

        let ru = uptime_secs as f64 * RU_PER_SECOND;
        st.ru_accrued = ru;
        st.earnings_usd = ru * 0.0001;

        let metrics = DaemonMetrics {
            timestamp: now_epoch_secs(),
            uptime_secs,
            cpu_pct: (cpu_pct * 100.0).round() / 100.0,
            cpu_temp_c: 0.0,
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
        };

        st.metrics_history.push_back(metrics.clone());
        if st.metrics_history.len() > METRICS_WINDOW {
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
            ip_address: None,
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
            active_nodes: Some(1),
            avg_latency_ms: Some(metrics.latency_ms),
            bandwidth_egress_mb: Some(metrics.bandwidth_up_mbps),
            bandwidth_ingress_mb: Some(metrics.bandwidth_down_mbps),
            packet_loss_pct: Some(metrics.packet_loss_pct),
            uptime_pct: Some(((uptime_secs as f64 / 86400.0).min(1.0) * 100.0).round() / 100.0),
            earnings_usd: Some(metrics.earnings_usd),
            recorded_at: Some(Utc::now().to_rfc3339()),
        };

        let feature_vector = serde_json::json!([
            metrics.cpu_pct,
            metrics.memory_pct,
            metrics.latency_ms,
            metrics.packet_loss_pct,
            metrics.bandwidth_down_mbps,
            metrics.bandwidth_up_mbps,
            metrics.uptime_secs as f64,
            metrics.quality_score,
            metrics.trust_score,
            metrics.available_upload_mbps,
            metrics.available_download_mbps,
        ]);

        let intel = NodeIntelligenceRow {
            node_id: st.node_id.clone(),
            user_id: st.user_id.clone(),
            quality_score: Some(metrics.quality_score),
            trust_score: Some(metrics.trust_score),
            anomaly_score: Some(metrics.packet_loss_pct / 100.0),
            is_anomalous: Some(metrics.packet_loss_pct > 10.0 || metrics.latency_ms > 500.0),
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
        ip_address: None,
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

    let overall = if age > 30 {
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
            10,
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
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
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

    let database_url = std::env::var("DATABASE_URL").ok();
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| {
        let ts = now_epoch_secs();
        format!("node-{}", ts)
    });
    let user_id = std::env::var("USER_ID").unwrap_or_else(|_| "anonymous".to_string());
    let node_name = std::env::var("NODE_NAME").unwrap_or_else(|_| "EchoNode".to_string());
    let region = std::env::var("NODE_REGION").unwrap_or_else(|_| "auto".to_string());
    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3001".to_string());
    let db_path = std::env::var("SQLITE_DB").unwrap_or_else(|_| "sqlite:echo_node.db".to_string());
    let ping_targets = std::env::var("PING_TARGETS")
        .unwrap_or_else(|_| DEFAULT_PING_TARGETS.join(","))
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    let max_upload: f64 = std::env::var("MAX_UPLOAD_MBPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100.0);
    let max_download: f64 = std::env::var("MAX_DOWNLOAD_MBPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100.0);
    let max_sessions: u32 = std::env::var("MAX_SESSIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    // Step 1: Load or generate node identity
    let identity_path = PathBuf::from(
        std::env::var("IDENTITY_PATH").unwrap_or_else(|_| "data/identity.json".to_string()),
    );
    let identity = NodeIdentity::load_or_generate(&identity_path, &region)?;
    info!(
        peer_id = identity.peer_id_str(),
        "node identity loaded"
    );

    // Step 2: Initialize database backend
    let backend: Arc<dyn MetricsBackend> = if let Some(url) = &database_url {
        if !url.is_empty() {
            info!(backend = "neon", url = %url, "database backend initialized");
            Arc::new(neon::NeonStore::new(url).await?)
        } else {
            info!(backend = "sqlite", reason = "DATABASE_URL empty", "database backend initialized");
            Arc::new(sqlite_store::SqliteStore::new(&db_path).await?)
        }
    } else {
        info!(backend = "sqlite", reason = "DATABASE_URL not set", "database backend initialized");
        Arc::new(sqlite_store::SqliteStore::new(&db_path).await?)
    };

    // Step 3: Initialize availability engine
    let availability = Arc::new(RwLock::new(AvailabilityEngine::new(
        max_upload,
        max_download,
        max_sessions,
    )));

    // Step 4: Initialize metering engine
    let (metering, receipt_rx) = MeteringEngine::new(identity.clone());

    // Step 4.5: Initialize tunnel service (holds metering engine alive)
    let tunnel_listen_addr = std::env::var("TUNNEL_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3002".to_string());
    let local_peer_id: libp2p::PeerId = identity
        .peer_id_str()
        .parse()
        .context("failed to parse peer_id")?;
    let relay_config = echo_daemon::tunnel::RelayConfig {
        backend: backend.clone(),
        metering: metering.clone(),
        availability: availability.clone(),
        target_addr: std::env::var("RELAY_TARGET")
            .unwrap_or_else(|_| "127.0.0.1:80".to_string()),
        local_peer_id: identity.peer_id_str().to_string(),
        max_upload_mbps: max_upload,
        max_download_mbps: max_download,
    };
    let (tunnel_service, mut tunnel_rx) = TunnelService::new(
        local_peer_id,
        metering,
        availability.clone(),
        relay_config,
        max_sessions as usize,
        identity.noise_secret_key_bytes().to_vec(),
    );

    // Spawn tunnel event handler
    // NOTE: end_session is called inside handle_incoming_connection (provider side)
    // and should NOT be called here to avoid the double-end-session bug.
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
                } => {
                    info!(
                        session = %session_id,
                        bytes_sent,
                        bytes_received,
                        "tunnel session closed"
                    );
                    // Do NOT call end_session here — it's already handled by
                    // handle_incoming_connection on the provider side.
                }
            }
        }
    });

    // Spawn incoming tunnel listener (provider side)
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
    tokio::spawn(async move {
        if let Err(e) = TunnelService::accept_incoming(
            &tunnel_listen_addr,
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
    let state = Arc::new(RwLock::new(MetricsState::new(
        &node_id,
        &user_id,
        &node_name,
        &region,
        ping_targets,
        identity.peer_id_str(),
    )));

    let state_clone = state.clone();
    let backend_clone = backend.clone();
    let avail_clone = availability.clone();
    tokio::spawn(async move {
        heartbeat(state_clone, backend_clone, avail_clone).await;
    });

    // Step 7: Spawn data retention cleanup (daily)
    let cleanup_backend = backend.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(86400)).await;
            match cleanup_backend.cleanup_old_metrics(30).await {
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
    let identity_clone = identity.clone();
    let backend_clone2 = backend.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(CAPABILITY_PUBLISH_INTERVAL_SECS)).await;
            // Build and store capability descriptor
            let mut avail = availability.write().await;
            let cap = avail.build_capability_descriptor(
                identity_clone.peer_id_str(),
                identity_clone.public_key_bytes.clone(),
                &region,
                0.5, // default reputation
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

    // Step 9: Spawn receipt settlement task
    // The metering engine is kept alive inside TunnelService.
    // Use a separate channel for shutdown signaling.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut receipt_rx_task = receipt_rx;
    let receipt_backend = backend.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    // Drain any remaining receipts before shutdown
                    let mut count = 0u64;
                    while let Ok(receipt) = receipt_rx_task.try_recv() {
                        if let Err(e) = receipt_backend.insert_receipt(&receipt).await {
                            error!(error = %e, "receipt flush failed during shutdown");
                        } else {
                            count += 1;
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
                            if let Err(e) = receipt_backend.insert_receipt(&receipt).await {
                                error!(error = %e, "receipt settlement failed");
                            } else {
                                debug!(
                                    session = %receipt.session_id,
                                    seq = receipt.sequence_number,
                                    "receipt settled"
                                );
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    // Step 10: Set up HTTP API
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // API key for /control endpoint (optional — if unset, auth is disabled)
    let api_key = std::env::var("API_KEY").ok().filter(|v| !v.is_empty());
    if api_key.is_some() {
        info!("API key authentication enabled for /control");
    } else {
        info!("API key authentication disabled (set API_KEY env var to enable)");
    }
    let auth = ApiKeyAuth::new(api_key);

    // /control is protected by API key middleware
    let control_routes = Router::new()
        .route("/control", post(handle_control))
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
        });

    let addr: SocketAddr = listen_addr.parse()?;
    info!(addr = %addr, "server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Signal shutdown to receipt settlement task (flush remaining receipts)
    let _ = shutdown_tx.send(true);
    // Give the drain a moment to complete
    sleep(Duration::from_secs(1)).await;

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
}
