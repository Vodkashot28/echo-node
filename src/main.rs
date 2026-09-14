mod sqlite_store;
mod supabase;

use anyhow::Result;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use sysinfo::{Components, CpuRefreshKind, Disks, Networks, RefreshKind, System};
use tokio::{sync::RwLock, time::sleep};
use tower_http::cors::{Any, CorsLayer};

const HEARTBEAT_SECS: u64 = 10;
const METRICS_WINDOW: usize = 60;
const PING_TARGETS: &[&str] = &["1.1.1.1", "8.8.8.8", "208.67.222.222"];
const RU_PER_SECOND: f64 = 1.0;

#[derive(Clone)]
enum Backend {
    Supabase(supabase::SupabaseClient),
    Sqlite(sqlite_store::SqliteStore),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonMetrics {
    pub timestamp: u64,
    pub uptime_secs: u64,
    pub cpu_pct: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_pct: f64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonNode {
    pub id: String,
    pub name: String,
    pub region: String,
    pub status: String,
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
    pub ru_total: f64,
    pub earnings_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub running: bool,
    pub node_id: String,
    pub uptime_secs: u64,
    pub heartbeat_count: u64,
    pub last_heartbeat: Option<u64>,
    pub supabase_connected: bool,
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

struct NetworkSample {
    rx_bytes: u64,
    tx_bytes: u64,
    timestamp: Instant,
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
    supabase_connected: bool,
    system: System,
    networks: Networks,
    components: Components,
    disks: Disks,
    metrics_history: VecDeque<DaemonMetrics>,
    network_samples: VecDeque<NetworkSample>,
    latency_history: VecDeque<f64>,
    packet_loss_history: VecDeque<f64>,
    ru_accrued: f64,
    earnings_usd: f64,
}

impl MetricsState {
    fn new(node_id: &str, user_id: &str, node_name: &str, region: &str) -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(sysinfo::MemoryRefreshKind::everything()),
        );
        system.refresh_all();

        let networks = Networks::new_with_refreshed_list();

        Self {
            running: true,
            node_id: node_id.to_string(),
            user_id: user_id.to_string(),
            node_name: node_name.to_string(),
            region: region.to_string(),
            start_time: Instant::now(),
            heartbeat_count: 0,
            last_heartbeat_ts: None,
            supabase_connected: false,
            system,
            networks,
            components: Components::new(),
            disks: Disks::new(),
            metrics_history: VecDeque::with_capacity(METRICS_WINDOW),
            network_samples: VecDeque::with_capacity(METRICS_WINDOW),
            latency_history: VecDeque::with_capacity(METRICS_WINDOW),
            packet_loss_history: VecDeque::with_capacity(METRICS_WINDOW),
            ru_accrued: 0.0,
            earnings_usd: 0.0,
        }
    }
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn measure_latency_and_loss() -> (f64, f64) {
    let mut successes = 0u32;
    let mut total_ms = 0.0;
    let total = PING_TARGETS.len() as f64;

    for target in PING_TARGETS {
        let start = Instant::now();
        let result = std::process::Command::new("ping")
            .args(["-c", "1", "-W", "2", target])
            .output();
        let elapsed = start.elapsed().as_millis() as f64;

        match result {
            Ok(output) => {
                if output.status.success() {
                    successes += 1;
                    total_ms += elapsed;
                }
            }
            Err(_) => {}
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
    let uptime_bonus = (uptime_secs as f64 / 86400.0).min(1.0) * 0.1;

    let raw = latency_score * 0.3 + loss_score * 0.3 + cpu_score * 0.2 + mem_score * 0.1 + uptime_bonus * 0.1;
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

    let raw = (quality_factor * 0.4 + uptime_factor * 0.3 + consistency_factor * 0.3) - loss_penalty;
    (raw.clamp(0.0, 1.0) * 100.0).round() / 100.0
}

fn compute_bandwidth_rate(
    samples: &VecDeque<NetworkSample>,
) -> (f64, f64) {
    if samples.len() < 2 {
        return (0.0, 0.0);
    }

    let newest = samples.back().unwrap();
    let oldest = samples.front().unwrap();
    let dt = newest.timestamp.duration_since(oldest.timestamp).as_secs_f64();

    if dt <= 0.0 {
        return (0.0, 0.0);
    }

    let rx_delta = newest.rx_bytes.saturating_sub(oldest.rx_bytes) as f64;
    let tx_delta = newest.tx_bytes.saturating_sub(oldest.tx_bytes) as f64;

    let rx_bps = (rx_delta / dt) * 8.0;
    let tx_bps = (tx_delta / dt) * 8.0;

    (rx_bps, tx_bps)
}

fn bps_to_mbps(bps: f64) -> f64 {
    (bps / 1_000_000.0 * 100.0).round() / 100.0
}

async fn heartbeat(state: Arc<RwLock<MetricsState>>, backend: Backend) {
    loop {
        sleep(Duration::from_secs(HEARTBEAT_SECS)).await;

        let mut st = state.write().await;
        if !st.running {
            continue;
        }

        st.system.refresh_all();
        st.networks.refresh();

        let cpu_pct = st.system.global_cpu_info().cpu_usage() as f64;
        let used_mem = st.system.used_memory();
        let total_mem = st.system.total_memory();
        let mem_pct = if total_mem > 0 {
            (used_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        let mut total_rx: u64 = 0;
        let mut total_tx: u64 = 0;
        for (_name, data) in st.networks.iter() {
            total_rx += data.total_received();
            total_tx += data.total_transmitted();
        }

        st.network_samples.push_back(NetworkSample {
            rx_bytes: total_rx,
            tx_bytes: total_tx,
            timestamp: Instant::now(),
        });
        if st.network_samples.len() > METRICS_WINDOW {
            st.network_samples.pop_front();
        }

        let (rx_rate_bps, tx_rate_bps) = compute_bandwidth_rate(&st.network_samples);
        let (latency_ms, packet_loss_pct) = measure_latency_and_loss().await;

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

        let quality_score = compute_quality_score(
            avg_latency,
            packet_loss_pct,
            cpu_pct,
            mem_pct,
            st.start_time.elapsed().as_secs(),
        );
        let trust_score = compute_trust_score(
            quality_score,
            st.start_time.elapsed().as_secs(),
            st.heartbeat_count,
            packet_loss_pct,
        );

        let uptime_secs = st.start_time.elapsed().as_secs();
        let ru = uptime_secs as f64 * RU_PER_SECOND;
        st.ru_accrued = ru;
        st.earnings_usd = ru * 0.0001;

        let metrics = DaemonMetrics {
            timestamp: now_epoch_secs(),
            uptime_secs,
            cpu_pct: (cpu_pct * 100.0).round() / 100.0,
            memory_used_bytes: used_mem,
            memory_total_bytes: total_mem,
            memory_pct: (mem_pct * 100.0).round() / 100.0,
            network_rx_bytes: total_rx,
            network_tx_bytes: total_tx,
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
        };

        st.metrics_history.push_back(metrics.clone());
        if st.metrics_history.len() > METRICS_WINDOW {
            st.metrics_history.pop_front();
        }

        st.heartbeat_count += 1;
        st.last_heartbeat_ts = Some(now_epoch_secs());

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
            metrics.network_rx_rate_bps,
            metrics.network_tx_rate_bps,
        ]);

        let node_row_sqlite = sqlite_store::NodeRow {
            id: st.node_id.clone(),
            name: st.node_name.clone(),
            user_id: st.user_id.clone(),
            ip_address: None,
            region: Some(st.region.clone()),
            status: Some("online".to_string()),
            last_seen_at: Some(Utc::now().to_rfc3339()),
            uptime_pct: Some(((uptime_secs as f64 / 86400.0).min(1.0) * 100.0).round() / 100.0),
            avg_latency_ms: Some(metrics.latency_ms),
            bandwidth_down_mbps: Some(metrics.bandwidth_down_mbps),
            bandwidth_up_mbps: Some(metrics.bandwidth_up_mbps),
            packet_loss_pct: Some(metrics.packet_loss_pct),
            quality_score: Some(metrics.quality_score),
            trust_score: Some(metrics.trust_score),
            sessions_count: Some(0),
            earnings_usd: Some(metrics.earnings_usd),
        };

        let node_row_supabase = supabase::NodeRow {
            id: st.node_id.clone(),
            name: st.node_name.clone(),
            user_id: st.user_id.clone(),
            ip_address: None,
            region: Some(st.region.clone()),
            status: Some("online".to_string()),
            last_seen_at: Some(Utc::now().to_rfc3339()),
            uptime_pct: Some(((uptime_secs as f64 / 86400.0).min(1.0) * 100.0).round() / 100.0),
            avg_latency_ms: Some(metrics.latency_ms),
            bandwidth_down_mbps: Some(metrics.bandwidth_down_mbps),
            bandwidth_up_mbps: Some(metrics.bandwidth_up_mbps),
            packet_loss_pct: Some(metrics.packet_loss_pct),
            quality_score: Some(metrics.quality_score),
            trust_score: Some(metrics.trust_score),
            sessions_count: Some(0),
            earnings_usd: Some(metrics.earnings_usd),
        };

        let node_metrics_sqlite = sqlite_store::NodeMetricsRow {
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

        let node_metrics_supabase = supabase::NodeMetricsRow {
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

        let network_metrics_sqlite = sqlite_store::NetworkMetricsRow {
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

        let network_metrics_supabase = supabase::NetworkMetricsRow {
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

        let intel_sqlite = sqlite_store::NodeIntelligenceRow {
            node_id: st.node_id.clone(),
            user_id: st.user_id.clone(),
            quality_score: Some(metrics.quality_score),
            trust_score: Some(metrics.trust_score),
            anomaly_score: Some(metrics.packet_loss_pct / 100.0),
            is_anomalous: Some(metrics.packet_loss_pct > 10.0 || metrics.latency_ms > 500.0),
            cluster_id: Some(0),
            feature_vector: Some(feature_vector.clone()),
            recorded_at: Some(Utc::now().to_rfc3339()),
        };

        let intel_supabase = supabase::NodeIntelligenceRow {
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

        match &backend {
            Backend::Supabase(sb) => {
                match sb.upsert_node(&node_row_supabase).await {
                    Ok(_) => { st.supabase_connected = true; }
                    Err(e) => { eprintln!("[supabase] node upsert failed: {}", e); st.supabase_connected = false; }
                }
                if let Err(e) = sb.upsert_node_metrics(&[node_metrics_supabase]).await {
                    eprintln!("[supabase] metrics upsert failed: {}", e);
                }
                if let Err(e) = sb.upsert_network_metrics(&network_metrics_supabase).await {
                    eprintln!("[supabase] network metrics upsert failed: {}", e);
                }
                if let Err(e) = sb.upsert_node_intelligence(&intel_supabase).await {
                    eprintln!("[supabase] intelligence upsert failed: {}", e);
                }
            }
            Backend::Sqlite(sql) => {
                match sql.upsert_node(&node_row_sqlite).await {
                    Ok(_) => { st.supabase_connected = true; }
                    Err(e) => { eprintln!("[sqlite] node upsert failed: {}", e); st.supabase_connected = false; }
                }
                if let Err(e) = sql.insert_node_metrics(&node_metrics_sqlite).await {
                    eprintln!("[sqlite] metrics insert failed: {}", e);
                }
                if let Err(e) = sql.insert_network_metrics(&network_metrics_sqlite).await {
                    eprintln!("[sqlite] network metrics insert failed: {}", e);
                }
                if let Err(e) = sql.insert_node_intelligence(&intel_sqlite).await {
                    eprintln!("[sqlite] intelligence insert failed: {}", e);
                }
            }
        }

        println!(
            "[heartbeat #{}] cpu={:.1}% mem={:.1}% latency={:.1}ms loss={:.1}% q={:.2} t={:.2} ru={:.2} $={:.4} supabase={}",
            st.heartbeat_count,
            metrics.cpu_pct,
            metrics.memory_pct,
            metrics.latency_ms,
            metrics.packet_loss_pct,
            metrics.quality_score,
            metrics.trust_score,
            metrics.ru_total,
            metrics.earnings_usd,
            st.supabase_connected,
        );
    }
}

async fn handle_metrics(
    State(state): State<Arc<RwLock<MetricsState>>>,
) -> Result<Json<DaemonMetrics>, StatusCode> {
    let st = state.read().await;
    st.metrics_history
        .back()
        .cloned()
        .map(Json)
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)
}

async fn handle_nodes(
    State(state): State<Arc<RwLock<MetricsState>>>,
) -> Json<Vec<DaemonNode>> {
    let st = state.read().await;
    let uptime = st.start_time.elapsed().as_secs();
    let metrics = st.metrics_history.back().cloned();

    let node = DaemonNode {
        id: st.node_id.clone(),
        name: st.node_name.clone(),
        region: st.region.clone(),
        status: if st.running { "online" } else { "offline" }.to_string(),
        ip_address: None,
        last_seen: now_epoch_secs(),
        uptime_secs: uptime,
        cpu_pct: metrics.as_ref().map_or(0.0, |m| m.cpu_pct),
        memory_pct: metrics.as_ref().map_or(0.0, |m| m.memory_pct),
        latency_ms: metrics.as_ref().map_or(0.0, |m| m.latency_ms),
        bandwidth_down_mbps: metrics.as_ref().map_or(0.0, |m| m.bandwidth_down_mbps),
        bandwidth_up_mbps: metrics.as_ref().map_or(0.0, |m| m.bandwidth_up_mbps),
        packet_loss_pct: metrics.as_ref().map_or(0.0, |m| m.packet_loss_pct),
        quality_score: metrics.as_ref().map_or(0.0, |m| m.quality_score),
        trust_score: metrics.as_ref().map_or(0.0, |m| m.trust_score),
        ru_total: metrics.as_ref().map_or(0.0, |m| m.ru_total),
        earnings_usd: metrics.as_ref().map_or(0.0, |m| m.earnings_usd),
    };

    Json(vec![node])
}

async fn handle_history(
    State(state): State<Arc<RwLock<MetricsState>>>,
) -> Json<Vec<DaemonMetrics>> {
    let st = state.read().await;
    Json(st.metrics_history.iter().cloned().collect())
}

async fn handle_status(
    State(state): State<Arc<RwLock<MetricsState>>>,
) -> Json<DaemonStatus> {
    let st = state.read().await;
    Json(DaemonStatus {
        running: st.running,
        node_id: st.node_id.clone(),
        uptime_secs: st.start_time.elapsed().as_secs(),
        heartbeat_count: st.heartbeat_count,
        last_heartbeat: st.last_heartbeat_ts,
        supabase_connected: st.supabase_connected,
    })
}

async fn handle_control(
    State(state): State<Arc<RwLock<MetricsState>>>,
    Json(req): Json<ControlRequest>,
) -> Result<Json<ControlResponse>, StatusCode> {
    let mut st = state.write().await;
    let response = match req.action.as_str() {
        "start" => {
            st.running = true;
            ControlResponse {
                success: true,
                message: "Daemon started".to_string(),
                status: DaemonStatus {
                    running: true,
                    node_id: st.node_id.clone(),
                    uptime_secs: st.start_time.elapsed().as_secs(),
                    heartbeat_count: st.heartbeat_count,
                    last_heartbeat: st.last_heartbeat_ts,
                    supabase_connected: st.supabase_connected,
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
                    uptime_secs: st.start_time.elapsed().as_secs(),
                    heartbeat_count: st.heartbeat_count,
                    last_heartbeat: st.last_heartbeat_ts,
                    supabase_connected: st.supabase_connected,
                },
            }
        }
        "restart" => {
            st.running = true;
            st.heartbeat_count = 0;
            st.start_time = Instant::now();
            st.metrics_history.clear();
            ControlResponse {
                success: true,
                message: "Daemon restarted".to_string(),
                status: DaemonStatus {
                    running: true,
                    node_id: st.node_id.clone(),
                    uptime_secs: 0,
                    heartbeat_count: 0,
                    last_heartbeat: None,
                    supabase_connected: st.supabase_connected,
                },
            }
        }
        _ => {
            return Err(StatusCode::BAD_REQUEST);
        }
    };
    Ok(Json(response))
}

async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "echo-daemon",
        "version": "0.2.0"
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    let supabase_url = std::env::var("SUPABASE_URL").ok();
    let supabase_key = std::env::var("SUPABASE_ANON_KEY").unwrap_or_default();
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| {
        let ts = now_epoch_secs();
        format!("node-{}", ts)
    });
    let user_id = std::env::var("USER_ID").unwrap_or_else(|_| "anonymous".to_string());
    let node_name = std::env::var("NODE_NAME").unwrap_or_else(|_| "EchoNode".to_string());
    let region = std::env::var("NODE_REGION").unwrap_or_else(|_| "auto".to_string());
    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3001".to_string());
    let db_path = std::env::var("SQLITE_DB").unwrap_or_else(|_| "sqlite:echo_node.db".to_string());

    let backend = if let Some(url) = &supabase_url {
        if !supabase_key.is_empty() && url != "http://localhost:54321" {
            println!("[daemon] backend=supabase url={}", url);
            Backend::Supabase(supabase::SupabaseClient::new(url, &supabase_key))
        } else {
            println!("[daemon] backend=sqlite (supabase url/key not configured)");
            Backend::Sqlite(sqlite_store::SqliteStore::new(&db_path).await?)
        }
    } else {
        println!("[daemon] backend=sqlite (SUPABASE_URL not set)");
        Backend::Sqlite(sqlite_store::SqliteStore::new(&db_path).await?)
    };

    println!("[daemon] node_id={} name={} region={}", node_id, node_name, region);

    let state = Arc::new(RwLock::new(MetricsState::new(
        &node_id, &user_id, &node_name, &region,
    )));

    let state_clone = state.clone();
    let backend_clone = backend.clone();
    tokio::spawn(async move {
        heartbeat(state_clone, backend_clone).await;
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/nodes", get(handle_nodes))
        .route("/history", get(handle_history))
        .route("/status", get(handle_status))
        .route("/health", get(handle_health))
        .route("/control", post(handle_control))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = listen_addr.parse()?;
    println!("[daemon] listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
