//! Dashboard telemetry bridge: pushes real metrics to a remote control-plane
//! endpoint (a Supabase edge function for the echomesh dashboard). Two kinds
//! of payload are federated (see DASHBOARD.md):
//!
//! - **Heartbeat frames** — one per heartbeat cycle: node row, per-node and
//!   network metric samples, and the 32-dim intelligence feature vector.
//! - **Session events** — one per completed provider-side relay session,
//!   carrying the full lifetime accounting (bytes, duration, latency, peer,
//!   earnings) so the dashboard shows real sessions instead of seeded ones.
//!
//! This is the daemon side of the daemon → dashboard ingestion path. It is
//! fully opt-in and off by default:
//!
//! - `DASHBOARD_TELEMETRY_URL` — edge function URL
//!   (e.g. `https://<project>.supabase.co/functions/v1/report-telemetry`)
//! - `ECHO_API_KEY`            — API key sent as `Authorization: Bearer <key>`
//!
//! When either required variable is missing the bridge stays disabled and the
//! daemon behaves exactly as before. Reporting never blocks the heartbeat:
//! frames are handed off through a small bounded channel and posted by a
//! dedicated background task.
//!
//! Every payload includes the node's `peer_id` (libp2p PeerId string) so the
//! dashboard can correlate rows without a separate `node_id`/`user_id` pair.
//!
//! Delivery is queue-backed and durable:
//!
//! - the reporter task owns a bounded in-memory queue (FIFO) in front of the
//!   HTTP client, so a slow/unreachable endpoint does not drop frames the
//!   moment the channel drains;
//! - failed posts are retried with capped exponential backoff + jitter
//!   (`DASHBOARD_TELEMETRY_BACKOFF_BASE_MS` / `_MAX_MS`);
//! - on shutdown the task makes a time-boxed final delivery attempt, then
//!   spools whatever is left to `DASHBOARD_TELEMETRY_SPOOL` (JSONL) and
//!   replays it on the next boot — a restart cannot lose queued frames;
//! - queue overflow drops the *oldest* frame (`DASHBOARD_TELEMETRY_MAX_QUEUE`)
//!   so the dashboard sees the freshest state.

use crate::tunnel::SessionCloseDetails;
use crate::{NetworkMetricsRow, NodeIntelligenceRow, NodeMetricsRow, NodeRow};
use anyhow::{Context, Result};
#[cfg(test)]
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

/// Payload schema version — bump when the frame shape changes; the edge
/// function rejects unknown versions.
pub const TELEMETRY_SCHEMA_VERSION: u32 = 1;

/// Number of dimensions in the intelligence feature vector, matching the
/// echomesh `node_intelligence.feature_vector` contract (see its migration
/// comment and the dashboard's `FEATURE_LABELS`).
pub const FEATURE_VECTOR_DIM: usize = 32;

/// Maximum number of frames queued on the channel before the sender starts
/// dropping (heartbeat must never block).
const QUEUE_CAPACITY: usize = 64;
/// Per-request HTTP timeout.
const POST_TIMEOUT_SECS: u64 = 10;
/// Time-boxed delivery budget for the final flush on shutdown.
const SHUTDOWN_FLUSH_SECS: u64 = 5;

/// Dashboard reporting configuration.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub url: String,
    /// API key sent as `Authorization: Bearer <api_key>`.
    pub api_key: String,
    /// JSONL spool file for crash/restart durability.
    pub spool_path: PathBuf,
    /// Exponential backoff base (first retry delay), milliseconds.
    pub backoff_base_ms: u64,
    /// Backoff ceiling, milliseconds.
    pub backoff_max_ms: u64,
    /// Max frames held in the reporter's in-memory queue.
    pub max_queue: usize,
}

impl DashboardConfig {
    /// Read the `DASHBOARD_TELEMETRY_URL` and `ECHO_API_KEY` env vars.
    /// Returns `None` when the bridge is disabled (either var unset/empty).
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("DASHBOARD_TELEMETRY_URL")
            .ok()
            .filter(|v| !v.is_empty());
        let api_key = std::env::var("ECHO_API_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        match (url, api_key) {
            (Some(url), Some(api_key)) => {
                let spool_path = std::env::var("DASHBOARD_TELEMETRY_SPOOL")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("data/telemetry_spool.jsonl"));
                let backoff_base_ms = env_u64("DASHBOARD_TELEMETRY_BACKOFF_BASE_MS", 500).max(1);
                let backoff_max_ms =
                    env_u64("DASHBOARD_TELEMETRY_BACKOFF_MAX_MS", 30_000).max(backoff_base_ms);
                let max_queue = env_usize("DASHBOARD_TELEMETRY_MAX_QUEUE", 256).max(1);
                Some(Self {
                    url,
                    api_key,
                    spool_path,
                    backoff_base_ms,
                    backoff_max_ms,
                    max_queue,
                })
            }
            _ => None,
        }
    }
}

/// One heartbeat frame pushed to the dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct DashboardTelemetry {
    /// Payload schema version.
    pub version: u32,
    /// Frame timestamp (RFC3339).
    pub timestamp: String,
    /// The node's libp2p PeerId — used by the dashboard to correlate rows.
    pub peer_id: String,
    pub node: NodeRow,
    pub node_metrics: NodeMetricsRow,
    pub network_metrics: NetworkMetricsRow,
    pub intelligence: NodeIntelligenceRow,
}

/// A session-close event federated to the dashboard (Slice 2). Produced only
/// on the provider side, where the relay accounting is authoritative.
///
/// Serialized field names are snake_case to match the heartbeat frame; the
/// edge function discriminates on `kind == "session"`.
#[derive(Debug, Clone, Serialize)]
pub struct SessionEvent {
    /// Payload discriminator: `"session"`.
    pub kind: &'static str,
    /// `"closed"` (the only session action federated today).
    pub action: &'static str,
    /// Payload schema version.
    pub version: u32,
    /// Event timestamp (RFC3339) — the session ended_at.
    pub timestamp: String,
    /// The node's libp2p PeerId — used by the dashboard to correlate rows.
    pub peer_id: String,
    /// Daemon-side tunnel session id (`connection_history` counterpart).
    pub session_id: String,
    pub remote_peer_id: String,
    pub remote_ip: Option<String>,
    pub remote_port: Option<u16>,
    /// `"inbound"` (provider accepts) / `"outbound"` (consumer dials).
    pub direction: String,
    /// Session end status — `"completed"` (matches the sessions CHECK).
    pub status: &'static str,
    /// `"tunnel"` (matches the sessions type CHECK).
    pub session_type: &'static str,
    /// Wire protocol — `"tcp"`.
    pub protocol: &'static str,
    pub started_at: String,
    pub ended_at: String,
    pub duration_secs: f64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub bytes_relayed: u64,
    pub avg_latency_ms: f64,
    pub exit_reason: String,
    /// The daemon's configured payout rate (USD/GB), for transparency.
    pub settlement_rate_usd_per_gb: f64,
    /// Approximated payout for this session: bytes_relayed/1e9 × rate.
    pub earnings_usd: f64,
}

impl SessionEvent {
    /// Build a session-close frame from a provider-side relay teardown.
    pub fn from_close(
        peer_id: &str,
        session_id: &str,
        bytes_sent: u64,
        bytes_received: u64,
        settlement_rate_usd_per_gb: f64,
        details: &SessionCloseDetails,
    ) -> Self {
        let bytes_relayed = bytes_sent + bytes_received;
        let earnings_usd = (bytes_relayed as f64 / 1_000_000_000.0) * settlement_rate_usd_per_gb;
        Self {
            kind: "session",
            action: "closed",
            version: TELEMETRY_SCHEMA_VERSION,
            timestamp: details.ended_at.clone(),
            peer_id: peer_id.to_string(),
            session_id: session_id.to_string(),
            remote_peer_id: details.remote_peer_id.clone(),
            remote_ip: details.remote_ip.clone(),
            remote_port: details.remote_port,
            direction: details.direction.clone(),
            status: "completed",
            session_type: "tunnel",
            protocol: "tcp",
            started_at: details.started_at.clone(),
            ended_at: details.ended_at.clone(),
            duration_secs: (details.duration_secs * 100.0).round() / 100.0,
            bytes_sent,
            bytes_received,
            bytes_relayed,
            avg_latency_ms: (details.avg_latency_ms * 100.0).round() / 100.0,
            exit_reason: details.exit_reason.clone(),
            settlement_rate_usd_per_gb,
            earnings_usd: (earnings_usd * 100_000.0).round() / 100_000.0,
        }
    }
}

/// Messages queued on the bridge channel. Both payloads are boxed so the enum
/// stays eight bytes regardless of frame size (clippy::large_enum_variant).
#[derive(Debug)]
pub enum ReporterMessage {
    Telemetry(Box<DashboardTelemetry>),
    Session(Box<SessionEvent>),
}

/// Real signals available for one heartbeat sample. Kept separate from
/// `DaemonMetrics` so this module stays library-independent.
#[derive(Debug, Clone, Copy)]
pub struct FeatureInputs {
    pub cpu_pct: f64,
    pub memory_pct: f64,
    pub disk_usage_pct: f64,
    pub latency_ms: f64,
    pub packet_loss_pct: f64,
    pub bandwidth_up_mbps: f64,
    pub bandwidth_down_mbps: f64,
    pub uptime_secs: u64,
    pub earnings_usd: f64,
    pub available_upload_mbps: f64,
    pub available_download_mbps: f64,
    pub active_sessions: u32,
    pub max_sessions: u32,
}

fn norm(v: f64) -> f64 {
    v.clamp(0.0, 1.0)
}

/// Mean of `f(s)` over the series.
fn series_mean(series: &[FeatureInputs], f: impl Fn(&FeatureInputs) -> f64) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let n = series.len() as f64;
    series.iter().map(f).sum::<f64>() / n
}

/// Standard deviation of `f(s)` over the series, scaled to [0, 1] (3σ → 1).
fn series_stddev(series: &[FeatureInputs], f: impl Fn(&FeatureInputs) -> f64) -> f64 {
    if series.len() < 2 {
        return 0.0;
    }
    let n = series.len() as f64;
    let mean = series_mean(series, &f);
    let var = series
        .iter()
        .map(|s| (f(s) - mean).powi(2))
        .sum::<f64>()
        / n;
    norm(var.sqrt() * 3.0)
}

/// Build the 32-dimension intelligence feature vector expected by the echomesh
/// dashboard. Dimensions (in order, matching the dashboard's `FEATURE_LABELS`
/// and the `node_intelligence` migration comment):
///
/// 1..16  throughput, latency_avg, latency_p99, jitter, packet_loss, uptime,
///        bandwidth_util, connection_density, session_duration_avg, earnings_rate,
///        error_rate, retry_rate, timeout_rate, dns_resolution, tls_handshake, ttfb
/// 17..20 cpu_entropy, mem_entropy, io_entropy, net_entropy
/// 21..24 throughput_trend, latency_trend, loss_trend, earnings_trend
/// 25..28 throughput_vol, latency_vol, loss_vol, earnings_vol
/// 29..32 peer_diversity, geo_spread, protocol_mix, time_consistency
///
/// Values that have a real daemon signal behind them are computed from it;
/// the rest are stable neutral baselines (documented inline) rather than
/// random noise — the previous synthetic edge function fabricated these.
pub fn build_feature_vector(series: &[FeatureInputs]) -> Vec<f64> {
    let mut v = vec![0.0_f64; FEATURE_VECTOR_DIM];
    let Some(cur) = series.last() else {
        return v;
    };
    let prev = series.get(series.len().saturating_sub(2));

    // Dim 1: throughput — current up+down normalized against a 1600 Mbps link.
    v[0] = norm((cur.bandwidth_up_mbps + cur.bandwidth_down_mbps) / 1600.0);
    // Dim 2: latency avg — current latency vs 200 ms ceiling.
    v[1] = norm(cur.latency_ms / 200.0);
    // Dim 3: latency p99 — approximated by the max latency seen in the window.
    let max_latency = series.iter().map(|s| s.latency_ms).fold(0.0_f64, f64::max);
    v[2] = norm((max_latency * 1.05) / 200.0);
    // Dim 4: jitter — stddev of the latency series.
    let latency_std = series_stddev(series, |s| s.latency_ms);
    v[3] = norm(latency_std / 200.0);
    // Dim 5: packet loss — real, vs 5% ceiling.
    v[4] = norm(cur.packet_loss_pct / 5.0);
    // Dim 6: uptime — real percentage-of-one-day uptime.
    v[5] = norm((cur.uptime_secs as f64 / 86400.0) * 100.0 / 100.0);
    // Dim 7: bandwidth util — link usage vs a 400 Mbps nominal link.
    v[6] = norm((cur.bandwidth_up_mbps + cur.bandwidth_down_mbps) / 400.0);
    // Dim 8: connection density — active sessions vs configured max.
    let max_sessions = cur.max_sessions.max(1);
    v[7] = norm(cur.active_sessions as f64 / max_sessions as f64);
    // Dim 9: session duration avg — the heartbeat has no per-session durations
    // yet (federation of connection_history is a follow-up); neutral baseline.
    v[8] = 0.15;
    // Dim 10: earnings rate — daemon RU/uptime earnings vs $200 ceiling.
    v[9] = norm(cur.earnings_usd / 200.0);
    // Dims 11..13: error/retry/timeout rates — no client-flow visibility in the
    // heartbeat; neutral baselines.
    v[10] = 0.02;
    v[11] = 0.015;
    v[12] = 0.01;
    // Dims 14..16: dns/tls/ttfb — measured on the client side, not in the
    // daemon; neutral baselines.
    v[13] = 0.1;
    v[14] = 0.05;
    v[15] = 0.1;
    // Dim 17..20: CPU/Mem/IO/Net entropy — CPU and memory util are real; disk
    // usage is a reasonable IO proxy; net entropy reuses bandwidth util.
    v[16] = norm(cur.cpu_pct / 100.0);
    v[17] = norm(cur.memory_pct / 100.0);
    v[18] = norm(cur.disk_usage_pct / 100.0);
    v[19] = v[0];
    // Dim 21..24: trends — absolute change vs previous sample.
    if let Some(prev) = prev {
        let cu = norm(cur.bandwidth_up_mbps + cur.bandwidth_down_mbps);
        let pu = norm(prev.bandwidth_up_mbps + prev.bandwidth_down_mbps);
        v[20] = (cu - pu).max(0.0);
        let cl = norm(cur.latency_ms / 200.0);
        let pl = norm(prev.latency_ms / 200.0);
        v[21] = (cl - pl).max(0.0);
        let cp = norm(cur.packet_loss_pct / 5.0);
        let pp = norm(prev.packet_loss_pct / 5.0);
        v[22] = (cp - pp).max(0.0);
        let ce = norm(cur.earnings_usd / 200.0);
        let pe = norm(prev.earnings_usd / 200.0);
        v[23] = (ce - pe).max(0.0);
    }
    // Dim 25..28: volatility — stddev over the window.
    v[24] = series_stddev(series, |s| norm((s.bandwidth_up_mbps + s.bandwidth_down_mbps) / 1600.0));
    v[25] = series_stddev(series, |s| norm(s.latency_ms / 200.0));
    v[26] = series_stddev(series, |s| norm(s.packet_loss_pct / 5.0));
    v[27] = series_stddev(series, |s| norm(s.earnings_usd / 200.0));
    // Dim 29..31: peer diversity / geo spread / protocol mix — require
    // multi-peer federation the daemon does not expose yet; neutral baselines.
    v[28] = 0.1;
    v[29] = 0.1;
    v[30] = 0.5;
    // Dim 32: time consistency — uptime-weighted.
    v[31] = norm(0.7 * norm(cur.uptime_secs as f64 / 86400.0) + 0.3);

    v.iter().map(|x| (x * 10000.0).round() / 10000.0).collect()
}

/// One queued delivery attempt: a discriminator plus the already-serialized
/// payload (posted as-is, spooled as-is — the payload types stay
/// serialize-only).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolRecord {
    kind: String,
    payload: serde_json::Value,
}

impl From<ReporterMessage> for SpoolRecord {
    fn from(message: ReporterMessage) -> Self {
        match message {
            ReporterMessage::Telemetry(frame) => Self {
                kind: "telemetry".to_string(),
                payload: serde_json::to_value(&*frame).unwrap_or(serde_json::Value::Null),
            },
            ReporterMessage::Session(event) => Self {
                kind: "session".to_string(),
                payload: serde_json::to_value(&*event).unwrap_or(serde_json::Value::Null),
            },
        }
    }
}

/// Append to the reporter queue, dropping the *oldest* records on overflow
/// so the dashboard keeps receiving fresh state instead of stale backlog.
fn enqueue(queue: &mut VecDeque<SpoolRecord>, record: SpoolRecord, max_queue: usize) {
    if max_queue == 0 {
        return;
    }
    while queue.len() >= max_queue {
        queue.pop_front();
    }
    queue.push_back(record);
}

/// Capped exponential backoff with jitter: `base * 2^(attempts-1)`, capped
/// at `max_ms`, plus up to `jitter_frac` of the capped delay (fraction in
/// `[0,1]`; `0.0` makes the result deterministic for tests).
fn backoff_delay_ms(base_ms: u64, max_ms: u64, attempts: u32, jitter_frac: f64) -> u64 {
    if base_ms == 0 || max_ms == 0 {
        return 0;
    }
    let shift = attempts.max(1).saturating_sub(1).min(20);
    let uncapped = base_ms.saturating_mul(1u64 << shift);
    let capped = uncapped.min(max_ms);
    let jitter = (capped as f64 * jitter_frac.clamp(0.0, 1.0)) as u64;
    capped.saturating_add(jitter).min(max_ms)
}

/// Random jitter fraction in `[0.0, 0.3)` — keeps retry storms from
/// synchronizing across nodes.
fn random_jitter() -> f64 {
    use rand::Rng;
    rand::thread_rng().gen_range(0.0..0.3)
}

/// Load previously spooled frames (oldest first). Unparseable lines are
/// skipped so one corrupt record cannot wedge the replay forever.
fn load_spool(path: &Path, max_queue: usize) -> VecDeque<SpoolRecord> {
    let mut queue: VecDeque<SpoolRecord> = VecDeque::new();
    let Ok(content) = std::fs::read_to_string(path) else {
        return queue;
    };
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<SpoolRecord>(line) {
            Ok(record) => enqueue(&mut queue, record, max_queue),
            Err(e) => warn!(error = %e, "skipping corrupt telemetry spool line"),
        }
    }
    queue
}

/// Atomically write the queue to the spool file (temp file + rename).
fn save_spool(path: &Path, queue: &VecDeque<SpoolRecord>) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create spool dir {}", parent.display()))?;
        }
    }
    let tmp = path.with_extension("jsonl.tmp");
    {
        let mut file = std::fs::File::create(&tmp)
            .with_context(|| format!("failed to create spool file {}", tmp.display()))?;
        for record in queue {
            let line = serde_json::to_string(record).context("spool serialization failed")?;
            writeln!(file, "{line}").context("spool write failed")?;
        }
        file.sync_all().context("spool sync failed")?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to replace spool file {}", path.display()))?;
    Ok(())
}

fn clear_spool(path: &Path) {
    if path.exists() {
        if let Err(e) = std::fs::remove_file(path) {
            warn!(error = %e, path = %path.display(), "failed to remove empty telemetry spool");
        }
    }
}

/// Sender half of the telemetry bridge. Clonable; one instance per daemon.
#[derive(Debug, Clone)]
pub struct DashboardReporter {
    tx: mpsc::Sender<ReporterMessage>,
    shutdown_tx: watch::Sender<bool>,
}

impl DashboardReporter {
    /// Spawn the background posting task and return the sender handle.
    pub fn spawn(config: DashboardConfig) -> Self {
        let (tx, mut rx) = mpsc::channel::<ReporterMessage>(QUEUE_CAPACITY);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_config = config.clone();
        tokio::spawn(async move {
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(POST_TIMEOUT_SECS))
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    warn!(error = %e, "dashboard telemetry client build failed; reporter stopped");
                    return;
                }
            };

            let max_queue = task_config.max_queue.max(1);

            // Replay anything a previous shutdown spooled to disk.
            let mut queue = load_spool(&task_config.spool_path, max_queue);
            if !queue.is_empty() {
                info!(
                    count = queue.len(),
                    path = %task_config.spool_path.display(),
                    "replaying spooled dashboard telemetry"
                );
            }

            let mut rx_open = true;
            let mut retry_at: Option<tokio::time::Instant> = None;
            let mut attempts: u32 = 0;

            loop {
                let retry_sleep = async {
                    match retry_at {
                        Some(when) => tokio::time::sleep_until(when).await,
                        None => std::future::pending::<()>().await,
                    }
                };

                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        flush_remaining(&client, &task_config, &mut queue).await;
                        if queue.is_empty() {
                            clear_spool(&task_config.spool_path);
                        } else {
                            match save_spool(&task_config.spool_path, &queue) {
                                Ok(()) => info!(
                                    count = queue.len(),
                                    path = %task_config.spool_path.display(),
                                    "spooled unsent dashboard telemetry for next boot"
                                ),
                                Err(e) => warn!(error = %e, "failed to spool unsent telemetry on shutdown"),
                            }
                        }
                        break;
                    }
                    message = rx.recv(), if rx_open => {
                        match message {
                            Some(message) => enqueue(&mut queue, SpoolRecord::from(message), max_queue),
                            None => rx_open = false,
                        }
                    }
                    _ = retry_sleep, if retry_at.is_some() => {
                        retry_at = None;
                    }
                }

                // Deliver from the head of the queue while we are not in a
                // backoff window. Each success resets the attempt counter.
                while retry_at.is_none() {
                    let Some(head) = queue.front().cloned() else {
                        break;
                    };
                    match post_payload(&client, &task_config.url, &task_config.api_key, &head.payload).await
                    {
                        Ok(()) => {
                            queue.pop_front();
                            attempts = 0;
                            debug!(kind = %head.kind, "dashboard frame pushed");
                        }
                        Err(e) => {
                            attempts = attempts.saturating_add(1);
                            let delay_ms = backoff_delay_ms(
                                task_config.backoff_base_ms,
                                task_config.backoff_max_ms,
                                attempts,
                                random_jitter(),
                            );
                            warn!(
                                error = %e,
                                kind = %head.kind,
                                attempts,
                                retry_in_ms = delay_ms,
                                "dashboard push failed; frame kept for retry"
                            );
                            retry_at = Some(tokio::time::Instant::now() + Duration::from_millis(delay_ms));
                            break;
                        }
                    }
                }

                if !rx_open && queue.is_empty() {
                    break;
                }
            }
        });
        Self {
            tx,
            shutdown_tx,
        }
    }

    /// Queue a frame without blocking the heartbeat. Frames are dropped on
    /// backlog overflow or when the posting task is gone.
    pub fn try_push(&self, frame: DashboardTelemetry) {
        self.try_send(ReporterMessage::Telemetry(Box::new(frame)));
    }

    /// Queue a session-close event (best-effort, never blocks the relay).
    pub fn try_push_session(&self, event: SessionEvent) {
        self.try_send(ReporterMessage::Session(Box::new(event)));
    }

    /// Signal the reporting task to run a time-boxed final delivery and
    /// spool whatever remains. Non-blocking; returns `false` when the task
    /// has already exited.
    pub fn shutdown(&self) -> bool {
        self.shutdown_tx.send(true).is_ok()
    }

    fn try_send(&self, message: ReporterMessage) {
        use tokio::sync::mpsc::error::TrySendError;
        match self.tx.try_send(message) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                debug!("dashboard telemetry channel full; dropping frame");
            }
            Err(TrySendError::Closed(_)) => {
                warn!("dashboard telemetry task not running; dropping frame");
            }
        }
    }


}

/// Final delivery attempt on shutdown: keep posting until the queue is
/// empty or the time budget runs out; the caller spools whatever is left.
async fn flush_remaining(
    client: &reqwest::Client,
    config: &DashboardConfig,
    queue: &mut VecDeque<SpoolRecord>,
) {
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(SHUTDOWN_FLUSH_SECS);
    while let Some(head) = queue.front() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(
            remaining,
            post_payload(client, &config.url, &config.api_key, &head.payload),
        )
        .await
        {
            Ok(Ok(())) => {
                debug!(kind = %head.kind, "dashboard frame flushed on shutdown");
                queue.pop_front();
            }
            Ok(Err(e)) => {
                warn!(error = %e, kind = %head.kind, "shutdown flush failed; frame will be spooled");
                break;
            }
            Err(_elapsed) => {
                warn!("shutdown flush timed out; remaining frames will be spooled");
                break;
            }
        }
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

async fn post_payload<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    payload: &T,
) -> Result<()> {
    let resp = client
        .post(url)
        .header("Authorization", format!("Bearer {api_key}"))
        .json(payload)
        .send()
        .await
        .context("dashboard POST failed")?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .unwrap_or_else(|_| "<unreadable body>".to_string());
    if !status.is_success() {
        anyhow::bail!("dashboard endpoint returned {status}: {body}");
    }
    Ok(())
}

/// Generate a random RFC 4122 version-4 UUID without the `uuid` crate.
#[cfg(test)]
fn uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // RFC 4122 variant bits
    let hex: Vec<String> = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        hex[0..4].concat(),
        hex[4..6].concat(),
        hex[6..8].concat(),
        hex[8..10].concat(),
        hex[10..16].concat()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(latency_ms: f64, loss: f64, bw_up: f64, bw_down: f64) -> FeatureInputs {
        FeatureInputs {
            cpu_pct: 12.0,
            memory_pct: 40.0,
            disk_usage_pct: 30.0,
            latency_ms,
            packet_loss_pct: loss,
            bandwidth_up_mbps: bw_up,
            bandwidth_down_mbps: bw_down,
            uptime_secs: 3600,
            earnings_usd: 0.5,
            available_upload_mbps: 90.0,
            available_download_mbps: 90.0,
            active_sessions: 2,
            max_sessions: 10,
        }
    }

    #[test]
    fn uuid_v4_is_valid_shape_and_version() {
        let id = uuid_v4();
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
        // Version nibble is '4' in chars.
        assert!(parts[2].starts_with('4'));
        // Variant nibble is 8/9/a/b in chars.
        assert!(parts[3].starts_with(['8', '9', 'a', 'b']));
        assert_ne!(uuid_v4(), uuid_v4());
    }

    #[test]
    fn feature_vector_is_32_dimensions_bounded() {
        let series = (0..10)
            .map(|i| sample(20.0 + i as f64, 0.5, 30.0, 40.0))
            .collect::<Vec<_>>();
        let v = build_feature_vector(&series);
        assert_eq!(v.len(), FEATURE_VECTOR_DIM);
        assert_eq!(v.len(), 32);
        for x in &v {
            assert!((0.0..=1.0).contains(x), "feature {x} out of [0,1]");
        }
    }

    #[test]
    fn feature_vector_tracks_real_signal() {
        let healthy = vec![sample(10.0, 0.1, 20.0, 30.0); 3];
        let degraded = vec![sample(400.0, 9.0, 20.0, 30.0); 3];
        let v_healthy = build_feature_vector(&healthy);
        let v_degraded = build_feature_vector(&degraded);
        // Higher latency degrades the latency feature and cloud-wise raises it.
        assert!(
            v_degraded[1] > v_healthy[1],
            "latency feature should rise with more latency"
        );
        assert!(
            v_degraded[4] > v_healthy[4],
            "packet-loss feature should rise with more loss"
        );
    }

    #[test]
    fn empty_series_produces_zero_vector() {
        let v = build_feature_vector(&[]);
        assert_eq!(v.len(), FEATURE_VECTOR_DIM);
        assert!(v.iter().all(|&x| x == 0.0));
    }

    #[test]
    fn session_event_from_close_maps_fields_and_earnings() {
        let details = SessionCloseDetails {
            remote_peer_id: "12D3KooWabc".to_string(),
            remote_ip: Some("1.2.3.4".to_string()),
            remote_port: Some(52311),
            direction: "inbound".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            ended_at: "2026-01-01T00:01:00Z".to_string(),
            duration_secs: 60.0,
            avg_latency_ms: 0.42,
            exit_reason: "completed".to_string(),
        };
        let event = SessionEvent::from_close(
            "12D3KooWprovider",
            "tun_123",
            1_000_000_000, // 1 GB sent
            1_000_000_000, // 1 GB received
            0.50,
            &details,
        );
        assert_eq!(event.kind, "session");
        assert_eq!(event.action, "closed");
        assert_eq!(event.status, "completed");
        assert_eq!(event.session_type, "tunnel");
        assert_eq!(event.protocol, "tcp");
        assert_eq!(event.bytes_relayed, 2_000_000_000);
        assert_eq!(event.peer_id, "12D3KooWprovider");
        assert_eq!(event.remote_peer_id, "12D3KooWabc");
        assert_eq!(event.remote_ip.as_deref(), Some("1.2.3.4"));
        assert_eq!(event.remote_port, Some(52311));
        // 2 GB × $0.50/GB = $1.00.
        assert!(
            (event.earnings_usd - 1.0).abs() < 1e-9,
            "earnings {} != 1.0",
            event.earnings_usd
        );
        assert_eq!(event.duration_secs, 60.0);
        // Field names are snake_case so the edge function matches the heartbeat.
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "session");
        assert_eq!(json["session_id"], "tun_123");
        assert_eq!(json["peer_id"], "12D3KooWprovider");
        assert_eq!(json["exit_reason"], "completed");
        assert_eq!(json["remote_ip"], "1.2.3.4");
        // node_id and user_id must not appear in the payload.
        assert!(json.get("node_id").is_none(), "node_id must not be present");
        assert!(json.get("user_id").is_none(), "user_id must not be present");
    }

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        // Deterministic with zero jitter.
        assert_eq!(backoff_delay_ms(500, 30_000, 1, 0.0), 500);
        assert_eq!(backoff_delay_ms(500, 30_000, 2, 0.0), 1_000);
        assert_eq!(backoff_delay_ms(500, 30_000, 3, 0.0), 2_000);
        assert_eq!(backoff_delay_ms(500, 30_000, 7, 0.0), 30_000);
        assert_eq!(backoff_delay_ms(500, 30_000, 100, 0.0), 30_000, "caps at max");
    }

    #[test]
    fn backoff_jitter_stays_within_cap() {
        for _ in 0..50 {
            let d = backoff_delay_ms(500, 30_000, 4, random_jitter());
            assert!((4_000..=30_000).contains(&d), "delay {d} outside bounds");
        }
    }

    #[test]
    fn backoff_handles_degenerate_inputs() {
        assert_eq!(backoff_delay_ms(0, 30_000, 3, 0.0), 0);
        assert_eq!(backoff_delay_ms(500, 0, 3, 0.0), 0);
        // attempts=0 is treated as the first attempt.
        assert_eq!(backoff_delay_ms(500, 30_000, 0, 0.0), 500);
    }

    #[test]
    fn enqueue_drops_oldest_on_overflow() {
        let mut queue: VecDeque<SpoolRecord> = VecDeque::new();
        for i in 0..5 {
            enqueue(
                &mut queue,
                SpoolRecord {
                    kind: format!("k{i}"),
                    payload: serde_json::json!({}),
                },
                3,
            );
        }
        assert_eq!(queue.len(), 3);
        assert_eq!(queue[0].kind, "k2");
        assert_eq!(queue[2].kind, "k4");
    }

    #[test]
    fn spool_record_roundtrips_through_json() {
        let record = SpoolRecord {
            kind: "session".to_string(),
            payload: serde_json::json!({"session_id": "abc", "bytes_sent": 7}),
        };
        let json = serde_json::to_string(&record).unwrap();
        let back: SpoolRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, "session");
        assert_eq!(back.payload["session_id"], "abc");
        assert_eq!(back.payload["bytes_sent"], 7);
    }

    #[test]
    fn reporter_message_converts_to_spool_record() {
        let details = SessionCloseDetails {
            remote_peer_id: "peer".to_string(),
            remote_ip: None,
            remote_port: None,
            direction: "inbound".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            ended_at: "2026-01-01T00:01:00Z".to_string(),
            duration_secs: 60.0,
            avg_latency_ms: 1.0,
            exit_reason: "completed".to_string(),
        };
        let event = SessionEvent::from_close(
            "12D3KooWprovider",
            "tun_1",
            10,
            20,
            0.5,
            &details,
        );
        let record = SpoolRecord::from(ReporterMessage::Session(Box::new(event)));
        assert_eq!(record.kind, "session");
        assert_eq!(record.payload["session_id"], "tun_1");
        assert_eq!(record.payload["peer_id"], "12D3KooWprovider");
        assert_eq!(record.payload["bytes_relayed"], 30);
        assert!(record.payload.get("node_id").is_none());
        assert!(record.payload.get("user_id").is_none());
    }

    #[test]
    fn spool_save_load_roundtrip_and_corruption_tolerance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("spool.jsonl");

        let mut queue: VecDeque<SpoolRecord> = VecDeque::new();
        for i in 0..3 {
            enqueue(
                &mut queue,
                SpoolRecord {
                    kind: "telemetry".to_string(),
                    payload: serde_json::json!({"n": i}),
                },
                16,
            );
        }
        save_spool(&path, &queue).unwrap();

        // Corrupt middle line must be skipped, not abort the replay.
        let mut content = std::fs::read_to_string(&path).unwrap();
        content.push_str("{not json at all\n");
        std::fs::write(&path, content).unwrap();

        let loaded = load_spool(&path, 16);
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].payload["n"], 0);
        assert_eq!(loaded[2].payload["n"], 2);

        // Load caps at max_queue (drop-oldest).
        let capped = load_spool(&path, 2);
        assert_eq!(capped.len(), 2);
        assert_eq!(capped[0].payload["n"], 1);

        clear_spool(&path);
        assert!(!path.exists());
        // Clearing a missing spool is a no-op.
        clear_spool(&path);
        assert!(load_spool(&path, 16).is_empty());
    }

    #[test]
    fn config_from_env_reads_durability_knobs() {
        // Consolidated (env is process-global; tests run in parallel).
        for key in [
            "DASHBOARD_TELEMETRY_URL",
            "ECHO_API_KEY",
        ] {
            std::env::remove_var(key);
        }
        std::env::remove_var("DASHBOARD_TELEMETRY_SPOOL");
        std::env::remove_var("DASHBOARD_TELEMETRY_BACKOFF_BASE_MS");
        std::env::remove_var("DASHBOARD_TELEMETRY_BACKOFF_MAX_MS");
        std::env::remove_var("DASHBOARD_TELEMETRY_MAX_QUEUE");
        assert!(DashboardConfig::from_env().is_none());

        std::env::set_var("DASHBOARD_TELEMETRY_URL", "https://example.test/ingest");
        std::env::set_var("ECHO_API_KEY", "eak_test_key");
        std::env::set_var("DASHBOARD_TELEMETRY_SPOOL", "/tmp/x/spool.jsonl");
        std::env::set_var("DASHBOARD_TELEMETRY_BACKOFF_BASE_MS", "250");
        std::env::set_var("DASHBOARD_TELEMETRY_BACKOFF_MAX_MS", "9000");
        std::env::set_var("DASHBOARD_TELEMETRY_MAX_QUEUE", "32");
        let cfg = DashboardConfig::from_env().unwrap();
        assert_eq!(cfg.api_key, "eak_test_key");
        assert_eq!(cfg.spool_path, PathBuf::from("/tmp/x/spool.jsonl"));
        assert_eq!(cfg.backoff_base_ms, 250);
        assert_eq!(cfg.backoff_max_ms, 9000);
        assert_eq!(cfg.max_queue, 32);

        // Defaults when durability knobs are unset.
        std::env::remove_var("DASHBOARD_TELEMETRY_SPOOL");
        std::env::remove_var("DASHBOARD_TELEMETRY_BACKOFF_BASE_MS");
        std::env::remove_var("DASHBOARD_TELEMETRY_BACKOFF_MAX_MS");
        std::env::remove_var("DASHBOARD_TELEMETRY_MAX_QUEUE");
        let cfg = DashboardConfig::from_env().unwrap();
        assert_eq!(cfg.spool_path, PathBuf::from("data/telemetry_spool.jsonl"));
        assert_eq!(cfg.backoff_base_ms, 500);
        assert_eq!(cfg.backoff_max_ms, 30_000);
        assert_eq!(cfg.max_queue, 256);

        for key in [
            "DASHBOARD_TELEMETRY_URL",
            "ECHO_API_KEY",
        ] {
            std::env::remove_var(key);
        }
    }
}