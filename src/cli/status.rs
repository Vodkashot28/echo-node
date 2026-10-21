//! `echo-node status` and `echo-node doctor`.
//!
//! `status` is a live snapshot from `/status` + `/metrics`; `doctor` is an
//! offline-friendly diagnostic that walks the XDG home, the identity file,
//! the database, the PID file, the health endpoint and the REST port, and
//! exits non-zero when a check hard-fails.

use anyhow::Result;
use serde::Deserialize;

use super::config::{daemon_addr, EchoHome};
use super::daemon::{get_json, ping_daemon, process_alive, read_pid_file, reputation_from_quality};

// ── REST response shapes (subset we actually display) ──────────────────────

#[derive(Debug, Deserialize)]
pub struct StatusResponse {
    pub running: Option<bool>,
    pub node_id: Option<String>,
    pub peer_id: Option<String>,
    pub uptime_secs: Option<u64>,
    pub heartbeat_count: Option<u64>,
    pub last_heartbeat: Option<u64>,
    pub db_connected: Option<bool>,
    pub active_sessions: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
pub struct MetricsResponse {
    pub latency_ms: Option<f64>,
    pub bandwidth_up_mbps: Option<f64>,
    pub bandwidth_down_mbps: Option<f64>,
    pub available_upload_mbps: Option<f64>,
    pub available_download_mbps: Option<f64>,
    pub quality_score: Option<f64>,
    pub trust_score: Option<f64>,
    pub peer_id: Option<String>,
    pub region: Option<String>,
    pub active_sessions: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub status: Option<String>,
    pub database: Option<String>,
    pub last_heartbeat_age_secs: Option<u64>,
}

// ── status ─────────────────────────────────────────────────────────────────

/// `echo-node status` — print the daemon's live status + key metrics.
pub async fn cmd_status(home: &EchoHome) -> Result<()> {
    let cfg = home.load_config()?;
    let addr = daemon_addr(cfg.port);

    if !ping_daemon(&addr).await {
        println!("daemon:  not running (no response from {addr})");
        match read_pid_file(&home.pid_path()) {
            Some(pid) if process_alive(pid) => {
                println!("pid:     {pid} (alive — health endpoint still warming up?)");
            }
            Some(pid) => println!("pid:     {pid} (stale PID file — run `echo-node doctor`)"),
            None => println!("pid:     none"),
        }
        return Ok(());
    }

    let status: StatusResponse = get_json(&addr, "/status").await?;
    // `/metrics` answers 503 until the first heartbeat sample lands (up to
    // HEARTBEAT_SECS after boot) — degrade to defaults rather than failing.
    let (metrics, warming) = match get_json::<MetricsResponse>(&addr, "/metrics").await {
        Ok(m) => (m, false),
        Err(_) => (MetricsResponse::default(), true),
    };

    println!("daemon:  running on {addr}");
    row(
        "state",
        if status.running.unwrap_or(true) {
            "active"
        } else {
            "paused (stopped via /control — heartbeat halted)"
        },
    );
    row("node id", status.node_id.as_deref().unwrap_or("unknown"));
    row(
        "peer id",
        metrics
            .peer_id
            .or(status.peer_id)
            .as_deref()
            .unwrap_or("unknown"),
    );
    row("region", metrics.region.as_deref().unwrap_or("unknown"));
    row("uptime", format_u64_secs(status.uptime_secs.unwrap_or(0)));
    row("heartbeats", status.heartbeat_count.unwrap_or(0));
    row(
        "last heartbeat",
        match status.last_heartbeat {
            Some(ts) => format!("{}s ago", now_secs().saturating_sub(ts)),
            None => "never".to_string(),
        },
    );
    row(
        "database",
        if status.db_connected.unwrap_or(false) {
            "connected"
        } else {
            "disconnected"
        },
    );
    row(
        "sessions",
        status
            .active_sessions
            .or(metrics.active_sessions)
            .unwrap_or(0),
    );
    row(
        "latency",
        format!("{:.1} ms", metrics.latency_ms.unwrap_or_default()),
    );
    row(
        "capacity",
        format!(
            "↓{:.1} / ↑{:.1} Mbps available",
            metrics.available_download_mbps.unwrap_or_default(),
            metrics.available_upload_mbps.unwrap_or_default(),
        ),
    );
    row(
        "throughput",
        format!(
            "↓{:.1} / ↑{:.1} Mbps measured",
            metrics.bandwidth_down_mbps.unwrap_or_default(),
            metrics.bandwidth_up_mbps.unwrap_or_default(),
        ),
    );
    let quality = metrics.quality_score.unwrap_or_default();
    row(
        "quality",
        format!(
            "{quality:.2} (reputation {}/100)",
            reputation_from_quality(quality)
        ),
    );
    row(
        "trust",
        format!("{:.2}", metrics.trust_score.unwrap_or_default()),
    );
    if warming {
        println!("  note: /metrics still warming up (503 until the first heartbeat sample)");
    }
    Ok(())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn row(label: &str, value: impl std::fmt::Display) {
    println!("  {label:<16} {value}");
}

/// Compact uptime rendering: `3d 4h`, `4h 5m`, `5m 6s`, `9s`.
fn format_u64_secs(secs: u64) -> String {
    let d = secs / 86_400;
    let h = (secs % 86_400) / 3_600;
    let m = (secs % 3_600) / 60;
    let s = secs % 60;
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

// ── doctor ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Pass,
    Warn,
    Fail,
}

/// `echo-node doctor` — walk the install and report ✓/⚠/✗ per check.
/// Exits 1 when any check hard-fails (CI/script friendly).
pub async fn cmd_doctor(home: &EchoHome) -> Result<()> {
    let mut results: Vec<(Verdict, String)> = Vec::new();

    // 1. Config home exists and is writable.
    match probe_dir(&home.dir) {
        Ok(()) => results.push((
            Verdict::Pass,
            format!("config home writable — {}", home.dir.display()),
        )),
        Err(e) => results.push((Verdict::Fail, format!("config home not writable — {:#}", e))),
    }

    // 2. Identity file: present, parses, owner-only perms, has a peer id.
    results.push(check_identity(home));

    // 3. config.toml present + parses.
    results.push(check_config(home));

    // 4. SQLite store (created on first daemon run).
    let db = home.db_path();
    if db.exists() {
        let size = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
        results.push((
            Verdict::Pass,
            format!("database present — {} ({size} bytes)", db.display()),
        ));
    } else {
        results.push((
            Verdict::Warn,
            format!("database absent — {} (created on first run)", db.display()),
        ));
    }

    // 5. Daemon + PID file consistency.
    let cfg = home.load_config()?;
    let addr = daemon_addr(cfg.port);
    let pid = read_pid_file(&home.pid_path());
    let running = ping_daemon(&addr).await;
    match (running, pid) {
        (true, Some(p)) if process_alive(p) => results.push((
            Verdict::Pass,
            format!("daemon running — pid {p}, REST API on {addr}"),
        )),
        (true, None) => results.push((
            Verdict::Warn,
            format!(
                "daemon answering on {addr} without a PID file (started out-of-band) — `echo-node stop` cannot stop it"
            ),
        )),
        (true, Some(p)) => results.push((
            Verdict::Warn,
            format!("daemon answering on {addr} but PID file holds dead pid {p}"),
        )),
        (false, Some(p)) if process_alive(p) => results.push((
            Verdict::Warn,
            format!("pid {p} alive but /health not answering yet — retry shortly"),
        )),
        (false, Some(p)) => results.push((
            Verdict::Fail,
            format!("stale PID file — pid {p} is gone (remove {})", home.pid_path().display()),
        )),
        (false, None) => results.push((
            Verdict::Warn,
            "daemon not running — start it with `echo-node start`".to_string(),
        )),
    }

    // 6. Health detail (only meaningful when reachable).
    if running {
        match get_json::<HealthResponse>(&addr, "/health").await {
            Ok(h) => {
                let status = h.status.as_deref().unwrap_or("unknown");
                let db = h.database.as_deref().unwrap_or("unknown");
                let age = h.last_heartbeat_age_secs.unwrap_or(0);
                let verdict = if status == "healthy" {
                    Verdict::Pass
                } else {
                    Verdict::Warn
                };
                results.push((
                    verdict,
                    format!("health — status={status} db={db} last_heartbeat={age}s ago"),
                ));
            }
            Err(e) => results.push((Verdict::Fail, format!("health check failed — {e:#}"))),
        }
    }

    // 7. REST port: free, owned by the daemon, or squatted.
    results.push(check_port(cfg.port, running).await);

    // 8. Logs directory + latest daemon log.
    let log_dir = home.log_dir();
    if log_dir.is_dir() {
        results.push((Verdict::Pass, format!("logs dir — {}", log_dir.display())));
    } else {
        results.push((
            Verdict::Warn,
            format!(
                "logs dir absent — {} (created on startup)",
                log_dir.display()
            ),
        ));
    }
    let log = home.daemon_log_path();
    if log.exists() {
        let size = std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
        results.push((
            Verdict::Pass,
            format!("log file — {} ({size} bytes)", log.display()),
        ));
    } else {
        results.push((
            Verdict::Warn,
            format!(
                "log file absent — {} (created on first start)",
                log.display()
            ),
        ));
    }

    // Report.
    println!("echo-node doctor — {}", home.dir.display());
    let (mut passes, mut warns, mut fails) = (0usize, 0usize, 0usize);
    for (verdict, msg) in &results {
        match verdict {
            Verdict::Pass => {
                passes += 1;
                println!("  ✓ {msg}");
            }
            Verdict::Warn => {
                warns += 1;
                println!("  ⚠ {msg}");
            }
            Verdict::Fail => {
                fails += 1;
                println!("  ✗ {msg}");
            }
        }
    }
    println!("{passes} passed, {warns} warnings, {fails} failed");

    if fails > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn check_identity(home: &EchoHome) -> (Verdict, String) {
    let path = home.identity_path();
    if !path.exists() {
        return (
            Verdict::Fail,
            format!(
                "identity missing — {} (run `echo-node onboard`)",
                path.display()
            ),
        );
    }
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) => r,
        Err(e) => return (Verdict::Fail, format!("identity unreadable — {e}")),
    };
    let identity: echo_daemon::identity::NodeIdentity = match serde_json::from_str(&raw) {
        Ok(i) => i,
        Err(e) => return (Verdict::Fail, format!("identity corrupt — {e}")),
    };
    if identity.peer_id_str().is_empty() {
        return (Verdict::Fail, "identity has an empty peer id".into());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path)
            .map(|m| m.permissions().mode() & 0o777)
            .unwrap_or(0);
        if mode & 0o077 != 0 {
            return (
                Verdict::Warn,
                format!(
                    "identity permissions {mode:o} — should be 600 ({})",
                    path.display()
                ),
            );
        }
    }

    (
        Verdict::Pass,
        format!("identity ok — peer {}", identity.peer_id_str()),
    )
}

fn check_config(home: &EchoHome) -> (Verdict, String) {
    let path = home.config_path();
    if !path.exists() {
        return (
            Verdict::Warn,
            format!("config.toml absent — {} (defaults in use)", path.display()),
        );
    }
    match home.load_config() {
        Ok(cfg) => (
            Verdict::Pass,
            format!(
                "config ok — region={} port={} stage={}",
                cfg.region, cfg.port, cfg.onboard_stage
            ),
        ),
        Err(e) => (Verdict::Fail, format!("config unusable — {e:#}")),
    }
}

async fn check_port(port: u16, daemon_running: bool) -> (Verdict, String) {
    let probe = tokio::net::TcpListener::bind(("127.0.0.1", port)).await;
    match (probe.is_ok(), daemon_running) {
        (true, true) => (
            Verdict::Fail,
            format!("port {port} free but the daemon claims to be running"),
        ),
        (true, false) => (
            Verdict::Warn,
            format!("port {port} free — daemon not listening"),
        ),
        (false, true) => (Verdict::Pass, format!("port {port} bound by the daemon")),
        (false, false) => (
            Verdict::Fail,
            format!("port {port} in use by another process"),
        ),
    }
}

/// Create (if needed) + probe write access to the home directory.
fn probe_dir(dir: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let probe = dir.join(".doctor-probe");
    std::fs::write(&probe, b"ok")?;
    let _ = std::fs::remove_file(&probe);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uptime_formatting() {
        assert_eq!(format_u64_secs(9), "9s");
        assert_eq!(format_u64_secs(65), "1m 5s");
        assert_eq!(format_u64_secs(3_700), "1h 1m");
        assert_eq!(format_u64_secs(90_000), "1d 1h");
    }

    #[test]
    fn health_json_parses_daemon_shape() {
        let raw = r#"{"status":"healthy","database":"ok","last_heartbeat_age_secs":7}"#;
        let h: HealthResponse = serde_json::from_str(raw).expect("parse");
        assert_eq!(h.status.as_deref(), Some("healthy"));
        assert_eq!(h.database.as_deref(), Some("ok"));
        assert_eq!(h.last_heartbeat_age_secs, Some(7));
    }
}
