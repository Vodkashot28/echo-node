//! Daemon lifecycle management: spawn, stop, and query the background daemon.
//!
//! `echo-node start` writes a PID file and launches the daemon subprocess with
//! `LISTEN_ADDR=127.0.0.1:<port>`, pointing `SQLITE_DB` and `IDENTITY_PATH`
//! at the XDG home. `echo-node stop` sends **SIGTERM** to the PID recorded in
//! the PID file — the REST `POST /control {"action":"stop"}` only flips an
//! in-memory flag and does not exit the process, so it cannot stop the
//! daemon. Other subcommands call the `query_*`/`get_json` helpers that hit
//! the REST endpoints.
//!
//! All request helpers are async: the CLI runs inside the same tokio runtime
//! as the rest of the binary (`#[tokio::main]`), and building a second
//! runtime here would panic ("cannot start a runtime from within a
//! runtime").

use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

use super::config::{daemon_addr, EchoHome, NodeConfig};

/// Seconds to wait for the daemon to become reachable after spawning.
pub const STARTUP_TIMEOUT_SECS: u64 = 15;
/// Poll interval while waiting for daemon readiness.
const POLL_INTERVAL_MS: u64 = 300;
/// Seconds to wait for the daemon process to exit after SIGTERM.
const STOP_TIMEOUT_SECS: u64 = 10;

// ── HTTP plumbing ──────────────────────────────────────────────────────────

fn client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("building HTTP client")
}

/// GET `addr + path` and deserialize the JSON body.
pub async fn get_json<T: DeserializeOwned>(addr: &str, path: &str) -> Result<T> {
    let url = format!("{addr}{path}");
    let resp = client(Duration::from_secs(5))?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("GET {url} → {status}: {}", body.trim());
    }
    resp.json::<T>()
        .await
        .with_context(|| format!("parsing response from {url}"))
}

/// POST `addr + path` with a JSON body, returning `(status, body)`.
///
/// Bid/close handlers answer errors as `(StatusCode, plain text)`, so the
/// caller inspects the status instead of this helper failing for it.
pub async fn post_json(addr: &str, path: &str, body: &impl Serialize) -> Result<(u16, String)> {
    let url = format!("{addr}{path}");
    let resp = client(Duration::from_secs(5))?
        .post(&url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

/// Returns true if the daemon's `/health` endpoint answers 2xx.
pub async fn ping_daemon(addr: &str) -> bool {
    let Ok(client) = client(Duration::from_millis(800)) else {
        return false;
    };
    match client.get(format!("{addr}/health")).send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// Poll the daemon's `/health` endpoint until it responds or the timeout
/// elapses. Callers that want a spinner should tick it around this call
/// (`indicatif`'s steady tick runs on its own thread).
pub async fn wait_for_daemon(addr: &str, timeout_secs: u64) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if ping_daemon(addr).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    bail!(
        "daemon did not become reachable within {timeout_secs}s — check ~/.config/echomesh/logs/daemon.log"
    )
}

// ── Spawn ──────────────────────────────────────────────────────────────────

/// Spawn the daemon as a detached background subprocess and wait for its
/// health endpoint to answer.
///
/// Sets env vars so the daemon uses the XDG home locations and listens on
/// `127.0.0.1:<config port>`. Returns `Ok(pid)` once the daemon is reachable.
pub async fn spawn_daemon(home: &EchoHome, cfg: &NodeConfig, region: &str) -> Result<u32> {
    // Resolve our own binary path.
    let exe = std::env::current_exe().context("cannot determine binary path")?;

    let log_path = home.daemon_log_path();
    // `tracing_subscriber::fmt()` writes to stdout, while panics and any
    // early eprintln go to stderr — open the log twice so neither is lost.
    let open_log = || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("opening log file {}", log_path.display()))
    };
    let log_stdout = open_log()?;
    let log_stderr = open_log()?;

    let db_url = format!("sqlite:{}", home.db_path().display());
    let identity_path = home.identity_path().display().to_string();

    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("daemon")
        .env("LISTEN_ADDR", format!("127.0.0.1:{}", cfg.port))
        .env("SQLITE_DB", &db_url)
        .env("IDENTITY_PATH", &identity_path)
        .env("NODE_REGION", region)
        .env("NODE_MODE", "provider")
        .env("MAX_UPLOAD_MBPS", cfg.max_upload_mbps.to_string())
        .env("MAX_DOWNLOAD_MBPS", cfg.max_download_mbps.to_string())
        .env("RUST_LOG", "echo_daemon=info")
        // A CLI-managed daemon always runs on the XDG SQLite store — a stray
        // DATABASE_URL in the caller's shell must not silently redirect it.
        .env_remove("DATABASE_URL")
        .stdin(std::process::Stdio::null())
        .stdout(log_stdout)
        .stderr(log_stderr);

    // Marketplace knobs from config (absent → daemon defaults).
    if cfg.auto_bid {
        cmd.env("BID_AUTO", "true");
        if let Some(price) = cfg.bid_price_usd_per_gb {
            cmd.env("BID_PRICE_USD_PER_GB", price.to_string());
        }
    }

    // Detach from the current process group on Unix so the daemon survives
    // the CLI process exiting.
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| {
            // New session — daemon becomes its own process group leader.
            libc::setsid();
            Ok(())
        });
    }

    let child = cmd.spawn().context("failed to spawn daemon subprocess")?;
    let pid = child.id();

    // Write PID file.
    write_pid_file(&home.pid_path(), pid)?;

    // Dropping the Child handle does NOT kill the subprocess — it is simply
    // never waited on (fully detached from this CLI process).
    drop(child);

    // Wait until the daemon's /health endpoint responds.
    wait_for_daemon(&daemon_addr(cfg.port), STARTUP_TIMEOUT_SECS).await?;

    Ok(pid)
}

/// Write a PID file.
fn write_pid_file(path: &std::path::Path, pid: u32) -> Result<()> {
    std::fs::write(path, pid.to_string())
        .with_context(|| format!("writing PID file {}", path.display()))
}

/// Read the PID file, returning `None` if absent or unparseable.
pub fn read_pid_file(path: &std::path::Path) -> Option<u32> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// True when a process with this PID exists (signal 0 probe).
#[cfg(unix)]
pub fn process_alive(pid: u32) -> bool {
    // SAFETY: kill(pid, 0) only probes for the process's existence; the
    // PID is validated (parsed from our own PID file) before reaching here.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
pub fn process_alive(_pid: u32) -> bool {
    false
}

/// Send SIGTERM (graceful shutdown — `shutdown_signal()` handles it).
#[cfg(unix)]
fn signal_term(pid: u32) -> Result<()> {
    // SAFETY: signalling our own daemon's PID; SIGTERM is handled by the
    // daemon's graceful-shutdown path, never an unkillable state.
    let rc = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    if rc != 0 {
        bail!(
            "failed to signal pid {pid}: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn signal_term(pid: u32) -> Result<()> {
    bail!(
        "stopping by PID is only supported on Unix (pid {pid}); stop the daemon out-of-band instead"
    )
}

// ── Stop ───────────────────────────────────────────────────────────────────

/// Stop the running daemon: SIGTERM the PID from the PID file, wait for the
/// process to exit, then clean the PID file up.
///
/// A stale PID file (process gone) is removed and reported as "not running".
/// A daemon answering HTTP with no PID file was started outside
/// `echo-node start` (e.g. systemd/`cargo run`) — we cannot safely signal an
/// unknown PID, so this reports how to stop it instead of guessing.
pub async fn stop_daemon(home: &EchoHome, addr: &str) -> Result<()> {
    let pid = read_pid_file(&home.pid_path());
    let alive = pid.is_some_and(process_alive);

    if !alive {
        if pid.is_some() {
            let _ = std::fs::remove_file(home.pid_path());
        }
        if ping_daemon(addr).await {
            bail!(
                "daemon is answering on {addr} but has no PID file (started outside `echo-node start`?) — stop it with SIGTERM or its service manager"
            );
        }
        println!("daemon is not running");
        return Ok(());
    }
    let pid = pid.expect("alive implies a parsed PID");

    signal_term(pid)?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(STOP_TIMEOUT_SECS);
    while process_alive(pid) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if process_alive(pid) {
        bail!("daemon (pid {pid}) did not exit within {STOP_TIMEOUT_SECS}s of SIGTERM");
    }

    let _ = std::fs::remove_file(home.pid_path());
    println!("daemon stopped (pid {pid})");
    Ok(())
}

// ── Reputation helper ──────────────────────────────────────────────────────

/// Derive a 0-100 reputation integer from the metrics quality score.
pub fn reputation_from_quality(quality: f64) -> u8 {
    ((quality * 100.0).clamp(0.0, 100.0)) as u8
}
