//! Onboarding state machine — the seven steps from a fresh machine to an
//! active marketplace node.
//!
//! The current position lives in `config.toml` as `onboard_stage`
//! (`OnboardStage`), so `echo-node onboard` is **resumable**: each step
//! persists the stage it completed and a later run picks up where the
//! previous one stopped. `--reset` rewinds to `NEW`.
//!
//! | Step | Action                              | Stage after             |
//! |------|-------------------------------------|-------------------------|
//! | 1    | Prepare XDG config home             | `NEW` (nothing to record) |
//! | 2    | Create/load `identity.json`         | `IDENTITY_CREATED`      |
//! | 3    | Claim node at backend (best effort) | `NODE_CLAIMED`          |
//! | 4    | Join network (start + verify daemon)| `NETWORK_JOINED`        |
//! | 5    | Profile capabilities from `/metrics`| `CAPABILITY_PROFILED`   |
//! | 6    | Enable marketplace (auto-bid)       | `MARKETPLACE_ENABLED`   |
//! | 7    | Activate (auto-start + verify)      | `ACTIVE`                |
//!
//! Network-dependent steps degrade gracefully: `--offline` marks 3/4/5 as
//! skipped and still advances (dev machines have no backend and often no
//! daemon), while a *failed* claim falls back to a local claim with a
//! warning — the locally derived `provider_id` is what marketplace
//! settlement actually keys on.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use echo_daemon::identity::NodeIdentity;

use super::config::{EchoHome, NodeConfig, OnboardStage, derive_provider_id, daemon_addr};
use super::daemon;
use super::status::{MetricsResponse, StatusResponse};

/// Human-readable step titles (index = step number - 1).
const STEPS: [&str; 7] = [
    "prepare config home",
    "create node identity",
    "claim node with backend",
    "join network (start daemon)",
    "profile capabilities",
    "enable marketplace",
    "activate node",
];

/// Default USD/GB ask when `SETTLEMENT_RATE_USD_PER_GB` is unset.
const DEFAULT_PRICE_USD_PER_GB: f64 = 0.50;

/// How long capability profiling waits for the daemon's first heartbeat
/// sample (`/metrics` is 503 until one exists; heartbeat period is 10 s).
const METRICS_WAIT_SECS: u64 = 30;

/// Backend claim payload (`POST {backend_url}/api/v1/nodes/claim`).
#[derive(Debug, Serialize)]
struct ClaimRequest<'a> {
    provider_id: &'a str,
    peer_id: &'a str,
    region: &'a str,
}

/// Backend claim response — `claim_code` is optional (the backend may 200
/// with no body or a JSON object without it).
#[derive(Debug, Deserialize)]
struct ClaimResponse {
    #[serde(default)]
    claim_code: Option<String>,
}

/// First step to run for a given stage.
fn start_step(stage: &OnboardStage) -> u8 {
    match stage {
        // Nothing done yet — start from the top.
        OnboardStage::New => 1,
        // Steps 1-2 (home + identity) are complete.
        OnboardStage::IdentityCreated => 3,
        OnboardStage::NodeClaimed => 4,
        OnboardStage::NetworkJoined => 5,
        OnboardStage::CapabilityProfiled => 6,
        OnboardStage::MarketplaceEnabled => 7,
        // Everything done — only the final report remains.
        OnboardStage::Active => 8,
    }
}

/// Run every pending step (from the persisted stage) up to `ACTIVE`.
pub async fn run(home: &EchoHome, offline: bool, reset: bool) -> Result<()> {
    let mut cfg = home.load_config()?;

    if reset {
        cfg.onboard_stage = OnboardStage::New;
        cfg.provider_id = None;
        cfg.claim_code = None;
        cfg.measured_latency_ms = None;
        cfg.measured_upload_mbps = None;
        cfg.measured_download_mbps = None;
        cfg.auto_bid = false;
        cfg.bid_price_usd_per_gb = None;
        home.save_config(&cfg)?;
        println!("onboarding reset → {}", OnboardStage::New);
    }

    let start = start_step(&cfg.onboard_stage);
    for n in 1..start {
        println!("  {n}. {} — done", STEPS[(n - 1) as usize]);
    }

    if start > STEPS.len() as u8 {
        println!(
            "onboarding already complete — stage {}",
            cfg.onboard_stage
        );
        return Ok(());
    }

    println!(
        "onboarding: step {start}/7 — stage {}{}\n",
        cfg.onboard_stage,
        if offline { " (offline)" } else { "" }
    );

    for n in start..=STEPS.len() as u8 {
        let title = STEPS[(n - 1) as usize];
        step(n, home, &mut cfg, offline)
            .await
            .with_context(|| format!("step {n} ({title}) failed"))?;
    }

    println!("\nonboarding complete — stage {}", cfg.onboard_stage);
    Ok(())
}

/// Persist the stage reached by a completed step.
fn advance(home: &EchoHome, cfg: &mut NodeConfig, stage: OnboardStage, detail: &str) -> Result<()> {
    cfg.onboard_stage = stage.clone();
    home.save_config(cfg)?;
    println!("  {}. {} — ✓ {detail}", next_step_number(&stage), stage);
    Ok(())
}

/// The step number that *produced* this stage (for the progress line).
fn next_step_number(stage: &OnboardStage) -> u8 {
    match stage {
        OnboardStage::New => 1,
        OnboardStage::IdentityCreated => 2,
        OnboardStage::NodeClaimed => 3,
        OnboardStage::NetworkJoined => 4,
        OnboardStage::CapabilityProfiled => 5,
        OnboardStage::MarketplaceEnabled => 6,
        OnboardStage::Active => 7,
    }
}

async fn step(n: u8, home: &EchoHome, cfg: &mut NodeConfig, offline: bool) -> Result<()> {
    match n {
        1 => {
            // EchoHome::open() already created the directories (the CLI opens
            // the home before dispatch); make sure config.toml exists so the
            // layout is observable on disk.
            if !home.config_path().exists() {
                home.save_config(&NodeConfig::default())?;
            }
            println!("  1. {} — ✓ {}", STEPS[0], home.dir.display());
            Ok(())
        }
        2 => {
            let identity =
                NodeIdentity::load_or_generate(&home.identity_path(), &cfg.region)?;
            advance(
                home,
                cfg,
                OnboardStage::IdentityCreated,
                identity.peer_id_str(),
            )
        }
        3 => {
            let identity = read_identity(home)?;
            let provider_id = derive_provider_id(identity.peer_id_str());
            cfg.provider_id = Some(provider_id.clone());

            match claim(cfg, identity.peer_id_str(), offline).await {
                Ok(Some(code)) => {
                    cfg.claim_code = Some(code.clone());
                    home.save_config(cfg)?;
                    advance(
                        home,
                        cfg,
                        OnboardStage::NodeClaimed,
                        &format!("provider_id={provider_id} claim_code={code}"),
                    )
                }
                Ok(None) => advance(
                    home,
                    cfg,
                    OnboardStage::NodeClaimed,
                    &format!("provider_id={provider_id} (offline claim)"),
                ),
                Err(e) => {
                    // Network claims are best-effort: the derived provider_id
                    // is what settlement keys on, so the step still advances.
                    eprintln!("    ⚠ backend claim failed: {e:#} — using local claim");
                    advance(
                        home,
                        cfg,
                        OnboardStage::NodeClaimed,
                        &format!("provider_id={provider_id} (local fallback)"),
                    )
                }
            }
        }
        4 => {
            if offline {
                println!("  4. {} — ↷ skipped (offline)", STEPS[3]);
                cfg.onboard_stage = OnboardStage::NetworkJoined;
                home.save_config(cfg)?;
                return Ok(());
            }
            let addr = daemon_addr(cfg.port);
            if !daemon::ping_daemon(&addr).await {
                daemon::spawn_daemon(home, cfg, &cfg.region)
                    .await
                    .context("could not start the daemon (see logs/daemon.log)")?;
            }
            let status: StatusResponse = daemon::get_json(&addr, "/status").await?;
            let identity = read_identity(home)?;
            match status.peer_id {
                Some(ref peer) if peer != identity.peer_id_str() => {
                    println!(
    "    ⚠ daemon peer_id {peer} != identity {} (delete state.db to re-register)",
                        identity.peer_id_str()
                    );
                }
                _ => {}
            }
            advance(home, cfg, OnboardStage::NetworkJoined, &addr)
        }
        5 => {
            if offline {
                println!("  5. {} — ↷ skipped (offline)", STEPS[4]);
                cfg.onboard_stage = OnboardStage::CapabilityProfiled;
                home.save_config(cfg)?;
                return Ok(());
            }
            let addr = daemon_addr(cfg.port);
            // `/metrics` answers 503 until the first heartbeat sample lands —
            // poll briefly instead of failing a fresh daemon.
            let m: MetricsResponse = {
                let deadline =
                    tokio::time::Instant::now() + Duration::from_secs(METRICS_WAIT_SECS);
                loop {
                    match daemon::get_json(&addr, "/metrics").await {
                        Ok(m) => break m,
                        Err(e) if tokio::time::Instant::now() < deadline => {
                            let _ = e; // keep polling; last error surfaces below
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        Err(e) => {
                            return Err(e).context("daemon never produced /metrics samples");
                        }
                    }
                }
            };
            cfg.measured_latency_ms = m.latency_ms;
            cfg.measured_upload_mbps = m.bandwidth_up_mbps;
            cfg.measured_download_mbps = m.bandwidth_down_mbps;
            home.save_config(cfg)?;
            let detail = format!(
                "latency={:.0}ms up={:.1}Mbps down={:.1}Mbps",
                m.latency_ms.unwrap_or(0.0),
                m.bandwidth_up_mbps.unwrap_or(0.0),
                m.bandwidth_down_mbps.unwrap_or(0.0)
            );
            advance(home, cfg, OnboardStage::CapabilityProfiled, &detail)
        }
        6 => {
            cfg.auto_bid = true;
            if cfg.bid_price_usd_per_gb.is_none() {
                cfg.bid_price_usd_per_gb = Some(
                    std::env::var("SETTLEMENT_RATE_USD_PER_GB")
                        .ok()
                        .and_then(|v| v.parse::<f64>().ok())
                        .filter(|v| v.is_finite() && *v > 0.0)
                        .unwrap_or(DEFAULT_PRICE_USD_PER_GB),
                );
            }
            home.save_config(cfg)?;
            let detail = format!(
                "auto_bid=true price=${:.2}/GB",
                cfg.bid_price_usd_per_gb.unwrap_or(DEFAULT_PRICE_USD_PER_GB)
            );
            advance(home, cfg, OnboardStage::MarketplaceEnabled, &detail)
        }
        7 => {
            cfg.auto_start = true;
            home.save_config(cfg)?;
            advance(home, cfg, OnboardStage::Active, "auto_start=true")?;

            // Marketplace knobs are passed to the daemon as env vars at spawn
            // time — a daemon started before step 6 needs a restart to pick
            // them up.
            let addr = daemon_addr(cfg.port);
            if !offline && daemon::ping_daemon(&addr).await {
                println!("    note: run `echo-node restart` to apply marketplace settings");
            }
            Ok(())
        }
        other => anyhow::bail!("unknown onboarding step {other}"),
    }
}

/// Best-effort backend claim; `Ok(None)` means "offline, nothing to do".
///
/// Any transport/HTTP/parse problem is an `Err` the caller downgrades to a
/// local claim (see `step` 3).
async fn claim(
    cfg: &NodeConfig,
    peer_id: &str,
    offline: bool,
) -> Result<Option<String>> {
    if offline {
        return Ok(None);
    }
    let provider_id = cfg
        .provider_id
        .clone()
        .unwrap_or_else(|| derive_provider_id(peer_id));
    let url = format!(
        "{}/api/v1/nodes/claim",
        cfg.backend_url.trim_end_matches('/')
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let resp = client
        .post(&url)
        .json(&ClaimRequest {
            provider_id: &provider_id,
            peer_id,
            region: &cfg.region,
        })
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("backend returned {} for POST {url}", resp.status());
    }
    // The body may be empty/HTML (unknown backend contract) — tolerate it.
    let body = resp.text().await.unwrap_or_default();
    let code = serde_json::from_str::<ClaimResponse>(&body)
        .ok()
        .and_then(|r| r.claim_code);
    Ok(code)
}

/// Read `identity.json` without ever generating a new one (step 4+ must not
/// silently rotate keys).
fn read_identity(home: &EchoHome) -> Result<NodeIdentity> {
    let path = home.identity_path();
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stage → start-step mapping is the resumability contract: it must be
    /// monotonic and `Active` must mean "nothing left to do".
    #[test]
    fn stage_progression_is_monotonic() {
        let order = [
            (OnboardStage::New, 1),
            (OnboardStage::IdentityCreated, 3),
            (OnboardStage::NodeClaimed, 4),
            (OnboardStage::NetworkJoined, 5),
            (OnboardStage::CapabilityProfiled, 6),
            (OnboardStage::MarketplaceEnabled, 7),
            (OnboardStage::Active, 8),
        ];
        let mut last = 0;
        for (stage, want) in order {
            let got = start_step(&stage);
            assert_eq!(got, want, "stage {stage}");
            assert!(got > last, "stages must advance: {stage}");
            last = got;
        }
        assert_eq!(next_step_number(&OnboardStage::Active), 7);
    }
}
