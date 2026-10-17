//! `echo-node config show|set` — inspect and edit `config.toml`.
//!
//! Writes are atomic (temp → rename, see `config::EchoHome::save_config`).

use anyhow::{bail, Context, Result};
use clap::Subcommand;

use super::config::{EchoHome, NodeConfig, OnboardStage};

/// Keys accepted by `config set`.
const KEYS: [&str; 9] = [
    "region",
    "port",
    "max_upload_mbps",
    "max_download_mbps",
    "auto_start",
    "backend_url",
    "auto_bid",
    "bid_price_usd_per_gb",
    "onboard_stage",
];

#[derive(Debug, Subcommand)]
pub enum ConfigCmd {
    /// Print the config path and every value.
    Show,
    /// Set one config value.
    Set {
        /// Key (one of: region, port, max_upload_mbps, max_download_mbps,
        /// auto_start, backend_url, auto_bid, bid_price_usd_per_gb, onboard_stage).
        key: String,
        /// New value (parsed according to the key's type).
        value: String,
    },
}

pub fn run(home: &EchoHome, cmd: ConfigCmd) -> Result<()> {
    match cmd {
        ConfigCmd::Show => show(home),
        ConfigCmd::Set { key, value } => set(home, &key, &value),
    }
}

fn show(home: &EchoHome) -> Result<()> {
    let cfg = home.load_config()?;
    let path = home.config_path();
    println!(
        "config — {}{}",
        path.display(),
        if path.exists() { "" } else { " (not written yet — defaults shown)" }
    );
    row("region", &cfg.region);
    row("port", cfg.port);
    row("max_upload_mbps", cfg.max_upload_mbps);
    row("max_download_mbps", cfg.max_download_mbps);
    row("auto_start", cfg.auto_start);
    row("backend_url", &cfg.backend_url);
    row("onboard_stage", cfg.onboard_stage.to_string());
    row(
        "provider_id",
        cfg.provider_id.as_deref().unwrap_or("(unset)"),
    );
    row(
        "claim_code",
        if cfg.claim_code.is_some() { "(set)" } else { "(unset)" },
    );
    row(
        "measured_latency_ms",
        opt(cfg.measured_latency_ms.map(|v| v.to_string())),
    );
    row(
        "measured_upload_mbps",
        opt(cfg.measured_upload_mbps.map(|v| v.to_string())),
    );
    row(
        "measured_download_mbps",
        opt(cfg.measured_download_mbps.map(|v| v.to_string())),
    );
    row("auto_bid", cfg.auto_bid);
    row(
        "bid_price_usd_per_gb",
        opt(cfg.bid_price_usd_per_gb.map(|v| format!("{v:.4}"))),
    );
    Ok(())
}

fn set(home: &EchoHome, key: &str, value: &str) -> Result<()> {
    if !KEYS.contains(&key) {
        bail!("unknown key '{key}' — valid keys: {}", KEYS.join(", "));
    }
    let mut cfg = home.load_config()?;

    match key {
        "region" => cfg.region = parse_region(value)?,
        "port" => {
            cfg.port = value
                .parse::<u16>()
                .with_context(|| format!("port must be 1-65535, got '{value}'"))?;
            if cfg.port == 0 {
                bail!("port must be 1-65535");
            }
        }
        "max_upload_mbps" => cfg.max_upload_mbps = parse_positive(value, key)?,
        "max_download_mbps" => cfg.max_download_mbps = parse_positive(value, key)?,
        "auto_start" => cfg.auto_start = parse_bool(value)?,
        "auto_bid" => cfg.auto_bid = parse_bool(value)?,
        "backend_url" => cfg.backend_url = parse_url(value)?,
        "bid_price_usd_per_gb" => cfg.bid_price_usd_per_gb = Some(parse_positive(value, key)?),
        "onboard_stage" => cfg.onboard_stage = parse_stage(value)?,
        _ => unreachable!("validated against KEYS"),
    }

    home.save_config(&cfg)?;
    println!("config: {key} = {}", cfg_value(&cfg, key));
    Ok(())
}

/// Re-render the value we just stored (so the echo shows the normalized form).
fn cfg_value(cfg: &NodeConfig, key: &str) -> String {
    match key {
        "region" => cfg.region.clone(),
        "port" => cfg.port.to_string(),
        "max_upload_mbps" => cfg.max_upload_mbps.to_string(),
        "max_download_mbps" => cfg.max_download_mbps.to_string(),
        "auto_start" => cfg.auto_start.to_string(),
        "backend_url" => cfg.backend_url.clone(),
        "auto_bid" => cfg.auto_bid.to_string(),
        "bid_price_usd_per_gb" => cfg
            .bid_price_usd_per_gb
            .map(|v| v.to_string())
            .unwrap_or_default(),
        "onboard_stage" => cfg.onboard_stage.to_string(),
        _ => String::new(),
    }
}

// ── Parsers ────────────────────────────────────────────────────────────────

/// Mirrors the daemon's `/match` region hygiene: non-empty, ≤ 64 bytes, no
/// control characters.
fn parse_region(value: &str) -> Result<String> {
    let v = value.trim();
    if v.is_empty() || v.len() > 64 {
        bail!("region must be 1..=64 bytes (got {})", v.len());
    }
    if v.chars().any(|c| c.is_control()) {
        bail!("region must not contain control characters");
    }
    Ok(v.to_string())
}

fn parse_positive(value: &str, key: &str) -> Result<f64> {
    let v: f64 = value
        .parse()
        .with_context(|| format!("{key} must be a number, got '{value}'"))?;
    if !v.is_finite() || v <= 0.0 {
        bail!("{key} must be a finite value > 0 (got {v})");
    }
    Ok(v)
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => bail!("expected a boolean (true/false/1/0/yes/no), got '{other}'"),
    }
}

fn parse_url(value: &str) -> Result<String> {
    let v = value.trim().trim_end_matches('/');
    if !(v.starts_with("http://") || v.starts_with("https://")) {
        bail!("backend_url must start with http:// or https://");
    }
    Ok(v.to_string())
}

fn parse_stage(value: &str) -> Result<OnboardStage> {
    // Accept both the stored SCREAMING_SNAKE_CASE form and lowercase.
    let normalized = value.trim().to_ascii_uppercase();
    let parsed = serde_json::from_value::<OnboardStage>(serde_json::Value::String(normalized))
        .with_context(|| format!("unknown onboarding stage '{value}'"))?;
    Ok(parsed)
}

fn opt(v: Option<String>) -> String {
    v.unwrap_or_else(|| "(unset)".to_string())
}

fn row(label: &str, value: impl std::fmt::Display) {
    println!("  {label:<22} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One consolidated test for the `config set` value parsers (they only
    /// touch pure functions — no env mutation).
    #[test]
    fn config_value_parsers() {
        // region
        assert_eq!(parse_region(" PH-SOCSKSARGEN ").unwrap(), "PH-SOCSKSARGEN");
        assert!(parse_region("").is_err());
        assert!(parse_region(&"x".repeat(65)).is_err());
        assert!(parse_region("bad\nregion").is_err());

        // positive numbers
        assert_eq!(parse_positive("100", "port_like").unwrap(), 100.0);
        assert!(parse_positive("0", "x").is_err());
        assert!(parse_positive("-1", "x").is_err());
        assert!(parse_positive("nan", "x").is_err());
        assert!(parse_positive("abc", "x").is_err());

        // booleans
        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("YES").unwrap());
        assert!(!parse_bool("off").unwrap());
        assert!(!parse_bool("0").unwrap());
        assert!(parse_bool("maybe").is_err());

        // urls
        assert_eq!(parse_url("https://a.b/").unwrap(), "https://a.b");
        assert!(parse_url("ftp://a.b").is_err());

        // onboarding stages
        assert_eq!(parse_stage("new").unwrap(), OnboardStage::New);
        assert_eq!(
            parse_stage("MARKETPLACE_ENABLED").unwrap(),
            OnboardStage::MarketplaceEnabled
        );
        assert!(parse_stage("nonsense").is_err());
    }
}
