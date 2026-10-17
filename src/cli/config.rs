//! XDG-compliant config home layout for EchoMesh.
//!
//! Directory: `$XDG_CONFIG_HOME/echomesh/` (falls back to `~/.config/echomesh/`).
//!
//! Contents:
//! - `identity.json` — Ed25519 + X25519 keypairs
//! - `config.toml`   — node config (region, ports, env overrides)
//! - `state.db`      — SQLite database
//! - `logs/`         — daemon log output

use anyhow::{Context, Result};
use directories::BaseDirs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Default daemon REST API port (`config.toml` `port`, env-independent).
pub const DAEMON_PORT: u16 = 7777;

/// The on-disk node configuration (`config.toml`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Advertised region tag (e.g. "PH-SOCSKSARGEN").
    #[serde(default = "default_region")]
    pub region: String,
    /// Daemon REST API bind port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Maximum upload capacity in Mbps.
    #[serde(default = "default_upload")]
    pub max_upload_mbps: f64,
    /// Maximum download capacity in Mbps.
    #[serde(default = "default_download")]
    pub max_download_mbps: f64,
    /// Whether to automatically start on boot.
    #[serde(default)]
    pub auto_start: bool,
    /// The EchoMesh backend registration URL.
    #[serde(default = "default_backend_url")]
    pub backend_url: String,
    /// Onboarding state machine stage.
    #[serde(default)]
    pub onboard_stage: OnboardStage,
    /// Provider ID (sha256(peer_id)[0..8] hex).
    #[serde(default)]
    pub provider_id: Option<String>,
    /// Claim code returned from backend.
    #[serde(default)]
    pub claim_code: Option<String>,
    /// Measured RTT from onboarding capability profiling (ms).
    #[serde(default)]
    pub measured_latency_ms: Option<f64>,
    /// Measured uplink throughput from onboarding profiling (Mbps).
    #[serde(default)]
    pub measured_upload_mbps: Option<f64>,
    /// Measured downlink throughput from onboarding profiling (Mbps).
    #[serde(default)]
    pub measured_download_mbps: Option<f64>,
    /// Marketplace: auto-submit bids on open auctions (`BID_AUTO`).
    #[serde(default)]
    pub auto_bid: bool,
    /// Marketplace: auto-bid ask price in USD/GB (`BID_PRICE_USD_PER_GB`).
    #[serde(default)]
    pub bid_price_usd_per_gb: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OnboardStage {
    #[default]
    New,
    IdentityCreated,
    NodeClaimed,
    NetworkJoined,
    CapabilityProfiled,
    MarketplaceEnabled,
    Active,
}

impl std::fmt::Display for OnboardStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnboardStage::New => write!(f, "NEW"),
            OnboardStage::IdentityCreated => write!(f, "IDENTITY_CREATED"),
            OnboardStage::NodeClaimed => write!(f, "NODE_CLAIMED"),
            OnboardStage::NetworkJoined => write!(f, "NETWORK_JOINED"),
            OnboardStage::CapabilityProfiled => write!(f, "CAPABILITY_PROFILED"),
            OnboardStage::MarketplaceEnabled => write!(f, "MARKETPLACE_ENABLED"),
            OnboardStage::Active => write!(f, "ACTIVE"),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            region: default_region(),
            port: default_port(),
            max_upload_mbps: default_upload(),
            max_download_mbps: default_download(),
            auto_start: false,
            backend_url: default_backend_url(),
            onboard_stage: OnboardStage::New,
            provider_id: None,
            claim_code: None,
            measured_latency_ms: None,
            measured_upload_mbps: None,
            measured_download_mbps: None,
            auto_bid: false,
            bid_price_usd_per_gb: None,
        }
    }
}

fn default_region() -> String {
    "auto".to_string()
}
fn default_port() -> u16 {
    DAEMON_PORT
}
fn default_upload() -> f64 {
    100.0
}
fn default_download() -> f64 {
    100.0
}
fn default_backend_url() -> String {
    "https://app.echo-mesh.xyz".to_string()
}

/// Base URL of the local daemon REST API for a configured port.
pub fn daemon_addr(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// Paths inside the EchoMesh config directory.
pub struct EchoHome {
    /// `~/.config/echomesh/`
    pub dir: PathBuf,
}

impl EchoHome {
    /// Resolve the XDG config home, creating it if absent.
    pub fn open() -> Result<Self> {
        let dir = echomesh_dir()?;
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating config dir {}", dir.display()))?;
        // Create logs/ subdirectory.
        std::fs::create_dir_all(dir.join("logs"))
            .with_context(|| format!("creating logs dir {}", dir.join("logs").display()))?;
        Ok(Self { dir })
    }

    pub fn identity_path(&self) -> PathBuf {
        self.dir.join("identity.json")
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.join("config.toml")
    }

    pub fn db_path(&self) -> PathBuf {
        self.dir.join("state.db")
    }

    pub fn log_dir(&self) -> PathBuf {
        self.dir.join("logs")
    }

    pub fn daemon_log_path(&self) -> PathBuf {
        self.dir.join("logs").join("daemon.log")
    }

    pub fn pid_path(&self) -> PathBuf {
        self.dir.join("daemon.pid")
    }

    /// Load config from disk, returning defaults if the file is absent.
    pub fn load_config(&self) -> Result<NodeConfig> {
        let path = self.config_path();
        if !path.exists() {
            return Ok(NodeConfig::default());
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
    }

    /// Persist config to disk atomically.
    pub fn save_config(&self, cfg: &NodeConfig) -> Result<()> {
        let path = self.config_path();
        let raw = toml::to_string_pretty(cfg).context("serializing config")?;
        // Atomic write: temp → rename.
        let tmp = path.with_extension("toml.tmp");
        std::fs::write(&tmp, &raw)
            .with_context(|| format!("writing temp config {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("replacing config {}", path.display()))?;
        Ok(())
    }
}

/// Resolve `$XDG_CONFIG_HOME/echomesh` or `~/.config/echomesh`.
pub fn echomesh_dir() -> Result<PathBuf> {
    // Allow override for testing.
    if let Ok(p) = std::env::var("ECHOMESH_HOME") {
        return Ok(PathBuf::from(p));
    }
    let base = BaseDirs::new().context("cannot determine home directory")?;
    Ok(base.config_dir().join("echomesh"))
}

/// Derive the short provider id: hex(sha256(peer_id_bytes)[0..8]).
pub fn derive_provider_id(peer_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(peer_id.as_bytes());
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("provider_{hex}")
}
