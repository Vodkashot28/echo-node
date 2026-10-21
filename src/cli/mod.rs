//! EchoMesh node control CLI.
//!
//! The binary doubles as the daemon and its control client:
//!
//! - no subcommand, or `echo-node daemon` → run the daemon in the foreground
//!   (backward compatible with `cargo run` / service deployments);
//! - any other subcommand → talk to the running daemon over REST, or manage
//!   its on-disk XDG home (`~/.config/echomesh/`).
//!
//! Layout:
//! - `config.rs`       — XDG home + `config.toml` model (node configuration)
//! - `daemon.rs`       — spawn / SIGTERM-stop / REST query helpers
//! - `onboarding.rs`   — the 7-step onboarding state machine
//! - `status.rs`       — `status` + `doctor`
//! - `market.rs`       — `marketplace auctions|bid|awards`
//! - `wallet.rs`       — `wallet` (settlement summary)
//! - `identity_cmd.rs` — `identity` (peer id / keys / provider id)
//! - `logs.rs`         — `logs path|tail`
//! - `config_cmd.rs`   — `config show|set`

pub mod config;
pub mod config_cmd;
pub mod daemon;
pub mod identity_cmd;
pub mod logs;
pub mod market;
pub mod onboarding;
pub mod status;
pub mod wallet;

use anyhow::Result;
use clap::{Parser, Subcommand};

use config::EchoHome;

#[derive(Debug, Parser)]
#[command(
    name = "echo-node",
    version,
    about = "EchoMesh node control CLI",
    long_about = "EchoMesh node control CLI.\n\nWith no subcommand the node daemon runs in the foreground \
                  (same as `echo-node daemon`)."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Run the node daemon in the foreground (default with no args).
    #[command(hide = true)]
    Daemon,
    /// Run the onboarding state machine to completion (resumable).
    Onboard {
        /// Claim/profile without touching the network (dev machines).
        #[arg(long)]
        offline: bool,
        /// Reset the stage back to NEW before running.
        #[arg(long)]
        reset: bool,
        /// Print the current onboarding stage and exit.
        #[arg(long)]
        status: bool,
    },
    /// Start the daemon in the background.
    Start {
        /// Region tag override (persisted to config.toml).
        #[arg(long)]
        region: Option<String>,
    },
    /// Stop the running daemon (SIGTERM).
    Stop,
    /// Stop the daemon, then start it again.
    Restart {
        /// Region tag override (persisted to config.toml).
        #[arg(long)]
        region: Option<String>,
    },
    /// Show live daemon status.
    Status,
    /// Diagnose common installation / runtime problems.
    Doctor,
    /// Marketplace: list auctions, submit bids, show awards.
    #[command(subcommand)]
    Marketplace(market::MarketCmd),
    /// Earnings and settlement totals.
    Wallet,
    /// Show this node's identity (peer id, keys, provider id).
    Identity,
    /// Inspect daemon logs.
    #[command(subcommand)]
    Logs(logs::LogsCmd),
    /// Show or edit config.toml.
    #[command(subcommand)]
    Config(config_cmd::ConfigCmd),
}

/// Dispatch a CLI subcommand (already parsed by `main`).
pub async fn run(cmd: Commands) -> Result<()> {
    // `daemon` never touches the XDG home — the daemon resolves its own
    // paths from env vars (and `main` runs it directly).
    if matches!(cmd, Commands::Daemon) {
        return Ok(());
    }

    let home = EchoHome::open()?;

    match cmd {
        Commands::Daemon => unreachable!("handled above"),
        Commands::Onboard {
            offline,
            reset,
            status: stage_only,
        } => {
            if stage_only {
                let cfg = home.load_config()?;
                println!("{}", cfg.onboard_stage);
                return Ok(());
            }
            onboarding::run(&home, offline, reset).await
        }
        Commands::Start { region } => start(&home, region).await,
        Commands::Stop => daemon::stop_daemon(&home, &addr(&home)?).await,
        Commands::Restart { region } => {
            let addr = addr(&home)?;
            if let Err(e) = daemon::stop_daemon(&home, &addr).await {
                eprintln!("warning: {e:#}");
            }
            start(&home, region).await
        }
        Commands::Status => status::cmd_status(&home).await,
        Commands::Doctor => status::cmd_doctor(&home).await,
        Commands::Marketplace(sub) => market::run(&home, sub).await,
        Commands::Wallet => wallet::run(&home).await,
        Commands::Identity => identity_cmd::run(&home),
        Commands::Logs(sub) => logs::run(&home, sub).await,
        Commands::Config(sub) => config_cmd::run(&home, sub),
    }
}

/// Base URL of this node's daemon REST API (from config).
fn addr(home: &EchoHome) -> Result<String> {
    Ok(config::daemon_addr(home.load_config()?.port))
}

/// `echo-node start` — spawn the daemon subprocess (idempotent: a reachable
/// daemon is left alone).
async fn start(home: &EchoHome, region: Option<String>) -> Result<()> {
    use indicatif::ProgressBar;

    let mut cfg = home.load_config()?;
    if let Some(region) = region {
        cfg.region = region.clone();
        home.save_config(&cfg)?;
    }

    let addr = config::daemon_addr(cfg.port);
    if daemon::ping_daemon(&addr).await {
        println!("daemon already running at {addr}");
        return Ok(());
    }

    let pb = ProgressBar::new_spinner();
    pb.set_message("starting daemon…");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let started = daemon::spawn_daemon(home, &cfg, &cfg.region).await;
    match started {
        Ok(pid) => {
            pb.finish_with_message("daemon ready");
            println!("daemon started (pid {pid}) — REST API on {addr}");
            println!("logs: {}", home.daemon_log_path().display());
        }
        Err(e) => {
            pb.finish_and_clear();
            return Err(e);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// One consolidated test for CLI parsing (clap reads argv only via
    /// `try_parse_from`, so no env mutation is involved).
    #[test]
    fn cli_parses_expected_shapes() {
        // No args → daemon (backward compatible with `cargo run`).
        let cli = Cli::try_parse_from(["echo-node"]).expect("bare invocation");
        assert!(cli.command.is_none());

        // Explicit daemon subcommand (what `start` spawns).
        let cli = Cli::try_parse_from(["echo-node", "daemon"]).expect("daemon");
        assert!(matches!(cli.command, Some(Commands::Daemon)));

        // Flags on onboard.
        let cli = Cli::try_parse_from(["echo-node", "onboard", "--offline", "--reset"])
            .expect("onboard flags");
        match cli.command {
            Some(Commands::Onboard {
                offline,
                reset,
                status,
            }) => {
                assert!(offline);
                assert!(reset);
                assert!(!status);
            }
            other => panic!("unexpected command: {other:?}"),
        }

        // Nested marketplace bid with a price.
        let cli = Cli::try_parse_from([
            "echo-node",
            "marketplace",
            "bid",
            "auc_42",
            "--price",
            "0.42",
        ])
        .expect("marketplace bid");
        match cli.command {
            Some(Commands::Marketplace(market::MarketCmd::Bid { auction_id, price })) => {
                assert_eq!(auction_id, "auc_42");
                assert!((price - 0.42).abs() < f64::EPSILON);
            }
            other => panic!("unexpected command: {other:?}"),
        }

        // config set keeps string values verbatim.
        let cli = Cli::try_parse_from(["echo-node", "config", "set", "region", "PH-XXX"])
            .expect("config set");
        match cli.command {
            Some(Commands::Config(config_cmd::ConfigCmd::Set { key, value })) => {
                assert_eq!(key, "region");
                assert_eq!(value, "PH-XXX");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
