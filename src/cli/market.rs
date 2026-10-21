//! `echo-node marketplace auctions|bid|awards` — thin REST views over the
//! node's marketplace endpoints.

use anyhow::{bail, Result};
use clap::Subcommand;
use serde::Deserialize;

use super::config::{daemon_addr, EchoHome};
use super::daemon::{get_json, post_json};

#[derive(Debug, Subcommand)]
pub enum MarketCmd {
    /// List auctions this node can see (open + recently closed).
    Auctions,
    /// Submit a sealed bid on an auction.
    Bid {
        /// Auction id (from `marketplace auctions`).
        auction_id: String,
        /// Offered price in USD per GB (must be ≤ the auction's budget cap).
        #[arg(long)]
        price: f64,
    },
    /// Recent auction awards with their locked (market) prices.
    Awards,
}

pub async fn run(home: &EchoHome, cmd: MarketCmd) -> Result<()> {
    let addr = daemon_addr(home.load_config()?.port);
    match cmd {
        MarketCmd::Auctions => auctions(&addr).await,
        MarketCmd::Bid { auction_id, price } => bid(&addr, &auction_id, price).await,
        MarketCmd::Awards => awards(&addr).await,
    }
}

// ── Wire views (only the fields the CLI renders) ───────────────────────────

#[derive(Debug, Deserialize)]
struct AuctionView {
    id: String,
    region: String,
    min_upload_mbps: f64,
    min_download_mbps: f64,
    max_price_usd_per_gb: f64,
    status: String,
    closes_at: u64,
    bids: Vec<BidView>,
    award: Option<AwardView>,
}

#[derive(Debug, Deserialize)]
struct BidView {
    provider_peer_id: String,
    price_usd_per_gb: f64,
}

#[derive(Debug, Deserialize)]
struct AwardView {
    provider_peer_id: String,
    price_usd_per_gb: f64,
}

#[derive(Debug, Deserialize)]
struct AuctionListResponse {
    auctions: Vec<AuctionView>,
}

#[derive(Debug, Deserialize)]
struct AwardListResponse {
    awards: Vec<AwardView>,
}

/// Bid echo from `POST /auctions/:id/bid`.
#[derive(Debug, Deserialize)]
struct BidResponse {
    id: String,
    provider_peer_id: String,
    price_usd_per_gb: f64,
}

// ── Commands ───────────────────────────────────────────────────────────────

async fn auctions(addr: &str) -> Result<()> {
    let resp: AuctionListResponse = get_json(addr, "/auctions").await?;
    if resp.auctions.is_empty() {
        println!("no auctions");
        return Ok(());
    }

    let now = now_secs();
    let (open, closed): (Vec<_>, Vec<_>) = resp.auctions.iter().partition(|a| a.status == "open");

    if !open.is_empty() {
        println!("open auctions ({}):", open.len());
        for a in open {
            let closes_in = a.closes_at.saturating_sub(now);
            println!(
                "  {}  {}  ≤{:.2} $/GB  ↑≥{:.0}/↓≥{:.0} Mbps  bids {}  closes in {}s",
                a.id,
                a.region,
                a.max_price_usd_per_gb,
                a.min_upload_mbps,
                a.min_download_mbps,
                a.bids.len(),
                closes_in,
            );
        }
    }
    if !closed.is_empty() {
        println!("recently closed ({}):", closed.len());
        for a in closed {
            let locked = a
                .award
                .as_ref()
                .map(|aw| {
                    format!(
                        "locked {:.2} $/GB → {}",
                        aw.price_usd_per_gb,
                        short(&aw.provider_peer_id)
                    )
                })
                .unwrap_or_else(|| "no bids".to_string());
            println!("  {}  {}  {}  {locked}", a.id, a.region, a.status);
            if a.bids.len() > 1 {
                for b in &a.bids {
                    println!(
                        "      bid {:.2} $/GB by {}",
                        b.price_usd_per_gb,
                        short(&b.provider_peer_id)
                    );
                }
            }
        }
    }
    Ok(())
}

async fn bid(addr: &str, auction_id: &str, price: f64) -> Result<()> {
    if !price.is_finite() || price < 0.0 {
        bail!("--price must be a non-negative finite USD/GB amount");
    }
    let path = format!("/auctions/{auction_id}/bid");
    let (status, body) = post_json(
        addr,
        &path,
        &serde_json::json!({
            "price_usd_per_gb": price,
        }),
    )
    .await?;
    if status != 200 {
        bail!("bid rejected ({status}): {}", body.trim());
    }
    let bid: BidResponse =
        serde_json::from_str(&body).map_err(|e| anyhow::anyhow!("parsing bid response: {e}"))?;
    println!(
        "bid submitted — {:.2} $/GB on {} by {} (bid {})",
        bid.price_usd_per_gb,
        auction_id,
        short(&bid.provider_peer_id),
        bid.id
    );
    Ok(())
}

async fn awards(addr: &str) -> Result<()> {
    let resp: AwardListResponse = get_json(addr, "/awards").await?;
    if resp.awards.is_empty() {
        println!("no awards yet");
        return Ok(());
    }
    println!("awards ({}):", resp.awards.len());
    for a in &resp.awards {
        println!(
            "  {}  locked {:.2} $/GB",
            short(&a.provider_peer_id),
            a.price_usd_per_gb
        );
    }
    Ok(())
}

/// First/last 6 chars of a peer id — enough to tell two of them apart in a
/// terminal without wrapping every line.
fn short(peer: &str) -> String {
    if peer.len() <= 16 {
        return peer.to_string();
    }
    format!("{}…{}", &peer[..6], &peer[peer.len() - 6..])
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_peer_ids() {
        assert_eq!(short("12D3KooWQooCi"), "12D3KooWQooCi");
        let long = "12D3KooWQooCi89ABCDEFGhijklmnopqrstuvwxyz0123456789";
        let s = short(long);
        // 6 head chars + "…" + 6 tail chars.
        assert_eq!(s.chars().count(), 13);
        assert!(s.starts_with("12D3Ko"));
        assert!(s.ends_with("456789"));
    }

    #[test]
    fn auction_list_parses_daemon_shape() {
        let raw = r#"{"auctions":[{
            "id":"auc_1","region":"auto","min_upload_mbps":10.0,"min_download_mbps":50.0,
            "max_price_usd_per_gb":0.5,"window_secs":30,"created_at":100,"closes_at":200,
            "status":"open","bids":[],"award":null
        }],"count":1}"#;
        let parsed: AuctionListResponse = serde_json::from_str(raw).expect("parse");
        assert_eq!(parsed.auctions.len(), 1);
        assert_eq!(parsed.auctions[0].status, "open");
        assert_eq!(parsed.auctions[0].max_price_usd_per_gb, 0.5);
    }
}
