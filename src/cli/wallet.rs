//! `echo-node wallet` — settlement totals and recent payouts.

use anyhow::Result;
use serde::Deserialize;

use super::config::{EchoHome, daemon_addr};

#[derive(Debug, Deserialize)]
struct SettlementsResponse {
    summary: SummaryView,
    settlements: Vec<SettlementView>,
}

#[derive(Debug, Deserialize)]
struct SummaryView {
    receipts_settled: u64,
    total_bytes_settled: u64,
    total_earnings_usd: f64,
}

#[derive(Debug, Deserialize)]
struct SettlementView {
    counterparty_peer_id: String,
    bytes_settled: u64,
    earnings_usd: f64,
    settled_at: String,
}

/// `GET /settlements` is behind the `settlements` scope — surface a clear
/// hint instead of a bare 401.
pub async fn run(home: &EchoHome) -> Result<()> {
    let addr = daemon_addr(home.load_config()?.port);
    let url = format!("{addr}/settlements");
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("GET {url} (is the daemon running?): {e}"))?;

    let status = resp.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        anyhow::bail!(
            "{url} → {status} — /settlements needs a Bearer token with the `settlements` scope \
             (set API_KEYS/API_KEY in the daemon's environment)"
        );
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("GET {url} → {status}: {}", body.trim());
    }

    let view: SettlementsResponse = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("parsing {url}: {e}"))?;

    println!("wallet — {addr}");
    println!("  {:<18} {}", "receipts settled", view.summary.receipts_settled);
    println!(
        "  {:<18} {:.2} MiB ({} bytes)",
        "total relayed",
        view.summary.total_bytes_settled as f64 / (1024.0 * 1024.0),
        view.summary.total_bytes_settled
    );
    println!(
        "  {:<18} ${:.4}",
        "total earnings", view.summary.total_earnings_usd
    );

    if view.settlements.is_empty() {
        println!("\nno settled receipts yet");
        return Ok(());
    }

    println!("\nrecent settlements ({}):", view.settlements.len());
    for s in view.settlements.iter().take(10) {
        println!(
            "  {}  {:>10} bytes  ${:.6}  {}",
            s.settled_at,
            s.bytes_settled,
            s.earnings_usd,
            short(&s.counterparty_peer_id),
        );
    }
    Ok(())
}

fn short(peer: &str) -> String {
    if peer.len() <= 16 {
        return peer.to_string();
    }
    format!("{}…{}", &peer[..6], &peer[peer.len() - 6..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_view_parses_daemon_shape() {
        let raw = r#"{
            "summary":{"receipts_settled":3,"total_bytes_settled":1048576,"total_earnings_usd":0.5},
            "settlements":[
                {"receipt_id":"r1","signer_peer_id":"12D3KooWAAA","counterparty_peer_id":"12D3KooWBBB",
                 "bytes_settled":1048576,"earnings_usd":0.5,"settled_at":"2026-10-17T12:00:00Z"}
            ]
        }"#;
        let view: SettlementsResponse = serde_json::from_str(raw).expect("parse");
        assert_eq!(view.summary.receipts_settled, 3);
        assert_eq!(view.settlements.len(), 1);
        assert_eq!(view.settlements[0].bytes_settled, 1_048_576);
    }
}
