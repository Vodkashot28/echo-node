//! End-to-end test for the marketplace bidding engine.
//!
//! Exercises the full price-discovery pipeline without networking:
//!
//! ```text
//! consumer request → open auction → provider bids → deadline sweep →
//! award (Vickrey-locked price) → market-priced settlement
//! ```
//!
//! The award's locked USD/GB price overrides the flat
//! `SETTLEMENT_RATE_USD_PER_GB` when the winning provider's signed
//! receipts are settled — this is the seam that turns a VPN-style mesh
//! into a bandwidth marketplace.

use echo_daemon::base64_encode;
use echo_daemon::bidding::{
    AuctionStatus, BidConfig, BidEngine, BidError, CreateAuction, PricingRule,
};
use echo_daemon::identity::NodeIdentity;
use echo_daemon::models::StateReceipt;
use echo_daemon::settlement::SettlementEngine;
use echo_daemon::sqlite_store::SqliteStore;
use std::sync::Arc;
use tempfile::TempDir;

/// Sign a receipt as `identity` for traffic relayed to `counterparty`.
fn signed_receipt(
    identity: &NodeIdentity,
    id: &str,
    counterparty: &str,
    bytes: u64,
) -> StateReceipt {
    use ed25519_dalek::{Signer, SigningKey};

    let signing_key = SigningKey::from_bytes(identity.keypair_bytes.as_slice().try_into().unwrap());
    let mut receipt = StateReceipt {
        receipt_id: id.to_string(),
        session_id: "session-market".to_string(),
        signer_peer_id: identity.peer_id_str().to_string(),
        counterparty_peer_id: counterparty.to_string(),
        bytes_transferred: bytes,
        direction: "bidirectional".to_string(),
        sequence_number: 1,
        timestamp_secs: 1_700_000_000,
        signature: String::new(),
    };
    let msg = receipt.signed_message().into_bytes();
    let sig = signing_key.sign(&msg);
    receipt.signature = base64_encode(&sig.to_bytes());
    receipt
}

fn auction_req(cap: f64, window: u64) -> CreateAuction {
    CreateAuction {
        region: "us-west".into(),
        min_upload_mbps: 10.0,
        min_download_mbps: 50.0,
        max_price_usd_per_gb: cap,
        window_secs: Some(window),
    }
}

#[tokio::test]
async fn market_auction_award_settles_at_locked_price() {
    let dir = TempDir::new().unwrap();

    // ── Identities ──────────────────────────────────────────────
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-west").unwrap();
    let solid = NodeIdentity::load_or_generate(&dir.path().join("solid.json"), "us-west").unwrap();
    let flaky = NodeIdentity::load_or_generate(&dir.path().join("flaky.json"), "us-west").unwrap();
    let consumer_peer = consumer.peer_id_str().to_string();
    let solid_peer = solid.peer_id_str().to_string();
    let flaky_peer = flaky.peer_id_str().to_string();

    // ── Open auction: consumer wants bandwidth, caps budget at $1/GB ──
    let engine = BidEngine::new(BidConfig::default());
    let auction = engine
        .create_auction(auction_req(1.0, 30), &consumer_peer, 1_000)
        .unwrap();
    assert_eq!(auction.status, AuctionStatus::Open);
    assert_eq!(engine.open_auctions().len(), 1);

    // ── Two sealed bids ─────────────────────────────────────────
    // Solid: fair price, excellent reputation + latency.
    engine
        .submit_bid(&auction.id, &solid_peer, 0.40, 0.95, 15.0, 1_005)
        .unwrap();
    // Flaky: slightly cheaper but zero reputation + awful latency.
    engine
        .submit_bid(&auction.id, &flaky_peer, 0.35, 0.05, 800.0, 1_006)
        .unwrap();

    // One bid per provider is enforced end-to-end.
    assert_eq!(
        engine.submit_bid(&auction.id, &solid_peer, 0.30, 0.95, 15.0, 1_007),
        Err(BidError::DuplicateBid)
    );

    // ── Deadline sweep finalizes the auction ────────────────────
    let finalized = engine.expire_stale(1_030);
    assert_eq!(finalized.len(), 1);
    let closed = engine.get_auction(&auction.id).unwrap();
    assert_eq!(closed.status, AuctionStatus::Awarded);

    // Quality-adjusted scoring: solid wins despite flaky's lower price.
    let award = closed.award.expect("awarded auction has an award");
    assert_eq!(award.provider_peer_id, solid_peer);
    assert_eq!(award.winning_bid_price_usd_per_gb, 0.40);
    assert_eq!(award.runner_up_price_usd_per_gb, Some(0.35));
    // Procurement Vickrey: locked = max(own 0.40, runner-up 0.35) = 0.40.
    assert_eq!(award.price_usd_per_gb, 0.40);

    // The award log feeds settlement lookups, keyed by the consumer
    // (receipt counterparty) the price was locked for.
    assert_eq!(engine.awarded_price_for(&consumer_peer), Some(0.40));
    assert_eq!(engine.awarded_price_for("stranger-consumer"), None);
    assert_eq!(engine.recent_awards(10).len(), 1);

    // ── Market-priced settlement ────────────────────────────────
    // The winning provider settles its own signed receipts at the locked
    // market price, not the flat SETTLEMENT_RATE_USD_PER_GB ($0.50).
    let store = Arc::new(
        SqliteStore::new(&format!(
            "sqlite:{}",
            dir.path().join("market.db").display()
        ))
        .await
        .unwrap(),
    );
    let settlement = SettlementEngine::new(solid.clone(), store.clone(), 0.50);

    // 2 GB relayed to the consumer → 2 × $0.40 = $0.80 at market price
    // (vs $1.00 at the flat rate — price discovery actually moved money).
    let receipt = signed_receipt(&solid, "receipt-mkt-1", &consumer_peer, 2_000_000_000);
    let market_price = engine.awarded_price_for(&receipt.counterparty_peer_id);
    let settled = settlement
        .settle_receipt_at_price(receipt.clone(), market_price)
        .await
        .unwrap()
        .expect("valid own-signed receipt must settle");
    assert!((settled.earnings_usd - 0.80).abs() < 1e-9);

    // Replay is idempotent (receipt_id PK).
    let replayed = settlement
        .settle_receipt_at_price(receipt, market_price)
        .await
        .unwrap();
    assert!(replayed.is_none());

    // Receipts from a counterparty with no award fall back to the flat rate.
    let other = signed_receipt(&solid, "receipt-mkt-2", "stranger-peer", 1_000_000_000);
    let fallback_price = engine.awarded_price_for(&other.counterparty_peer_id);
    assert_eq!(fallback_price, None);
    let flat = settlement
        .settle_receipt_at_price(other, fallback_price)
        .await
        .unwrap()
        .expect("flat-rate receipt must settle");
    assert!((flat.earnings_usd - 0.50).abs() < 1e-9);
}

#[tokio::test]
async fn budget_cap_and_requester_guards_hold_end_to_end() {
    let dir = TempDir::new().unwrap();
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-west").unwrap();
    let provider =
        NodeIdentity::load_or_generate(&dir.path().join("provider.json"), "us-west").unwrap();
    let mallory =
        NodeIdentity::load_or_generate(&dir.path().join("mallory.json"), "us-west").unwrap();

    let engine = BidEngine::new(BidConfig::default());
    let auction = engine
        .create_auction(auction_req(0.50, 60), consumer.peer_id_str(), 100)
        .unwrap();

    // Over-budget bids are rejected; the cap is absolute.
    assert_eq!(
        engine.submit_bid(&auction.id, provider.peer_id_str(), 0.60, 0.8, 30.0, 101),
        Err(BidError::BudgetExceeded {
            price: 0.60,
            cap: 0.50
        })
    );
    // Exactly at the cap is fine.
    engine
        .submit_bid(&auction.id, provider.peer_id_str(), 0.50, 0.8, 30.0, 102)
        .unwrap();

    // Only the requester may close or cancel.
    assert_eq!(
        engine.close_auction(&auction.id, mallory.peer_id_str(), 110),
        Err(BidError::NotRequester)
    );
    assert_eq!(
        engine.cancel_auction(&auction.id, mallory.peer_id_str()),
        Err(BidError::NotRequester)
    );

    // Requester closes early → awarded at the winning (sole) bid.
    let closed = engine
        .close_auction(&auction.id, consumer.peer_id_str(), 110)
        .unwrap();
    let award = closed.award.unwrap();
    assert_eq!(award.provider_peer_id, provider.peer_id_str());
    assert_eq!(award.price_usd_per_gb, 0.50);

    // Auction is finalized — further bids are rejected even before the
    // original deadline.
    assert!(matches!(
        engine.submit_bid(&auction.id, mallory.peer_id_str(), 0.10, 0.5, 50.0, 111),
        Err(BidError::Closed(_))
    ));
}

#[tokio::test]
async fn no_bid_auction_expires_cleanly() {
    let dir = TempDir::new().unwrap();
    let consumer =
        NodeIdentity::load_or_generate(&dir.path().join("consumer.json"), "us-west").unwrap();

    let engine = BidEngine::new(BidConfig::default());
    let auction = engine
        .create_auction(auction_req(1.0, 10), consumer.peer_id_str(), 500)
        .unwrap();

    // Sweeper expires it with zero bids — no award, no price.
    let finalized = engine.expire_stale(510);
    assert_eq!(finalized.len(), 1);
    let closed = engine.get_auction(&auction.id).unwrap();
    assert_eq!(closed.status, AuctionStatus::Expired);
    assert!(closed.award.is_none());
    assert!(engine.recent_awards(10).is_empty());
    assert_eq!(engine.awarded_price_for("anyone"), None);
}

#[test]
fn pricing_rule_parsing() {
    assert_eq!(PricingRule::parse("vickrey"), PricingRule::Vickrey);
    assert_eq!(PricingRule::parse("first"), PricingRule::FirstPrice);
    assert_eq!(PricingRule::parse("first_price"), PricingRule::FirstPrice);
    assert_eq!(PricingRule::parse("pay-as-bid"), PricingRule::FirstPrice);
    assert_eq!(PricingRule::parse("garbage"), PricingRule::Vickrey);
    assert_eq!(PricingRule::parse(""), PricingRule::Vickrey);
}
