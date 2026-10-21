//! Marketplace bidding engine: sealed-bid auctions with quality-adjusted
//! winner selection and Vickrey (second-price) settlement.
//!
//! This is the price-discovery layer between a consumer's bandwidth request
//! and the tunnel handshake:
//!
//! ```text
//! consumer request → open auction → provider bids → close → award → tunnel
//! ```
//!
//! - **Sealed bids** — providers submit `price_usd_per_gb` while the auction
//!   is open; each provider may bid once per auction.
//! - **Budget cap** — the requester sets `max_price_usd_per_gb`; bids above
//!   the cap are rejected outright (no overpay, no sniping after close).
//! - **Quality-adjusted scoring** — winner = highest
//!   `(w_reputation·reputation + w_latency·latency_factor) / price`.
//!   Cheap-but-flaky providers lose to fair-priced reliable ones; the
//!   weights are tunable via `BID_W_REPUTATION` / `BID_W_LATENCY`.
//! - **Vickrey settlement** — the winner pays the *runner-up's price*
//!   (or its own price when sole bidder), which rewards truthful bidding.
//!   The locked price is attached to the [`Award`] and overrides the flat
//!   `SETTLEMENT_RATE_USD_PER_GB` when receipts from that counterparty are
//!   settled (see [`crate::settlement`]).
//! - **Short-lived state** — auctions live for seconds/minutes and are kept
//!   in memory; only the award log (settlement input) needs to survive
//!   queries. A background sweeper finalizes auctions past their deadline.
//!
//! Every state-changing method is synchronous and never holds a lock across
//! an `.await` (same discipline as [`crate::policy`]).

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{info, warn};

/// Lower bound for a bid price (USD/GB). Below this, division in the
/// scoring function would explode; also blocks "free" spoofed bids.
pub const MIN_PRICE_USD_PER_GB: f64 = 1.0e-6;
/// Upper bound for a bid or budget-cap price (USD/GB).
pub const MAX_PRICE_USD_PER_GB: f64 = 1.0e6;
/// Hard ceiling on auction window length (seconds).
pub const MAX_AUCTION_WINDOW_SECS: u64 = 3_600;
/// Default auction window when the requester omits `window_secs`.
pub const DEFAULT_AUCTION_WINDOW_SECS: u64 = 30;
/// Maximum accepted `region` length (bytes) — mirrors `/match`.
pub const MAX_REGION_LEN: usize = 64;
/// Maximum bids accepted per auction (bounds memory in the sweeper).
pub const MAX_BIDS_PER_AUCTION: usize = 256;

// ──────────────────────────────────────────────────────────────
// Domain model
// ──────────────────────────────────────────────────────────────

/// Lifecycle of an auction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuctionStatus {
    /// Accepting bids until `closes_at` (or early close by requester).
    Open,
    /// Finalized with a winner and a locked (Vickrey) price.
    Awarded,
    /// Deadline passed with zero bids — no price discovered.
    Expired,
    /// Requester aborted before any close/award.
    Cancelled,
}

impl AuctionStatus {
    pub fn is_open(self) -> bool {
        matches!(self, AuctionStatus::Open)
    }
}

/// A consumer's demand for bandwidth — the auction being auctioned off.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Auction {
    pub id: String,
    pub requester_peer_id: String,
    pub region: String,
    pub min_upload_mbps: f64,
    pub min_download_mbps: f64,
    /// Budget ceiling: no bid above this price may enter.
    pub max_price_usd_per_gb: f64,
    pub window_secs: u64,
    pub created_at: u64,
    pub closes_at: u64,
    pub status: AuctionStatus,
    /// Sealed bids (revealed only after close for full transparency of the
    /// marketplace; the API exposes them on the auction detail view).
    pub bids: Vec<Bid>,
    /// Present once `status == Awarded`.
    pub award: Option<Award>,
}

/// One provider's price offer for an auction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bid {
    pub id: String,
    pub auction_id: String,
    pub provider_peer_id: String,
    /// Offered price in USD per GB relayed.
    pub price_usd_per_gb: f64,
    /// Reputation snapshot at bid time (from the provider's own records).
    pub reputation_score: f64,
    /// Latency snapshot at bid time (ms; 0 = unrecorded).
    pub avg_latency_ms: f64,
    pub submitted_at: u64,
}

/// The auction outcome: winning provider + price locked for settlement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Award {
    pub auction_id: String,
    pub winning_bid_id: String,
    pub provider_peer_id: String,
    /// The consumer that requested the auction — receipts whose
    /// `counterparty_peer_id` matches this settle at the locked price.
    pub requester_peer_id: String,
    /// Locked price (winner's bid under first-price; `max(own, runner-up)`
    /// under procurement Vickrey).
    pub price_usd_per_gb: f64,
    /// The price the winner bid (may differ from the locked price).
    pub winning_bid_price_usd_per_gb: f64,
    /// Runner-up's price, when there was a second bid.
    pub runner_up_price_usd_per_gb: Option<f64>,
    pub awarded_at: u64,
}

/// Requester payload for `POST /bid`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateAuction {
    pub region: String,
    pub min_upload_mbps: f64,
    pub min_download_mbps: f64,
    /// Budget ceiling in USD/GB — the auction rejects bids above this.
    pub max_price_usd_per_gb: f64,
    /// Auction window in seconds; `None` → config default.
    #[serde(default)]
    pub window_secs: Option<u64>,
}

/// Provider payload for `POST /auctions/:id/bid`.
///
/// Two bid paths exist (G6 cross-node marketplace):
///
/// - **Local/legacy** — only `price_usd_per_gb`; the host attributes the
///   bid to itself (pre-G6 behavior, kept for compatibility).
/// - **Remote signed** — a foreign provider bidding over HTTP supplies all
///   three optional fields; the host verifies the ed25519 signature over
///   [`bid_sign_message`] and derives the bidder identity from the
///   embedded public key (spoof-proof attribution without auth tokens).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubmitBid {
    pub price_usd_per_gb: f64,
    /// Claimed bidder identity (libp2p PeerId) — required with `signature`.
    #[serde(default)]
    pub provider_peer_id: Option<String>,
    /// Base64 ed25519 public key (exactly 32 bytes) — required with `signature`.
    #[serde(default)]
    pub public_key: Option<String>,
    /// Base64 ed25519 signature over `bid:{auction_id}:{price.to_bits()}`.
    #[serde(default)]
    pub signature: Option<String>,
}

// ──────────────────────────────────────────────────────────────
// Errors
// ──────────────────────────────────────────────────────────────

/// Rejection reasons surfaced by the engine (mapped to HTTP by `main.rs`).
#[derive(Debug, Clone, PartialEq)]
pub enum BidError {
    Invalid(String),
    NotFound(String),
    /// Auction already finalized (awarded / expired / cancelled).
    Closed(String),
    /// Only the auction requester may close or cancel it.
    NotRequester,
    /// Peer is on `POLICY_BLOCKED_PEERS`.
    Blocked,
    /// Bid price exceeds the auction's budget cap.
    BudgetExceeded {
        price: f64,
        cap: f64,
    },
    /// This provider already bid on the auction.
    DuplicateBid,
    /// Auction is at the per-auction bid cap.
    TooManyBids,
}

impl std::fmt::Display for BidError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BidError::Invalid(m) => write!(f, "invalid request: {m}"),
            BidError::NotFound(id) => write!(f, "auction not found: {id}"),
            BidError::Closed(status) => write!(f, "auction is {status}"),
            BidError::NotRequester => {
                write!(f, "only the auction requester may perform this action")
            }
            BidError::Blocked => write!(f, "peer is blocked"),
            BidError::BudgetExceeded { price, cap } => {
                write!(f, "bid {price:.6} USD/GB exceeds budget cap {cap:.6}")
            }
            BidError::DuplicateBid => write!(f, "provider already bid on this auction"),
            BidError::TooManyBids => write!(f, "auction is at its bid cap"),
        }
    }
}

impl std::error::Error for BidError {}

// ──────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────

/// How the locked (settlement) price is derived from the ranked bids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PricingRule {
    /// Procurement Vickrey (default): the winner is paid
    /// `max(own bid, runner-up bid)` — the competitive second price,
    /// never below what the winner asked. Incentivizes truthful low bids:
    /// bid low, and you may still capture the runner-up's higher price.
    #[default]
    Vickrey,
    /// Pay-your-bid: locked price = the winner's own bid. Simpler,
    /// fully robust to quality-adjusted ranking, no truthfulness bonus.
    FirstPrice,
}

impl PricingRule {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "first" | "first_price" | "first-price" | "pay-as-bid" => PricingRule::FirstPrice,
            _ => PricingRule::Vickrey,
        }
    }

    /// Locked price for a winner with optional runner-up price.
    fn locked_price(&self, winner_bid: f64, runner_up: Option<f64>) -> f64 {
        match (self, runner_up) {
            (PricingRule::Vickrey, Some(second)) => winner_bid.max(second),
            _ => winner_bid,
        }
    }
}

/// Static bidding configuration, parsed once from the environment.
#[derive(Debug, Clone, PartialEq)]
pub struct BidConfig {
    /// Default auction window when the requester omits `window_secs`.
    pub default_window_secs: u64,
    /// Weight of reputation in the quality score (0..=1).
    pub w_reputation: f64,
    /// Weight of the latency factor in the quality score (0..=1).
    pub w_latency: f64,
    /// Latency (ms) at which the latency factor is 0.5. Default 100.
    pub ref_latency_ms: f64,
    /// Locked-price rule (`BID_PRICING`: `vickrey` | `first`).
    pub pricing: PricingRule,
    /// Peers that may not bid (shared with `POLICY_BLOCKED_PEERS`).
    pub blocked_peers: HashSet<String>,
}

impl Default for BidConfig {
    fn default() -> Self {
        Self {
            default_window_secs: DEFAULT_AUCTION_WINDOW_SECS,
            w_reputation: 0.6,
            w_latency: 0.4,
            ref_latency_ms: 100.0,
            pricing: PricingRule::default(),
            blocked_peers: HashSet::new(),
        }
    }
}

impl BidConfig {
    /// Read the `BID_*` env vars and merge in the shared blocked-peer list.
    pub fn from_env_with_blocked(blocked_peers: HashSet<String>) -> Self {
        let default_window_secs = env_u64_clamped(
            "BID_WINDOW_SECS",
            DEFAULT_AUCTION_WINDOW_SECS,
            1,
            MAX_AUCTION_WINDOW_SECS,
        );
        let w_reputation = env_f64_clamped("BID_W_REPUTATION", 0.6, 0.0, 1.0);
        let w_latency = env_f64_clamped("BID_W_LATENCY", 0.4, 0.0, 1.0);
        let ref_latency_ms = env_f64_clamped("BID_REF_LATENCY_MS", 100.0, 1.0, 10_000.0);
        let pricing = std::env::var("BID_PRICING")
            .map(|raw| PricingRule::parse(&raw))
            .unwrap_or_default();
        Self {
            default_window_secs,
            w_reputation,
            w_latency,
            ref_latency_ms,
            pricing,
            blocked_peers,
        }
    }
}

fn env_f64_clamped(key: &str, default: f64, min: f64, max: f64) -> f64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match raw.parse::<f64>() {
            Ok(v) if v.is_finite() => v.clamp(min, max),
            Ok(v) => {
                warn!(key, value = %v, default, "invalid numeric setting; using default");
                default
            }
            Err(_) => {
                warn!(key, value = %raw, default, "unparseable numeric setting; using default");
                default
            }
        },
    }
}

fn env_u64_clamped(key: &str, default: u64, min: u64, max: u64) -> u64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match raw.parse::<f64>() {
            Ok(v) if v.is_finite() => (v as u64).clamp(min, max),
            Ok(v) => {
                warn!(key, value = %v, default, "invalid numeric setting; using default");
                default
            }
            Err(_) => {
                warn!(key, value = %raw, default, "unparseable numeric setting; using default");
                default
            }
        },
    }
}

// ──────────────────────────────────────────────────────────────
// Scoring (price discovery)
// ──────────────────────────────────────────────────────────────

/// Latency factor in `[0, 1]`: `1 / (1 + latency_ms / ref_latency_ms)`.
/// 0 ms → 1.0, ref latency → 0.5, →∞ → 0.
pub fn latency_factor(latency_ms: f64, ref_latency_ms: f64) -> f64 {
    let lat = latency_ms.max(0.0);
    1.0 / (1.0 + lat / ref_latency_ms.max(1.0))
}

/// Quality-adjusted score for a bid: higher is better.
///
/// `score = (w_reputation · reputation + w_latency · latency_factor) / price`
///
/// Reputation is clamped to `[0, 1]`; price is floored at
/// [`MIN_PRICE_USD_PER_GB`] so a spoofed zero price cannot dominate.
pub fn bid_score(bid: &Bid, cfg: &BidConfig) -> f64 {
    let rep = bid.reputation_score.clamp(0.0, 1.0);
    let lat = latency_factor(bid.avg_latency_ms, cfg.ref_latency_ms);
    let quality = cfg.w_reputation * rep + cfg.w_latency * lat;
    quality / bid.price_usd_per_gb.max(MIN_PRICE_USD_PER_GB)
}

// ──────────────────────────────────────────────────────────────
// DHT wire format (G6: cross-node auction propagation)
// ──────────────────────────────────────────────────────────────

/// Wire cap for a serialized [`AuctionRecord`] on the DHT.
pub const MAX_AUCTION_RECORD_BYTES: usize = 2048;
/// Cap on auction ids carried in one host-index record.
pub const MAX_AUCTION_INDEX_IDS: usize = 32;
/// Cap on auction ids accepted from one host-index decode (defense for
/// hosts that ignore [`MAX_AUCTION_INDEX_IDS`] when encoding).
pub const MAX_AUCTION_INDEX_IDS_DECODE: usize = 64;
/// Maximum foreign (DHT-discovered) auctions kept in the [`AuctionBook`].
pub const MAX_FOREIGN_AUCTIONS: usize = 256;
/// Records that failed validation are remembered this long so we don't
/// refetch them every poll cycle.
pub const REJECTED_COOLDOWN_SECS: u64 = 120;
/// Cap on the rejected-id cooldown map.
pub const MAX_REJECTED_IDS: usize = 512;

/// DHT wire form of an *open* auction, published by the requester host.
///
/// Deliberately excludes sealed bids — those travel over signed HTTP to
/// the host, never through the DHT (Kademlia stores one value per key,
/// so a bid set could not be merged anyway). Includes the host's REST
/// address because a PeerId is not routable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuctionRecord {
    pub id: String,
    pub requester_peer_id: String,
    pub region: String,
    pub min_upload_mbps: f64,
    pub min_download_mbps: f64,
    /// Budget ceiling (USD/GB) — remote bids above this are rejected host-side.
    pub max_price_usd_per_gb: f64,
    pub window_secs: u64,
    pub created_at: u64,
    pub closes_at: u64,
    /// REST API base URL (`http://<ip>:<port>`) remote providers POST
    /// signed bids to.
    pub host_api_addr: String,
}

impl AuctionRecord {
    pub fn from_auction(auction: &Auction, host_api_addr: impl Into<String>) -> Self {
        Self {
            id: auction.id.clone(),
            requester_peer_id: auction.requester_peer_id.clone(),
            region: auction.region.clone(),
            min_upload_mbps: auction.min_upload_mbps,
            min_download_mbps: auction.min_download_mbps,
            max_price_usd_per_gb: auction.max_price_usd_per_gb,
            window_secs: auction.window_secs,
            created_at: auction.created_at,
            closes_at: auction.closes_at,
            host_api_addr: host_api_addr.into(),
        }
    }

    /// Local view of a foreign auction: open, no bids (ours travel over
    /// HTTP), no award yet.
    pub fn to_auction(&self) -> Auction {
        Auction {
            id: self.id.clone(),
            requester_peer_id: self.requester_peer_id.clone(),
            region: self.region.clone(),
            min_upload_mbps: self.min_upload_mbps,
            min_download_mbps: self.min_download_mbps,
            max_price_usd_per_gb: self.max_price_usd_per_gb,
            window_secs: self.window_secs,
            created_at: self.created_at,
            closes_at: self.closes_at,
            status: AuctionStatus::Open,
            bids: Vec::new(),
            award: None,
        }
    }

    pub fn is_expired(&self, now: u64) -> bool {
        now >= self.closes_at
    }

    /// Sanity-check every field that came off the wire. Returns a reason
    /// string on rejection (logged by the importer, never trusted as-is).
    pub fn validate(&self) -> Result<(), String> {
        if !valid_record_id(&self.id) {
            return Err("auction id has invalid characters or length".into());
        }
        if self.requester_peer_id.is_empty() || self.requester_peer_id.len() > 128 {
            return Err("requester peer id missing or too long".into());
        }
        if self.region.is_empty() || self.region.len() > MAX_REGION_LEN {
            return Err("region missing or too long".into());
        }
        if self.region.chars().any(|c| c.is_control()) {
            return Err("region contains control characters".into());
        }
        for (name, value) in [
            ("min_upload_mbps", self.min_upload_mbps),
            ("min_download_mbps", self.min_download_mbps),
            ("max_price_usd_per_gb", self.max_price_usd_per_gb),
        ] {
            if !value.is_finite() || !(0.0..=1_000_000.0).contains(&value) {
                return Err(format!("{name} out of range"));
            }
        }
        if !(MIN_PRICE_USD_PER_GB..=MAX_PRICE_USD_PER_GB).contains(&self.max_price_usd_per_gb) {
            return Err("max_price_usd_per_gb out of range".into());
        }
        if self.window_secs == 0 || self.window_secs > MAX_AUCTION_WINDOW_SECS {
            return Err("window_secs out of range".into());
        }
        if self.closes_at < self.created_at {
            return Err("closes_at precedes created_at".into());
        }
        if self.closes_at - self.created_at > MAX_AUCTION_WINDOW_SECS {
            return Err("auction lifetime exceeds the window ceiling".into());
        }
        if !self.host_api_addr.starts_with("http://") && !self.host_api_addr.starts_with("https://")
        {
            return Err("host_api_addr must be an http(s) base URL".into());
        }
        if self.host_api_addr.len() > 128
            || self
                .host_api_addr
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
        {
            return Err("host_api_addr malformed".into());
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, BidError> {
        let bytes =
            serde_json::to_vec(self).map_err(|e| BidError::Invalid(format!("encode: {e}")))?;
        if bytes.len() > MAX_AUCTION_RECORD_BYTES {
            return Err(BidError::Invalid(format!(
                "auction record exceeds {MAX_AUCTION_RECORD_BYTES} bytes"
            )));
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BidError> {
        if bytes.len() > MAX_AUCTION_RECORD_BYTES {
            return Err(BidError::Invalid(format!(
                "auction record exceeds {MAX_AUCTION_RECORD_BYTES} bytes"
            )));
        }
        let rec: Self =
            serde_json::from_slice(bytes).map_err(|e| BidError::Invalid(format!("decode: {e}")))?;
        rec.validate().map_err(BidError::Invalid)?;
        Ok(rec)
    }
}

/// Auction-id character policy for everything that crosses the DHT
/// (`auc-<millis>-<seq>` locally; foreign hosts may use any safe id).
fn valid_record_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Record key for a single open auction.
pub fn auction_key(id: &str) -> String {
    format!("auction/{id}")
}

/// Record key for a host's index of its open auction ids. The host also
/// announces this key as a provider record so peers can find hosts they
/// have never heard of.
pub fn host_index_key(host_peer_id: &str) -> String {
    format!("auctions_by_host/{host_peer_id}")
}

/// Provider-record key (no value) for "who hosts auctions in region R".
/// Auction hosts `start_providing` this; providers `get_providers` it.
pub fn region_providers_key(region: &str) -> String {
    format!("auctions/{region}")
}

/// Record key for an award, published by the requester host so the
/// winning provider can settle its receipts at the locked market price.
pub fn award_key(consumer_peer_id: &str, winner_peer_id: &str) -> String {
    format!("award/{consumer_peer_id}/{winner_peer_id}")
}

/// Serialize a host's open-auction ids (deduped + capped by the caller's
/// index build; see [`MAX_AUCTION_INDEX_IDS`]).
pub fn encode_auction_index(ids: &[String]) -> Result<Vec<u8>, BidError> {
    if ids.len() > MAX_AUCTION_INDEX_IDS {
        return Err(BidError::Invalid(format!(
            "auction index exceeds {MAX_AUCTION_INDEX_IDS} ids"
        )));
    }
    serde_json::to_vec(ids).map_err(|e| BidError::Invalid(format!("encode index: {e}")))
}

/// Parse a host-index record: JSON array of auction ids, each validated,
/// deduped, capped at [`MAX_AUCTION_INDEX_IDS_DECODE`].
pub fn decode_auction_index(bytes: &[u8]) -> Result<Vec<String>, BidError> {
    if bytes.len() > 4096 {
        return Err(BidError::Invalid("auction index exceeds 4096 bytes".into()));
    }
    let raw: Vec<String> = serde_json::from_slice(bytes)
        .map_err(|e| BidError::Invalid(format!("decode index: {e}")))?;
    let mut out = Vec::new();
    for id in raw {
        if !valid_record_id(&id) {
            return Err(BidError::Invalid(
                "index contains an invalid auction id".into(),
            ));
        }
        if !out.contains(&id) {
            out.push(id);
        }
        if out.len() >= MAX_AUCTION_INDEX_IDS_DECODE {
            break;
        }
    }
    Ok(out)
}

/// Foreign auctions discovered over the DHT (the provider side of G6).
///
/// Kept separate from [`BidEngine`]: we never hold sealed bids for
/// auctions we don't host, and we bid on these over signed HTTP instead.
#[derive(Debug, Default)]
pub struct AuctionBook {
    inner: Mutex<BookInner>,
}

#[derive(Debug, Default)]
struct BookInner {
    auctions: HashMap<String, AuctionRecord>,
    /// Ids whose records failed validation → import time (epoch secs), so
    /// a garbage publisher cannot make us refetch every index poll.
    rejected: HashMap<String, u64>,
}

impl AuctionBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or refresh a record. Returns `false` when rejected: our own
    /// record, already expired, invalid (remembered for
    /// [`REJECTED_COOLDOWN_SECS`]), or currently within the rejected
    /// cooldown. Capacity is enforced by evicting the earliest deadline.
    pub fn upsert(&self, rec: AuctionRecord, self_peer_id: &str, now: u64) -> bool {
        let mut inner = self.inner.lock().expect("auction book lock poisoned");
        if rec.requester_peer_id == self_peer_id {
            return false;
        }
        if rec.is_expired(now) {
            return false;
        }
        if let Err(reason) = rec.validate() {
            warn!(auction = %rec.id, reason = %reason, "rejected DHT auction record");
            inner.mark_rejected(rec.id, now);
            return false;
        }
        if inner.is_rejected(&rec.id, now) {
            return false;
        }
        let id = rec.id.clone();
        inner.auctions.insert(id.clone(), rec);
        inner.auctions.retain(|_, r| !r.is_expired(now));
        while inner.auctions.len() > MAX_FOREIGN_AUCTIONS {
            let Some(earliest) = inner
                .auctions
                .iter()
                .min_by_key(|(_, r)| r.closes_at)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            inner.auctions.remove(&earliest);
        }
        // May report `false` when the book was full and this record had
        // the earliest deadline (evicted immediately after insert).
        inner.auctions.contains_key(&id)
    }

    /// Drop records whose deadline passed (plus stale reject entries).
    /// Returns how many auctions were removed.
    pub fn prune(&self, now: u64) -> usize {
        let mut inner = self.inner.lock().expect("auction book lock poisoned");
        let before = inner.auctions.len();
        inner.auctions.retain(|_, r| !r.is_expired(now));
        inner.prune_rejected(now);
        before - inner.auctions.len()
    }

    /// Open foreign auctions, soonest-deadline first (auto-bid bids on
    /// each once; expiry is enforced host-side at submit time anyway).
    pub fn open(&self, now: u64) -> Vec<AuctionRecord> {
        let inner = self.inner.lock().expect("auction book lock poisoned");
        let mut out: Vec<AuctionRecord> = inner
            .auctions
            .values()
            .filter(|r| !r.is_expired(now))
            .cloned()
            .collect();
        out.sort_by_key(|r| r.closes_at);
        out
    }

    /// True when a validated record for `id` is already tracked.
    pub fn contains(&self, id: &str) -> bool {
        self.inner
            .lock()
            .expect("auction book lock poisoned")
            .auctions
            .contains_key(id)
    }

    /// True when `id` failed validation recently — skip refetching it.
    pub fn recently_rejected(&self, id: &str, now: u64) -> bool {
        self.inner
            .lock()
            .expect("auction book lock poisoned")
            .is_rejected(id, now)
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("auction book lock poisoned")
            .auctions
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl BookInner {
    fn mark_rejected(&mut self, id: String, now: u64) {
        if self.rejected.len() >= MAX_REJECTED_IDS {
            self.rejected.clear();
        }
        self.rejected.insert(id, now);
    }

    fn is_rejected(&self, id: &str, now: u64) -> bool {
        self.rejected
            .get(id)
            .is_some_and(|at| now.saturating_sub(*at) < REJECTED_COOLDOWN_SECS)
    }

    fn prune_rejected(&mut self, now: u64) {
        self.rejected
            .retain(|_, at| now.saturating_sub(*at) < REJECTED_COOLDOWN_SECS);
    }
}

/// Message a remote provider signs when bidding over HTTP. The price is
/// bound as its `f64` bit pattern so no float mangling can alter a bid
/// in flight.
pub fn bid_sign_message(auction_id: &str, price_usd_per_gb: f64) -> String {
    format!("bid:{auction_id}:{}", price_usd_per_gb.to_bits())
}

/// Sign a bid with the node's ed25519 key → base64 signature
/// (what the auto-bid client puts in [`SubmitBid::signature`]).
pub fn sign_bid(
    signing_key: &ed25519_dalek::SigningKey,
    auction_id: &str,
    price_usd_per_gb: f64,
) -> String {
    use ed25519_dalek::Signer;
    let msg = bid_sign_message(auction_id, price_usd_per_gb);
    let sig = signing_key.sign(msg.as_bytes());
    crate::base64_encode(&sig.to_bytes())
}

/// Verify a remote bid's signature and identity binding.
///
/// Returns the bidder's derived PeerId (which must equal `claimed`) on
/// success. Every failure mode maps to [`BidError::Invalid`] — the host
/// treats unverified bids exactly like malformed requests.
pub fn verify_signed_bid(
    auction_id: &str,
    price_usd_per_gb: f64,
    claimed_peer_id: &str,
    public_key_b64: &str,
    signature_b64: &str,
) -> Result<String, BidError> {
    let pk_raw: [u8; 32] = crate::base64_decode(public_key_b64)
        .try_into()
        .map_err(|_| BidError::Invalid("public_key must be base64 of 32 bytes".into()))?;
    let sig_raw: [u8; 64] = crate::base64_decode(signature_b64)
        .try_into()
        .map_err(|_| BidError::Invalid("signature must be base64 of 64 bytes".into()))?;

    let vk = ed25519_dalek::VerifyingKey::from_bytes(&pk_raw)
        .map_err(|_| BidError::Invalid("invalid ed25519 public key".into()))?;
    let sig = ed25519_dalek::Signature::from_bytes(&sig_raw);
    let msg = bid_sign_message(auction_id, price_usd_per_gb);
    vk.verify_strict(msg.as_bytes(), &sig)
        .map_err(|_| BidError::Invalid("bid signature verification failed".into()))?;

    // The signature proves ownership of the key; the PeerId is derived
    // from that key, never taken on trust from the payload.
    let ed_pk = libp2p_identity::ed25519::PublicKey::try_from_bytes(&pk_raw)
        .map_err(|_| BidError::Invalid("invalid ed25519 public key".into()))?;
    let derived = libp2p_identity::PublicKey::from(ed_pk)
        .to_peer_id()
        .to_string();
    if derived != claimed_peer_id {
        return Err(BidError::Invalid(format!(
            "peer id mismatch: claimed {claimed_peer_id}, signature proves {derived}"
        )));
    }
    Ok(derived)
}

// ──────────────────────────────────────────────────────────────
// Engine
// ──────────────────────────────────────────────────────────────

/// In-memory auction registry + append-only award log.
///
/// Auctions are ephemeral (seconds-long windows) so they live in a map;
/// awards are the durable price-discovery output consumed by settlement.
#[derive(Debug)]
pub struct BidEngine {
    auctions: Mutex<HashMap<String, Auction>>,
    /// Append-only log of finalized awards (newest last).
    awards: Mutex<Vec<Award>>,
    cfg: BidConfig,
    seq: AtomicU64,
}

impl BidEngine {
    pub fn new(cfg: BidConfig) -> Self {
        Self {
            auctions: Mutex::new(HashMap::new()),
            awards: Mutex::new(Vec::new()),
            cfg,
            seq: AtomicU64::new(1),
        }
    }

    pub fn config(&self) -> &BidConfig {
        &self.cfg
    }

    fn next_id(&self, prefix: &str) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{}-{n}", crate::bidding::now_millis())
    }

    /// Validate an auction request (pure; unit-tested directly).
    pub fn validate_create(req: &CreateAuction) -> Result<(), BidError> {
        let region = req.region.trim();
        if region.is_empty() {
            return Err(BidError::Invalid("region must not be empty".into()));
        }
        if region.len() > MAX_REGION_LEN {
            return Err(BidError::Invalid(format!(
                "region must be at most {MAX_REGION_LEN} bytes"
            )));
        }
        if region.chars().any(|c| c.is_control()) {
            return Err(BidError::Invalid(
                "region must not contain control characters".into(),
            ));
        }
        for (name, value) in [
            ("min_upload_mbps", req.min_upload_mbps),
            ("min_download_mbps", req.min_download_mbps),
        ] {
            if !value.is_finite() || !(0.0..=1_000_000.0).contains(&value) {
                return Err(BidError::Invalid(format!(
                    "{name} must be finite and within 0..=1000000"
                )));
            }
        }
        if !req.max_price_usd_per_gb.is_finite()
            || !(MIN_PRICE_USD_PER_GB..=MAX_PRICE_USD_PER_GB).contains(&req.max_price_usd_per_gb)
        {
            return Err(BidError::Invalid(format!(
                "max_price_usd_per_gb must be finite and within {MIN_PRICE_USD_PER_GB}..={MAX_PRICE_USD_PER_GB}"
            )));
        }
        if let Some(w) = req.window_secs {
            if w == 0 || w > MAX_AUCTION_WINDOW_SECS {
                return Err(BidError::Invalid(format!(
                    "window_secs must be within 1..={MAX_AUCTION_WINDOW_SECS}"
                )));
            }
        }
        Ok(())
    }

    /// Open a new auction. `requester` is the consumer's peer id.
    pub fn create_auction(
        &self,
        req: CreateAuction,
        requester: &str,
        now: u64,
    ) -> Result<Auction, BidError> {
        Self::validate_create(&req)?;
        if self.cfg.blocked_peers.contains(requester) {
            return Err(BidError::Blocked);
        }
        let window_secs = req.window_secs.unwrap_or(self.cfg.default_window_secs);
        let auction = Auction {
            id: self.next_id("auc"),
            requester_peer_id: requester.to_string(),
            region: req.region.trim().to_string(),
            min_upload_mbps: req.min_upload_mbps,
            min_download_mbps: req.min_download_mbps,
            max_price_usd_per_gb: req.max_price_usd_per_gb,
            window_secs,
            created_at: now,
            closes_at: now.saturating_add(window_secs),
            status: AuctionStatus::Open,
            bids: Vec::new(),
            award: None,
        };
        info!(
            auction = %auction.id,
            region = %auction.region,
            cap = auction.max_price_usd_per_gb,
            window_secs,
            "auction opened"
        );
        self.auctions
            .lock()
            .expect("bid engine auctions lock poisoned")
            .insert(auction.id.clone(), auction.clone());
        Ok(auction)
    }

    /// Submit a sealed bid. One bid per provider per auction.
    ///
    /// `reputation` / `latency_ms` are snapshots the caller (provider node)
    /// fills from its own records so scoring is deterministic per bid.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_bid(
        &self,
        auction_id: &str,
        provider: &str,
        price_usd_per_gb: f64,
        reputation: f64,
        latency_ms: f64,
        now: u64,
    ) -> Result<Bid, BidError> {
        if self.cfg.blocked_peers.contains(provider) {
            return Err(BidError::Blocked);
        }
        if !price_usd_per_gb.is_finite()
            || !(MIN_PRICE_USD_PER_GB..=MAX_PRICE_USD_PER_GB).contains(&price_usd_per_gb)
        {
            return Err(BidError::Invalid(format!(
                "price_usd_per_gb must be finite and within {MIN_PRICE_USD_PER_GB}..={MAX_PRICE_USD_PER_GB}"
            )));
        }

        let mut auctions = self
            .auctions
            .lock()
            .expect("bid engine auctions lock poisoned");
        let auction = auctions
            .get_mut(auction_id)
            .ok_or_else(|| BidError::NotFound(auction_id.to_string()))?;

        if !auction.status.is_open() {
            return Err(BidError::Closed(
                format!("{:?}", auction.status).to_lowercase(),
            ));
        }
        // Late submissions race the sweeper: treat past-deadline as closed.
        if now >= auction.closes_at {
            return Err(BidError::Closed("open".into()));
        }
        if auction.bids.iter().any(|b| b.provider_peer_id == provider) {
            return Err(BidError::DuplicateBid);
        }
        if auction.bids.len() >= MAX_BIDS_PER_AUCTION {
            return Err(BidError::TooManyBids);
        }
        if price_usd_per_gb > auction.max_price_usd_per_gb {
            return Err(BidError::BudgetExceeded {
                price: price_usd_per_gb,
                cap: auction.max_price_usd_per_gb,
            });
        }

        let bid = Bid {
            id: self.next_id("bid"),
            auction_id: auction_id.to_string(),
            provider_peer_id: provider.to_string(),
            price_usd_per_gb,
            reputation_score: reputation.clamp(0.0, 1.0),
            avg_latency_ms: latency_ms.max(0.0),
            submitted_at: now,
        };
        auction.bids.push(bid.clone());
        info!(
            auction = %auction_id,
            provider = %provider,
            price = price_usd_per_gb,
            bids = auction.bids.len(),
            "bid submitted"
        );
        Ok(bid)
    }

    /// Close an auction now and award the winner (only the requester may).
    pub fn close_auction(
        &self,
        auction_id: &str,
        caller: &str,
        now: u64,
    ) -> Result<Auction, BidError> {
        let mut auctions = self
            .auctions
            .lock()
            .expect("bid engine auctions lock poisoned");
        let auction = auctions
            .get_mut(auction_id)
            .ok_or_else(|| BidError::NotFound(auction_id.to_string()))?;
        if auction.requester_peer_id != caller {
            return Err(BidError::NotRequester);
        }
        if !auction.status.is_open() {
            return Err(BidError::Closed(
                format!("{:?}", auction.status).to_lowercase(),
            ));
        }
        let award = finalize(auction, &self.cfg, now);
        let snapshot = auction.clone();
        drop(auctions);
        if let Some(award) = award {
            self.record_award(award);
        }
        Ok(snapshot)
    }

    /// Cancel an open auction (requester only, before any award).
    pub fn cancel_auction(&self, auction_id: &str, caller: &str) -> Result<Auction, BidError> {
        let mut auctions = self
            .auctions
            .lock()
            .expect("bid engine auctions lock poisoned");
        let auction = auctions
            .get_mut(auction_id)
            .ok_or_else(|| BidError::NotFound(auction_id.to_string()))?;
        if auction.requester_peer_id != caller {
            return Err(BidError::NotRequester);
        }
        if !auction.status.is_open() {
            return Err(BidError::Closed(
                format!("{:?}", auction.status).to_lowercase(),
            ));
        }
        auction.status = AuctionStatus::Cancelled;
        info!(auction = %auction_id, "auction cancelled by requester");
        Ok(auction.clone())
    }

    pub fn get_auction(&self, auction_id: &str) -> Option<Auction> {
        self.auctions
            .lock()
            .expect("bid engine auctions lock poisoned")
            .get(auction_id)
            .cloned()
    }

    /// All currently-open auctions (provider auto-bid polls this).
    pub fn open_auctions(&self) -> Vec<Auction> {
        self.auctions
            .lock()
            .expect("bid engine auctions lock poisoned")
            .values()
            .filter(|a| a.status.is_open())
            .cloned()
            .collect()
    }

    /// Finalize every open auction past its deadline. Returns the
    /// auctions finalized by *this* call (for logging/telemetry).
    pub fn expire_stale(&self, now: u64) -> Vec<Auction> {
        let mut finalized = Vec::new();
        let mut awards = Vec::new();
        {
            let mut auctions = self
                .auctions
                .lock()
                .expect("bid engine auctions lock poisoned");
            for auction in auctions.values_mut() {
                if auction.status.is_open() && now >= auction.closes_at {
                    let was_bid_count = auction.bids.len();
                    if let Some(award) = finalize(auction, &self.cfg, now) {
                        awards.push(award);
                    } else if was_bid_count == 0 {
                        info!(auction = %auction.id, "auction expired with no bids");
                    }
                    finalized.push(auction.clone());
                }
            }
        }
        for award in awards {
            self.record_award(award);
        }
        finalized
    }

    fn record_award(&self, award: Award) {
        info!(
            auction = %award.auction_id,
            provider = %award.provider_peer_id,
            locked_price = award.price_usd_per_gb,
            winning_bid_price = award.winning_bid_price_usd_per_gb,
            "auction awarded"
        );
        self.awards
            .lock()
            .expect("bid engine awards lock poisoned")
            .push(award);
    }

    /// Most recent awards, newest first.
    pub fn recent_awards(&self, limit: usize) -> Vec<Award> {
        let awards = self.awards.lock().expect("bid engine awards lock poisoned");
        awards.iter().rev().take(limit).cloned().collect()
    }

    /// Locked settlement price (USD/GB) for traffic relayed *for* the
    /// consumer `requester_id` — matches awards by their requester.
    /// `None` when that consumer never won an auction against any
    /// provider here; callers fall back to the flat rate.
    pub fn awarded_price_for(&self, requester_id: &str) -> Option<f64> {
        let awards = self.awards.lock().expect("bid engine awards lock poisoned");
        awards
            .iter()
            .rev()
            .find(|a| a.requester_peer_id == requester_id)
            .map(|a| a.price_usd_per_gb)
    }

    /// Total auctions tracked (open + finalized) — observability.
    pub fn auction_count(&self) -> usize {
        self.auctions
            .lock()
            .expect("bid engine auctions lock poisoned")
            .len()
    }

    /// Import an award discovered over the DHT: we (the provider) won an
    /// auction hosted elsewhere, so future receipts from that consumer
    /// settle at the locked market price via [`Self::awarded_price_for`].
    /// Returns `true` when newly recorded.
    ///
    /// G6 trust note: award records are self-attested by the requester
    /// host (no co-signature) — accepted only when *we* are the winner,
    /// bounds-checked, and deduped by auction id; they affect pricing of
    /// our own earnings only.
    pub fn import_foreign_award(&self, award: Award, local_peer_id: &str) -> bool {
        if award.provider_peer_id != local_peer_id {
            return false;
        }
        if award.requester_peer_id.is_empty() || award.auction_id.is_empty() {
            return false;
        }
        for (name, price) in [
            ("price_usd_per_gb", award.price_usd_per_gb),
            (
                "winning_bid_price_usd_per_gb",
                award.winning_bid_price_usd_per_gb,
            ),
        ] {
            if !price.is_finite() || !(MIN_PRICE_USD_PER_GB..=MAX_PRICE_USD_PER_GB).contains(&price)
            {
                warn!(auction = %award.auction_id, name, price, "ignored DHT award with out-of-range price");
                return false;
            }
        }
        let mut awards = self.awards.lock().expect("bid engine awards lock poisoned");
        if awards.iter().any(|a| a.auction_id == award.auction_id) {
            return false;
        }
        info!(
            auction = %award.auction_id,
            consumer = %award.requester_peer_id,
            locked_price = award.price_usd_per_gb,
            "imported award discovered via DHT"
        );
        awards.push(award);
        true
    }

    /// Awards recorded after `cursor` (index into the log), plus the new
    /// cursor. Used by the DHT award publisher to stream each new outcome
    /// exactly once per process run.
    pub fn awards_from(&self, cursor: usize) -> (Vec<Award>, usize) {
        let awards = self.awards.lock().expect("bid engine awards lock poisoned");
        let len = awards.len();
        if cursor >= len {
            return (Vec::new(), len);
        }
        (awards[cursor..].to_vec(), len)
    }
}

/// Rank bids, pick the winner, apply the configured pricing rule, mutate
/// the auction to its final status. Returns the award when one was created.
fn finalize(auction: &mut Auction, cfg: &BidConfig, now: u64) -> Option<Award> {
    debug_assert!(auction.status.is_open());
    if auction.bids.is_empty() {
        auction.status = AuctionStatus::Expired;
        return None;
    }

    rank_bids(&mut auction.bids, cfg);

    let winner = auction.bids[0].clone();
    let runner_up_price = auction.bids.get(1).map(|b| b.price_usd_per_gb);
    let locked_price = cfg
        .pricing
        .locked_price(winner.price_usd_per_gb, runner_up_price);

    let award = Award {
        auction_id: auction.id.clone(),
        winning_bid_id: winner.id.clone(),
        provider_peer_id: winner.provider_peer_id.clone(),
        requester_peer_id: auction.requester_peer_id.clone(),
        price_usd_per_gb: locked_price,
        winning_bid_price_usd_per_gb: winner.price_usd_per_gb,
        runner_up_price_usd_per_gb: runner_up_price,
        awarded_at: now,
    };
    auction.status = AuctionStatus::Awarded;
    auction.award = Some(award.clone());
    Some(award)
}

/// Sort bids in-place into winner-first order under `cfg`'s weights.
fn rank_bids(bids: &mut [Bid], cfg: &BidConfig) {
    bids.sort_by(|a, b| {
        bid_score(b, cfg)
            .partial_cmp(&bid_score(a, cfg))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                a.price_usd_per_gb
                    .partial_cmp(&b.price_usd_per_gb)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                a.avg_latency_ms
                    .partial_cmp(&b.avg_latency_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ──────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base64_encode;

    fn engine() -> BidEngine {
        BidEngine::new(BidConfig::default())
    }

    fn create_req(cap: f64, window: u64) -> CreateAuction {
        CreateAuction {
            region: "us-west".into(),
            min_upload_mbps: 10.0,
            min_download_mbps: 50.0,
            max_price_usd_per_gb: cap,
            window_secs: Some(window),
        }
    }

    #[test]
    fn validate_rejects_bad_inputs() {
        let mut req = create_req(1.0, 30);
        req.region = "".into();
        assert!(matches!(
            BidEngine::validate_create(&req),
            Err(BidError::Invalid(_))
        ));

        let mut req = create_req(1.0, 30);
        req.region = "x".repeat(MAX_REGION_LEN + 1);
        assert!(BidEngine::validate_create(&req).is_err());

        let mut req = create_req(1.0, 30);
        req.region = "us\u{0}-west".into();
        assert!(BidEngine::validate_create(&req).is_err());

        let mut req = create_req(1.0, 30);
        req.min_upload_mbps = f64::NAN;
        assert!(BidEngine::validate_create(&req).is_err());

        let mut req = create_req(1.0, 30);
        req.min_upload_mbps = -1.0;
        assert!(BidEngine::validate_create(&req).is_err());

        // Price below floor / above ceiling / non-finite
        assert!(BidEngine::validate_create(&create_req(0.0, 30)).is_err());
        assert!(BidEngine::validate_create(&create_req(1.0e9, 30)).is_err());
        assert!(BidEngine::validate_create(&create_req(f64::INFINITY, 30)).is_err());

        // Window 0 and over ceiling
        assert!(BidEngine::validate_create(&create_req(1.0, 0)).is_err());
        assert!(BidEngine::validate_create(&create_req(1.0, MAX_AUCTION_WINDOW_SECS + 1)).is_err());

        // Omitted window is fine (config default)
        let mut req = create_req(1.0, 30);
        req.window_secs = None;
        assert!(BidEngine::validate_create(&req).is_ok());
    }

    #[test]
    fn create_and_get_auction() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(1.0, 30), "consumer-1", 1_000)
            .unwrap();
        assert_eq!(a.status, AuctionStatus::Open);
        assert_eq!(a.closes_at, 1_030);
        assert_eq!(eng.get_auction(&a.id).unwrap().id, a.id);
        assert_eq!(eng.open_auctions().len(), 1);
        assert_eq!(eng.auction_count(), 1);
    }

    #[test]
    fn blocked_peer_cannot_create_or_bid() {
        let mut cfg = BidConfig::default();
        cfg.blocked_peers.insert("bad-peer".into());
        let eng = BidEngine::new(cfg);
        assert_eq!(
            eng.create_auction(create_req(1.0, 30), "bad-peer", 0),
            Err(BidError::Blocked)
        );
        let a = eng
            .create_auction(create_req(1.0, 30), "consumer-1", 0)
            .unwrap();
        assert_eq!(
            eng.submit_bid(&a.id, "bad-peer", 0.5, 0.0, 0.0, 0),
            Err(BidError::Blocked)
        );
    }

    #[test]
    fn submit_bid_validates_budget_and_duplicates() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(1.0, 60), "consumer-1", 0)
            .unwrap();

        // Over budget
        assert_eq!(
            eng.submit_bid(&a.id, "p1", 1.5, 0.5, 50.0, 1),
            Err(BidError::BudgetExceeded {
                price: 1.5,
                cap: 1.0
            })
        );
        // Non-finite / zero / negative
        assert!(matches!(
            eng.submit_bid(&a.id, "p1", f64::NAN, 0.5, 50.0, 1),
            Err(BidError::Invalid(_))
        ));
        assert!(matches!(
            eng.submit_bid(&a.id, "p1", 0.0, 0.5, 50.0, 1),
            Err(BidError::Invalid(_))
        ));

        // Valid bid, then duplicate
        let bid = eng.submit_bid(&a.id, "p1", 0.8, 0.9, 20.0, 2).unwrap();
        assert_eq!(bid.provider_peer_id, "p1");
        assert_eq!(bid.price_usd_per_gb, 0.8);
        assert_eq!(
            eng.submit_bid(&a.id, "p1", 0.5, 0.9, 20.0, 3),
            Err(BidError::DuplicateBid)
        );

        // Unknown auction
        assert!(matches!(
            eng.submit_bid("nope", "p2", 0.5, 0.5, 50.0, 3),
            Err(BidError::NotFound(_))
        ));
    }

    #[test]
    fn vickrey_pays_competitive_second_price() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        // Quality winner bids lower than the runner-up…
        eng.submit_bid(&a.id, "winner", 0.6, 0.9, 20.0, 1).unwrap();
        eng.submit_bid(&a.id, "runner", 0.9, 0.9, 20.0, 2).unwrap();

        let closed = eng.close_auction(&a.id, "consumer-1", 10).unwrap();
        assert_eq!(closed.status, AuctionStatus::Awarded);
        let award = closed.award.unwrap();
        assert_eq!(award.provider_peer_id, "winner");
        assert_eq!(award.winning_bid_price_usd_per_gb, 0.6);
        assert_eq!(award.runner_up_price_usd_per_gb, Some(0.9));
        // Procurement Vickrey: winner is paid max(own, runner-up) — the
        // competitive second price, never below their own bid.
        assert_eq!(award.price_usd_per_gb, 0.9);
    }

    #[test]
    fn vickrey_quality_winner_bid_above_runner_keeps_own_price() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        // Quality winner bid 0.8; runner-up asked cheaper 0.3 but lost on
        // quality. Locked price must not drop below the winner's bid.
        eng.submit_bid(&a.id, "solid", 0.8, 1.0, 10.0, 1).unwrap();
        eng.submit_bid(&a.id, "cheap", 0.3, 0.1, 400.0, 2).unwrap();
        let closed = eng.close_auction(&a.id, "consumer-1", 10).unwrap();
        let award = closed.award.unwrap();
        assert_eq!(award.provider_peer_id, "solid");
        assert_eq!(award.price_usd_per_gb, 0.8);
        assert_eq!(award.runner_up_price_usd_per_gb, Some(0.3));
    }

    #[test]
    fn first_price_pays_winning_bid_only() {
        let cfg = BidConfig {
            pricing: PricingRule::FirstPrice,
            ..Default::default()
        };
        let eng = BidEngine::new(cfg);
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        eng.submit_bid(&a.id, "winner", 0.6, 0.9, 20.0, 1).unwrap();
        eng.submit_bid(&a.id, "runner", 0.9, 0.9, 20.0, 2).unwrap();
        let closed = eng.close_auction(&a.id, "consumer-1", 10).unwrap();
        let award = closed.award.unwrap();
        assert_eq!(award.price_usd_per_gb, 0.6);
        assert_eq!(award.runner_up_price_usd_per_gb, Some(0.9));
    }

    #[test]
    fn sole_bidder_pays_own_price() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        eng.submit_bid(&a.id, "solo", 0.4, 0.8, 30.0, 1).unwrap();
        let closed = eng.close_auction(&a.id, "consumer-1", 5).unwrap();
        let award = closed.award.unwrap();
        assert_eq!(award.provider_peer_id, "solo");
        assert_eq!(award.price_usd_per_gb, 0.4);
        assert_eq!(award.runner_up_price_usd_per_gb, None);
    }

    #[test]
    fn no_bids_expires_without_award() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        let closed = eng.close_auction(&a.id, "consumer-1", 5).unwrap();
        assert_eq!(closed.status, AuctionStatus::Expired);
        assert!(closed.award.is_none());
        assert!(eng.recent_awards(10).is_empty());
    }

    #[test]
    fn close_and_cancel_require_requester() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        assert_eq!(
            eng.close_auction(&a.id, "mallory", 5),
            Err(BidError::NotRequester)
        );
        assert_eq!(
            eng.cancel_auction(&a.id, "mallory"),
            Err(BidError::NotRequester)
        );

        let cancelled = eng.cancel_auction(&a.id, "consumer-1").unwrap();
        assert_eq!(cancelled.status, AuctionStatus::Cancelled);
        // Bids after cancel are rejected
        assert!(matches!(
            eng.submit_bid(&a.id, "p1", 0.5, 0.5, 50.0, 6),
            Err(BidError::Closed(_))
        ));
        // Double-close is rejected
        assert!(matches!(
            eng.close_auction(&a.id, "consumer-1", 6),
            Err(BidError::Closed(_))
        ));
    }

    #[test]
    fn quality_adjusted_winner_beats_cheap_flaky_bid() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        // Cheap but zero reputation + terrible latency
        eng.submit_bid(&a.id, "cheap-flaky", 0.10, 0.0, 900.0, 1)
            .unwrap();
        // Fair price, excellent reputation + latency
        eng.submit_bid(&a.id, "solid", 0.50, 1.0, 10.0, 2).unwrap();
        let closed = eng.close_auction(&a.id, "consumer-1", 5).unwrap();
        let award = closed.award.unwrap();
        assert_eq!(award.provider_peer_id, "solid");
        // Vickrey: winner is paid max(own 0.50, runner-up 0.10) = 0.50
        // (quality premium honored at the winner's ask).
        assert_eq!(award.price_usd_per_gb, 0.50);
    }

    #[test]
    fn expire_stale_finalizes_past_deadline() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 10), "consumer-1", 100)
            .unwrap();
        eng.submit_bid(&a.id, "p1", 0.5, 0.7, 40.0, 105).unwrap();

        // Before deadline: nothing finalized
        assert!(eng.expire_stale(109).is_empty());
        assert_eq!(eng.get_auction(&a.id).unwrap().status, AuctionStatus::Open);

        // At deadline: finalized + awarded + logged
        let finalized = eng.expire_stale(110);
        assert_eq!(finalized.len(), 1);
        let auction = eng.get_auction(&a.id).unwrap();
        assert_eq!(auction.status, AuctionStatus::Awarded);
        assert_eq!(eng.recent_awards(10).len(), 1);
        // Sweeper is idempotent
        assert!(eng.expire_stale(200).is_empty());
    }

    #[test]
    fn bid_after_deadline_rejected_even_before_sweeper() {
        let eng = engine();
        let a = eng
            .create_auction(create_req(2.0, 10), "consumer-1", 100)
            .unwrap();
        assert!(matches!(
            eng.submit_bid(&a.id, "p1", 0.5, 0.7, 40.0, 110),
            Err(BidError::Closed(_))
        ));
    }

    #[test]
    fn awarded_price_for_settlement_lookup() {
        let eng = engine();
        assert_eq!(eng.awarded_price_for("consumer-1"), None);

        let a = eng
            .create_auction(create_req(2.0, 60), "consumer-1", 0)
            .unwrap();
        eng.submit_bid(&a.id, "p1", 0.7, 0.8, 30.0, 1).unwrap();
        eng.close_auction(&a.id, "consumer-1", 5).unwrap();
        // Lookup is by auction requester (the receipt counterparty).
        assert_eq!(eng.awarded_price_for("consumer-1"), Some(0.7));
        assert_eq!(eng.awarded_price_for("consumer-2"), None);
    }

    #[test]
    fn latency_factor_bounds() {
        assert_eq!(latency_factor(0.0, 100.0), 1.0);
        assert!((latency_factor(100.0, 100.0) - 0.5).abs() < 1e-9);
        assert!(latency_factor(10_000.0, 100.0) < 0.02);
        // Negative latency clamps to the 0-ms value
        assert_eq!(latency_factor(-5.0, 100.0), 1.0);
    }

    #[test]
    fn scoring_prefers_lower_price_at_equal_quality() {
        let cfg = BidConfig::default();
        let cheap = Bid {
            id: "b1".into(),
            auction_id: "a".into(),
            provider_peer_id: "p1".into(),
            price_usd_per_gb: 0.3,
            reputation_score: 0.8,
            avg_latency_ms: 40.0,
            submitted_at: 0,
        };
        let pricey = Bid {
            id: "b2".into(),
            ..cheap.clone()
        };
        let mut pricey = pricey;
        pricey.price_usd_per_gb = 0.6;
        assert!(bid_score(&cheap, &cfg) > bid_score(&pricey, &cfg));
    }

    #[test]
    fn tie_break_equal_score_prefers_lower_price_then_latency() {
        let cfg = BidConfig::default();
        let mk = |id: &str, price: f64, lat: f64| Bid {
            id: id.into(),
            auction_id: "a".into(),
            provider_peer_id: id.into(),
            price_usd_per_gb: price,
            reputation_score: 0.5,
            avg_latency_ms: lat,
            submitted_at: 0,
        };
        // Same price & latency → id ordering decides (deterministic)
        let mut bids = vec![mk("b2", 0.5, 50.0), mk("b1", 0.5, 50.0)];
        rank_bids(&mut bids, &cfg);
        assert_eq!(bids[0].id, "b1");

        // Equal price → lower latency wins
        let mut bids = vec![mk("b2", 0.5, 80.0), mk("b1", 0.5, 20.0)];
        rank_bids(&mut bids, &cfg);
        assert_eq!(bids[0].id, "b1");
    }

    // ── G6: DHT wire format ──────────────────────────────────

    fn sample_record(id: &str, closes_at: u64) -> AuctionRecord {
        AuctionRecord {
            id: id.into(),
            requester_peer_id: "peer-consumer".into(),
            region: "us-west".into(),
            min_upload_mbps: 10.0,
            min_download_mbps: 50.0,
            max_price_usd_per_gb: 1.0,
            window_secs: closes_at.saturating_sub(1_000).min(MAX_AUCTION_WINDOW_SECS),
            created_at: 1_000,
            closes_at,
            host_api_addr: "http://203.0.113.7:3001".into(),
        }
    }

    fn bidder() -> (ed25519_dalek::SigningKey, String) {
        let sk = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let ed_pk =
            libp2p_identity::ed25519::PublicKey::try_from_bytes(&sk.verifying_key().to_bytes())
                .expect("verifying key bytes");
        let peer = libp2p_identity::PublicKey::from(ed_pk)
            .to_peer_id()
            .to_string();
        (sk, peer)
    }

    #[test]
    fn auction_record_round_trip() {
        let auction = Auction {
            id: "auc-1729-1".into(),
            requester_peer_id: "peer-consumer".into(),
            region: "us-west".into(),
            min_upload_mbps: 10.0,
            min_download_mbps: 50.0,
            max_price_usd_per_gb: 0.75,
            window_secs: 30,
            created_at: 1_000,
            closes_at: 1_030,
            status: AuctionStatus::Open,
            bids: Vec::new(),
            award: None,
        };
        let rec = AuctionRecord::from_auction(&auction, "http://198.51.100.4:3001");
        let bytes = rec.to_bytes().expect("encode");
        let back = AuctionRecord::from_bytes(&bytes).expect("decode");
        assert_eq!(back, rec);

        let view = back.to_auction();
        assert_eq!(view.id, auction.id);
        assert_eq!(view.region, auction.region);
        assert_eq!(view.status, AuctionStatus::Open);
        assert!(view.bids.is_empty());
        assert!(!back.is_expired(1_029));
        assert!(back.is_expired(1_030));
    }

    #[test]
    fn auction_record_rejects_oversize_and_garbage() {
        // Encode-side size cap (validate is bypassed until from_bytes).
        let mut rec = sample_record("auc-1", 4_000);
        rec.requester_peer_id = "x".repeat(4_096);
        assert!(rec.to_bytes().is_err());

        // Decode-side size cap fires before JSON parsing.
        assert!(AuctionRecord::from_bytes(&vec![b' '; MAX_AUCTION_RECORD_BYTES + 1]).is_err());
        // …and garbage JSON under the cap fails cleanly too.
        assert!(AuctionRecord::from_bytes(b"{not json").is_err());
    }

    #[test]
    fn auction_record_validate_rejects_bad_fields() {
        let mut rec = sample_record("auc-1", 4_000);
        rec.id = "bad id/../".into();
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.host_api_addr = "ftp://example.com".into();
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.host_api_addr = "http://has space/".into();
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.region = "us\u{0}-west".into();
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.closes_at = 999; // precedes created_at
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.closes_at = 1_000 + MAX_AUCTION_WINDOW_SECS + 1; // lifetime over ceiling
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.window_secs = 0;
        assert!(rec.validate().is_err());

        let mut rec = sample_record("auc-1", 4_000);
        rec.max_price_usd_per_gb = 0.0;
        assert!(rec.validate().is_err());

        assert!(sample_record("auc-1", 4_000).validate().is_ok());
    }

    #[test]
    fn dht_key_helpers_and_prefix_routing() {
        assert_eq!(auction_key("auc-9"), "auction/auc-9");
        assert_eq!(host_index_key("12D3KooW"), "auctions_by_host/12D3KooW");
        assert_eq!(region_providers_key("us-west"), "auctions/us-west");
        assert_eq!(award_key("c1", "w1"), "award/c1/w1");

        // Event routing relies on unambiguous prefix strips: `auction/`
        // must never match `auctions…` keys and vice versa.
        let auction = auction_key("auc-9");
        assert_eq!(auction.strip_prefix("auction/"), Some("auc-9"));
        assert_eq!(auction.strip_prefix("auctions_by_host/"), None);
        assert_eq!(auction.strip_prefix("auctions/"), None);

        let index = host_index_key("peer-x");
        assert_eq!(index.strip_prefix("auction/"), None);
        assert_eq!(index.strip_prefix("auctions_by_host/"), Some("peer-x"));

        let region = region_providers_key("us-west");
        assert_eq!(region.strip_prefix("auction/"), None);
        assert_eq!(region.strip_prefix("auctions/"), Some("us-west"));

        assert_eq!(award_key("c1", "w1").strip_prefix("award/"), Some("c1/w1"));
    }

    #[test]
    fn auction_index_round_trip_and_caps() {
        let ids: Vec<String> = (0..3).map(|i| format!("auc-{i}")).collect();
        let bytes = encode_auction_index(&ids).expect("encode");
        assert_eq!(decode_auction_index(&bytes).expect("decode"), ids);

        // Encode refuses oversized lists.
        let too_many: Vec<String> = (0..MAX_AUCTION_INDEX_IDS + 1)
            .map(|i| format!("a{i}"))
            .collect();
        assert!(encode_auction_index(&too_many).is_err());

        // Decode dedupes, then caps at the decode ceiling.
        let dup = serde_json::to_vec(&["auc-1", "auc-1", "auc-2"]).unwrap();
        assert_eq!(
            decode_auction_index(&dup).unwrap(),
            vec!["auc-1".to_string(), "auc-2".to_string()]
        );
        let big: Vec<String> = (0..200).map(|i| format!("auc-{i}")).collect();
        assert_eq!(
            decode_auction_index(&serde_json::to_vec(&big).unwrap())
                .unwrap()
                .len(),
            MAX_AUCTION_INDEX_IDS_DECODE
        );

        // Bad ids and non-arrays are rejected outright.
        let bad = serde_json::to_vec(&["auc/../x"]).unwrap();
        assert!(decode_auction_index(&bad).is_err());
        assert!(decode_auction_index(b"{\"not\":\"an array\"}").is_err());
        assert!(decode_auction_index(&vec![b'x'; 5000]).is_err());
    }

    #[test]
    fn auction_book_upsert_and_prune() {
        let book = AuctionBook::new();
        let now = 1_000;

        assert!(book.upsert(sample_record("auc-1", now + 30), "peer-me", now));
        assert!(book.contains("auc-1"));
        assert_eq!(book.len(), 1);

        // Our own record is never imported.
        let mut own = sample_record("auc-own", now + 30);
        own.requester_peer_id = "peer-me".into();
        assert!(!book.upsert(own, "peer-me", now));

        // Expired records are dropped.
        assert!(!book.upsert(sample_record("auc-old", now), "peer-me", now));
        assert!(!book.contains("auc-old"));

        // Invalid records are rejected *and* remembered on cooldown.
        let mut junk = sample_record("auc-junk", now + 30);
        junk.host_api_addr = "nope".into();
        assert!(!book.upsert(junk, "peer-me", now));
        assert!(book.recently_rejected("auc-junk", now));
        // Cooldown expires after REJECTED_COOLDOWN_SECS.
        assert!(!book.recently_rejected("auc-junk", now + REJECTED_COOLDOWN_SECS));

        // open() hides expired records; prune() removes them.
        assert!(book.upsert(sample_record("auc-2", now + 10), "peer-me", now));
        let open = book.open(now);
        assert_eq!(open.len(), 2);
        assert_eq!(open[0].id, "auc-2"); // soonest deadline first
        assert_eq!(book.prune(now + 20), 1);
        assert_eq!(book.len(), 1);
    }

    #[test]
    fn auction_book_capacity_evicts_earliest_deadline() {
        let book = AuctionBook::new();
        let now = 1_000;
        // Fill past the cap with increasing deadlines: 100..=355.
        for i in 0..(MAX_FOREIGN_AUCTIONS + 100) {
            let closes = now + 100 + i as u64;
            book.upsert(sample_record(&format!("auc-{i}"), closes), "peer-me", now);
        }
        assert_eq!(book.len(), MAX_FOREIGN_AUCTIONS);
        // The earliest-deadline records were evicted to make room.
        assert!(!book.contains("auc-0"));
        assert!(book.contains(&format!("auc-{}", MAX_FOREIGN_AUCTIONS + 99)));
    }

    #[test]
    fn signed_bid_round_trip() {
        let (sk, peer) = bidder();
        let sig = sign_bid(&sk, "auc-1", 0.42);
        let derived = verify_signed_bid(
            "auc-1",
            0.42,
            &peer,
            &base64_encode(&sk.verifying_key().to_bytes()),
            &sig,
        )
        .expect("valid signature");
        assert_eq!(derived, peer);
    }

    #[test]
    fn signed_bid_rejects_tampered_price() {
        let (sk, peer) = bidder();
        let sig = sign_bid(&sk, "auc-1", 0.42);
        let pk = base64_encode(&sk.verifying_key().to_bytes());
        let err = verify_signed_bid("auc-1", 0.43, &peer, &pk, &sig).unwrap_err();
        assert!(matches!(err, BidError::Invalid(_)));
        // …and a tampered auction id.
        assert!(verify_signed_bid("auc-2", 0.42, &peer, &pk, &sig).is_err());
    }

    #[test]
    fn signed_bid_rejects_wrong_or_missing_identity() {
        let (sk, _peer) = bidder();
        let (_sk2, other) = bidder();
        let sig = sign_bid(&sk, "auc-1", 0.5);
        let pk = base64_encode(&sk.verifying_key().to_bytes());
        // Claimed peer id doesn't match the signing key.
        let err = verify_signed_bid("auc-1", 0.5, &other, &pk, &sig).unwrap_err();
        match err {
            BidError::Invalid(msg) => assert!(msg.contains("peer id mismatch"), "msg: {msg}"),
            other => panic!("unexpected error: {other:?}"),
        }
        // Malformed key / signature lengths.
        assert!(verify_signed_bid("auc-1", 0.5, &other, "not-base64!", &sig).is_err());
        assert!(verify_signed_bid("auc-1", 0.5, &other, &base64_encode(&[0u8; 31]), &sig).is_err());
        assert!(verify_signed_bid("auc-1", 0.5, &other, &pk, &base64_encode(&[0u8; 63])).is_err());
        // Wrong (valid) key: signature won't verify under it.
        let (sk2, peer2) = bidder();
        let pk2 = base64_encode(&sk2.verifying_key().to_bytes());
        assert!(verify_signed_bid("auc-1", 0.5, &peer2, &pk2, &sig).is_err());
    }

    #[test]
    fn import_foreign_award_validation_and_dedupe() {
        let eng = engine();
        let award = |winner: &str, price: f64| Award {
            auction_id: "auc-x".into(),
            winning_bid_id: "bid-1".into(),
            provider_peer_id: winner.into(),
            requester_peer_id: "peer-consumer".into(),
            price_usd_per_gb: price,
            winning_bid_price_usd_per_gb: price,
            runner_up_price_usd_per_gb: None,
            awarded_at: 1_000,
        };

        // Not our win → ignored.
        assert!(!eng.import_foreign_award(award("peer-other", 0.4), "peer-me"));
        // Out-of-range locked price → ignored.
        assert!(!eng.import_foreign_award(award("peer-me", 0.0), "peer-me"));
        assert!(!eng.import_foreign_award(award("peer-me", f64::INFINITY), "peer-me"));
        // Empty requester → ignored.
        let mut empty = award("peer-me", 0.4);
        empty.requester_peer_id = String::new();
        assert!(!eng.import_foreign_award(empty, "peer-me"));
        // Happy path → visible to settlement pricing.
        assert!(eng.import_foreign_award(award("peer-me", 0.4), "peer-me"));
        assert_eq!(eng.awarded_price_for("peer-consumer"), Some(0.4));
        // Duplicate auction id → exactly once.
        assert!(!eng.import_foreign_award(award("peer-me", 0.4), "peer-me"));
        assert_eq!(eng.recent_awards(8).len(), 1);
    }

    #[test]
    fn awards_from_cursor_streaming() {
        let eng = engine();
        assert_eq!(eng.awards_from(0), (Vec::new(), 0));

        let req = create_req(1.0, 30);
        let auction = eng.create_auction(req, "peer-consumer", 1_000).unwrap();
        eng.submit_bid(&auction.id, "peer-me", 0.5, 0.5, 50.0, 1_001)
            .unwrap();
        eng.close_auction(&auction.id, "peer-consumer", 1_002)
            .unwrap();

        let (batch, cursor) = eng.awards_from(0);
        assert_eq!(batch.len(), 1);
        assert_eq!(cursor, 1);
        // Nothing new since the cursor.
        assert_eq!(eng.awards_from(cursor), (Vec::new(), 1));
        // A past-the-end cursor self-heals back to the log length.
        assert_eq!(eng.awards_from(99), (Vec::new(), 1));
    }
}
