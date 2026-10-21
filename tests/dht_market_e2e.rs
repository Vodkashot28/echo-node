//! Two-swarm end-to-end test for G6 cross-node marketplace propagation.
//!
//! Two in-process Kademlia swarms on loopback (unique network-id per run)
//! exercise the real wire path a consumer host and a provider traverse:
//!
//! ```text
//! A (consumer host)                    DHT                     B (provider)
//!   StartProviding(auctions/<region>)  ◄── region host discovery ──┐
//!   PutRecord(auctions_by_host/<A>)    ◄── host index            ──┤
//!   PutRecord(auction/<id>)            ◄── auction record        ──┤
//!   PutRecord(award/<A>/<B>)            ◄── locked-price import   ──┘
//! ```
//!
//! Value lookups are re-issued on timeout (Kademlia queries are async; a
//! 15 s retry bound keeps slow CI from flaking). `start_providing` has no
//! ack event — the provider lookup's own retries cover it.

use echo_daemon::bidding::{
    auction_key, award_key, decode_auction_index, encode_auction_index, host_index_key,
    region_providers_key, AuctionBook, AuctionRecord, Award, BidConfig, BidEngine, CreateAuction,
};
use echo_daemon::discovery::{DiscoveryCommand, DiscoveryConfig, DiscoveryEvent, DiscoveryService};
use echo_daemon::identity::NodeIdentity;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

type Cmd = UnboundedSender<DiscoveryCommand>;
type EvtRx = UnboundedReceiver<DiscoveryEvent>;

/// Bind an ephemeral loopback port and release it for the swarm to use
/// (the standard free-port pattern for in-process network tests).
fn free_loopback_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral loopback port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

struct Pair {
    a_peer: String,
    a_cmd: Cmd,
    a_rx: EvtRx,
    b_peer: String,
    b_cmd: Cmd,
    b_rx: EvtRx,
}

/// Bring up two swarms: A local-only on a fixed loopback port, B booting
/// from A. `tag` namespaces the Kademlia protocol so parallel tests (same
/// process, same pid) can never cross-talk.
async fn swarm_pair(tag: &str) -> Pair {
    // Visible under `cargo test -- --nocapture` + RUST_LOG; try_init so
    // parallel tests sharing the process tolerate the second call.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
    let net = format!("dhtmarket-{}-{}", std::process::id(), tag);
    let dir_a = tempfile::tempdir().expect("tempdir A");
    let dir_b = tempfile::tempdir().expect("tempdir B");
    let id_a = NodeIdentity::load_or_generate(&dir_a.path().join("id.json"), "us-west")
        .expect("identity A");
    let id_b = NodeIdentity::load_or_generate(&dir_b.path().join("id.json"), "us-west")
        .expect("identity B");

    let port_a = free_loopback_port();
    let listen_a: libp2p::Multiaddr = format!("/ip4/127.0.0.1/tcp/{port_a}")
        .parse()
        .expect("listen addr A");
    let peer_a: libp2p::PeerId = id_a.peer_id_str().parse().expect("peer id A");

    let (mut svc_a, a_cmd, a_rx) = DiscoveryService::new(
        &id_a,
        &DiscoveryConfig {
            listen_addr: listen_a.clone(),
            bootstrap: Vec::new(),
            network_id: net.clone(),
            local_only: true,
            ..DiscoveryConfig::default()
        },
    )
    .await
    .expect("discovery A");
    let (mut svc_b, b_cmd, b_rx) = DiscoveryService::new(
        &id_b,
        &DiscoveryConfig {
            listen_addr: "/ip4/127.0.0.1/tcp/0".parse().expect("listen addr B"),
            bootstrap: vec![(peer_a, listen_a)],
            network_id: net,
            local_only: false,
            ..DiscoveryConfig::default()
        },
    )
    .await
    .expect("discovery B");

    tokio::spawn(async move {
        svc_a.run().await;
    });
    tokio::spawn(async move {
        svc_b.run().await;
    });
    // Let B's bootstrap + identify handshake establish the first edge
    // (Kademlia quorums need at least one known peer).
    tokio::time::sleep(Duration::from_millis(750)).await;

    Pair {
        a_peer: id_a.peer_id_str().to_string(),
        a_cmd,
        a_rx,
        b_peer: id_b.peer_id_str().to_string(),
        b_cmd,
        b_rx,
    }
}

/// Send `make_cmd` immediately and once per second until an event passes
/// `pred` (or panic after 15 s). Unrelated events are drained.
async fn expect_event<T, F>(
    cmd: &Cmd,
    rx: &mut EvtRx,
    make_cmd: impl Fn() -> DiscoveryCommand,
    mut pred: F,
) -> T
where
    F: FnMut(DiscoveryEvent) -> Option<T>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut retry = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = retry.tick() => {
                let _ = cmd.send(make_cmd());
            }
            ev = rx.recv() => {
                if let Some(ev) = ev {
                    if let Some(out) = pred(ev) {
                        return out;
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for expected DHT event"
        );
    }
}

fn test_auction_request(region: &str) -> CreateAuction {
    CreateAuction {
        region: region.to_string(),
        min_upload_mbps: 10.0,
        min_download_mbps: 50.0,
        max_price_usd_per_gb: 1.0,
        window_secs: Some(30),
    }
}

/// A (consumer host) publishes a region announce + host index + auction
/// record; B discovers the region host, walks the index, fetches the
/// record, and imports it into its foreign auction book — the exact
/// chain the production event handler performs.
#[tokio::test]
async fn auction_record_propagates_between_two_swarms() {
    let mut pair = swarm_pair("auction").await;
    let region = "dht-test-region";
    let ttl = Some(Duration::from_secs(45));

    // A hosts an auction (the consumer side).
    let engine_a = BidEngine::new(BidConfig::default());
    let auction = engine_a
        .create_auction(test_auction_request(region), &pair.a_peer, now_secs())
        .expect("create auction");
    let record = AuctionRecord::from_auction(&auction, "http://127.0.0.1:3999");
    let record_bytes = record.to_bytes().expect("encode record");
    let index_bytes =
        encode_auction_index(std::slice::from_ref(&auction.id)).expect("encode index");

    // Publish the host index; wait until the put is accepted (this also
    // proves A has a quorum peer — B's bootstrap edge).
    let index_key = host_index_key(&pair.a_peer);
    let idx_key_for_pred = index_key.clone();
    expect_event(
        &pair.a_cmd,
        &mut pair.a_rx,
        move || DiscoveryCommand::PutRecord {
            key: index_key.clone(),
            value: index_bytes.clone(),
            ttl,
        },
        move |ev| match ev {
            DiscoveryEvent::RecordPublished { key } if key == idx_key_for_pred => Some(()),
            _ => None,
        },
    )
    .await;

    // Publish the auction record itself.
    let rec_key = auction_key(&auction.id);
    let rec_key_for_pred = rec_key.clone();
    expect_event(
        &pair.a_cmd,
        &mut pair.a_rx,
        move || DiscoveryCommand::PutRecord {
            key: rec_key.clone(),
            value: record_bytes.clone(),
            ttl,
        },
        move |ev| match ev {
            DiscoveryEvent::RecordPublished { key } if key == rec_key_for_pred => Some(()),
            _ => None,
        },
    )
    .await;

    // Announce A as a host for this region (no ack event; B's provider
    // lookup retries until it lands).
    let _ = pair.a_cmd.send(DiscoveryCommand::StartProviding {
        key: region_providers_key(region),
    });

    // B: who hosts auctions in this region? (must include A)
    let a_peer = pair.a_peer.clone();
    let region_key = region_providers_key(region);
    let region_key_for_pred = region_key.clone();
    expect_event(
        &pair.b_cmd,
        &mut pair.b_rx,
        move || DiscoveryCommand::GetProviders {
            key: region_key.clone(),
        },
        move |ev| match ev {
            DiscoveryEvent::ProvidersFound { key, providers }
                if key == region_key_for_pred
                    && providers.iter().any(|p| p.to_string() == a_peer) =>
            {
                Some(())
            }
            _ => None,
        },
    )
    .await;

    // B: fetch A's host index → auction ids.
    let index_key_b = host_index_key(&pair.a_peer);
    let idx_pred = index_key_b.clone();
    let ids = expect_event(
        &pair.b_cmd,
        &mut pair.b_rx,
        move || DiscoveryCommand::GetRecord {
            key: index_key_b.clone(),
        },
        move |ev| match ev {
            DiscoveryEvent::RecordFound { key, value } if key == idx_pred => {
                decode_auction_index(&value).ok()
            }
            _ => None,
        },
    )
    .await;
    assert_eq!(ids, vec![auction.id.clone()]);

    // B: fetch the auction record itself and parse it off the wire.
    let rec_key_b = auction_key(&auction.id);
    let rec_pred = rec_key_b.clone();
    let got = expect_event(
        &pair.b_cmd,
        &mut pair.b_rx,
        move || DiscoveryCommand::GetRecord {
            key: rec_key_b.clone(),
        },
        move |ev| match ev {
            DiscoveryEvent::RecordFound { key, value } if key == rec_pred => {
                AuctionRecord::from_bytes(&value).ok()
            }
            _ => None,
        },
    )
    .await;
    assert_eq!(got, record);

    // B: import into the book — the production event-handler step.
    let book = AuctionBook::new();
    assert!(book.upsert(got, &pair.b_peer, now_secs()));
    assert_eq!(book.len(), 1);
    let open = book.open(now_secs());
    assert_eq!(open[0].id, auction.id);
    assert_eq!(open[0].host_api_addr, "http://127.0.0.1:3999");
}

/// A (consumer host) finalizes an auction won by B and publishes the
/// award record; B fetches it and imports it — unlocking locked-price
/// settlement for that consumer's receipts (the settlement task's DHT
/// lookup path), including the wrong-winner / duplicate guards.
#[tokio::test]
async fn award_record_reaches_the_winning_provider() {
    let mut pair = swarm_pair("award").await;
    let ttl = Some(Duration::from_secs(45));

    // A runs an auction that B wins with a sealed bid.
    let engine_a = BidEngine::new(BidConfig::default());
    let now = now_secs();
    let auction = engine_a
        .create_auction(test_auction_request("us-west"), &pair.a_peer, now)
        .expect("create auction");
    engine_a
        .submit_bid(&auction.id, &pair.b_peer, 0.42, 0.5, 50.0, now + 1)
        .expect("sealed bid");
    engine_a
        .close_auction(&auction.id, &pair.a_peer, now + 2)
        .expect("close auction");
    let award = engine_a
        .recent_awards(1)
        .pop()
        .expect("award recorded on A");
    assert_eq!(award.price_usd_per_gb, 0.42); // sole bidder → own price

    // A publishes the award under the consumer→winner key.
    let key = award_key(&award.requester_peer_id, &award.provider_peer_id);
    let value = serde_json::to_vec(&award).expect("encode award");
    let key_for_pred = key.clone();
    expect_event(
        &pair.a_cmd,
        &mut pair.a_rx,
        move || DiscoveryCommand::PutRecord {
            key: key.clone(),
            value: value.clone(),
            ttl,
        },
        move |ev| match ev {
            DiscoveryEvent::RecordPublished { key } if key == key_for_pred => Some(()),
            _ => None,
        },
    )
    .await;

    // B: the settlement path fetches `award/<counterparty>/<self>`.
    let key_b = award_key(&award.requester_peer_id, &award.provider_peer_id);
    let pred_key = key_b.clone();
    let got: Award = expect_event(
        &pair.b_cmd,
        &mut pair.b_rx,
        move || DiscoveryCommand::GetRecord { key: key_b.clone() },
        move |ev| match ev {
            DiscoveryEvent::RecordFound { key, value } if key == pred_key => {
                serde_json::from_slice::<Award>(&value).ok()
            }
            _ => None,
        },
    )
    .await;
    assert_eq!(got, award);

    // B: import → settlement now prices that consumer's receipts at 0.42.
    let engine_b = BidEngine::new(BidConfig::default());
    assert!(engine_b.import_foreign_award(got.clone(), &pair.b_peer));
    assert_eq!(engine_b.awarded_price_for(&pair.a_peer), Some(0.42));
    // Duplicate import is a no-op.
    assert!(!engine_b.import_foreign_award(got.clone(), &pair.b_peer));
    // An award naming a different winner is ignored entirely.
    let mut foreign = got;
    foreign.provider_peer_id = "12D3KooWSomeOtherProvider".to_string();
    assert!(!engine_b.import_foreign_award(foreign, &pair.b_peer));
    assert_eq!(engine_b.awarded_price_for(&pair.a_peer), Some(0.42));
}
