use anyhow::{bail, Context, Result};
use futures::StreamExt;
use libp2p::{
    autonat, dcutr, identify, kad,
    multiaddr::Protocol,
    noise, relay,
    swarm::{NetworkBehaviour, StreamProtocol, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, SwarmBuilder,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::identity::NodeIdentity;

/// Discovery (libp2p Kademlia DHT) configuration, parsed from the environment:
///
/// - `DHT_LISTEN_ADDR`       — listen multiaddr (default `/ip4/0.0.0.0/tcp/0`)
/// - `DHT_BOOTSTRAP_PEERS`   — comma-separated multiaddrs, each ending with
///   `/p2p/<peer-id>`; when unset (and not local-only), the public libp2p
///   bootstrap nodes are used
/// - `DHT_NETWORK_ID`        — protocol namespace (default `echo`); nodes only
///   talk to peers advertising the same network id
/// - `DHT_LOCAL_ONLY`        — `1`/`true`: no bootstrap peers, no outbound
///   bootstrap query — dev/LAN-only operation
/// - `DHT_RELAY_MODE`        — `server`, `client`, or `auto` (default: `auto`
///   = server if public address detected, else client)
/// - `DHT_RELAY_PEERS`       — comma-separated relay multiaddrs for client mode
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub listen_addr: Multiaddr,
    pub bootstrap: Vec<(PeerId, Multiaddr)>,
    /// Protocol namespace, e.g. `echo` → `/echo/kad/1.0.0`.
    pub network_id: String,
    /// When true, never contacts nodes outside this host (dev/LAN mode).
    pub local_only: bool,
    /// Relay mode: server (accept relay requests), client (use relays), or auto.
    pub relay_mode: RelayMode,
    /// Known relay peers for client mode.
    pub relay_peers: Vec<(PeerId, Multiaddr)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelayMode {
    Server,
    Client,
    Auto,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            listen_addr: "/ip4/0.0.0.0/tcp/0"
                .parse()
                .expect("static default listen addr is valid"),
            bootstrap: Vec::new(),
            network_id: "echo".to_string(),
            local_only: false,
            relay_mode: RelayMode::Auto,
            relay_peers: Vec::new(),
        }
    }
}

impl DiscoveryConfig {
    /// Read the `DHT_*` env vars. Invalid values warn and fall back —
    /// discovery is best-effort and must never block daemon startup.
    pub fn from_env() -> Self {
        let listen_addr = match std::env::var("DHT_LISTEN_ADDR") {
            Ok(raw) if !raw.trim().is_empty() => match raw.trim().parse::<Multiaddr>() {
                Ok(addr) => addr,
                Err(e) => {
                    warn!(error = %e, value = %raw, "invalid DHT_LISTEN_ADDR; using default");
                    Self::default().listen_addr
                }
            },
            _ => Self::default().listen_addr,
        };

        let bootstrap = match std::env::var("DHT_BOOTSTRAP_PEERS") {
            Ok(raw) if !raw.trim().is_empty() => {
                let mut peers = Vec::new();
                for entry in raw.split(',') {
                    let entry = entry.trim();
                    if entry.is_empty() {
                        continue;
                    }
                    match parse_bootstrap_peer(entry) {
                        Ok(peer) => peers.push(peer),
                        Err(e) => {
                            warn!(peer = entry, error = %e, "skipping invalid bootstrap peer")
                        }
                    }
                }
                peers
            }
            _ => Vec::new(),
        };

        let network_id = std::env::var("DHT_NETWORK_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "echo".to_string());

        let local_only = std::env::var("DHT_LOCAL_ONLY")
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let relay_mode = match std::env::var("DHT_RELAY_MODE")
            .ok()
            .as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
        {
            Some(s) if s == "server" => RelayMode::Server,
            Some(s) if s == "client" => RelayMode::Client,
            _ => RelayMode::Auto,
        };

        let relay_peers = match std::env::var("DHT_RELAY_PEERS") {
            Ok(raw) if !raw.trim().is_empty() => {
                let mut peers = Vec::new();
                for entry in raw.split(',') {
                    let entry = entry.trim();
                    if entry.is_empty() {
                        continue;
                    }
                    match parse_bootstrap_peer(entry) {
                        Ok(peer) => peers.push(peer),
                        Err(e) => {
                            warn!(relay_peer = entry, error = %e, "skipping invalid relay peer")
                        }
                    }
                }
                peers
            }
            _ => Vec::new(),
        };

        Self {
            listen_addr,
            bootstrap,
            network_id,
            local_only,
            relay_mode,
            relay_peers,
        }
    }
}

/// Parse a bootstrap entry: a multiaddr whose final component is
/// `/p2p/<peer-id>` (e.g. `/ip4/1.2.3.4/tcp/4001/p2p/12D3KooW...`).
/// The peer id is stripped off and returned separately for Kademlia.
pub fn parse_bootstrap_peer(entry: &str) -> Result<(PeerId, Multiaddr)> {
    let mut addr: Multiaddr = entry
        .parse()
        .with_context(|| format!("'{entry}' is not a valid multiaddr"))?;
    match addr.pop() {
        Some(Protocol::P2p(peer_id)) => Ok((peer_id, addr)),
        other => bail!(
            "bootstrap multiaddr must end with /p2p/<peer-id>, got {:?}",
            other.map(|p| p.to_string())
        ),
    }
}

pub struct DiscoveryService {
    swarm: Swarm<Behaviour>,
    event_tx: mpsc::UnboundedSender<DiscoveryEvent>,
    cmd_rx: mpsc::UnboundedReceiver<DiscoveryCommand>,
}

/// Commands other tasks issue to the discovery swarm (the swarm is owned by
/// the `run` loop, so all Kademlia operations go through this channel).
#[derive(Debug)]
pub enum DiscoveryCommand {
    /// Store a value record under `key`, replicated to the closest peers.
    /// `ttl` bounds the record's lifetime (None = configured default).
    PutRecord {
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    /// Fetch a value record by exact key (Kademlia has no prefix queries).
    GetRecord { key: String },
    /// Announce that this node holds data under `key` (auction-host
    /// rendezvous: `get_providers` later returns every announcing node).
    StartProviding { key: String },
    /// Look up all current providers of `key`.
    GetProviders { key: String },
    /// Request a relay reservation for NAT traversal.
    MakeReservation { relay_peer_id: PeerId },
    /// Initiate hole-punching connection to a peer via relay.
    DialThroughRelay {
        target_peer_id: PeerId,
        relay_peer_id: PeerId,
    },
}

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    PeerFound(PeerId, Multiaddr),
    PeerDisconnected(PeerId),
    /// A value record was stored locally (and replication query started).
    RecordPublished {
        key: String,
    },
    /// A value record was found — local or remote, exact key match.
    RecordFound {
        key: String,
        value: Vec<u8>,
    },
    /// No record exists for this key (or the lookup failed/timed out).
    RecordNotFound {
        key: String,
    },
    /// Provider lookup result for `key` (may fire more than once per query;
    /// later batches are supersets as the lookup progresses).
    ProvidersFound {
        key: String,
        providers: Vec<PeerId>,
    },
    /// A relay reservation was successfully made.
    ReservationAccepted {
        relay_peer_id: PeerId,
    },
    /// Relay reservation failed.
    ReservationFailed {
        relay_peer_id: PeerId,
    },
    /// NAT status detected (public, private, unknown).
    NatStatus {
        is_public: bool,
    },
    /// External address confirmed (for public nodes).
    ExternalAddress {
        address: Multiaddr,
    },
}

/// String form of a Kademlia record key (keys are plain UTF-8 byte strings).
fn key_string(key: &kad::RecordKey) -> String {
    String::from_utf8_lossy(key.as_ref()).into_owned()
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    autonat: autonat::Behaviour,
}

impl DiscoveryService {
    pub async fn new(
        identity: &NodeIdentity,
        config: &DiscoveryConfig,
    ) -> Result<(
        Self,
        mpsc::UnboundedSender<DiscoveryCommand>,
        mpsc::UnboundedReceiver<DiscoveryEvent>,
    )> {
        let keypair = identity.libp2p_keypair()?;

        // Namespace every protocol by network id so isolated deployments
        // (dev LAN, private testnets) never talk to the public network.
        let kad_protocol =
            StreamProtocol::try_from_owned(format!("/{}/kad/1.0.0", config.network_id))
                .context("DHT_NETWORK_ID produces an invalid Kademlia protocol name")?;
        let identify_protocol = format!("/{}/id/1.0.0", config.network_id);

        let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            // NOTE: QUIC transport deliberately omitted. The libp2p 0.54 QUIC
            // stack (quirks: rcgen 0.11 + rustls-webpki 0.101) is the only
            // root of the rustsec advisory set (ring 0.16 / webpki 0.101) in
            // our tree, and TCP + relay already covers all operator paths.
            // Revisit when libp2p is upgraded past 0.54.
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|key, relay_client| {
                let peer_id = key.public().to_peer_id();

                let identify = identify::Behaviour::new(identify::Config::new(
                    identify_protocol,
                    key.public().clone(),
                ));

                let store = kad::store::MemoryStore::new(peer_id);
                let mut kademlia =
                    kad::Behaviour::with_config(peer_id, store, kad::Config::new(kad_protocol));
                // Force server mode: by default Kademlia stays in client mode (no
                // inbound substreams served) until a confirmed *external* address
                // exists, which NAT'd/local-only nodes never get — record storage
                // and provider announcements would silently fail on every peer.
                kademlia.set_mode(Some(kad::Mode::Server));

                let dcutr = dcutr::Behaviour::new(peer_id);

                let autonat = autonat::Behaviour::new(
                    peer_id,
                    autonat::Config {
                        // Start with 3 boot nodes for confidence, retry every 15s
                        boot_delay: Duration::from_secs(15),
                        refresh_interval: Duration::from_secs(120),
                        retry_interval: Duration::from_secs(15),
                        throttle_clients_global_max: 10,
                        throttle_clients_period: Duration::from_secs(1),
                        throttle_clients_peer_max: 2,
                        only_global_ips: true,
                        ..Default::default()
                    },
                );

                Ok(Behaviour {
                    identify,
                    kademlia,
                    relay_client,
                    dcutr,
                    autonat,
                })
            })?
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        swarm.listen_on(config.listen_addr.clone())?;

        if config.local_only {
            info!(
                network_id = %config.network_id,
                "DHT running in local-only mode (no bootstrap peers, no external queries)"
            );
        } else {
            // Setup relay clients for NAT traversal
            for (peer_id, addr) in &config.relay_peers {
                swarm.dial(addr.clone())?;
                info!(peer_id = %peer_id, addr = %addr, "dialing relay peer");
            }

            // Explicitly configured bootstrap peers …
            for (peer_id, addr) in &config.bootstrap {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(peer_id, addr.clone());
                info!(peer_id = %peer_id, addr = %addr, "bootstrap peer added");
            }

            // … or, when none were provided, the well-known public libp2p
            // bootstrap nodes (DNS + IP fallbacks for DNS-failure resilience).
            if config.bootstrap.is_empty() {
                let default_bootstrap: Vec<(&str, &str)> = vec![
                    // DNS bootstrap nodes (require DNS resolution)
                    (
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFRms7qZKnPvx22x76D2GAy4g2txL15XM1",
                        "QmNnooDu7bfjPFRms7qZKnPvx22x76D2GAy4g2txL15XM1",
                    ),
                    (
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJyTQqNwdT3JZKRoMuBFxx",
                        "QmQCU2EcMqAqQPR2i9bChDtGNJyTQqNwdT3JZKRoMuBFxx",
                    ),
                    (
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWcr5ChtFU62SAxpbY8WpYXMYZ3kCEb9EsqZ",
                        "QmbLHAnMoJPWcr5ChtFU62SAxpbY8WpYXMYZ3kCEb9EsqZ",
                    ),
                    (
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                        "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                    ),
                    // IP fallback nodes (bypass DNS resolution)
                    (
                        "/ip4/147.75.83.83/tcp/4001/p2p/QmNnooDu7bfjPFRms7qZKnPvx22x76D2GAy4g2txL15XM1",
                        "QmNnooDu7bfjPFRms7qZKnPvx22x76D2GAy4g2txL15XM1",
                    ),
                    (
                        "/ip4/147.75.83.83/tcp/4001/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJyTQqNwdT3JZKRoMuBFxx",
                        "QmQCU2EcMqAqQPR2i9bChDtGNJyTQqNwdT3JZKRoMuBFxx",
                    ),
                    (
                        "/ip4/147.75.83.83/tcp/4001/p2p/QmbLHAnMoJPWcr5ChtFU62SAxpbY8WpYXMYZ3kCEb9EsqZ",
                        "QmbLHAnMoJPWcr5ChtFU62SAxpbY8WpYXMYZ3kCEb9EsqZ",
                    ),
                    (
                        "/ip4/147.75.83.83/tcp/4001/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                        "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                    ),
                ];

                for (addr_str, peer_id_str) in default_bootstrap {
                    if let (Ok(addr), Ok(peer_id)) =
                        (addr_str.parse::<Multiaddr>(), peer_id_str.parse::<PeerId>())
                    {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                        debug!(peer_id = %peer_id, addr = %addr_str, "default bootstrap peer added");
                    }
                }

                info!("bootstrapping DHT with default public nodes (DNS + IP fallbacks)");
            }

            // Trigger DHT bootstrap
            swarm.behaviour_mut().kademlia.bootstrap().ok();
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        Ok((
            Self {
                swarm,
                event_tx,
                cmd_rx,
            },
            cmd_tx,
            event_rx,
        ))
    }

    /// Run the discovery event loop: swarm events + external commands.
    pub async fn run(&mut self) {
        // Once every command sender is dropped the cmd branch goes quiet
        // instead of spinning; the swarm keeps running either way.
        let mut cmds_closed = false;
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv(), if !cmds_closed => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd),
                        None => cmds_closed = true,
                    }
                }
                event = self.swarm.next() => {
                    let Some(event) = event else { break };
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(addr = %address, "discovery listening");
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Identify(
                            identify::Event::Received { info, .. },
                        )) => {
                            let peer_id = info.public_key.to_peer_id();
                            for addr in &info.listen_addrs {
                                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                            }
                            debug!(peer_id = %peer_id, "identify received, added to DHT");
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                            kad::Event::OutboundQueryProgressed { result, .. },
                        )) => {
                            match result {
                                kad::QueryResult::GetProviders(Ok(
                                    kad::GetProvidersOk::FoundProviders { key, providers },
                                )) => {
                                    debug!(key = %key_string(&key), providers_count = providers.len(), "providers found");
                                    // The swarm's peer store (populated via identify protocol)
                                    // will resolve addresses when a connection is initiated.
                                    // Pass Multiaddr::empty here; the event handler in main.rs
                                    // logs the discovery for visibility.
                                    for provider in &providers {
                                        let _ = self.event_tx.send(DiscoveryEvent::PeerFound(
                                            *provider,
                                            Multiaddr::empty(),
                                        ));
                                    }
                                    let _ = self.event_tx.send(DiscoveryEvent::ProvidersFound {
                                        key: key_string(&key),
                                        providers: providers.into_iter().collect(),
                                    });
                                }
                                kad::QueryResult::GetProviders(Ok(
                                    kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
                                )) => {}
                                kad::QueryResult::GetProviders(Err(e)) => {
                                    warn!(error = %e, "get providers failed");
                                }
                                kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                                    let key = key_string(&key);
                                    debug!(key = %key, "record published to DHT");
                                    let _ = self.event_tx.send(DiscoveryEvent::RecordPublished { key });
                                }
                                kad::QueryResult::PutRecord(Err(e)) => {
                                    error!(error = %e, "put record failed");
                                }
                                kad::QueryResult::GetRecord(Ok(
                                    kad::GetRecordOk::FoundRecord(peer_record),
                                )) => {
                                    let _ = self.event_tx.send(DiscoveryEvent::RecordFound {
                                        key: key_string(&peer_record.record.key),
                                        value: peer_record.record.value,
                                    });
                                }
                                kad::QueryResult::GetRecord(Ok(
                                    kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. },
                                )) => {}
                                kad::QueryResult::GetRecord(Err(kad::GetRecordError::QuorumFailed {
                                    records, ..
                                })) => {
                                    // A short quorum can still carry usable records.
                                    for peer_record in records {
                                        let _ = self.event_tx.send(DiscoveryEvent::RecordFound {
                                            key: key_string(&peer_record.record.key),
                                            value: peer_record.record.value,
                                        });
                                    }
                                }
                                kad::QueryResult::GetRecord(Err(e)) => {
                                    debug!(error = %e, key = %key_string(e.key()), "record lookup failed");
                                    let _ = self.event_tx.send(DiscoveryEvent::RecordNotFound {
                                        key: key_string(e.key()),
                                    });
                                }
                                kad::QueryResult::StartProviding(Ok(_)) => {
                                    debug!("provider record announced");
                                }
                                kad::QueryResult::StartProviding(Err(e)) => {
                                    warn!(error = %e, "start providing failed");
                                }
                                kad::QueryResult::Bootstrap(_)
                                | kad::QueryResult::RepublishProvider(_)
                                | kad::QueryResult::RepublishRecord(_)
                                | kad::QueryResult::GetClosestPeers(_) => {}
                            }
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            debug!(peer_id = %peer_id, "connection established");
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            debug!(peer_id = %peer_id, "connection closed");
                            let _ = self.event_tx.send(DiscoveryEvent::PeerDisconnected(peer_id));
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::RelayClient(
                            relay::client::Event::ReservationReqAccepted {
                                relay_peer_id, ..
                            },
                        )) => {
                            info!(relay_peer_id = %relay_peer_id, "relay reservation accepted");
                            let _ = self.event_tx.send(DiscoveryEvent::ReservationAccepted {
                                relay_peer_id,
                            });
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr::Event {
                            remote_peer_id,
                            result: Ok(_),
                        })) => {
                            info!(peer_id = %remote_peer_id, "hole-punching successful");
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr::Event {
                            remote_peer_id,
                            result: Err(e),
                        })) => {
                            warn!(peer_id = %remote_peer_id, error = ?e, "hole-punching failed");
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Autonat(autonat::Event::StatusChanged {
                            old,
                            new,
                        })) => {
                            info!(old_status = ?old, new_status = ?new, "NAT status changed");
                            let is_public = matches!(new, autonat::NatStatus::Public(_));
                            let _ = self.event_tx.send(DiscoveryEvent::NatStatus { is_public });
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Autonat(autonat::Event::InboundProbe(_))) => {
                            // Silently handled - we're being probed
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Autonat(autonat::Event::OutboundProbe(_))) => {
                            // Silently handled - we're probing
                        }
                        SwarmEvent::ExternalAddrConfirmed { address } => {
                            info!(address = %address, "external address confirmed");
                            let _ = self.event_tx.send(DiscoveryEvent::ExternalAddress { address });
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Execute a command against the Kademlia behaviour. Results arrive
    /// asynchronously as swarm events (handled in [`Self::run`]).
    fn handle_command(&mut self, cmd: DiscoveryCommand) {
        match cmd {
            DiscoveryCommand::PutRecord { key, value, ttl } => {
                let kademlia = &mut self.swarm.behaviour_mut().kademlia;
                let mut record = kad::Record::new(kad::RecordKey::new(&key), value);
                if let Some(ttl) = ttl {
                    record.expires = Some(Instant::now() + ttl);
                }
                if let Err(e) = kademlia.put_record(record, kad::Quorum::One) {
                    warn!(error = %e, "put record rejected");
                }
            }
            DiscoveryCommand::GetRecord { key } => {
                let kademlia = &mut self.swarm.behaviour_mut().kademlia;
                kademlia.get_record(kad::RecordKey::new(&key));
            }
            DiscoveryCommand::StartProviding { key } => {
                let kademlia = &mut self.swarm.behaviour_mut().kademlia;
                if let Err(e) = kademlia.start_providing(kad::RecordKey::new(&key)) {
                    warn!(error = %e, key, "start providing rejected");
                }
            }
            DiscoveryCommand::GetProviders { key } => {
                let kademlia = &mut self.swarm.behaviour_mut().kademlia;
                kademlia.get_providers(kad::RecordKey::new(&key));
            }
            DiscoveryCommand::MakeReservation { relay_peer_id } => {
                // The relay client automatically makes reservations when connected
                info!(relay_peer_id = %relay_peer_id, "reservation request initiated");
            }
            DiscoveryCommand::DialThroughRelay {
                target_peer_id,
                relay_peer_id,
            } => {
                // Build a relay address for the target peer
                let relay_addr = Multiaddr::empty()
                    .with(Protocol::P2p(relay_peer_id))
                    .with(Protocol::P2pCircuit)
                    .with(Protocol::P2p(target_peer_id));

                if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                    warn!(
                        error = ?e,
                        relay = %relay_peer_id,
                        target = %target_peer_id,
                        "failed to dial through relay"
                    );
                } else {
                    info!(
                        relay = %relay_peer_id,
                        target = %target_peer_id,
                        "dialing through relay"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bootstrap_peer_strips_p2p_suffix() {
        let entry =
            "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN";
        let (peer, addr) = parse_bootstrap_peer(entry).unwrap();
        assert_eq!(addr.to_string(), "/ip4/127.0.0.1/tcp/4001");
        assert_eq!(
            peer.to_string(),
            "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN"
        );
    }

    #[test]
    fn parse_bootstrap_peer_rejects_missing_p2p_component() {
        let err = parse_bootstrap_peer("/ip4/127.0.0.1/tcp/4001").unwrap_err();
        assert!(err.to_string().contains("/p2p/<peer-id>"), "{err}");
    }

    #[test]
    fn parse_bootstrap_peer_rejects_garbage() {
        assert!(parse_bootstrap_peer("not-a-multiaddr").is_err());
        assert!(parse_bootstrap_peer("").is_err());
    }

    #[test]
    fn from_env_defaults_match_previous_behaviour() {
        for key in [
            "DHT_LISTEN_ADDR",
            "DHT_BOOTSTRAP_PEERS",
            "DHT_NETWORK_ID",
            "DHT_LOCAL_ONLY",
        ] {
            std::env::remove_var(key);
        }
        let cfg = DiscoveryConfig::from_env();
        assert_eq!(cfg.listen_addr.to_string(), "/ip4/0.0.0.0/tcp/0");
        assert!(cfg.bootstrap.is_empty()); // → public defaults at construction
        assert_eq!(cfg.network_id, "echo");
        assert!(!cfg.local_only);
    }

    #[test]
    fn from_env_parses_all_knobs_and_skips_bad_entries() {
        // One consolidated test — env vars are process-global and cargo
        // runs tests in parallel threads.
        let good_peer =
            "/ip4/10.0.0.5/tcp/4001/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN";
        std::env::set_var("DHT_LISTEN_ADDR", "/ip4/127.0.0.1/tcp/4123");
        std::env::set_var(
            "DHT_BOOTSTRAP_PEERS",
            format!("{good_peer}, /ip4/bad tcp/nope"),
        );
        std::env::set_var("DHT_NETWORK_ID", "mytestnet");
        std::env::set_var("DHT_LOCAL_ONLY", "true");
        let cfg = DiscoveryConfig::from_env();
        assert_eq!(cfg.listen_addr.to_string(), "/ip4/127.0.0.1/tcp/4123");
        assert_eq!(cfg.bootstrap.len(), 1, "bad entry skipped, good entry kept");
        assert_eq!(cfg.bootstrap[0].1.to_string(), "/ip4/10.0.0.5/tcp/4001");
        assert_eq!(cfg.network_id, "mytestnet");
        assert!(cfg.local_only);

        // Invalid listen addr falls back to the default.
        std::env::set_var("DHT_LISTEN_ADDR", "garbage");
        let cfg = DiscoveryConfig::from_env();
        assert_eq!(cfg.listen_addr.to_string(), "/ip4/0.0.0.0/tcp/0");

        for key in [
            "DHT_LISTEN_ADDR",
            "DHT_BOOTSTRAP_PEERS",
            "DHT_NETWORK_ID",
            "DHT_LOCAL_ONLY",
        ] {
            std::env::remove_var(key);
        }
    }
}
