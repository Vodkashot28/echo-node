use anyhow::{bail, Context, Result};
use futures::StreamExt;
use libp2p::{
    identify, kad,
    multiaddr::Protocol,
    noise,
    swarm::{NetworkBehaviour, StreamProtocol, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, SwarmBuilder,
};
use std::time::Duration;
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
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub listen_addr: Multiaddr,
    pub bootstrap: Vec<(PeerId, Multiaddr)>,
    /// Protocol namespace, e.g. `echo` → `/echo/kad/1.0.0`.
    pub network_id: String,
    /// When true, never contacts nodes outside this host (dev/LAN mode).
    pub local_only: bool,
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
                        Err(e) => warn!(peer = entry, error = %e, "skipping invalid bootstrap peer"),
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
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);

        Self {
            listen_addr,
            bootstrap,
            network_id,
            local_only,
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
}

#[derive(Debug)]
pub enum DiscoveryEvent {
    PeerFound(PeerId, Multiaddr),
    PeerDisconnected(PeerId),
    CapabilityPublished,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
}

impl DiscoveryService {
    pub async fn new(
        identity: &NodeIdentity,
        config: &DiscoveryConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<DiscoveryEvent>)> {
        let keypair = identity.libp2p_keypair()?;

        // Namespace every protocol by network id so isolated deployments
        // (dev LAN, private testnets) never talk to the public network.
        let kad_protocol = StreamProtocol::try_from_owned(format!(
            "/{}/kad/1.0.0",
            config.network_id
        ))
        .context("DHT_NETWORK_ID produces an invalid Kademlia protocol name")?;
        let identify_protocol = format!("/{}/id/1.0.0", config.network_id);

        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|key| {
                let peer_id = key.public().to_peer_id();

                let identify = identify::Behaviour::new(identify::Config::new(
                    identify_protocol,
                    key.public().clone(),
                ));

                let store = kad::store::MemoryStore::new(peer_id);
                let kademlia =
                    kad::Behaviour::with_config(peer_id, store, kad::Config::new(kad_protocol));

                Ok(Behaviour {
                    identify,
                    kademlia,
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
                    if let (Ok(addr), Ok(peer_id)) = (
                        addr_str.parse::<Multiaddr>(),
                        peer_id_str.parse::<PeerId>(),
                    ) {
                        swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, addr);
                        debug!(peer_id = %peer_id, addr = %addr_str, "default bootstrap peer added");
                    }
                }

                info!("bootstrapping DHT with default public nodes (DNS + IP fallbacks)");
            }

            // Trigger DHT bootstrap
            swarm.behaviour_mut().kademlia.bootstrap().ok();
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Ok((
            Self {
                swarm,
                event_tx,
            },
            event_rx,
        ))
    }

    /// Run the discovery event loop
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
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
                                    kad::GetProvidersOk::FoundProviders { providers, .. },
                                )) => {
                                    debug!(providers_count = providers.len(), "providers found");
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
                                }
                                kad::QueryResult::GetProviders(Err(e)) => {
                                    warn!(error = %e, "get providers failed");
                                }
                                kad::QueryResult::PutRecord(Ok(_)) => {
                                    info!("record published to DHT");
                                    let _ = self.event_tx.send(DiscoveryEvent::CapabilityPublished);
                                }
                                kad::QueryResult::PutRecord(Err(e)) => {
                                    error!(error = %e, "put record failed");
                                }
                                _ => {}
                            }
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            debug!(peer_id = %peer_id, "connection established");
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            debug!(peer_id = %peer_id, "connection closed");
                            let _ = self.event_tx.send(DiscoveryEvent::PeerDisconnected(peer_id));
                        }
                        _ => {}
                    }
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
        let entry = "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN";
        let (peer, addr) = parse_bootstrap_peer(entry).unwrap();
        assert_eq!(
            addr.to_string(),
            "/ip4/127.0.0.1/tcp/4001"
        );
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
        for key in ["DHT_LISTEN_ADDR", "DHT_BOOTSTRAP_PEERS", "DHT_NETWORK_ID", "DHT_LOCAL_ONLY"] {
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
        std::env::set_var("DHT_BOOTSTRAP_PEERS", format!("{good_peer}, /ip4/bad tcp/nope"));
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

        for key in ["DHT_LISTEN_ADDR", "DHT_BOOTSTRAP_PEERS", "DHT_NETWORK_ID", "DHT_LOCAL_ONLY"] {
            std::env::remove_var(key);
        }
    }
}
