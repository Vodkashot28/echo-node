use anyhow::Result;
use futures::StreamExt;
use libp2p::{
    identify, kad,
    noise,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, SwarmBuilder,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::identity::NodeIdentity;

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
        listen_addr: Multiaddr,
        bootstrap_peers: Vec<(PeerId, Multiaddr)>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<DiscoveryEvent>)> {
        let keypair = identity.libp2p_keypair()?;

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
                    "echo-dht/0.3.0".into(),
                    key.public().clone(),
                ));

                let store = kad::store::MemoryStore::new(peer_id);
                let kademlia =
                    kad::Behaviour::new(peer_id, store);

                Ok(Behaviour {
                    identify,
                    kademlia,
                })
            })?
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        swarm.listen_on(listen_addr)?;

        // Bootstrap DHT with known peers
        for (peer_id, addr) in &bootstrap_peers {
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(peer_id, addr.clone());
            info!(peer_id = %peer_id, addr = %addr, "bootstrap peer added");
        }

        // If no custom bootstrap peers were provided, use the well-known
        // public libp2p bootstrap nodes so the DHT can discover other peers.
        if bootstrap_peers.is_empty() {
            let default_bootstrap: Vec<(&str, &str)> = vec![
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
                    debug!(peer_id = %peer_id, "default bootstrap peer added");
                }
            }

            info!("bootstrapping DHT with default public nodes");
        }

        // Trigger DHT bootstrap
        swarm.behaviour_mut().kademlia.bootstrap().ok();

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
                                    for provider in &providers {
                                        // Note: libp2p-kad 0.46 does not expose
                                        // `addresses_of_peer` on the Behaviour struct.
                                        // The address is empty here; actual connectivity
                                        // is resolved through the identify protocol and
                                        // the swarm's peer store when a connection is
                                        // initiated.  This event is informational only.
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
