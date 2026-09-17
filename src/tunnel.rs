use anyhow::{anyhow, bail, Context, Result};
use snow::{Builder, TransportState};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{error, info, warn};

use crate::availability::AvailabilityEngine;
use crate::meter::{MeteringEngine, TokenBucket};
use crate::{ConnectionRecord, MetricsBackend, PeerReputation};

use crate::NOISE_PARAMS;

/// Domain-separated prologue prefix for Noise_XX handshakes.
/// Prologue = "EchoMesh|v1|<session_id>"
pub const PROTOCOL_PROLOGUE_PREFIX: &[u8] = b"EchoMesh|v1|";

/// Maximum Noise message size (snow default).
const NOISE_MAX_MSG_LEN: usize = 65535;

// ──────────────────────────────────────────────────────────────
// Encrypted tunnel session — holds Noise transport + connection
// permit for the entire lifetime of the session.
// ──────────────────────────────────────────────────────────────

/// An established Noise_XX encrypted tunnel session.
///
/// The `_permit` field holds a `Semaphore` permit for the entire
/// lifetime of the session. When this struct is dropped (on teardown),
/// the permit is released, freeing a connection slot.
pub struct EncryptedTunnelSession {
    pub transport: Arc<tokio::sync::Mutex<TransportState>>,
    pub stream: TcpStream,
    pub session_id: String,
    pub remote_peer_id: libp2p::PeerId,
    _permit: OwnedSemaphorePermit,
}

// ──────────────────────────────────────────────────────────────
// Tunnel Service
// ──────────────────────────────────────────────────────────────

pub struct TunnelService {
    local_peer_id: libp2p::PeerId,
    event_tx: mpsc::UnboundedSender<TunnelEvent>,
    metering: MeteringEngine,
    availability: Arc<RwLock<AvailabilityEngine>>,
    relay_config: RelayConfig,
    /// Active relay sessions — keyed by session_id.
    active_sessions: Arc<RwLock<HashMap<String, ()>>>,
    /// Connection semaphore: limits concurrent tunnel sessions.
    conn_semaphore: Arc<Semaphore>,
    /// X25519 static private key for Noise_XX.
    static_private_key: Vec<u8>,
}

/// Shared state passed into the incoming connection handler so it can
/// perform relay, metering, DB writes, and session lifecycle tracking.
pub struct RelayConfig {
    pub backend: Arc<dyn MetricsBackend>,
    pub metering: MeteringEngine,
    pub availability: Arc<RwLock<AvailabilityEngine>>,
    pub target_addr: String,
    pub local_peer_id: String,
    /// Maximum upload bandwidth (Mbps) for token-bucket rate limiting.
    pub max_upload_mbps: f64,
    /// Maximum download bandwidth (Mbps) for token-bucket rate limiting.
    pub max_download_mbps: f64,
}

#[derive(Debug)]
pub enum TunnelEvent {
    SessionEstablished {
        session_id: String,
        remote_peer_id: libp2p::PeerId,
    },
    SessionClosed {
        session_id: String,
        bytes_sent: u64,
        bytes_received: u64,
    },
}

impl TunnelService {
    pub fn new(
        local_peer_id: libp2p::PeerId,
        metering: MeteringEngine,
        availability: Arc<RwLock<AvailabilityEngine>>,
        relay_config: RelayConfig,
        max_connections: usize,
        static_private_key: Vec<u8>,
    ) -> (Self, mpsc::UnboundedReceiver<TunnelEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        (
            Self {
                local_peer_id,
                event_tx,
                metering,
                availability,
                relay_config,
                active_sessions: Arc::new(RwLock::new(HashMap::new())),
                conn_semaphore: Arc::new(Semaphore::new(max_connections)),
                static_private_key,
            },
            event_rx,
        )
    }

    pub fn local_peer_id(&self) -> libp2p::PeerId {
        self.local_peer_id
    }

    /// Reference to the embedded metering engine.
    pub fn metering(&self) -> &MeteringEngine {
        &self.metering
    }

    /// Reference to the availability engine.
    pub fn availability(&self) -> &Arc<RwLock<AvailabilityEngine>> {
        &self.availability
    }

    /// Get a clone of the event sender for use by the incoming listener.
    pub fn event_tx(&self) -> &mpsc::UnboundedSender<TunnelEvent> {
        &self.event_tx
    }

    /// Get a reference to the relay config for passing to the incoming listener.
    pub fn relay_config(&self) -> &RelayConfig {
        &self.relay_config
    }

    /// Get a clone of the connection semaphore for passing to the incoming listener.
    pub fn conn_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.conn_semaphore)
    }

    /// Get a clone of the static private key for passing to the incoming listener.
    pub fn static_private_key(&self) -> Vec<u8> {
        self.static_private_key.clone()
    }

    // ──────────────────────────────────────────────────────────
    // Consumer-side: connect to a remote provider with Noise_XX
    // ──────────────────────────────────────────────────────────

    /// Establish an encrypted tunnel to a remote provider.
    ///
    /// Performs a full Noise_XX handshake with session_id binding
    /// in the prologue, and verifies the provider's static public
    /// key against the expected key from the discovery record.
    ///
    /// The returned `EncryptedTunnelSession` holds a semaphore
    /// permit for the entire lifetime of the session.
    pub async fn connect_to_provider(
        &self,
        remote_addr: &str,
        remote_peer_id: libp2p::PeerId,
        session_id: &str,
        expected_provider_noise_pubkey: &[u8],
    ) -> Result<EncryptedTunnelSession> {
        // 1. Acquire connection semaphore permit — stays bound to session
        let permit = Arc::clone(&self.conn_semaphore)
            .try_acquire_owned()
            .map_err(|_| anyhow!("Connection limit reached: semaphore exhausted"))?;

        info!(
            remote_addr = remote_addr,
            remote_peer = %remote_peer_id,
            session = session_id,
            "establishing encrypted tunnel connection"
        );

        // 2. TCP connect
        let mut stream = TcpStream::connect(remote_addr)
            .await
            .context("failed to connect to provider")?;

        // 3. Send session_id + local peer_id (plaintext, before Noise handshake)
        send_session_id(&mut stream, session_id, &self.local_peer_id.to_string()).await?;

        // 4. Construct domain-separated prologue: "EchoMesh|v1|<session_id>"
        let mut prologue = Vec::from(PROTOCOL_PROLOGUE_PREFIX);
        prologue.extend_from_slice(session_id.as_bytes());

        // 5. Initialize Noise_XX Initiator
        let params = NOISE_PARAMS
            .parse()
            .context("invalid noise params")?;
        let mut noise = Builder::new(params)
            .prologue(&prologue)
            .local_private_key(&self.static_private_key)
            .build_initiator()
            .context("failed to build noise initiator")?;

        let mut buf = [0u8; NOISE_MAX_MSG_LEN];

        // ── Noise Handshake ──

        // Msg 1 (Initiator → Responder): -> e
        let len = noise
            .write_message(&[], &mut buf)
            .context("noise msg1 write failed")?;
        send_wire_msg(&mut stream, &buf[..len]).await?;

        // Msg 2 (Responder → Initiator): <- e, ee, s, es
        let len = recv_wire_msg(&mut stream, &mut buf).await?;
        noise
            .read_message(&buf[..len], &mut [])
            .context("noise msg2 read failed")?;

        // 6. Cryptographic verification: check responder's static key
        let remote_static = noise
            .get_remote_static()
            .ok_or_else(|| anyhow!("responder failed to provide static public key"))?;

        if remote_static != expected_provider_noise_pubkey {
            bail!(
                "MITM detected: remote noise public key does not match discovery record \
                 (expected {} bytes, got {} bytes)",
                expected_provider_noise_pubkey.len(),
                remote_static.len()
            );
        }

        // Msg 3 (Initiator → Responder): -> s, se
        let len = noise
            .write_message(&[], &mut buf)
            .context("noise msg3 write failed")?;
        send_wire_msg(&mut stream, &buf[..len]).await?;

        // 6. Transition to symmetric transport mode
        let transport = noise
            .into_transport_mode()
            .context("failed to transition to transport mode")?;

        // 7. Register with metering engine
        self.metering
            .start_session(session_id, &remote_peer_id.to_string())
            .await;
        self.active_sessions
            .write()
            .await
            .insert(session_id.to_string(), ());

        let _ = self.event_tx.send(TunnelEvent::SessionEstablished {
            session_id: session_id.to_string(),
            remote_peer_id,
        });

        info!(session = session_id, "encrypted tunnel established");

        Ok(EncryptedTunnelSession {
            transport: Arc::new(tokio::sync::Mutex::new(transport)),
            stream,
            session_id: session_id.to_string(),
            remote_peer_id,
            _permit: permit,
        })
    }

    // ──────────────────────────────────────────────────────────
    // Consumer-side relay: encrypted tunnel ↔ local target
    // ──────────────────────────────────────────────────────────

    /// Relay data through an encrypted tunnel to a local target,
    /// with metering and rate limiting via a token bucket.
    pub async fn relay_data(
        &self,
        session: EncryptedTunnelSession,
        target_addr: &str,
    ) -> Result<(u64, u64)> {
        let target_stream = TcpStream::connect(target_addr)
            .await
            .context("failed to connect to target")?;

        // Token bucket: use configured max upload bandwidth for rate limiting
        let mut bucket = TokenBucket::new(self.relay_config.max_upload_mbps, self.relay_config.max_upload_mbps);

        let (bytes_sent, bytes_received) = Self::encrypted_relay_with_metering(
            tokio::io::split(session.stream),
            target_stream,
            session.transport.clone(),
            &session.session_id,
            &self.metering,
            &mut bucket,
        )
        .await;

        // Notify metering that session ended
        self.session_closed(&session.session_id).await;

        Ok((bytes_sent, bytes_received))
    }

    // ──────────────────────────────────────────────────────────
    // Session teardown
    // ──────────────────────────────────────────────────────────

    /// Notify the metering engine that a session has ended and
    /// remove it from the active set.
    pub async fn session_closed(&self, session_id: &str) {
        self.active_sessions.write().await.remove(session_id);

        // Release the session slot from the availability engine
        {
            let mut avail = self.availability.write().await;
            avail.release_session();
        }

        if let Err(e) = self.metering.end_session(session_id).await {
            warn!(error = %e, session = session_id, "failed to finalize metered session");
        }
        info!(session = session_id, "session finalized in metering engine");
    }

    // ──────────────────────────────────────────────────────────
    // Provider-side: accept incoming tunnel connections
    // ──────────────────────────────────────────────────────────

    /// Listen for incoming tunnel connections (provider side).
    pub async fn accept_incoming(
        listen_addr: &str,
        event_tx: mpsc::UnboundedSender<TunnelEvent>,
        config: RelayConfig,
        conn_semaphore: Arc<Semaphore>,
        static_private_key: Vec<u8>,
    ) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .context("failed to bind tunnel listener")?;

        info!(addr = listen_addr, "tunnel listener started");

        loop {
            let (stream, addr) = listener.accept().await?;
            let event_tx = event_tx.clone();
            let config = RelayConfig {
                backend: config.backend.clone(),
                metering: config.metering.clone(),
                availability: config.availability.clone(),
                target_addr: config.target_addr.clone(),
                local_peer_id: config.local_peer_id.clone(),
                max_upload_mbps: config.max_upload_mbps,
                max_download_mbps: config.max_download_mbps,
            };
            let sem = Arc::clone(&conn_semaphore);
            let sk = static_private_key.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::handle_incoming_connection(stream, addr, event_tx, config, sem, sk).await
                {
                    error!(error = %e, addr = %addr, "failed to handle incoming tunnel");
                }
            });
        }
    }

    /// Full provider-side relay: Noise_XX responder handshake,
    /// acquire session, record connection, encrypted bidirectional
    /// copy, metering, teardown with DB writes.
    async fn handle_incoming_connection(
        mut stream: TcpStream,
        addr: std::net::SocketAddr,
        event_tx: mpsc::UnboundedSender<TunnelEvent>,
        config: RelayConfig,
        conn_semaphore: Arc<Semaphore>,
        static_private_key: Vec<u8>,
    ) -> Result<()> {
        // 1. Acquire connection semaphore permit
        let _permit = conn_semaphore
            .try_acquire_owned()
            .map_err(|_| anyhow!("connection limit reached: semaphore exhausted"))?;

        // 2. Noise_XX Responder Handshake
        let mut prologue = Vec::from(PROTOCOL_PROLOGUE_PREFIX);
        // We'll fill in the session_id after reading Msg 1's prologue
        // Actually, prologue is set by both sides independently.
        // The responder needs the SAME prologue as the initiator.
        // We'll read Msg 1 first, then extract the prologue from it.

        // Actually, in Noise_XX, the prologue is NOT transmitted in the handshake.
        // Both sides must agree on it out-of-band. Since we're using the session_id
        // in the prologue, we need to get it from the consumer first.
        //
        // The problem: the prologue is set BEFORE the handshake starts, but we don't
        // know the session_id yet.
        //
        // Solution: Read the session_id as a length-prefixed message BEFORE the
        // Noise handshake. This is safe because the handshake hasn't started yet
        // and the session_id is not secret (it's just a binding value).

        // Read session_id and peer_id from consumer (plaintext, before Noise handshake)
        let (session_id, remote_peer_id_str) = recv_session_and_peer_id(&mut stream).await?;
        info!(
            session = session_id,
            remote_peer = %remote_peer_id_str,
            addr = %addr,
            "incoming tunnel connection accepted"
        );

        // Now construct the prologue with the session_id
        prologue.extend_from_slice(session_id.as_bytes());

        let params = NOISE_PARAMS
            .parse()
            .context("invalid noise params")?;
        let mut noise = Builder::new(params)
            .prologue(&prologue)
            .local_private_key(&static_private_key)
            .build_responder()
            .context("failed to build noise responder")?;

        let mut buf = [0u8; NOISE_MAX_MSG_LEN];

        // Msg 1 (Initiator → Responder): -> e
        let len = recv_wire_msg(&mut stream, &mut buf).await?;
        noise
            .read_message(&buf[..len], &mut [])
            .context("noise msg1 read failed")?;

        // Msg 2 (Responder → Initiator): <- e, ee, s, es
        let len = noise
            .write_message(&[], &mut buf)
            .context("noise msg2 write failed")?;
        send_wire_msg(&mut stream, &buf[..len]).await?;

        // Msg 3 (Initiator → Responder): -> s, se
        let len = recv_wire_msg(&mut stream, &mut buf).await?;
        noise
            .read_message(&buf[..len], &mut [])
            .context("noise msg3 read failed")?;

        // Transition to symmetric transport mode
        let transport = noise
            .into_transport_mode()
            .context("failed to transition to transport mode")?;

        info!(session = session_id, "Noise_XX handshake completed");

        // 3. Acquire a session slot from the availability engine
        {
            let mut avail = config.availability.write().await;
            if let Err(e) = avail.try_acquire_session() {
                warn!(session = %session_id, error = e, "rejecting connection: no sessions available");
                return Err(anyhow::anyhow!(e));
            }
        }

        // 4. Start metering session using the consumer's peer_id (not socket addr)
        config
            .metering
            .start_session(&session_id, &remote_peer_id_str)
            .await;

        // 5. Record initial connection in DB using peer_id
        let connection_record = ConnectionRecord {
            id: None,
            local_peer_id: config.local_peer_id.clone(),
            remote_peer_id: remote_peer_id_str.clone(),
            remote_ip: Some(addr.ip().to_string()),
            remote_port: Some(addr.port()),
            direction: "inbound".to_string(),
            bytes_sent: 0,
            bytes_received: 0,
            duration_secs: 0.0,
            avg_latency_ms: 0.0,
            exit_reason: None,
            started_at: chrono::Utc::now().to_rfc3339(),
            ended_at: None,
        };
        config.backend.insert_connection(&connection_record).await
            .context("failed to insert connection record")?;

        let start_time = std::time::Instant::now();

        // 6. Connect to the target resource
        let target_stream = match TcpStream::connect(&config.target_addr).await {
            Ok(s) => s,
            Err(e) => {
                error!(session = %session_id, error = %e, "failed to connect to target");
                // Release session on failure
                let mut avail = config.availability.write().await;
                avail.release_session();
                let _ = config.metering.end_session(&session_id).await;
                return Err(e.into());
            }
        };

        let transport = Arc::new(tokio::sync::Mutex::new(transport));

        // 7. Bidirectional encrypted relay with metering
        let (bytes_sent, bytes_received) = Self::encrypted_relay_with_metering(
            tokio::io::split(stream),
            target_stream,
            transport,
            &session_id,
            &config.metering,
            &mut TokenBucket::new(config.max_download_mbps, config.max_download_mbps),
        )
        .await;

        let duration = start_time.elapsed().as_secs_f64();

        // 8. Teardown: release session, update DB history & reputation
        {
            let mut avail = config.availability.write().await;
            avail.release_session();
        }

        // Update connection end in DB (using peer_id, not socket addr)
        if let Err(e) = config
            .backend
            .update_connection_end(
                &config.local_peer_id,
                &remote_peer_id_str,
                bytes_sent,
                bytes_received,
                duration,
                0.0, // avg_latency_ms (not measured for individual connections)
                "completed",
            )
            .await
        {
            warn!(error = %e, "failed to update connection end");
        }

        // Update peer reputation using peer_id (not socket addr)
        let reputation = PeerReputation {
            peer_id: remote_peer_id_str,
            reputation_score: 1.0,
            total_bytes_relayed: bytes_sent + bytes_received,
            successful_sessions: 1,
            failed_sessions: 0,
            avg_latency_ms: 0.0,
            last_active_at: Some(chrono::Utc::now().to_rfc3339()),
            recorded_at: Some(chrono::Utc::now().to_rfc3339()),
        };
        if let Err(e) = config.backend.upsert_peer_reputation(&reputation).await {
            warn!(error = %e, "failed to update peer reputation");
        }

        // Finalize metered session (single end_session call — event handler
        // does NOT call end_session, avoiding the double-end-session bug).
        if let Err(e) = config.metering.end_session(&session_id).await {
            warn!(error = %e, session = %session_id, "failed to finalize metered session");
        }

        let _ = event_tx.send(TunnelEvent::SessionClosed {
            session_id: session_id.clone(),
            bytes_sent,
            bytes_received,
        });

        info!(
            session = %session_id,
            bytes_sent,
            bytes_received,
            duration_secs = (duration * 100.0).round() / 100.0,
            "relay session completed"
        );

        // _permit is dropped here, releasing the connection semaphore slot

        Ok(())
    }

    // ──────────────────────────────────────────────────────────
    // Encrypted bidirectional relay
    // ──────────────────────────────────────────────────────────

    /// Perform bidirectional encrypted relay between a Noise tunnel
    /// and a plaintext target stream, recording each chunk to the
    /// metering engine.
    ///
    /// Uses `Arc<Mutex<TransportState>>` for concurrent encrypt/decrypt.
    async fn encrypted_relay_with_metering(
        tunnel_stream: (tokio::io::ReadHalf<TcpStream>, tokio::io::WriteHalf<TcpStream>),
        target_stream: TcpStream,
        transport: Arc<tokio::sync::Mutex<TransportState>>,
        session_id: &str,
        metering: &MeteringEngine,
        bucket: &mut TokenBucket,
    ) -> (u64, u64) {
        use tokio::io::split;

        let (mut tunnel_read, mut tunnel_write) = tunnel_stream;
        let (mut target_read, mut target_write) = split(target_stream);

        let mut bytes_sent: u64 = 0;
        let mut bytes_received: u64 = 0;

        // Consumer → Target (egress): decrypt from tunnel, forward to target
        let transport_egress = transport.clone();
        let metering_egress = metering.clone();
        let sid_e = session_id.to_string();
        let to_target = async {
            let mut raw_buf = [0u8; NOISE_MAX_MSG_LEN];
            let mut plain_buf = [0u8; NOISE_MAX_MSG_LEN];
            loop {
                // Read raw encrypted data from tunnel
                let n = match tunnel_read.read(&mut raw_buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };

                // Rate-limit via token bucket — apply real backpressure
                let mbps_requested = (n as f64 * 8.0) / 1_000_000.0;
                let granted = bucket.consume(mbps_requested);
                if granted < mbps_requested {
                    let deficit = mbps_requested - granted;
                    // Sleep proportional to the deficit: how long until
                    // enough tokens refill to cover the shortfall.
                    let sleep_secs = deficit / bucket.refill_rate();
                    warn!(
                        session = %sid_e,
                        requested_mbps = mbps_requested,
                        granted_mbps = granted,
                        backpressure_ms = (sleep_secs * 1000.0).round() as u64,
                        "backpressure: rate limit exceeded, sleeping"
                    );
                    tokio::time::sleep(Duration::from_secs_f64(sleep_secs)).await;
                }

                // Decrypt
                let plain_len = {
                    let mut transport = transport_egress.lock().await;
                    match transport.read_message(&raw_buf[..n], &mut plain_buf) {
                        Ok(len) => len,
                        Err(e) => {
                            warn!(session = %sid_e, error = %e, "noise decrypt failed");
                            break;
                        }
                    }
                };

                // Forward plaintext to target
                if target_write.write_all(&plain_buf[..plain_len]).await.is_err() {
                    break;
                }
                bytes_sent += plain_len as u64;
                let _ = metering_egress
                    .record_transfer(&sid_e, "egress", plain_len as u64)
                    .await;
            }
        };

        // Target → Consumer (ingress): read from target, encrypt, forward to tunnel
        let transport_ingress = transport.clone();
        let metering_ingress = metering.clone();
        let sid_i = session_id.to_string();
        let to_tunnel = async {
            let mut plain_buf = [0u8; NOISE_MAX_MSG_LEN];
            let mut enc_buf = [0u8; NOISE_MAX_MSG_LEN];
            loop {
                // Read plaintext from target
                let n = match target_read.read(&mut plain_buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };

                // Encrypt
                let enc_len = {
                    let mut transport = transport_ingress.lock().await;
                    match transport.write_message(&plain_buf[..n], &mut enc_buf) {
                        Ok(len) => len,
                        Err(e) => {
                            warn!(session = %sid_i, error = %e, "noise encrypt failed");
                            break;
                        }
                    }
                };

                // Forward encrypted data to tunnel
                if tunnel_write.write_all(&enc_buf[..enc_len]).await.is_err() {
                    break;
                }
                bytes_received += n as u64;
                let _ = metering_ingress
                    .record_transfer(&sid_i, "ingress", n as u64)
                    .await;
            }
        };

        tokio::select! {
            _ = to_target => {}
            _ = to_tunnel => {}
        }

        (bytes_sent, bytes_received)
    }
}

// ──────────────────────────────────────────────────────────────
// Wire protocol helpers
// ──────────────────────────────────────────────────────────────

/// Send a length-prefixed message (2-byte big-endian header).
async fn send_wire_msg(stream: &mut TcpStream, msg: &[u8]) -> Result<()> {
    let len =
        u16::try_from(msg.len()).map_err(|_| anyhow!("message exceeds 65535 byte max limit"))?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(msg).await?;
    Ok(())
}

/// Receive a length-prefixed message (2-byte big-endian header).
async fn recv_wire_msg(stream: &mut TcpStream, buf: &mut [u8]) -> Result<usize> {
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;

    if len > buf.len() {
        bail!(
            "received wire packet length {} exceeds buffer size {}",
            len,
            buf.len()
        );
    }

    stream.read_exact(&mut buf[..len]).await?;
    Ok(len)
}

/// Send session ID and local peer ID as length-prefixed messages
/// (4-byte big-endian header each). Used before the Noise handshake.
async fn send_session_id(stream: &mut TcpStream, session_id: &str, peer_id: &str) -> Result<()> {
    let session_bytes = session_id.as_bytes();
    stream
        .write_u32(session_bytes.len() as u32)
        .await
        .context("failed to send session ID length")?;
    stream
        .write_all(session_bytes)
        .await
        .context("failed to send session ID")?;

    let peer_bytes = peer_id.as_bytes();
    stream
        .write_u32(peer_bytes.len() as u32)
        .await
        .context("failed to send peer ID length")?;
    stream
        .write_all(peer_bytes)
        .await
        .context("failed to send peer ID")?;
    Ok(())
}

/// Receive session ID and remote peer ID as length-prefixed messages (4-byte
/// big-endian header each). Used before the Noise handshake.
///
/// The session ID is capped at 256 bytes and the peer ID at 512 bytes to
/// prevent an attacker from allocating unbounded memory via a crafted u32.
async fn recv_session_and_peer_id(stream: &mut TcpStream) -> Result<(String, String)> {
    let session_len = stream
        .read_u32()
        .await
        .context("failed to read session ID length")? as usize;

    if session_len > 256 {
        bail!(
            "session ID length {} exceeds maximum of 256 bytes",
            session_len
        );
    }
    let mut session_buf = vec![0u8; session_len];
    stream
        .read_exact(&mut session_buf)
        .await
        .context("failed to read session ID")?;
    let session_id =
        String::from_utf8(session_buf).context("invalid session ID encoding")?;

    let peer_len = stream
        .read_u32()
        .await
        .context("failed to read peer ID length")? as usize;

    if peer_len > 512 {
        bail!(
            "peer ID length {} exceeds maximum of 512 bytes",
            peer_len
        );
    }
    let mut peer_buf = vec![0u8; peer_len];
    stream
        .read_exact(&mut peer_buf)
        .await
        .context("failed to read peer ID")?;
    let peer_id =
        String::from_utf8(peer_buf).context("invalid peer ID encoding")?;

    Ok((session_id, peer_id))
}
