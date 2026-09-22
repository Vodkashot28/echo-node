//! Consumer-side tunnel listener.
//!
//! In consumer mode the daemon does not provide shared capacity itself;
//! instead it runs a local forward listener that local applications connect
//! to. Each accepted local connection opens a fresh Noise_XX tunnel to a
//! configured provider node (static-key pinned against the provider's
//! discovery-public Noise key) and relays the traffic through it.
//!
//! The provider performs the authoritative metering and issues signed
//! receipts; the consumer relays with metering disabled so it never
//! accumulates `earnings` for traffic it only forwards.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::tunnel::TunnelService;

/// Monotonic session-id sequence, unique per daemon run.
static SESSION_SEQ: AtomicU64 = AtomicU64::new(0);

/// Configuration for the consumer-side forward listener.
#[derive(Clone)]
pub struct ConsumerConfig {
    /// Local address local applications connect to (default `127.0.0.1:3003`).
    pub listen_addr: String,
    /// Provider's tunnel listener address (`TUNNEL_ADDR` on the provider).
    pub provider_addr: String,
    /// Provider's libp2p `PeerId` (used for session binding + metering).
    pub provider_peer_id: libp2p::PeerId,
    /// Provider's X25519 Noise static public key (from the provider's
    /// capability/discovery record). Base64-decoded from `PROVIDER_NOISE_PUBKEY`,
    /// pinned during the handshake to prevent MITM.
    pub provider_noise_pubkey: Vec<u8>,
}

/// Accept local application connections on `listen_addr` and relay each one
/// through a fresh encrypted tunnel to the configured provider.
///
/// Runs forever; returns only on a fatal listener error.
pub async fn run_consumer_listener(
    service: Arc<TunnelService>,
    config: ConsumerConfig,
) -> Result<()> {
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("failed to bind consumer listener on {}", config.listen_addr))?;
    info!(
        listen_addr = %config.listen_addr,
        provider_addr = %config.provider_addr,
        provider_peer = %config.provider_peer_id,
        "consumer listener started (local apps → encrypted provider tunnel)"
    );

    loop {
        let (app_stream, peer_addr) = listener.accept().await?;
        let service = Arc::clone(&service);
        let config = config.clone();

        let session_id = next_session_id(&service);
        info!(
            peer = %peer_addr,
            session = %session_id,
            "local application connected; opening provider tunnel"
        );

        tokio::spawn(async move {
            if let Err(e) =
                run_consumer_session(&service, &config, app_stream, &session_id).await
            {
                error!(session = %session_id, error = %e, "consumer session failed");
            }
        });
    }
}

/// One consumer session: local app stream → Noise tunnel → provider target.
async fn run_consumer_session(
    service: &TunnelService,
    config: &ConsumerConfig,
    app_stream: TcpStream,
    session_id: &str,
) -> Result<()> {
    // Full XX handshake includes static-key pinning against the expected
    // provider Noise key — any mismatch fails the connection here.
    let session = service
        .connect_to_provider(
            &config.provider_addr,
            config.provider_peer_id,
            session_id,
            &config.provider_noise_pubkey,
        )
        .await
        .context("consumer tunnel handshake failed")?;

    info!(session = session_id, "consumer tunnel established, relaying");

    // Relay without metering; teardown releases the availability slot.
    let (bytes_sent, bytes_received) = service
        .relay_local_connection(session, app_stream)
        .await
        .context("consumer relay failed")?;

    info!(
        session = session_id,
        bytes_sent,
        bytes_received,
        "consumer session complete"
    );
    Ok(())
}

/// Build a unique, bounded session id from the per-run sequence plus the
/// local peer id (kept short, well under the 256-byte wire limit).
fn next_session_id(service: &TunnelService) -> String {
    let seq = SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
    let peer_b58 = service.local_peer_id().to_base58();
    let suffix: String = peer_b58
        .chars()
        .skip(peer_b58.len().saturating_sub(8))
        .collect();
    format!("consumer-{}-{}", seq, suffix)
}