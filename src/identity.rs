use anyhow::{Context, Result};
use ed25519_dalek::{SigningKey, Signer, VerifyingKey};
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::NOISE_PARAMS;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub peer_id: String,
    pub keypair_bytes: Vec<u8>,
    pub public_key_bytes: Vec<u8>,
    pub region: String,
    /// X25519 static secret key for Noise_XX tunnel encryption.
    #[serde(default)]
    pub noise_secret_key: Vec<u8>,
    /// X25519 static public key for Noise_XX tunnel encryption.
    #[serde(default)]
    pub noise_public_key: Vec<u8>,
}

impl NodeIdentity {
    /// Load or generate a persistent node identity.
    /// Stores keypair as raw bytes in a JSON file.
    pub fn load_or_generate(path: &Path, region: &str) -> Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(path).context("failed to read identity file")?;
            let mut identity: NodeIdentity =
                serde_json::from_str(&data).context("failed to parse identity file")?;
            // Migrate legacy identities: generate noise keypair if missing
            if identity.noise_secret_key.is_empty() || identity.noise_public_key.is_empty() {
                let noise_kp = snow::Builder::new(NOISE_PARAMS.parse().context("invalid noise params")?)
                    .generate_keypair()
                    .context("failed to generate noise keypair")?;
                identity.noise_secret_key = noise_kp.private;
                identity.noise_public_key = noise_kp.public;
                // Persist the updated identity
                let data = serde_json::to_string_pretty(&identity)
                    .context("failed to serialize identity")?;
                std::fs::write(path, data).context("failed to write identity file")?;
                // Re-apply restricted permissions after migration write.
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                        .context("failed to set identity file permissions")?;
                }
            }
            return Ok(identity);
        }

        let mut rng = rand::thread_rng();
        let signing_key = SigningKey::generate(&mut rng);
        let verifying_key: VerifyingKey = signing_key.verifying_key();

        // Derive libp2p PeerId from ed25519 secret key
        let libp2p_keypair =
            identity::Keypair::ed25519_from_bytes(signing_key.to_bytes().to_vec())
                .context("failed to create libp2p keypair from ed25519")?;
        let peer_id = libp2p_keypair.public().to_peer_id();

        // Generate Noise_XX static keypair (X25519)
        let noise_kp = snow::Builder::new(NOISE_PARAMS.parse().context("invalid noise params")?)
            .generate_keypair()
            .context("failed to generate noise keypair")?;

        let identity = NodeIdentity {
            peer_id: peer_id.to_string(),
            keypair_bytes: signing_key.to_bytes().to_vec(),
            public_key_bytes: verifying_key.to_bytes().to_vec(),
            region: region.to_string(),
            noise_secret_key: noise_kp.private,
            noise_public_key: noise_kp.public,
        };

        // Persist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("failed to create identity directory")?;
        }
        let data = serde_json::to_string_pretty(&identity)
            .context("failed to serialize identity")?;
        std::fs::write(path, data).context("failed to write identity file")?;

        // Restrict file permissions to owner-only (0600) so that secret
        // keys are not world-readable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .context("failed to set identity file permissions")?;
        }

        Ok(identity)
    }

    /// Reconstruct libp2p Keypair from stored secret key bytes
    pub fn libp2p_keypair(&self) -> Result<Keypair> {
        let kp = Keypair::ed25519_from_bytes(self.keypair_bytes.clone())
            .context("failed to reconstruct libp2p keypair")?;
        Ok(kp)
    }

    /// Get the ed25519 VerifyingKey (public key)
    pub fn verifying_key(&self) -> Result<VerifyingKey> {
        let bytes: [u8; 32] = self
            .public_key_bytes
            .clone()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid public key length"))?;
        VerifyingKey::from_bytes(&bytes).context("invalid ed25519 public key")
    }

    /// Get the ed25519 SigningKey (private key) from stored bytes
    pub fn signing_key(&self) -> Result<SigningKey> {
        let bytes: [u8; 32] = self
            .keypair_bytes
            .clone()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid keypair length"))?;
        Ok(SigningKey::from_bytes(&bytes))
    }

    /// Sign a message and return the signature as bytes
    pub fn sign(&self, message: &[u8]) -> Result<Vec<u8>> {
        let sk = self.signing_key()?;
        let signature = sk.sign(message);
        Ok(signature.to_bytes().to_vec())
    }

    /// Verify a signature against this node's public key
    pub fn verify(&self, message: &[u8], signature_bytes: &[u8]) -> Result<bool> {
        let vk = self.verifying_key()?;
        let sig_array: [u8; 64] = signature_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid signature length"))?;
        let signature = ed25519_dalek::Signature::from_bytes(&sig_array);
        Ok(vk.verify_strict(message, &signature).is_ok())
    }

    pub fn peer_id_str(&self) -> &str {
        &self.peer_id
    }

    /// Get the Noise_XX static public key bytes.
    pub fn noise_public_key_bytes(&self) -> &[u8] {
        &self.noise_public_key
    }

    /// Get the Noise_XX static secret key bytes.
    pub fn noise_secret_key_bytes(&self) -> &[u8] {
        &self.noise_secret_key
    }
}
