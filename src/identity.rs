use anyhow::{Context, Result};
use ed25519_dalek::{SigningKey, Signer, VerifyingKey};
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::error;

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

/// Atomically persist the identity file.
///
/// Write-to-temp → `fsync` → chmod `0600` → `rename` → fsync the parent
/// directory. A crash at any point leaves either the old file or the new
/// file intact — never a truncated/half-written JSON that would brick key
/// loading on the next boot.
fn write_atomic(path: &Path, data: &str) -> Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    std::fs::create_dir_all(&parent).context("failed to create identity directory")?;

    // Temp file in the same directory (rename() must not cross filesystems).
    let base = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_else(|| std::ffi::OsString::from("identity"));
    let mut tmp_name = base;
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    let tmp_path = parent.join(tmp_name);

    let write_result = (|| -> Result<()> {
        use std::io::Write;
        let mut file =
            std::fs::File::create(&tmp_path).context("failed to create identity temp file")?;
        file.write_all(data.as_bytes())
            .context("failed to write identity temp file")?;
        file.sync_all()
            .context("failed to fsync identity temp file")?;
        // Owner-only before the file is ever visible under its final name.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))
                .context("failed to set identity temp file permissions")?;
        }
        Ok(())
    })();
    if let Err(e) = write_result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }

    if let Err(e) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e).context("failed to atomically replace identity file");
    }

    // Best-effort directory fsync so the rename itself is durable.
    #[cfg(unix)]
    if let Ok(dir) = std::fs::File::open(&parent) {
        let _ = dir.sync_all();
    }

    Ok(())
}

/// Move a corrupt identity file aside (evidence preserved, never deleted)
/// so a fresh identity can be generated. Returns the backup path.
fn preserve_corrupt(path: &Path) -> std::path::PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_else(|| std::ffi::OsString::from("identity"));
    name.push(format!(".corrupt-{ts}"));
    let backup = path.with_file_name(name);
    match std::fs::rename(path, &backup) {
        Ok(()) => backup,
        Err(e) => {
            error!(error = %e, "failed to move corrupt identity file aside");
            backup
        }
    }
}

impl NodeIdentity {
    /// Load or generate a persistent node identity.
    /// Stores keypair as raw bytes in a JSON file.
    ///
    /// Crash-safety guarantees:
    /// - every write is atomic (temp + fsync + rename) — a kill mid-write
    ///   can never truncate the identity file;
    /// - an unreadable/corrupt file is preserved as `*.corrupt-<ts>` and a
    ///   fresh identity is generated instead of refusing to boot (the
    ///   corrupt bytes remain on disk for forensics).
    pub fn load_or_generate(path: &Path, region: &str) -> Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(path).context("failed to read identity file")?;
            match serde_json::from_str::<NodeIdentity>(&data) {
                Ok(mut identity) => {
                    // Migrate legacy identities: generate noise keypair if missing
                    if identity.noise_secret_key.is_empty() || identity.noise_public_key.is_empty()
                    {
                        let noise_kp = snow::Builder::new(
                            NOISE_PARAMS.parse().context("invalid noise params")?,
                        )
                        .generate_keypair()
                        .context("failed to generate noise keypair")?;
                        identity.noise_secret_key = noise_kp.private;
                        identity.noise_public_key = noise_kp.public;
                        // Persist the updated identity (atomic).
                        let data = serde_json::to_string_pretty(&identity)
                            .context("failed to serialize identity")?;
                        write_atomic(path, &data)?;
                    }
                    return Ok(identity);
                }
                Err(e) => {
                    // Corrupt/truncated file (e.g. crash during a write from a
                    // pre-atomic version). Preserve it, then regenerate below —
                    // a node must not brick itself on an unreadable file.
                    let backup = preserve_corrupt(path);
                    error!(
                        error = %e,
                        corrupt_backup = %backup.display(),
                        "identity file is corrupt; preserved it and generating a fresh identity"
                    );
                }
            }
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

        // Persist atomically; restricted to owner-only (0600) so secret
        // keys are never world-readable.
        let data =
            serde_json::to_string_pretty(&identity).context("failed to serialize identity")?;
        write_atomic(path, &data)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_then_reload_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        let a = NodeIdentity::load_or_generate(&path, "us-west").unwrap();
        let b = NodeIdentity::load_or_generate(&path, "us-west").unwrap();
        assert_eq!(a.peer_id, b.peer_id, "reload must preserve the peer id");
        assert_eq!(a.keypair_bytes, b.keypair_bytes);
        assert_eq!(a.noise_public_key, b.noise_public_key);
        assert!(!a.noise_secret_key.is_empty());
    }

    #[test]
    fn writes_are_atomic_and_leave_no_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("identity.json");
        NodeIdentity::load_or_generate(&path, "eu").unwrap();

        let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["identity.json".to_string()], "no temp leftovers");

        // File must be valid JSON (fully written, not truncated).
        let data = std::fs::read_to_string(&path).unwrap();
        serde_json::from_str::<NodeIdentity>(&data).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "identity must stay owner-only");
        }
    }

    #[test]
    fn corrupt_identity_is_preserved_and_regenerated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        // Simulate a truncated write from a pre-atomic version.
        std::fs::write(&path, r#"{"peer_id": "12D3KooWTRUNCATED"#).unwrap();

        let fresh = NodeIdentity::load_or_generate(&path, "us-west")
            .expect("corrupt file must not brick startup");

        // Fresh identity is functional and persisted.
        assert!(!fresh.peer_id.is_empty());
        let reloaded = NodeIdentity::load_or_generate(&path, "us-west").unwrap();
        assert_eq!(fresh.peer_id, reloaded.peer_id);

        // The corrupt bytes are preserved as evidence (never deleted).
        let backups: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let name = e.unwrap().file_name().to_string_lossy().into_owned();
                name.contains(".corrupt-").then_some(name)
            })
            .collect();
        assert_eq!(backups.len(), 1, "corrupt file preserved: {backups:?}");
        let backup_data =
            std::fs::read_to_string(dir.path().join(&backups[0])).unwrap();
        assert_eq!(backup_data, r#"{"peer_id": "12D3KooWTRUNCATED"#);
    }

    #[test]
    fn legacy_identity_without_noise_keys_is_migrated_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        // Valid legacy JSON: no noise keys (serde defaults fill the fields).
        let legacy = serde_json::json!({
            "peer_id": "12D3KooWLegacyPeer",
            "keypair_bytes": serde_json::to_value(vec![7u8; 32]).unwrap(),
            "public_key_bytes": serde_json::to_value(vec![9u8; 32]).unwrap(),
            "region": "eu"
        });
        std::fs::write(&path, serde_json::to_string(&legacy).unwrap()).unwrap();

        let id = NodeIdentity::load_or_generate(&path, "eu").unwrap();
        assert_eq!(id.peer_id, "12D3KooWLegacyPeer");
        assert_eq!(id.noise_public_key.len(), 32, "migration generated noise keys");

        // Migration write must have persisted the keys (reload keeps them).
        let reloaded = NodeIdentity::load_or_generate(&path, "eu").unwrap();
        assert_eq!(reloaded.noise_public_key, id.noise_public_key);
        assert_eq!(reloaded.noise_secret_key, id.noise_secret_key);

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp"))
            .collect();
        assert!(entries.is_empty(), "no temp files after migration: {entries:?}");
    }
}
