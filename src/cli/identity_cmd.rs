//! `echo-node identity` — show this node's keys and derived ids.

use anyhow::{Context, Result};

use echo_daemon::base64_encode;
use echo_daemon::identity::NodeIdentity;

use super::config::{EchoHome, derive_provider_id};

/// Read `identity.json` and print the public-facing identity. Never writes:
/// rotating keys is the daemon's (`load_or_generate`) job, not an inspection
/// command's.
pub fn run(home: &EchoHome) -> Result<()> {
    let path = home.identity_path();
    if !path.exists() {
        anyhow::bail!(
            "identity missing — {} (run `echo-node onboard`)",
            path.display()
        );
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let identity: NodeIdentity =
        serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;

    let cfg = home.load_config()?;
    let provider_id = derive_provider_id(identity.peer_id_str());

    println!("identity — {}", path.display());
    row("peer id", identity.peer_id_str());
    row("provider id", &provider_id);
    row("region (identity)", &identity.region);
    row("region (config)", &cfg.region);
    row(
        "ed25519 public key",
        base64_encode(&identity.public_key_bytes),
    );
    row(
        "noise public key",
        base64_encode(identity.noise_public_key_bytes()),
    );

    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    row("key file", format!("{size} bytes"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path)
            .map(|m| m.permissions().mode() & 0o777)
            .unwrap_or(0);
        row("permissions", format!("{mode:o}"));
        if mode & 0o077 != 0 {
            println!(
                "  ⚠ permissions should be 600 — the daemon rewrites the file owner-only on save"
            );
        }
    }

    // Cross-check the onboarding record against the derived id.
    match &cfg.provider_id {
        Some(recorded) if recorded == &provider_id => row("claimed as", recorded),
        Some(recorded) => {
            row("claimed as", recorded);
            println!("  ⚠ config provider_id does not match this identity (re-run `echo-node onboard`)");
        }
        None => row("claimed as", "(not claimed yet — run `echo-node onboard`)"),
    }
    Ok(())
}

fn row(label: &str, value: impl std::fmt::Display) {
    println!("  {label:<20} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The derived provider id is `provider_` + sha256(peer_id)[..8] as hex
    /// (8 bytes → 16 hex chars, so 25 chars total).
    #[test]
    fn derived_provider_id_shape() {
        let id = derive_provider_id("12D3KooWExamplePeerId");
        assert!(id.starts_with("provider_"), "{id}");
        assert_eq!(id.len(), "provider_".len() + 16);
        assert!(id["provider_".len()..].chars().all(|c| c.is_ascii_hexdigit()));
        // Deterministic for a fixed input.
        assert_eq!(id, derive_provider_id("12D3KooWExamplePeerId"));
    }
}
