# EchoNode One-Command Installer — Implementation Brief

**Status:** design document (canonical implementation brief).
Nothing described here ships yet except where marked ✅ shipped.

Related: [README “Operator CLI”](README.md#operator-cli) (what exists today),
[ARCHITECTURE.md](ARCHITECTURE.md), [P0_FIXES.md](P0_FIXES.md) (open gaps),
[DASHBOARD.md](DASHBOARD.md).

---

## 1. Vision

An operator should never run `cargo build --release`. The entire install is:

```bash
curl -fsSL https://install.echo-mesh.xyz | sh
```

producing:

```text
EchoNode Installer

✓ Platform detected        (linux-arm64)
✓ Binary downloaded        (v0.3.0)
✓ Checksum verified        (sha256)
✓ Signature verified       (minisign)
✓ Directories created
✓ Binary installed         (/usr/local/bin/echo-node)
✓ Configuration created
✓ Service installed        (systemd — skipped on Termux)
✓ Node started
✓ Onboarding complete

Node ID:     provider_8f4a7d
Marketplace: ACTIVE
```

Non-negotiable design pillars: **security (verify before exec), reliability
(fail-closed, idempotent), operator experience (one command), automation
(everything scriptable), upgradeability (atomic updates + rollback).**

---

## 2. Resolved design decisions

| Decision | Resolution | Status |
|----------|-----------|--------|
| Binary name | **`echo-node` everywhere** — `[[bin]] name = "echo-node"` in Cargo.toml; artifact, clap, docs, service, installer all match | ✅ shipped |
| Package name | Stays `echo-daemon` (internal crate organization only; never user-facing) | ✅ shipped |
| `--version` output | Keep clap default **`echo-node 0.3.0`** (standard, parseable); installer may *display* `v0.3.0` for UX | ✅ shipped |
| Registry install | Not on crates.io (verified 404) — install only from checkout / release artifacts | ✅ documented |
| CI artifact name | `echo-node-linux-amd64` (was `echo-daemon-linux-x86_64`) | ✅ shipped |
| API port for services | **Open decision** — see §10.1; recommend `127.0.0.1:7777` (CLI default) for v1 | ⬜ |
| XDG data/state split | **Roadmap** — today all state lives in one `ECHOMESH_HOME` dir; see §9 | ⬜ |

---

## 3. Current state vs target (verified)

| Capability | Today | Target |
|-----------|-------|--------|
| Build | `cargo build --release` locally | never touched by operators |
| Release artifacts | CI uploads a raw **amd64-only** binary, 14-day retention (`.github/workflows/ci.yml`) | GitHub Releases: per-target `.tar.gz` + `checksums.txt` + `.sig`, permanent |
| One-command install | ❌ none | `curl … \| sh` |
| Multi-arch | ❌ x86_64 CI only; ARM must build natively (this repo is developed on **aarch64/Termux**) | linux-amd64, linux-arm64 (P0), linux-armv7 (P1) |
| Service integration | ❌ none; CLI-managed daemon only (`echo-node start`: setsid + pid file) | systemd (servers) **and** CLI-managed (Termux/no-root) |
| Config/state | `ECHOMESH_HOME` > `$XDG_CONFIG_HOME/echomesh` > `~/.config/echomesh` — one dir: `config.toml`, `identity.json` (0600), `state.db`, `logs/daemon.log`, `daemon.pid` | installer bootstraps the same layout (v1) |
| Onboarding | ✅ `echo-node onboard` — 7 resumable stages, `--offline`/`--reset`/`--status` | invoked automatically at the end of install |
| Updates | ❌ no `update` command | `echo-node update` (P1) |
| Docker | ❌ no Dockerfile | image mirroring native install (P1) |
| Health tooling | ✅ `status`, `doctor` (config/identity/db/health/port/logs checks) | doctor extended: DHT/ledger/marketplace/storage (P2) |

---

## 4. Supported platforms & detection

**P0 targets:** `linux-amd64`, `linux-arm64` (build native on GitHub
`ubuntu-latest` and `ubuntu-24.04-arm` runners), `linux-armv7` (P1, cross).

**Roadmap:** `windows-amd64`, `darwin-amd64`, `darwin-arm64`.

Detection (`uname -s` / `uname -m`):

```bash
case "$(uname -s)" in Linux) OS=linux ;; *) unsupported ;; esac
case "$(uname -m)" in
  x86_64|amd64)   ARCH=amd64 ;;
  aarch64|arm64)  ARCH=arm64 ;;
  armv7l|armv8l)  ARCH=armv7 ;;
  *) unsupported "architecture: $(uname -m)" ;;
esac
TARGET="${OS}-${ARCH}"      # → linux-arm64
```

Unsupported platforms fail **gracefully** (no partial state):

```text
Unsupported platform: linux/riscv64
See: https://echo-mesh.xyz/platforms
```

**Termux/Android detection** (drives flow B, §5): `$PREFIX` contains
`com.termux`, or `$TERMUX_VERSION` is set, or `command -v termux-info`.
Termux is `linux-arm64`/`linux-armv7` for artifact selection, but must
**skip systemd, skip user creation, use `$PREFIX/bin`**.

---

## 5. Installation flows

```text
curl … | sh
   → detect platform            (§4)
   → branch: systemd available?  ── yes → Flow A (server)
   │                        termux/no-root ───→ Flow B (user)
   │                        docker          ───→ Flow C (§14)
   → download + verify           (§6 §7)
   → install binary              (§8)
   → bootstrap dirs + config     (§9)
   → register + start service    (§10)   [Flow A]
   │   or echo-node start        [Flow B]
   → run onboard                 (§11)
   → display node details        (§15)
```

- **Flow A — server/systemd:** dedicated `echomesh` system user, unit with
  `ECHOMESH_HOME=/var/lib/echomesh`, hardened sandbox (§10), root required
  once for install.
- **Flow B — Termux / no-root / desktop:** install to `$PREFIX/bin`, config
  in `~/.config/echomesh`, daemon via `echo-node start` (setsid + pid file +
  health wait — already implemented ✅), no users, no systemd.
- **Flow C — Docker:** §14.

Both flows are idempotent: re-running re-installs the binary and keeps
existing config/identity (never overwritten without explicit `--force`).

---

## 6. Release distribution & packaging

**Artifacts per release** (tag `v0.3.0`, semver):

```text
echo-node-linux-amd64.tar.gz
echo-node-linux-arm64.tar.gz
echo-node-linux-armv7.tar.gz        # P1
checksums.txt                       # sha256, one line per artifact
checksums.txt.sig                   # minisign detached signature
```

Tarball contents: `echo-node`, `README.md`, `packaging/echo-node.service`
(unit for reference/`cp`).

**Versioning:** `vMAJOR.MINOR.PATCH` git tags. The installer defaults to the
newest non-prerelease release; `--version vX.Y.Z` pins one.

**Release workflow (to build — P0):** `.github/workflows/release.yml` on
tag push `v*`:

```yaml
jobs:
  build:
    strategy:
      matrix:
        include:
          - { os: ubuntu-latest,      target: x86_64-unknown-linux-gnu,  artifact: linux-amd64 }
          - { os: ubuntu-24.04-arm,   target: aarch64-unknown-linux-gnu, artifact: linux-arm64 }
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - run: rustup target add ${{ matrix.target }}
      - run: cargo build --release --target ${{ matrix.target }}
      - run: tar czf echo-node-${{ matrix.artifact }}.tar.gz \
               -C target/${{ matrix.target }}/release echo-node
      - uses: actions/upload-artifact@v4
  # checksums + minisign sign job merges artifacts, emits checksums.txt(.sig),
  # then creates the GitHub Release and attaches all five files.
```

Note: `ubuntu-24.04-arm` is free for public repos; if the repo stays
private, fall back to `cross` (docker-based cross-compilation) for arm64.
Until release.yml exists, the only published artifact is the 14-day
amd64 CI upload — **not usable on ARM devices** (build natively, README
“Architectures”).

---

## 7. Security & verification

Required properties of `install.sh`:

1. **HTTPS everywhere** (script from `install.echo-mesh.xyz`, artifacts from
   `github.com` releases) — TLS is the transport root of trust.
2. **sha256 checksums** for integrity: `sha256sum -c --ignore-missing checksums.txt`.
3. **Detached signature over `checksums.txt`** for authenticity.
4. **Fail-closed:** any mismatch aborts *before* the binary is ever executed
   or installed; no `|| true`, no “warn and continue”.
5. **No shell-injection paths:** quote everything; artifacts selected from a
   fixed mapping, never from raw server-provided strings.

**Signature tool recommendation: minisign** (over cosign/GPG):

| Tool | Verdict | Why |
|------|---------|-----|
| **minisign** | ✅ recommended | one ~100 KB static binary, one embedded public key in `install.sh`, single verify command (`minisign -Vm artifact -P "$PUBKEY"`), TrivialPKI model — the operator never manages a keyring |
| GPG | ❌ | keyring/expire/trustdb ceremony is operator-hostile for curl\|sh |
| cosign/sigstore | later (P2) | excellent for containers + transparency logs, but pulls heavier tooling into a shell bootstrap; revisit for Docker image signing |

Embedding the minisign **public key as a constant in `install.sh`** means a
compromised CDN can serve old-but-unsigned artifacts but cannot forge new
ones. Rotation: publish a new script with the new key + announce on a second
channel (docs site, socials) — key rotation is an explicit P2 concern.

Verification failure output (exact behavior: abort):

```text
ERROR: release verification failed (sha256 mismatch for echo-node-linux-arm64.tar.gz)

Installation aborted. No files were changed.
```

---

## 8. install.sh design

Structure (fail-fast; every phase prints `✓ …` only after success):

```bash
#!/usr/bin/env bash
set -euo pipefail

main() {
  detect_platform      # §4  → OS/ARCH/TARGET, FLOW=a|b
  parse_args           # --version, --prefix, --force, --no-service, --dry-run
  fetch_release        # GitHub API → latest stable tag (or pinned)
  download_assets      # tarball + checksums.txt + checksums.txt.sig
  verify_checksums     # sha256sum -c  (fail-closed)
  verify_signature     # minisign -Vm … (fail-closed)
  install_binary       # install -m 755 → $PREFIX/bin/echo-node
  create_directories   # ECHOMESH_HOME + logs/ (§9)
  write_default_config # only if absent (§9)
  install_service      # Flow A: user + unit + daemon-reload + enable + start
                       # Flow B: skip, run `echo-node start` instead
  run_onboarding       # echo-node onboard (§11)
  print_summary        # §15
}
```

Reference implementation of the security-critical core (the full script
lives at `scripts/install.sh` when this brief is implemented):

```bash
REPO="echo-mesh/echo-node"
MINISIGN_PUBKEY="<embedded public key>"          # rotated via signed script
TARGET="linux-arm64"                             # from detect_platform

fetch_release() {
  VERSION="${INSTALL_VERSION:-$(curl -fsSL \
    "https://api.github.com/repos/$REPO/releases/latest" \
    | grep -oP '"tag_name":\s*"\K[^"]+')}"
  [ -n "$VERSION" ] || die "could not resolve latest release"
}

download_assets() {
  base="https://github.com/$REPO/releases/download/$VERSION"
  curl -fsSLO "$base/echo-node-$TARGET.tar.gz"
  curl -fsSLO "$base/checksums.txt"
  curl -fsSLO "$base/checksums.txt.sig"
}

verify_checksums() {
  sha256sum -c --ignore-missing checksums.txt >/dev/null \
    || die "ERROR: release verification failed (sha256). Installation aborted. No files were changed."
}

verify_signature() {
  verify_bin() { command -v minisign || die "minisign required"; }
  verify_bin
  minisign -Vm checksums.txt -P "$MINISIGN_PUBKEY" >/dev/null \
    || die "ERROR: signature verification failed. Installation aborted."
}

die() { printf '%s\n' "$1" >&2; exit 1; }
```

Installer flags: `--version vX.Y.Z` (pin), `--prefix DIR` (default
`/usr/local`; Termux default `$PREFIX/bin`), `--force` (overwrite existing
config — otherwise preserved), `--no-service` (binary + dirs only),
`--dry-run` (print the plan).

---

## 9. Directory bootstrap & configuration

**v1 layout (what actually exists in the code ✅):** everything under one
`ECHOMESH_HOME` directory:

```text
Flow A (system):   /var/lib/echomesh/          # unit sets ECHOMESH_HOME here
Flow B (user):     ~/.config/echomesh/         # XDG default; ECHOMESH_HOME overrides
  config.toml        node config (§10)
  identity.json      ed25519 + X25519 keys — chmod 600
  state.db           ONE SQLite DB: nodes, metrics, receipts, settlements,
                     capabilities, ledger_events, session_tickets, auctions,
                     auction_bids (migrations 001–007)
  logs/daemon.log    CLI-managed daemon log
  daemon.pid         written only by CLI `start` (Flow B)
```

Ownership: Flow A → `chown -R echomesh:echomesh /var/lib/echomesh`;
Flow B → the invoking user. `mkdir -p` both cases; umask 077 for the dir.

**Vision's split layout** (`~/.local/share/…` state, `~/.local/state/…`
logs, `node.db`/`ledger.db`/`marketplace.db` separate) does **not** match
the code — the ledger and marketplace are *tables inside `state.db`*.
Adopting the XDG data/state split is a **P2 code change** (`EchoHome` in
`src/cli/config.rs` + migration); the installer must not pretend otherwise.

**Config generation** — only when `config.toml` is absent (or `--force`).
Real keys (from `src/cli/config.rs`, type-checked by `config set`):

```toml
region = "auto"           # or detected/asked, e.g. "EU-FRA"
port = 7777               # CLI REST port — see §10.1
max_upload_mbps = 100.0
max_download_mbps = 100.0
auto_start = false
auto_bid = false          # step 6 of onboard may enable
```

Never written by the installer (env-only knobs, set in the unit / shell):
`LISTEN_ADDR`, `TUNNEL_ADDR`, `NODE_MODE`, `DATABASE_URL`, `RUST_LOG`,
`BID_*`, `POLICY_*`. Example defaults: `LISTEN_ADDR=0.0.0.0:3001`,
`TUNNEL_ADDR=0.0.0.0:3002`.

### 10.1 Open decision — API port

Two defaults disagree today: the **daemon's env default** is `0.0.0.0:3001`,
the **CLI's config default** is `127.0.0.1:7777` (`DAEMON_PORT`). The CLI
talks to `daemon_addr(config.port)` from *its own* config home.

- **Recommended for v1:** service binds `LISTEN_ADDR=127.0.0.1:7777` →
  `echo-node status/doctor/wallet` work with zero extra configuration.
- **Later (single PR):** converge on 3001 (change `DAEMON_PORT` + installer
  writes `port = 3001` into *both* homes' configs) — or teach the CLI to
  read the unit's env. Track before declaring a port contract.

---

## 11. Service integration

### 11.1 Flow A — systemd (servers)

Service account: system user/group `echomesh`, no login shell, owns
`/var/lib/echomesh` only (least privilege).

Reference hardened unit — `packaging/echo-node.service`:

```ini
[Unit]
Description=EchoMesh Node (echo-node)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=echomesh
Group=echomesh
Environment=ECHOMESH_HOME=/var/lib/echomesh
Environment=LISTEN_ADDR=127.0.0.1:7777
Environment=TUNNEL_ADDR=0.0.0.0:3002
Environment=NODE_MODE=provider
Environment=RUST_LOG=echo_daemon=info
EnvironmentFile=-/etc/default/echo-node

# `echo-node daemon` = hidden subcommand = foreground daemon
ExecStart=/usr/local/bin/echo-node daemon
Restart=always
RestartSec=5
TimeoutStopSec=20
LimitNOFILE=65535

# Keep `echo-node logs tail` working under systemd (stdout/stderr → file);
# journald can be layered later by dropping these two lines.
StandardOutput=append:/var/lib/echomesh/logs/daemon.log
StandardError=append:/var/lib/echomesh/logs/daemon.log
WorkingDirectory=/var/lib/echomesh

# Sandboxing (verify against daemon needs: net + own home + /proc for sysinfo)
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/echomesh
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictRealtime=true
LockPersonality=true
MemoryDenyWriteExecute=false   # ring/libp2p JIT-free; keep false if dlopen appears
CapabilityBoundingSet=
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX AF_NETLINK   # AF_NETLINK: interface discovery
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target
```

Bootstrap (installer, Flow A):

```bash
systemctl daemon-reload && systemctl enable --now echo-node
# verify: systemctl is-active echo-node → active
```

### 11.2 Flow B — Termux / no-root

No systemd, no users, no `/usr/local`:

```bash
install -m 755 echo-node "$PREFIX/bin/echo-node"    # ~/../usr/bin
echo-node onboard --offline      # or full onboard if network steps wanted
echo-node start                  # setsid + pid file + health wait (existing ✅)
```

Optional (if `termux-services`/`sv` present): drop a `run` script under
`$PREFIX/etc/sv/echo-node` mirroring the unit's env; otherwise document
`echo-node start` in the user's shell profile when `auto_start = true`.
Suggest `termux-wake-lock` for always-on nodes.

### 11.3 Known CLI ↔ systemd integration gaps (P1, honest list)

| Gap | Today | Fix direction |
|-----|-------|---------------|
| `echo-node stop` vs `Restart=always` | CLI stop SIGTERMs via pid file; systemd would restart it; pid file absent under systemd | CLI detects an active unit (`systemctl is-active echo-node`) → forwards stop/start |
| `echo-node start` under systemd | spawns a *second* daemon → port clash, confusing health-wait failure | same systemd detection; refuse with a clear message |
| Pid file | written only by CLI path; `status` falls back to REST ping (works ✅) | unit could `ExecStartPost=/bin/sh -c 'echo $MAINPID > …/daemon.pid'` |
| Two config homes | service uses `/var/lib/echomesh`, operator CLI defaults to `~/.config/echomesh` | ship `/etc/default/echo-node` + a tiny wrapper, or P1: CLI env discovery |

---

## 12. Onboarding hook

After start: `echo-node onboard` — 7 resumable stages
(identity → claim → join → profile → marketplace → activate), persisted in
`config.toml` as `onboard_stage`.

Verified behavior relevant to installers:

- **Step 4 only spawns the daemon if it isn't reachable** — under Flow A the
  unit is already up, so onboard connects instead of double-spawning
  (`src/cli/onboarding.rs:219`).
- Marketplace env knobs (`BID_AUTO`, price) are passed at *spawn* time — if
  the daemon predates step 6, print `restart` hint (code already does:
  `run echo-node restart` to apply marketplace settings`).
- Flow B: `echo-node onboard` works as-is; CI/non-interactive: `--offline`
  + `--status` + `--reset` (already implemented ✅).
- Exit summary prints provider id (`provider_<8-hex>`) — feeds the
  installer's final `Node ID:` line.

---

## 13. Update mechanism (P1 — design only)

```text
echo-node update
  → check release feed (GitHub API, latest stable)
  → no-op if current == latest (compare `echo-node --version`)
  → download to $TMP + sha256 + minisign verify (same code as install.sh)
  → atomic replace: write `echo-node.new` beside current binary, fsync, rename(2)
  → restart: systemctl restart echo-node | echo-node restart (flow-dependent)
  → verify health (GET /health) → report; on failure auto-rollback .old
```

Requirements: atomicity (no window with a half-written binary), rollback
(`echo-node` keeps `<binary>.old`), minimal downtime (rename is atomic; the
running process keeps its inode until restart), version tracking (single
source: `--version`).

---

## 14. Docker distribution (P1)

```dockerfile
# build
FROM rust:1.xx-slim AS build
WORKDIR /src && COPY . .
RUN cargo build --release --locked
# runtime
FROM gcr.io/distroless/cc-debian12   # no shell, nonroot
COPY --from=build /src/target/release/echo-node /usr/local/bin/echo-node
USER 1000
ENV ECHOMESH_HOME=/data
VOLUME /data
EXPOSE 3001 3002
HEALTHCHECK CMD ["/usr/local/bin/echo-node", "doctor", "--json"]   # doctor needs a --json mode (P2)
ENTRYPOINT ["echo-node", "daemon"]
```

Run:

```bash
docker run -d --name echo-node --restart unless-stopped \
  -p 127.0.0.1:7777:7777 -p 3002:3002 \
  -v echo-data:/data \
  ghcr.io/echo-mesh/echo-node:v0.3.0
```

Volume strategy: one volume at `ECHOMESH_HOME=/data` (identity + state.db +
logs — everything that must survive). Resource limits: `--memory 512m
--cpus 1`. Never bake identity into the image.

---

## 15. Operator UX (current reality vs target)

Existing commands are operator-complete except `update` (§13):

```bash
$ echo-node status                 # verified shape: daemon/uptime/sessions/quality
$ echo-node doctor                 # config home, identity perms, config parse,
                                   # sqlite, pid/process, health, port, logs dir
$ echo-node logs tail --follow     # (target alias: `logs -f`)
$ echo-node identity               # peer id, keys, provider id
$ echo-node marketplace auctions   # + bid / awards
$ echo-node wallet                 # settlement totals (scope-aware 401 hint)
```

`echo-node --version` → `echo-node 0.3.0` (clap default — do not “fix”).
Installer output shows `v0.3.0`.

---

## 16. Threat model (installer)

| Threat | Vector | Mitigation |
|--------|--------|-----------|
| Forged binary | compromised CDN/GitHub asset | sha256 **and** minisign signature; embedded pubkey; fail-closed |
| Malicious install script | DNS/TLS compromise of install host | HTTPS + published script hash on second channel; keep script <200 LoC auditable |
| Downgrade attack | attacker serves old vulnerable release | installer defaults to *latest*; pinned `--version` requires explicit operator intent; document CVE floor |
| Shell injection | attacker-controlled filenames/redirects | fixed artifact mapping, `set -euo pipefail`, full quoting, no `eval` |
| Privilege escalation via unit | writable service paths | `chown echomesh` + `ProtectSystem=strict` + `ReadWritePaths` + empty `CapabilityBoundingSet` |
| Identity theft | world-readable keys | dir umask 077, `identity.json` 0600 (enforced in code ✅ + checked by `doctor`) |
| Path hijack | `PATH` tampering pre-install | installer uses absolute paths for `curl`/`sha256sum`/`install`; validates `$PREFIX` |
| State loss on upgrade | overwrite config/identity | config never overwritten without `--force`; atomic binary replace only |

---

## 17. Phased roadmap & deliverables

**P0 — the one-command milestone**
1. ✅ `[[bin]] name = "echo-node"` + all doc/CI references renamed
2. `.github/workflows/release.yml` — tag matrix (amd64 + arm64), tarballs,
   `checksums.txt`, minisign sign job, GitHub Release
3. `scripts/install.sh` — §8, flows A + B, fail-closed verification
4. `packaging/echo-node.service` — §11.1 hardened unit
5. Port decision (§10.1) implemented + documented
6. README “Install” quick-start updated to the curl one-liner (keep manual
   build as fallback)

**P1 — lifecycle**
7. `echo-node update` (§13) with atomic replace + rollback
8. CLI ↔ systemd detection: `stop`/`start`/`logs -f` (§11.3)
9. `linux-armv7` target (cross), Docker image (§14)
10. Signed releases surfaced on the docs site (script hash channel)

**P2 — polish**
11. XDG data/state split in `EchoHome` (§9) — real `~/.local/share` layout
12. `doctor --json` + DHT/ledger/marketplace health checks
13. cosign/sigstore for container images; windows/darwin targets

Every phase keeps Flow B (Termux/native) working — the development platform
for this repository is an **aarch64 Termux box**, so `linux-arm64` is the
primary target in practice, not an afterthought.
