# Echo Node — Agent Guide

## Build & Test

Single Rust crate (`echo-daemon`), edition 2021, no workspace.

```sh
cargo check                       # type-check only (fast)
cargo clippy --all-targets        # lint
cargo test                        # 191 tests (146 lib, 20 main, 4 bidding e2e, 15 integration, 3 tunnel e2e, 2 dht market e2e, 1 consumer e2e)
cargo build                       # compile
```

- CI: `.github/workflows/ci.yml` runs check, clippy (`-D warnings`), test, and a
  release build (uploads the binary as an artifact) on push/PR — use it instead
  of building locally when disk/CPU is scarce (e.g. Termux).
- `sqlx` is a required dependency (SQLite + PostgreSQL), currently **0.8** (lockfile 0.8.6). There is no `persistence` feature flag anymore — if you see DB-related compile errors, the issue is elsewhere.
- `cargo test --no-run` can be slow on first build (~30s) due to dependency compilation.
- No `rustfmt.toml` or `clippy.toml` — uses all defaults.

## Architecture

| Module | Lines | Purpose |
|--------|-------|---------|
| `main.rs` | 2829 | Entry point, REST API (axum), heartbeat, background tasks, `NODE_MODE` gating |
| `tunnel.rs` | 1230 | Noise_XX handshake (deadline-guarded), bidirectional relay, policy admission, consumer-side `connect_to_provider` |
| `bidding.rs` | 1846 | Marketplace price discovery: sealed-bid auctions, quality-adjusted scoring, Vickrey/first-price awards, 2s deadline sweeper (`BID_*` env); G6 wire types (`AuctionRecord`, auction-index encoding), `AuctionBook`, signed-bid verify, foreign-award import |
| `consumer.rs` | 125 | Consumer forward listener: local apps → provider tunnel |
| `neon.rs` | 578 | PostgreSQL backend (runs `migrations/postgres/*.sql`) |
| `sqlite_store.rs` | 568 | SQLite backend (runs `migrations/sqlite/*.sql`) |
| `migrator.rs` | 408 | Versioned SQL migrations: SQL splitter, ordered apply, `schema_migrations` tracking |
| `meter.rs` | 524 | Token bucket rate limiter + signed receipt engine |
| `models.rs` | 341 | Data structs, all `Serialize + Deserialize`, receipt signature verify, settlement records |
| `settlement.rs` | 154 | Payout engine: verified receipts → idempotent settlement rows (bytes + USD); `settle_receipt_at_price` honors award-locked market prices |
| `availability.rs` | 517 | Capacity engine + EWMA estimator (`AVAIL_EWMA_ALPHA`), session slot tracking |
| `discovery.rs` | 528 | libp2p Kademlia DHT swarm (`DHT_*` env: bootstrap peers, network-id namespace, local-only mode); command channel (`DiscoveryCommand`) + result events (`DiscoveryEvent`); forced `kad::Mode::Server` so inbound record storage works without a confirmed external address |
| `auth.rs` | 414 | Scoped API keys (`API_KEYS` JSON + legacy `API_KEY`), `require_scope` axum middleware |
| `policy.rs` | 754 | Provider admission policy (`POLICY_*` env): reputation/latency gates + per-peer/region quotas (RAII permits), idle-time trust decay, soft-slashing gate |
| `security.rs` | 523 | `SecurityCounters` — failure counters + bounded event ring + sliding-window burst alerts (`SECURITY_ALERT_*`) + per-peer violation attribution, surfaced in `/status`, `/health`, `GET /security` |
| `accountability.rs` | 167 | Soft-slashing: `soft_slash_score` = decayed reputation × violation discount × availability factor (no on-chain staking); gates `POLICY_MIN_ACCOUNTABILITY` |
| `identity.rs` | 340 | Ed25519 + X25519 keypairs, file permissions, atomic writes + corrupt-file recovery |
| `telemetry.rs` | 1052 | Optional dashboard bridge: heartbeat frames + session-close events → Supabase `report-telemetry` edge function; 32-dim feature vector, UUID node id, non-blocking bounded channel (`ReporterMessage`), durable disk spool + exponential backoff + shutdown flush |
| `lib.rs` | 143 | `MetricsBackend` trait (17 methods), base64 utils, `constant_time_eq`, module declarations |

**Data flow:** `main.rs` wires everything. `MetricsBackend` trait abstracts storage — choose Neon or SQLite at startup based on `DATABASE_URL`.

Migrations live in `migrations/{postgres,sqlite}/*.sql` (numbered, applied in order) and are embedded at compile time via `include_str!`. `migrator.rs` tracks applied versions in a `schema_migrations` table.

## Backend Selection

The `DATABASE_URL` env var controls the backend:
- **Set** → `NeonStore` (PostgreSQL via `sqlx`)
- **Unset or empty** → `SqliteStore` (SQLite via `sqlx`)

Both implement `MetricsBackend`. Each runs versioned migrations on startup:
- **Postgres:** strict — every statement must succeed; each migration applies atomically in a transaction.
- **SQLite:** same, except migration statements that fail only with SQLite's `duplicate column name` error are tolerated (legacy column backfills, since SQLite lacks `ALTER TABLE ADD COLUMN IF NOT EXISTS`).

A migration that fails for any other reason is **not recorded** as applied and is retried on the next boot.

## Key Gotchas

### Lock hierarchy
The heartbeat in `main.rs` uses a strict 3-phase pattern to avoid nested locks:
1. `availability.write()` → compute stats → **release**
2. `state.read()` → check running, clone ping targets → **release**
3. `spawn_blocking(ping)` → `state.write()` → upsert to DB → **release**

If you modify the heartbeat or add new background tasks, never hold `state` while acquiring `availability`.

### Double-end-session bug (already fixed)
`end_session` is called only in `tunnel.rs:handle_incoming_connection` (provider side). The tunnel event handler in `main.rs` does NOT call it — the comments at lines 1377 and 1406 warn about this.

### Migration failures are retried
Migrations apply inside a per-file transaction. Postgres is strict; SQLite tolerates only the exact `duplicate column name` error class (legacy idempotent column backfills). Any other failure rolls back and leaves the migration unrecorded — it is retried on the next boot, so never "skip" a broken migration by ignoring the log line.

### Dev profile is non-default
`Cargo.toml` sets `[profile.dev] opt-level = 1` (not 0). Debug builds are partially optimized for performance (needed for `sysinfo` and libp2p to run sanely).

### Release profile
Thin LTO, `codegen-units = 1`. Expect longer compile times for `--release`.

## Environment Variables

Critical ones beyond README basics:
- `DATABASE_URL` — **controls which backend is used** (Neon vs SQLite)
- `NODE_MODE` — `provider` (default: tunnel listener + capability publisher + receipt settlement) or `consumer` (forward listener on `CONSUMER_LISTEN_ADDR`; requires `PROVIDER_ADDR`, `PROVIDER_PEER_ID`, and `PROVIDER_NOISE_PUBKEY` — base64, decodes to exactly 32 bytes). In consumer mode the tunnel listener, capability publisher, and receipt settlement task are not started.
- `TUNNEL_ADDR` — must match what consumers will connect to (default `0.0.0.0:3002`)
- `IDENTITY_PATH` — persists node keys; old identity files auto-migrate to include Noise keys
- `API_KEY` / `API_KEYS` — auth for `/control` + `/settlements`. `API_KEY` (legacy) grants admin (all scopes); `API_KEYS` is a JSON array of `{"token": "...", "name": "...", "scopes": ["control"|"settlements"]}`. Both unset → auth disabled (open control plane).
- `POLICY_*` — provider admission policy: `POLICY_MIN_REPUTATION` (default 0.0), `POLICY_MIN_ACCOUNTABILITY` (default 0.0; soft-slashing composite score gate), `POLICY_MAX_LATENCY_MS` (default 0 = off), `POLICY_MAX_SESSIONS_PER_PEER` (0 = off), `POLICY_MAX_SESSIONS_PER_REGION` (0 = off), `POLICY_BLOCKED_PEERS` (comma-separated peer ids). **All default to open** — with no constraint set the policy engine stays out of the relay hot path entirely.
- `REPUTATION_DECAY_HALF_LIFE_DAYS` — idle-time trust decay for the reputation gate (default 30, `0` = off); `score · 0.5^(idle/half_life)` is what the min-reputation gate compares.
- `AVAIL_EWMA_ALPHA` — availability estimator smoothing (default 0.3, must be in (0,1]; `1.0` = raw passthrough).
- `MATCH_MAX_STALE_SECS` — `/match` staleness filter for capability records (default 300, `0` = off).
- `TUNNEL_HANDSHAKE_TIMEOUT_MS` — handshake deadline for the preamble + every Noise handshake read + the consumer dial (default 10000, `0` = off).
- `SECURITY_ALERT_THRESHOLD` / `SECURITY_ALERT_WINDOW_SECS` — burst detector: threshold events inside the window trip one `error!` + `alert_active` (defaults 10 / 60; threshold `0` disables).
- `BID_*` — marketplace bidding: `BID_WINDOW_SECS` (default auction window, default 30), `BID_W_REPUTATION` / `BID_W_LATENCY` (quality-score weights, 0.6 / 0.4), `BID_REF_LATENCY_MS` (latency half-score point, 100), `BID_PRICING` (`vickrey` default = winner paid `max(own, runner-up)`; `first` = pay-your-bid), `BID_AUTO` (provider mode: auto-bid on open auctions, default `false`), `BID_PRICE_USD_PER_GB` (auto-bid ask, defaults to `SETTLEMENT_RATE_USD_PER_GB`), `BID_POLL_SECS` (auto-bid poll, default 3). Auctions are in-memory (swept every 2 s); awards override the flat settlement rate per counterparty. G6: open auctions + award notices propagate over the DHT (`auction/{id}`, `auctions_by_host/{host}`, `auctions/{region}`); remote providers submit **signed** HTTP bids; the auto-bid task also discovers remote auctions (exact `NODE_REGION` match).
- `DHT_*` — discovery: `DHT_LISTEN_ADDR` (default `/ip4/0.0.0.0/tcp/0`), `DHT_BOOTSTRAP_PEERS` (comma-separated multiaddrs each ending `/p2p/<peer-id>`), `DHT_NETWORK_ID` (default `echo`; namespaces the Kademlia protocol to `/<id>/kad/1.0.0`), `DHT_LOCAL_ONLY` (`true` = no bootstrap, dev mode).
- `API_ADVERTISE_ADDR` — (unset) publicly reachable `http://host:port` base URL of this node's REST API, published inside DHT auction records so remote bidders can reach it; falls back to detected public IP + `LISTEN_ADDR` port, otherwise auction publishing waits.
- `DASHBOARD_TELEMETRY_URL` + `DASHBOARD_TELEMETRY_TOKEN` + `DASHBOARD_USER_ID` — enable the opt-in heartbeat → dashboard bridge (`telemetry.rs`); `DASHBOARD_NODE_ID` pins the dashboard node UUID. See `GAPS.md`. The bridge is fully off by default and never blocks the heartbeat.
- `DASHBOARD_TELEMETRY_SPOOL` (default `data/telemetry_spool.jsonl`) + `DASHBOARD_TELEMETRY_MAX_QUEUE` (default 256) + `DASHBOARD_TELEMETRY_BACKOFF_BASE_MS` (500) / `..._MAX_MS` (30000) — durable frame queue: overflow drops oldest, failures back off exponentially with jitter, unsent frames are flushed to the spool on shutdown and replayed on next boot.
- `RUST_LOG` — default `echo_daemon=info`; set to `debug` for verbose output

## Dashboard Sync (not a cargo target)

`scripts/echo-sync.sh` is the **live** dashboard path: polls local `GET /metrics`
and POSTs to the Supabase `daemon-push` edge function every ~30s
(`ECHO_NODE_TOKEN=<nodes.ingest_token>`, header `x-node-token`). The daemon
never calls Supabase itself; the `DASHBOARD_*` telemetry bridge is separate
and off by default. Run it under a `while true` supervisor loop. See
`GAPS.md` + README “Dashboard Sync”.

## Testing

- `src/` has pure unit tests (146 total: meter 22, bidding 29, policy 15, availability 15, telemetry 13, security 10, migrator 8, auth 8, accountability 6, models 6, settlement 1, discovery 5, tunnel 4, identity 4, …). No external services required.
- `tests/integration.rs` has 15 tests against a real SQLite store (tempfile-based): migration lifecycle, CRUD, concurrency, identity persistence, receipt round-trips, settlement.
- `tests/bidding_e2e.rs` has 4 tests covering the market pipeline: auction → sealed bids → deadline sweep → Vickrey award → settlement at the locked price (vs flat-rate fallback), budget-cap/requester guards, clean no-bid expiry, pricing-rule parsing.
- `tests/dht_market_e2e.rs` has 2 tests running two in-process libp2p swarms (local-only, unique `network_id` per run): an auction record (index + record) propagates host→peer over Kademlia, and an award record reaches the winning provider. No external network required.
- `tests/tunnel_e2e.rs` runs an in-process Noise_XX tunnel end-to-end: handshake, encrypted relay through an echo target, metering receipts, provider-side DB teardown (connection history, reputation), a slowloris test (stalled handshake → deadline close + counter), and a soft-slashing test (`min_accountability` gate denies post-handshake, counts the rejection, attributes a per-peer violation).
- `tests/consumer_e2e.rs` runs the full consumer-mode path: `run_consumer_listener` → `connect_to_provider` → relay to an echo target; asserts echo round-trip, availability slot acquire/release balance, provider connection history + signed receipt, and that the consumer never persists its own receipts.
- `src/main.rs` has 20 tests: constant-time comparison, bps→Mbps, quality/trust scores, epoch sanity, ping RTT parsing, bandwidth env sanitization, `/match` request validation + provider filtering.
- To run a single test: `cargo test test_name` (e.g., `cargo test constant_time_eq`).
- Env-var tests must be **one consolidated test per var family** (cargo runs tests in parallel threads in the same process); use unique keys where possible (`ECHO_TEST_*`).

## OpenCode Config

- `opencode.json` references `README.md` for instructions.
- Agent definitions live in `.opencode/agent/build.md` (build agent) and `.opencode/agent/review.md` (review subagent, read-only).
- The review agent is configured with `permission.edit: "deny"` — it only reports, never modifies files.
