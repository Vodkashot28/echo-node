# Echo Node — Agent Guide

## Build & Test

Single Rust crate (`echo-daemon`), edition 2021, no workspace.

```sh
cargo check                       # type-check only (fast)
cargo clippy --all-targets        # lint
cargo test                        # 62 tests (32 lib, 13 main, 15 integration, 1 tunnel e2e, 1 consumer e2e)
cargo build                       # compile
```

- `sqlx` is a required dependency (SQLite + PostgreSQL), currently **0.8** (lockfile 0.8.6). There is no `persistence` feature flag anymore — if you see DB-related compile errors, the issue is elsewhere.
- `cargo test --no-run` can be slow on first build (~30s) due to dependency compilation.
- No `rustfmt.toml` or `clippy.toml` — uses all defaults.

## Architecture

| Module | Lines | Purpose |
|--------|-------|---------|
| `main.rs` | 1737 | Entry point, REST API (axum), heartbeat, background tasks, `NODE_MODE` gating |
| `tunnel.rs` | 941 | Noise_XX handshake, bidirectional relay, backpressure, consumer-side `connect_to_provider` |
| `consumer.rs` | 125 | Consumer forward listener: local apps → provider tunnel |
| `neon.rs` | 548 | PostgreSQL backend (runs `migrations/postgres/*.sql`) |
| `sqlite_store.rs` | 538 | SQLite backend (runs `migrations/sqlite/*.sql`) |
| `migrator.rs` | 408 | Versioned SQL migrations: SQL splitter, ordered apply, `schema_migrations` tracking |
| `meter.rs` | 355 | Token bucket rate limiter + signed receipt engine |
| `models.rs` | 341 | Data structs, all `Serialize + Deserialize`, receipt signature verify, settlement records |
| `settlement.rs` | 118 | Payout engine: verified receipts → idempotent settlement rows (bytes + USD) |
| `availability.rs` | 271 | Capacity engine — computes advertised bandwidth, session slot tracking |
| `discovery.rs` | 214 | libp2p Kademlia DHT swarm |
| `identity.rs` | 154 | Ed25519 + X25519 keypairs, file permissions |
| `telemetry.rs` | 604 | Optional dashboard bridge: heartbeat frames + session-close events → Supabase `report-telemetry` edge function; 32-dim feature vector, UUID node id, non-blocking bounded channel (`ReporterMessage`) |
| `lib.rs` | 123 | `MetricsBackend` trait (16 methods), base64 utils, module declarations |

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
`end_session` is called only in `tunnel.rs:handle_incoming_connection` (provider side). The tunnel event handler in `main.rs` does NOT call it — the comments at lines 1205 and 1234 warn about this.

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
- `DASHBOARD_TELEMETRY_URL` + `DASHBOARD_TELEMETRY_TOKEN` + `DASHBOARD_USER_ID` — enable the opt-in heartbeat → dashboard bridge (`telemetry.rs`); `DASHBOARD_NODE_ID` pins the dashboard node UUID. See `GAPS.md`. The bridge is fully off by default and never blocks the heartbeat.
- `RUST_LOG` — default `echo_daemon=info`; set to `debug` for verbose output

## Dashboard Sync (not a cargo target)

`scripts/echo-sync.sh` is the **live** dashboard path: polls local `GET /metrics`
and POSTs to the Supabase `daemon-push` edge function every ~30s
(`ECHO_NODE_TOKEN=<nodes.ingest_token>`, header `x-node-token`). The daemon
never calls Supabase itself; the `DASHBOARD_*` telemetry bridge is separate
and off by default. Run it under a `while true` supervisor loop. See
`GAPS.md` + README “Dashboard Sync”.

## Testing

- `src/` has pure unit tests (32 total: meter, models, migrator, main, telemetry). No external services required.
- `tests/integration.rs` has 15 tests against a real SQLite store (tempfile-based): migration lifecycle, CRUD, concurrency, identity persistence, receipt round-trips, settlement.
- `tests/tunnel_e2e.rs` runs an in-process Noise_XX tunnel end-to-end: handshake, encrypted relay through an echo target, metering receipts, and provider-side DB teardown (connection history, reputation).
- `tests/consumer_e2e.rs` runs the full consumer-mode path: `run_consumer_listener` → `connect_to_provider` → relay to an echo target; asserts echo round-trip, availability slot acquire/release balance, provider connection history + signed receipt, and that the consumer never persists its own receipts.
- `src/main.rs` has 13 tests: constant-time comparison, bps→Mbps, quality/trust scores, epoch sanity.
- To run a single test: `cargo test test_name` (e.g., `cargo test constant_time_eq`).

## OpenCode Config

- `opencode.json` references `README.md` for instructions.
- Agent definitions live in `.opencode/agent/build.md` (build agent) and `.opencode/agent/review.md` (review subagent, read-only).
- The review agent is configured with `permission.edit: "deny"` — it only reports, never modifies files.
