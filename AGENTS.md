# Echo Node — Agent Guide

## Build & Test

Single Rust crate (`echo-daemon`), edition 2021, no workspace.

```sh
cargo check                       # type-check only (fast)
cargo clippy --all-targets        # lint
cargo test                        # 24 unit tests (13 in main.rs, 11 in meter.rs)
cargo build                       # compile
```

- `sqlx` is behind the `persistence` Cargo feature (on by default). If you see DB-related compile errors, verify the feature is enabled.
- `cargo test --no-run` can be slow on first build (~30s) due to dependency compilation.
- Future-incompat warning exists for `sqlx-postgres v0.7.4` — ignore for now, it doesn't block compilation.
- No `rustfmt.toml` or `clippy.toml` — uses all defaults.

## Architecture

| Module | Lines | Purpose |
|--------|-------|---------|
| `main.rs` | 1175 | Entry point, REST API (axum), heartbeat, 8 background tasks |
| `tunnel.rs` | 800 | Noise_XX handshake, bidirectional relay, backpressure |
| `neon.rs` | 629 | PostgreSQL backend (inline DDL on startup) |
| `sqlite_store.rs` | 595 | SQLite backend (inline DDL on startup) |
| `meter.rs` | 240 | Token bucket rate limiter + signed receipt engine |
| `availability.rs` | 224 | Capacity engine — computes advertised bandwidth |
| `discovery.rs` | 197 | libp2p Kademlia DHT swarm |
| `identity.rs` | 155 | Ed25519 + X25519 keypairs, file permissions |
| `models.rs` | 161 | 9 data structs, all `Serialize + Deserialize` |
| `lib.rs` | 52 | `MetricsBackend` trait (13 methods), module declarations |

**Data flow:** `main.rs` wires everything. `MetricsBackend` trait abstracts storage — choose Neon or SQLite at startup based on `DATABASE_URL`.

## Backend Selection

The `DATABASE_URL` env var controls the backend:
- **Set** → `NeonStore` (PostgreSQL via `sqlx`)
- **Unset or empty** → `SqliteStore` (SQLite via `sqlx`)

Both implement `MetricsBackend`. Schema is created inline on startup (no migration tool).

## Key Gotchas

### Lock hierarchy
The heartbeat in `main.rs` uses a strict 3-phase pattern to avoid nested locks:
1. `availability.write()` → compute stats → **release**
2. `state.read()` → check running, clone ping targets → **release**
3. `spawn_blocking(ping)` → `state.write()` → upsert to DB → **release**

If you modify the heartbeat or add new background tasks, never hold `state` while acquiring `availability`.

### Double-end-session bug (already fixed)
`end_session` is called only in `tunnel.rs:handle_incoming_connection` (provider side). The tunnel event handler in `main.rs` does NOT call it — the comment at line 921-922 warns about this.

### Dev profile is non-default
`Cargo.toml` sets `[profile.dev] opt-level = 1` (not 0). Debug builds are partially optimized for performance (needed for `sysinfo` and libp2p to run sanely).

### Release profile
Thin LTO, `codegen-units = 1`. Expect longer compile times for `--release`.

## Environment Variables

Critical ones beyond README basics:
- `DATABASE_URL` — **controls which backend is used** (Neon vs SQLite)
- `TUNNEL_ADDR` — must match what consumers will connect to (default `0.0.0.0:3002`)
- `IDENTITY_PATH` — persists node keys; old identity files auto-migrate to include Noise keys
- `RUST_LOG` — default `echo_daemon=info`; set to `debug` for verbose output

## Testing

- All tests are pure unit tests in `src/` — no external deps, no integration tests.
- `src/main.rs` has 13 tests: constant-time comparison, bps→Mbps, quality/trust scores, epoch sanity.
- `src/meter.rs` has 11 tests: base64 encoding, token bucket logic.
- To run a single test: `cargo test test_name` (e.g., `cargo test constant_time_eq`).

## OpenCode Config

- `opencode.json` references `README.md` for instructions.
- Agent definitions live in `.opencode/agent/build.md` (build agent) and `.opencode/agent/review.md` (review subagent, read-only).
- The review agent is configured with `permission.edit: "deny"` — it only reports, never modifies files.
