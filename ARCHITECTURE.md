# Architecture Deep Dive

Detailed architectural analysis of the Echo Node daemon. For the high-level overview, see [README.md](README.md).

## Table of Contents

- [Component Inventory](#component-inventory)
- [Data Flow Diagrams](#data-flow-diagrams)
- [Wire Protocol](#wire-protocol)
- [Concurrency Model](#concurrency-model)
- [Security Analysis](#security-analysis)
- [Database Schema](#database-schema)
- [Error Handling Patterns](#error-handling-patterns)
- [Background Tasks](#background-tasks)

## Component Inventory

### `lib.rs` — Crate Root (123 lines)

Declares 12 modules plus shared base64 utilities and defines the `MetricsBackend` async trait with 16 methods.

```
pub mod { availability, consumer, discovery, identity, meter, migrator, models, neon, settlement, sqlite_store, telemetry, tunnel }
pub use models::*          // blanket re-export of all data models
pub use neon::NeonStore    // PostgreSQL backend
pub use sqlite_store::SqliteStore  // SQLite backend
```

**Shared utilities:**
- `base64_encode` / `base64_decode` — hand-rolled (avoids another dependency), used by receipt signing and backends
- `NOISE_PARAMS` — `"Noise_XX_25519_ChaChaPoly_BLAKE2s"` shared by identity and tunnel

**`MetricsBackend` trait** — the storage abstraction layer:
- `upsert_node` / `insert_node_metrics` / `insert_network_metrics` / `insert_node_intelligence`
- `cleanup_old_metrics` — purges data older than N days
- `upsert_peer_reputation` / `get_peer_reputation` — accumulative reputation
- `insert_connection` / `update_connection_end` — connection lifecycle
- `insert_receipt` / `get_receipts_for_session` — metering receipt persistence
- `record_settlement` / `get_settlements` / `settlement_summary` — payout accumulation
- `upsert_capability` / `get_capabilities_in_region` — DHT discovery support

Both `NeonStore` and `SqliteStore` implement this trait, allowing the daemon to switch backends at startup based on the `DATABASE_URL` env var.

---

### `models.rs` — Data Models (341 lines)

All 10 structs derive `Debug, Clone, Serialize, Deserialize`. They fall into three categories:

**Legacy models** (original EchoMesh schema):
| Struct | Purpose | Fields |
|--------|---------|--------|
| `NodeRow` | Provider node identity + metrics | 22 fields (id, name, user_id, peer_id, public_key, ip_address, region, status, uptime, latency, bandwidth_used, quality_score, earnings, packet_loss, trust_score, etc.) |
| `NodeMetricsRow` | Per-heartbeat metric snapshot | node_id, user_id, latency_ms, bandwidth_down/up, packet_loss_pct, quality_score, earnings_usd |
| `NetworkMetricsRow` | Network-wide aggregate metrics | user_id, active_nodes, avg_latency, bandwidth_egress/ingress, packet_loss_pct, uptime_pct |
| `NodeIntelligenceRow` | ML/anomaly detection features | quality_score, trust_score, anomaly_score, is_anomalous, cluster_id, feature_vector |

**Peer-centric models** (added for P2P infrastructure):
| Struct | Purpose | Fields |
|--------|---------|--------|
| `PeerReputation` | Accumulative reputation tracking | peer_id, reputation_score, total_bytes_relayed, successful/failed_sessions, avg_latency_ms |
| `ConnectionHistory` | Per-session connection record | local/remote_peer_id, remote_ip/port, direction, bytes_sent/received, duration, exit_reason |
| `StateReceipt` | Signed cryptographic receipt | receipt_id, session_id, signer/counterparty_peer_id, bytes_transferred, signature |
| `SettlementRecord` | Per-receipt payout entry | receipt_id (PK), signer/counterparty_peer_id, bytes_settled, earnings_usd, settled_at |
| `SettlementSummary` | Aggregated payout totals | receipts_settled, total_bytes_settled, total_earnings_usd |
| `CapabilityDescriptor` | DHT-published capacity info | peer_id, public_key, region, upload/download_cap_mbps, noise_public_key, max/active_sessions |
| `SystemStats` | Snapshot of system metrics | cpu_pct, memory_pct, disk_usage_pct, active_sessions, current_usage_rx/tx_mbps |

---

### `identity.rs` — Cryptographic Identity (154 lines)

Manages the node's long-lived key material: ed25519 (signing), libp2p PeerId, and X25519 (Noise_XX tunnel encryption).

```rust
pub struct NodeIdentity {
    pub peer_id: String,           // libp2p PeerId from ed25519
    pub keypair_bytes: Vec<u8>,    // ed25519 secret key (32 bytes)
    pub public_key_bytes: Vec<u8>, // ed25519 public key (32 bytes)
    pub region: String,
    pub noise_secret_key: Vec<u8>, // X25519 secret key (32 bytes)
    pub noise_public_key: Vec<u8>, // X25519 public key (32 bytes)
}
```

**Key behaviors:**
- `load_or_generate()` — reads from disk or creates new; auto-migrates legacy identities that lack Noise keys
- **File permissions 0600** — applied via `#[cfg(unix)] set_permissions()` after every write
- `sign()` / `verify()` — ed25519 operations used for receipt signing
- `libp2p_keypair()` — converts stored bytes into `libp2p::identity::Keypair`

---

### `discovery.rs` — DHT Peer Discovery (214 lines)

Wraps a libp2p swarm with Kademlia + Identify protocols.

```rust
pub struct DiscoveryService {
    swarm: Swarm<Behaviour>,
    event_tx: mpsc::UnboundedSender<DiscoveryEvent>,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
}

pub enum DiscoveryEvent {
    PeerFound(PeerId, Multiaddr),
    PeerDisconnected(PeerId),
    CapabilityPublished,
}
```

**Bootstrap strategy:**
- 4 default public libp2p bootstrap nodes (`bootstrap.libp2p.io`)
- `kademlia.bootstrap()` called on startup
- Identify protocol string: `"echo-dht/0.3.0"`
- Idle connection timeout: 60 seconds

**Event loop (`run`):**
- Forwards `GetProviders` results as `PeerFound` events
- Handles `PutRecord` success/failure logging
- Logs new listen addresses

**Design note:** `kademlia.addresses_of_peer()` is not available in libp2p-kad 0.46. The `PeerFound` event carries `Multiaddr::empty()` — actual peer addresses are resolved through the Identify protocol when a connection is initiated.

---

### `tunnel.rs` — Noise_XX Encrypted Tunnel (941 lines)

The largest and most complex module. Handles the full tunnel lifecycle: pre-handshake exchange, Noise_XX handshake, bidirectional encrypted relay, and session teardown.

**Constants:**
```rust
const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
const PROTOCOL_PROLOGUE_PREFIX: &[u8] = b"EchoMesh|v1|";
const NOISE_MAX_MSG_LEN: usize = 65535;
```

**Key structs:**

```rust
pub struct EncryptedTunnelSession {
    pub transport: Arc<tokio::sync::Mutex<TransportState>>,
    pub stream: TcpStream,
    pub session_id: String,
    pub remote_peer_id: libp2p::PeerId,
    _permit: OwnedSemaphorePermit,   // RAII: released on drop
}

pub struct TunnelService {
    local_peer_id: libp2p::PeerId,
    event_tx: mpsc::UnboundedSender<TunnelEvent>,
    metering: MeteringEngine,
    availability: Arc<RwLock<AvailabilityEngine>>,
    relay_config: RelayConfig,
    active_sessions: Arc<RwLock<HashMap<String, ()>>>,
    conn_semaphore: Arc<Semaphore>,
    static_private_key: Vec<u8>,
}

pub enum TunnelEvent {
    SessionEstablished { session_id: String, remote_peer_id: libp2p::PeerId },
    SessionClosed {
        session_id: String,
        bytes_sent: u64,
        bytes_received: u64,
        // Full provider-side teardown accounting (remote peer/ip/port, start/end,
        // duration, avg latency, exit_reason) — `Some` on the provider, `None`
        // reserved for consumer-side closes. Federated to the dashboard's
        // `sessions` table by `telemetry.rs`.
        details: Option<SessionCloseDetails>,
    },
}
```

**Dual role design:** `TunnelService` is mode-agnostic. In provider mode `accept_incoming` awaits handshakes; in consumer mode `connect_to_provider` initiates them (the same service instance backs both roles — see `consumer.rs`).

- `connect_to_provider(remote_addr, remote_peer_id, session_id, expected_noise_pubkey)` — consumer-side initiator: semaphore permit → TCP + session/peer pre-exchange → Noise_XX (Msg1/2/3) → **static-key pinning** against the expected X25519 key → acquires an availability slot (symmetric with the provider flow) → returns an `EncryptedTunnelSession`.
- `relay_data(session, target_addr)` — provider-style helper: connects to a target and relays with metering (`Some`).
- `relay_local_connection(session, app_stream)` — consumer-style helper: bridges the tunnel to an already-connected local app stream, relays with metering **disabled** (`None` — a consumer never issues authoritative receipts for forwarded traffic), then runs standard teardown (releases the availability slot).
- `encrypted_relay_with_metering(...)` — shared relay core; the metering argument is `Option<&MeteringEngine>` exactly to support the two roles above.

---

### `consumer.rs` — Consumer Forward Listener (125 lines)

Runs the daemon in consumer mode (`NODE_MODE=consumer`): instead of advertising capacity and accepting inbound tunnels, the node runs a local TCP listener that bridges local application traffic into an encrypted tunnel to a configured provider.

```rust
pub struct ConsumerConfig {
    pub listen_addr: String,          // CONSUMER_LISTEN_ADDR (default 127.0.0.1:3003)
    pub provider_addr: String,        // PROVIDER_ADDR — provider's TUNNEL_ADDR
    pub provider_peer_id: libp2p::PeerId,  // PROVIDER_PEER_ID
    pub provider_noise_pubkey: Vec<u8>,    // PROVIDER_NOISE_PUBKEY (base64, pinned in handshake)
}
pub async fn run_consumer_listener(service: Arc<TunnelService>, config: ConsumerConfig) -> Result<()>
```

**Per-connection flow:** `listener.accept()` → spawn → `connect_to_provider` (full XX handshake + key pinning) → `relay_local_connection` (relay without metering) → teardown. Session ids are generated as `consumer-<seq>-<peer_suffix>` (unique, bounded below the 256-byte wire limit).

**Availability invariant:** `connect_to_provider` acquires a session slot and `relay_local_connection`'s teardown releases it exactly once — the availability engine tracks consumer sessions symmetrically with provider sessions.

---

### `meter.rs` — Token Bucket + Signed Receipts (355 lines)

**Token Bucket** — per-session rate limiter:

```rust
pub struct TokenBucket {
    capacity: f64,       // max burst (Mbps)
    tokens: f64,         // current tokens
    refill_rate: f64,    // tokens/sec (= Mbps)
    last_refill: Instant,
}
```

- `consume(requested_mbps)` — returns granted amount; auto-refills based on elapsed time
- `refill_rate()` — getter used by tunnel backpressure to compute sleep duration

**Metering Engine:**

```rust
pub struct MeteringEngine {
    identity: NodeIdentity,
    sessions: Arc<RwLock<HashMap<String, MeteredSession>>>,
    receipt_tx: mpsc::UnboundedSender<StateReceipt>,
}
```

- `start_session(session_id, remote_peer_id)` — registers a new metered session
- `record_transfer(session_id, direction, bytes)` — increments counters; issues receipt every 100 packets or >1MB
- `end_session(session_id)` — issues final receipt, removes session

**Receipt format:** `"{session_id}:{total_bytes}:{seq}:{timestamp}:{signer_peer_id}"`, signed with ed25519.

---

### `settlement.rs` — Payout Engine (118 lines)

Turns signed, verified receipts into idempotent payout accumulation.

```rust
pub struct SettlementEngine {
    identity: NodeIdentity,              // local node — verifies receipt signatures
    backend: Arc<dyn MetricsBackend>,    // writes to `settlements` table
    usd_per_gb: f64,                     // conversion rate (SETTLEMENT_RATE_USD_PER_GB)
}
```

**Behavior:**
- `settle_receipt(receipt)` — rejects receipts not signed by this node; verifies the ed25519 signature; computes `earnings = bytes / 1e9 * usd_per_gb`; persists a `SettlementRecord` keyed by `receipt_id`.
- **Idempotency:** backends return whether a row was actually inserted (`INSERT OR IGNORE` / `ON CONFLICT DO NOTHING`). Replaying an already-settled receipt is a no-op — double-counting is structurally impossible.
- `settlements(limit)` / `summary()` — expose recent entries and aggregate totals for the `GET /settlements` endpoint.

**Wiring (main.rs Step 9):** after a receipt passes signature verification and is persisted via `insert_receipt`, the same task feeds it to `settlement_engine.settle_receipt()`, including the shutdown drain path.

---

### `availability.rs` — Capacity Engine (271 lines)

Computes how much bandwidth to advertise to the DHT based on current system load.

```rust
pub struct AvailabilityEngine {
    system: System, networks: Networks, components: Components, disks: Disks,
    max_upload_mbps: f64, max_download_mbps: f64,
    max_sessions: u32, active_sessions: u32,
    current_usage_rx_bps: f64, current_usage_tx_bps: f64,
    rx_samples: Vec<(u64, Instant)>, tx_samples: Vec<(u64, Instant)>,
}
```

**Capacity computation (`refresh_and_compute`):**
1. `refresh()` — single `sysinfo` refresh + network rate calculation from 10-sample sliding window
2. Start with physical link capacity (`max_upload/download_mbps`)
3. Subtract local bandwidth usage (measured from network counters)
4. Apply CPU factor: `(1.0 - cpu_pct/100.0).max(0.1)` — floor at 10%
5. Apply Memory factor: `(1.0 - mem_pct/100.0).max(0.1)` — floor at 10%
6. Apply Session factor: `0.0` if at max, else `1.0 - (active/max * 0.3)` — up to 30% reduction

**Session management:**
- `try_acquire_session()` — increments counter, returns `Err` if at max (called by both provider accepts and consumer `connect_to_provider`)
- `release_session()` — decrements counter (via standard session teardown)
- `active_sessions()` — read-only getter (used by tests to verify slot balance)

---

### `neon.rs` — PostgreSQL Backend (548 lines)

Neon PostgreSQL implementation of `MetricsBackend`.

**Schema management:** Versioned SQL migrations from `migrations/postgres/*.sql`, applied by `migrator.rs` on startup inside per-file transactions and tracked in `schema_migrations`. Postgres migration SQL is strict (any failure aborts the run, so the migration is retried on the next boot).

**Connection:** `PgPool` with `max_connections(5)`, uses `$1, $2, ...` bind parameter syntax.

**Key implementation details:**
- `upsert_node` — 24-column `ON CONFLICT ... DO UPDATE SET` for all non-key columns
- `upsert_peer_reputation` — **accumulative**: `total_bytes_relayed = peer_reputation.total_bytes_relayed + EXCLUDED.total_bytes_relayed` (additive, not overwrite)
- `capability_descriptors.supported_encryption` — PostgreSQL `TEXT[]` array
- `cleanup_old_metrics` — deletes from 4 tables in a single transaction

---

### `sqlite_store.rs` — SQLite Backend (538 lines)

SQLite implementation of `MetricsBackend`.

**Schema management:** Versioned SQL migrations from `migrations/sqlite/*.sql`, applied by `migrator.rs` on startup inside per-file transactions and tracked in `schema_migrations`. SQLite is strict except for SQLite's `duplicate column name` error class (legacy idempotent column backfills — see `migrator.rs`).

**Connection:** `SqlitePool` with `max_connections(1)` (single writer), `create_if_missing(true)`.

**Key differences from Neon:**
- `supported_encryption` stored as JSON text (serialized/deserialized manually)
- `cleanup_old_metrics` uses `chrono::Utc::now()` for timestamp calculation
- SQLite `UPSERT` uses `excluded.` prefix (lowercase) vs PostgreSQL `EXCLUDED.` (uppercase)

---

### `migrator.rs` — Versioned SQL Migrations (408 lines)

Applies embedded, versioned SQL migrations on startup.

- SQL files are embedded at compile time via `include_str!` from `migrations/{postgres,sqlite}/`, each with an integer `version` and a name.
- A `schema_migrations` table records applied versions; `run_postgres` / `run_sqlite` skip applied migrations and apply pending ones **in order**.
- `split_sql` is a hand-rolled multi-statement splitter that handles `--`/`/* */` comments, single- and double-quoted strings, and PostgreSQL dollar-quoted bodies (`$$ ... $$`).
- **Transactions:** each migration applies atomically. A genuine failure aborts, rolls back, and leaves the migration **unrecorded** so the next boot retries it.
- **SQLite idempotency:** because SQLite lacks `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, only statements failing with `duplicate column name` are tolerated (legacy column backfills in migrations 002/003); any other error aborts the run.

**Upgrade rule:** never edit an already-shipped migration file — add a new numbered file. A broken migration is retried on the next boot, never silently skipped.

---

### `telemetry.rs` — Dashboard Bridge (604 lines)

Optional, off-by-default push bridge from the daemon to the echomesh
dashboard's `report-telemetry` edge function. Enabled only when
`DASHBOARD_TELEMETRY_URL` + `DASHBOARD_TELEMETRY_TOKEN` + `DASHBOARD_USER_ID`
are set (see `GAPS.md`, Slice 1/2).

**Key pieces:**
- `DashboardConfig::from_env()` — reads the `DASHBOARD_*` env vars;
  `DASHBOARD_NODE_ID` pins the dashboard node UUID (a fresh v4 UUID is
  generated per startup if unset).
- `DashboardReporter` — bounded channel (64) with a background posting task.
  `try_push` / `try_push_session` are **non-blocking** (the heartbeat never
  waits on HTTP); 10s timeout; errors are logged, best-effort only.
- `ReporterMessage::{Telemetry, Session}` — heartbeat frames and
  session-close events share one queue.
- `build_feature_vector()` — the **32-dim** feature vector the dashboard's
  `node_intelligence` schema expects, in migration order; real signal where
  the daemon has it, documented neutral baselines elsewhere — no random noise.
- `SessionEvent::from_close(...)` — maps `TunnelEvent::SessionClosed`'s
  `SessionCloseDetails` into an idempotent `sessions` upsert
  (`kind="session"`, `action="closed"`, estimated per-session USD).

**Note:** this is *not* the live dashboard path. The active forwarder is
`scripts/echo-sync.sh` → `daemon-push` (README “Dashboard Sync”); the
reporter ships with the code but stays dormant without env vars.

---

### `main.rs` — Orchestrator (1737 lines)

The entry point. Wires together all services and manages the daemon lifecycle.

**Data structures:**
- `DaemonMetrics` — 26-field struct serialized as JSON for `/metrics` (includes the identity tail `peer_id`, `region`, `ip_address` — the latter omitted from JSON until public-IP detection lands)
- `DaemonNode` — 20-field struct for `/nodes`
- `DaemonStatus` — 8-field struct for `/status`
- `HealthResponse` — 8-field struct for `/health`
- `MetricsState` — shared mutable state behind `Arc<RwLock<_>>`
- `Config` — env-driven runtime configuration (adds `node_mode`, `consumer_listen_addr`, `provider_addr`, `provider_peer_id`, `provider_noise_pubkey` on top of the provider settings)
- `AppState` — `{ metrics: Arc<RwLock<MetricsState>>, backend: Arc<dyn MetricsBackend>, settlement: SettlementEngine }`

**Mode gating (`NODE_MODE`):**
- **`provider`** (default) — spawns the incoming tunnel listener (`accept_incoming`), the capability publisher, and the receipt settlement task.
- **`consumer`** — validates `PROVIDER_ADDR` + `PROVIDER_PEER_ID` + `PROVIDER_NOISE_PUBKEY` (base64, must decode to 32 bytes), decodes the provider's X25519 key, and spawns `run_consumer_listener` instead. The tunnel listener, capability publisher, and receipt settlement task are **not** started (a consumer advertises nothing and never settles its own forwarded traffic).
- `TunnelService` is wrapped in `Arc` so both the provider listener and the consumer forward listener can share one instance.

**API key authentication:**
```rust
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0 && a.len() == b.len()
}
```

**Heartbeat flow (3 phases, no nested locks):**
1. Collect system stats + compute available capacity (hold `availability` lock only)
2. Check running state + clone ping targets (hold `state` lock briefly)
3. Spawn blocking ping + upsert metrics to DB (no locks held)

---

## Wire Protocol

### Pre-Handshake Exchange

Before the Noise_XX handshake begins, the consumer sends two length-prefixed messages in plaintext:

```
+-------------------+-------------------+-------------------+-------------------+
| 4 bytes (u32 BE)  | N bytes (session) | 4 bytes (u32 BE)  | M bytes (peer_id) |
+-------------------+-------------------+-------------------+-------------------+
```

**Validation limits:**
- `session_id`: max 256 bytes (DoS prevention)
- `peer_id`: max 512 bytes (DoS prevention)

### Noise_XX Handshake

Using the `snow` crate with parameters `Noise_XX_25519_ChaChaPoly_BLAKE2s`:

```
Prologue: "EchoMesh|v1|{session_id}" (domain separation)

→ e            Initiator sends ephemeral key
← e, ee, s, es Responder sends ephemeral + static + DH proofs
→ s, se        Initiator sends static + DH proof
```

**MITM check:** After receiving Msg2, the initiator compares `noise.get_remote_static()` against the provider's expected Noise public key (from the `CapabilityDescriptor` in the database). Mismatch triggers connection abort.

### Encrypted Data Frames

After handshake, all data flows as Noise transport frames:

```
+-------------------+-------------------+
| 2 bytes (u16 BE)  | Noise frame       |
| (frame length)    | (encrypted data)  |
+-------------------+-------------------+
```

Maximum frame size: 65535 bytes (`NOISE_MAX_MSG_LEN`).

---

## Concurrency Model

### Lock Hierarchy

The codebase avoids nested locks through a 3-phase heartbeat pattern:

```
Phase 1: availability.write() → compute stats → release
Phase 2: state.read() → check running → clone targets → release
Phase 3: spawn_blocking(ping) → state.write() → upsert to DB → release
```

### Shared State

| Resource | Type | Access Pattern |
|----------|------|----------------|
| `MetricsState` | `Arc<RwLock<MetricsState>>` | Read-heavy (HTTP handlers), write rarely (heartbeat) |
| `AvailabilityEngine` | `Arc<RwLock<AvailabilityEngine>>` | Write on heartbeat + session acquire/release |
| `MeteringEngine` | `Arc` (internally `Arc<RwLock<HashMap>>`) | Concurrent writes during relay |
| `TokenBucket` | `Mutex<TokenBucket>` (per-session) | Single-writer per tunnel |
| `TransportState` | `Arc<tokio::sync::Mutex<TransportState>>` | Shared between encrypt/decrypt tasks |
| Connection semaphore | `Arc<Semaphore>` | RAII permits via `OwnedSemaphorePermit` |

### Background Task Spawning

```
// one-shot at startup:
tokio::spawn(detect_public_ip(...))     // fills ip_address on /metrics
// opt-in (only when DASHBOARD_* env vars are set):
DashboardReporter::spawn(...)           // telemetry.rs → report-telemetry
tokio::spawn(heartbeat(...))           // periodic, 10s interval
tokio::spawn(metric_cleanup(...))       // periodic, daily
tokio::spawn(discovery.run())           // continuous event loop
tokio::spawn(axum_server)               // continuous HTTP server
// graceful shutdown: signal handler + channel drain
// provider mode only:
tokio::spawn(capability_publisher(...)) // periodic, 60s interval
tokio::spawn(receipt_settlement(...))   // event-driven, shutdown-aware
tokio::spawn(tunnel_listener(...))      // continuous TCP accept loop
// consumer mode only:
tokio::spawn(consumer_listener(...))    // accept local apps → provider tunnel
```

---

## Security Analysis

### Threat Model

| Threat | Mitigation |
|--------|------------|
| **Man-in-the-Middle** | Noise_XX static key pinning against DB record |
| **Replay attacks** | Domain-separated prologue binds session to handshake |
| **Timing attacks** | Constant-time API key comparison |
| **Resource exhaustion** | Semaphore connection limiting + RAII permits |
| **Identity theft** | File permissions 0600 on identity file |
| **Memory exhaustion** | Length caps on pre-handshake messages (256/512 bytes) |
| **Reputation manipulation** | Accumulative reputation (cannot overwrite history) |
| **Cross-session data leakage** | Session-scoped metering, per-session token buckets |

### Cryptographic Operations

| Operation | Algorithm | Crate |
|-----------|-----------|-------|
| Identity signing | Ed25519 | `ed25519-dalek` 2.1 |
| Tunnel encryption | ChaChaPoly (Noise_XX) | `snow` 0.9 |
| Key agreement | X25519 | `snow` 0.9 (via `ring`) |
| Hashing | BLAKE2s | `snow` 0.9 (via `ring`) |

---

## Database Schema

9 business tables + `schema_migrations`. All DDL lives in versioned SQL
migrations (`migrations/{postgres,sqlite}/*.sql`, numbered 001–004), embedded
at compile time and applied in order by `migrator.rs` inside per-file
transactions.

### Table Summary

| Table | Primary Key | Purpose | Row Estimate |
|-------|-------------|---------|--------------|
| `nodes` | `id TEXT` | Provider identity + latest metrics | 1 per node |
| `node_metrics` | `id SERIAL` | Historical metric snapshots | ~8640/day/node |
| `network_metrics` | `id SERIAL` | Network-wide aggregates | ~8640/day |
| `node_intelligence` | `id SERIAL` | ML features + anomaly scores | ~8640/day/node |
| `peer_reputation` | `peer_id TEXT` | Accumulative reputation per peer | 1 per peer |
| `connection_history` | `id SERIAL` | Per-session connection records | 1 per session |
| `state_receipts` | `receipt_id TEXT` | Signed transfer receipts | ~N per session |
| `capability_descriptors` | `peer_id TEXT` | DHT-published capacity info | 1 per node |
| `settlements` | `receipt_id TEXT` | Idempotent USD payout rows (migration 004) | 1 per settled receipt |
| `schema_migrations` | `version INTEGER` | Applied-migration ledger | 1 per migration |

### Indexes

```sql
CREATE INDEX idx_conn_history_local  ON connection_history(local_peer_id);
CREATE INDEX idx_conn_history_remote ON connection_history(remote_peer_id);
CREATE INDEX idx_receipts_session    ON state_receipts(session_id);
CREATE INDEX idx_caps_region         ON capability_descriptors(region);
```

---

## Error Handling Patterns

The codebase uses `anyhow` throughout:

```rust
// Typical pattern: contextualize and propagate
stream.read_u32().await.context("failed to read session ID length")?;

// Tunnel teardown: catch and log, don't propagate
if let Err(e) = config.backend.insert_connection(&record).await
    .context("failed to insert connection record")? {
    warn!(error = %e, "failed to insert connection record");
}

// HTTP handlers: convert to StatusCode
async fn handle_metrics(...) -> Result<Json<DaemonMetrics>, StatusCode> { ... }
```

**Key patterns:**
- `anyhow::Result<T>` for fallible operations
- `.context("...")` for adding descriptive error context
- `bail!("...")` for early returns with error messages
- HTTP handlers map errors to `StatusCode` (500 for internal, 503 for unavailable, 400 for bad request)
