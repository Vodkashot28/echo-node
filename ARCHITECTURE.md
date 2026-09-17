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

### `lib.rs` — Crate Root (49 lines)

Declares 8 modules and defines the `MetricsBackend` async trait with 13 methods.

```
pub mod { availability, discovery, identity, meter, models, neon, sqlite_store, tunnel }
pub use models::*          // blanket re-export of all data models
pub use neon::NeonStore    // PostgreSQL backend
pub use sqlite_store::SqliteStore  // SQLite backend
```

**`MetricsBackend` trait** — the storage abstraction layer:
- `upsert_node` / `insert_node_metrics` / `insert_network_metrics` / `insert_node_intelligence`
- `cleanup_old_metrics` — purges data older than N days
- `upsert_peer_reputation` / `get_peer_reputation` — accumulative reputation
- `insert_connection` / `update_connection_end` — connection lifecycle
- `insert_receipt` / `get_receipts_for_session` — receipt settlement
- `upsert_capability` / `get_capabilities_in_region` — DHT discovery support

Both `NeonStore` and `SqliteStore` implement this trait, allowing the daemon to switch backends at startup based on the `DATABASE_URL` env var.

---

### `models.rs` — Data Models (161 lines)

All 9 structs derive `Debug, Clone, Serialize, Deserialize`. They fall into three categories:

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
| `CapabilityDescriptor` | DHT-published capacity info | peer_id, public_key, region, upload/download_cap_mbps, noise_public_key, max/active_sessions |
| `SystemStats` | Snapshot of system metrics | cpu_pct, memory_pct, disk_usage_pct, active_sessions, current_usage_rx/tx_mbps |

---

### `identity.rs` — Cryptographic Identity (155 lines)

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

### `discovery.rs` — DHT Peer Discovery (197 lines)

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

### `tunnel.rs` — Noise_XX Encrypted Tunnel (795 lines)

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
    SessionClosed { session_id: String, bytes_sent: u64, bytes_received: u64 },
}
```

---

### `meter.rs` — Token Bucket + Signed Receipts (240 lines)

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

### `availability.rs` — Capacity Engine (224 lines)

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
- `try_acquire_session()` — increments counter, returns `Err` if at max
- `release_session()` — decrements counter

---

### `neon.rs` — PostgreSQL Backend (629 lines)

Neon PostgreSQL implementation of `MetricsBackend`.

**Schema management:** Inline DDL (`SCHEMA` constant, 8 tables + 4 indexes) run on startup. Additional `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations for backward compatibility.

**Connection:** `PgPool` with `max_connections(5)`, uses `$1, $2, ...` bind parameter syntax.

**Key implementation details:**
- `upsert_node` — 24-column `ON CONFLICT ... DO UPDATE SET` for all non-key columns
- `upsert_peer_reputation` — **accumulative**: `total_bytes_relayed = peer_reputation.total_bytes_relayed + EXCLUDED.total_bytes_relayed` (additive, not overwrite)
- `capability_descriptors.supported_encryption` — PostgreSQL `TEXT[]` array
- `cleanup_old_metrics` — deletes from 4 tables in a single transaction

---

### `sqlite_store.rs` — SQLite Backend (595 lines)

SQLite implementation of `MetricsBackend`.

**Schema management:** Same inline DDL pattern as Neon, but with SQLite-specific syntax (`?` bind parameters, `AUTOINCREMENT`, `INTEGER` for booleans).

**Connection:** `SqlitePool` with `max_connections(1)` (single writer), `create_if_missing(true)`.

**Key differences from Neon:**
- `supported_encryption` stored as JSON text (serialized/deserialized manually)
- `cleanup_old_metrics` uses `chrono::Utc::now()` for timestamp calculation
- SQLite `UPSERT` uses `excluded.` prefix (lowercase) vs PostgreSQL `EXCLUDED.` (uppercase)

---

### `main.rs` — Orchestrator (1159 lines)

The entry point. Wires together all services and manages the daemon lifecycle.

**Data structures:**
- `DaemonMetrics` — 24-field struct serialized as JSON for `/metrics`
- `DaemonNode` — 20-field struct for `/nodes`
- `DaemonStatus` — 8-field struct for `/status`
- `HealthResponse` — 8-field struct for `/health`
- `MetricsState` — shared mutable state behind `Arc<RwLock<_>>`
- `AppState` — `{ metrics: Arc<RwLock<MetricsState>>, backend: Arc<dyn MetricsBackend> }`

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
tokio::spawn(heartbeat(...))           // periodic, 10s interval
tokio::spawn(capability_publisher(...)) // periodic, 60s interval
tokio::spawn(receipt_settlement(...))   // event-driven, shutdown-aware
tokio::spawn(metric_cleanup(...))       // periodic, daily
tokio::spawn(discovery.run())           // continuous event loop
tokio::spawn(tunnel_listener(...))      // continuous TCP accept loop
tokio::spawn(axum_server)               // continuous HTTP server
// graceful shutdown: signal handler + channel drain
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

8 tables, 4 indexes. All DDL is inline in both backend modules.

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
