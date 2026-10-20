# Architecture Deep Dive

Detailed architectural analysis of the Echo Node daemon. For the high-level overview, see [README.md](README.md).

## Table of Contents

- [Component Inventory](#component-inventory)
- [Cross-Node Marketplace (G6)](#cross-node-marketplace-g6)
- [Wire Protocol](#wire-protocol)
- [Concurrency Model](#concurrency-model)
- [Security Analysis](#security-analysis)
- [Database Schema](#database-schema)
- [Error Handling Patterns](#error-handling-patterns)

## Component Inventory

> Module sizes are deliberately not listed — they rot with every commit.
> Run `wc -l src/*.rs` for current numbers.

### `lib.rs` — Crate Root

Declares the library's 19 modules (`accountability`, `auth`, `availability`,
`bidding`, `consumer`, `discovery`, `identity`, `ledger`, `meter`,
`migrator`, `models`, `neon`, `policy`, `security`, `session_ticket`,
`settlement`, `sqlite_store`, `telemetry`, `tunnel`) and re-exports the
storage backends. The binary crate (`main.rs`) additionally declares `mod cli`.

**Shared utilities:**
- `base64_encode` / `base64_decode` — hand-rolled (avoids another dependency), used by receipt signing and backends
- `constant_time_eq` — XOR-based constant-time comparison for API keys
- `NOISE_PARAMS` — `"Noise_XX_25519_ChaChaPoly_BLAKE2s"` shared by identity and tunnel

**`MetricsBackend` trait** — the storage abstraction layer (29 async methods):
- **Node/metrics:** `upsert_node`, `insert_node_metrics`,
  `insert_network_metrics`, `insert_node_intelligence`, `cleanup_old_metrics`
- **Reputation/connections:** `upsert_peer_reputation`, `get_peer_reputation`,
  `insert_connection`, `update_connection_end`
- **Receipts/settlement:** `insert_receipt`, `get_receipts_for_session`,
  `record_settlement`, `get_settlements`, `settlement_summary`
- **Capabilities:** `upsert_capability`, `get_capabilities_in_region`,
  `get_capability_for_peer`
- **Ledger:** `append_ledger_event`, `load_ledger_chain_state`
- **Session tickets:** `store_session_ticket`, `get_session_ticket`,
  `cleanup_expired_tickets`
- **Auctions:** `upsert_auction`, `get_auction`, `list_auctions`,
  `update_auction_status`, `insert_bid`, `get_bids_for_auction`,
  `cleanup_old_auctions`

Both `NeonStore` and `SqliteStore` implement this trait, allowing the daemon to switch backends at startup based on the `DATABASE_URL` env var.

---

### `models.rs` — Data Models

All 13 structs derive `Debug, Clone, Serialize, Deserialize`. They fall into three categories:

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
| `ConnectionRecord` | Per-session connection record | local/remote_peer_id, remote_ip/port, direction, bytes_sent/received, duration, exit_reason |
| `StateReceipt` | Signed cryptographic receipt | receipt_id, session_id, signer/counterparty_peer_id, bytes_transferred, signature |
| `SettlementRecord` | Per-receipt payout entry | receipt_id (PK), signer/counterparty_peer_id, bytes_settled, earnings_usd, settled_at |
| `SettlementSummary` | Aggregated payout totals | receipts_settled, total_bytes_settled, total_earnings_usd |
| `CapabilityDescriptor` | DHT-published capacity info | peer_id, public_key, region, upload/download_cap_mbps, noise_public_key, max/active_sessions |
| `SystemStats` | Snapshot of system metrics | cpu_pct, memory_pct, disk_usage_pct, active_sessions, current_usage_rx/tx_mbps |

**Ledger models** (canonical audit log):
| Struct | Purpose | Fields |
|--------|---------|--------|
| `LedgerEventRow` | Persisted hash-chained event | event_id, sequence, event_type, event/schema versions, ts, network, node_id, actor_id, correlation/parent ids, hash, prev_hash, event_json |
| `LedgerChainState` | Per-node chain resume state | highest sequence + hash, genesis hash |

---

### `identity.rs` — Cryptographic Identity

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

### `discovery.rs` — DHT Peer Discovery

Wraps a libp2p swarm with Kademlia + Identify (+ AutoNAT, DCUtR, relay client) protocols.

```rust
pub struct DiscoveryService {
    swarm: Swarm<Behaviour>,
    event_tx: mpsc::UnboundedSender<DiscoveryEvent>,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    relay_client: relay::client::Behaviour,   // client-side relay reservations
    dcutr: dcutr::Behaviour,                  // hole punching (needs observed addr)
    autonat: autonat::Behaviour,              // external-address confirmation
}
```

**Command channel** — everything is driven from the async side via
`DiscoveryCommand` (`PutRecord`, `GetRecord`, `StartProviding`,
`GetProviders`, `Dial`, relay helpers) with matching `DiscoveryEvent`
results (`RecordStored`, `RecordFound`, `ProvidersFound`, `PeerFound`,
`PeerDisconnected`, plus market events below).

**Key behaviors:**
- Kademlia is **forced to `kad::Mode::Server`** at startup — inbound record
  storage must work without a confirmed external address (NAT'd / local-only
  nodes never get one, which would silently break auction publishing).
- Protocol namespacing: Kademlia `/<DHT_NETWORK_ID>/kad/1.0.0`, Identify
  `/<DHT_NETWORK_ID>/id/1.0.0` — two nodes with different `DHT_NETWORK_ID`
  never talk to each other.
- **Bootstrap:** `DHT_BOOTSTRAP_PEERS` if provided; otherwise 4 well-known
  public libp2p bootstrap nodes (DNS multiaddrs + IP fallbacks for
  DNS-failure resilience).
- **AutoNAT** config: boot delay 15 s, refresh 120 s, retry 15 s,
  global-IP-only assertions.
- Idle connection timeout: 60 seconds.

**Market events (G6):** the swarm also publishes/consumes the marketplace
records — `auction/{id}`, `auctions_by_host/{host}`, `auctions/{region}`,
`award/{consumer}/{winner}` — surfaced on the same `DiscoveryEvent` channel
(see [Cross-Node Marketplace (G6)](#cross-node-marketplace-g6)).

**Design note:** `kademlia.addresses_of_peer()` is not available in libp2p-kad 0.46. The `PeerFound` event carries `Multiaddr::empty()` — actual peer addresses are resolved through the Identify protocol when a connection is initiated.

---

### `tunnel.rs` — Noise_XX Encrypted Tunnel

The largest and most complex module. Handles the full tunnel lifecycle: pre-handshake exchange, Noise_XX handshake, bidirectional encrypted relay, session-ticket issuance/resumption, and session teardown.

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

**Session tickets:** after a successful handshake the provider issues an
ed25519-signed `SessionTicket` (see `session_ticket.rs`) and sends it as an
encrypted signal frame; the consumer persists it to `data/tickets/{peer_id}.ticket`
and the provider to its ticket store + DB. On reconnect the consumer sends the
ticket in the preamble and the provider validates it — resumption skips the
**policy admission checks only**; the Noise_XX handshake itself always re-runs
(snow's keys are not extractable, so this is a fresh cryptographic handshake
either way). Expired/unknown tickets fall back to the normal admission path.

---

### `session_ticket.rs` — Session Ticket Crypto

Ed25519-signed tickets that let a returning consumer resume a previous
session across provider restarts.

- `SessionTicket` — issue/verify/serialize; validity `TICKET_VALIDITY_SECS`
  (24 h), wire size capped at `MAX_TICKET_SIZE` (2 KiB).
- `SessionState` — session metadata (peer/session ids, counters) wrapped in
  **XOR-encrypted** bytes; only the provider that issued a ticket can
  decrypt it.
- `verify_and_decrypt(...)` — validates the signature + expiry before
  restoring state; failure falls back to normal policy admission.
- Issued by `tunnel.rs` after handshake, stored via `store_session_ticket`
  (DB) and on the consumer disk at `data/tickets/{peer_id}.ticket`;
  expired tickets are swept by `cleanup_expired_tickets`.
  See the session-tickets note in the tunnel section above for what
  resumption actually skips (policy admission only).

---

### `consumer.rs` — Consumer Forward Listener

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

### `meter.rs` — Token Bucket + Signed Receipts

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

### `settlement.rs` — Payout Engine

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
- `settle_receipt_at_price(receipt, price_usd_per_gb)` — same, but settles at a market price (a marketplace award's locked USD/GB) instead of the flat rate. The settlement task (`main.rs` Step 8) looks up an award for the counterparty first and falls back to the flat `SETTLEMENT_RATE_USD_PER_GB`.
- **Idempotency:** backends return whether a row was actually inserted (`INSERT OR IGNORE` / `ON CONFLICT DO NOTHING`). Replaying an already-settled receipt is a no-op — double-counting is structurally impossible.
- `settlements(limit)` / `summary()` — expose recent entries and aggregate totals for the `GET /settlements` endpoint.

**Wiring (main.rs Step 9):** after a receipt passes signature verification and is persisted via `insert_receipt`, the same task feeds it to `settlement_engine.settle_receipt()`, including the shutdown drain path.

---

### `availability.rs` — Capacity Engine

Computes how much bandwidth to advertise to the DHT based on current system load.

```rust
pub struct AvailabilityEngine {
    system: System, networks: Networks, components: Components, disks: Disks,
    max_upload_mbps: f64, max_download_mbps: f64,
    max_sessions: u32, active_sessions: u32,
    current_usage_rx_bps: f64, current_usage_tx_bps: f64,
    total_network_rx: u64, total_network_tx: u64,
    // sliding-window rate samples (VecDeque, O(1) front removal)
    rx_samples: VecDeque<(u64, Instant)>, tx_samples: VecDeque<(u64, Instant)>,
    latency_samples: VecDeque<f64>,        // avg_latency_ms for descriptors
    ewma_alpha: f64,                       // AVAIL_EWMA_ALPHA (0,1]
    ewma_upload: Option<f64>,              // smoothed advertised capacity
    ewma_download: Option<f64>,            //   (None until first sample)
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

### `policy.rs` — Provider Admission Policy

Env-gated admission engine (`POLICY_*`) evaluated when a consumer wants to
establish a session:

- **Gates:** `POLICY_MIN_REPUTATION` (compared against the trust-decayed
  score — `score · 0.5^(idle/half_life)`, half-life from
  `REPUTATION_DECAY_HALF_LIFE_DAYS`), `POLICY_MAX_LATENCY_MS`,
  `POLICY_MIN_ACCOUNTABILITY` (soft-slashing composite gate).
- **Quotas:** `POLICY_MAX_SESSIONS_PER_PEER` / `..._PER_REGION`, enforced
  with **RAII permits** that release the slot on drop.
- **Blocklist:** `POLICY_BLOCKED_PEERS` — always rejected (and filtered
  from `/match`).
- **All defaults are open** — with no constraint set the engine stays out
  of the relay hot path entirely (no DB lookups).

### `security.rs` — Security Counters & Alerts

`SecurityCounters` — process-lifetime failure counters (handshake failures,
pinned-key mismatches, replay/handshake-msg rejects, decrypt failures, API
401/403s, policy denials, settlement anomalies) surfaced in `/status`,
`/health`, and `GET /security`.

- **Bounded event ring** — last 32 security events with peer attribution,
  newest first.
- **Sliding-window burst detector** — ≥ `SECURITY_ALERT_THRESHOLD` events
  inside `SECURITY_ALERT_WINDOW_SECS` trips one loud `error!` +
  `alert_active` (instead of log spam); threshold `0` disables.
- **Per-peer violation attribution** — feeds the accountability discount
  below; the alert sink also feeds the ledger's `SecurityAlertRaised`.

### `accountability.rs` — Soft-Slashing Gate

Composite accountability score gating `POLICY_MIN_ACCOUNTABILITY`:

```
soft_slash_score = trust-decayed reputation
                 × per-peer security-violation discount
                 × session-availability factor
```

No stake is locked (no on-chain staking — deferred until a market is worth
slashing); judgment-based denials record new violations against the peer.
`POLICY_MIN_ACCOUNTABILITY = 0` (default) turns the gate off.

### `auth.rs` — Scoped API Keys

- `API_KEYS` — JSON array of `{token, name, scopes}` with `control` /
  `settlements` scopes; legacy `API_KEY` grants admin (all scopes).
- Neither set → auth disabled (open control plane).
- `require_scope` axum middleware; token comparison is always
  constant-time (`constant_time_eq` in `lib.rs`).
- Unknown/invalid token → `401` + `WWW-Authenticate: Bearer`; valid token
  missing the scope → `403`.

---

### `neon.rs` — PostgreSQL Backend

Neon PostgreSQL implementation of `MetricsBackend`.

**Schema management:** Versioned SQL migrations from `migrations/postgres/*.sql`, applied by `migrator.rs` on startup inside per-file transactions and tracked in `schema_migrations`. Postgres migration SQL is strict (any failure aborts the run, so the migration is retried on the next boot).

**Connection:** `PgPool` with `max_connections(5)`, uses `$1, $2, ...` bind parameter syntax.

**Key implementation details:**
- `upsert_node` — 24-column `ON CONFLICT ... DO UPDATE SET` for all non-key columns
- `upsert_peer_reputation` — **accumulative**: `total_bytes_relayed = peer_reputation.total_bytes_relayed + EXCLUDED.total_bytes_relayed` (additive, not overwrite)
- `capability_descriptors.supported_encryption` — PostgreSQL `TEXT[]` array
- `cleanup_old_metrics` — deletes from 4 tables in a single transaction

---

### `sqlite_store.rs` — SQLite Backend

SQLite implementation of `MetricsBackend`.

**Schema management:** Versioned SQL migrations from `migrations/sqlite/*.sql`, applied by `migrator.rs` on startup inside per-file transactions and tracked in `schema_migrations`. SQLite is strict except for SQLite's `duplicate column name` error class (legacy idempotent column backfills — see `migrator.rs`).

**Connection:** `SqlitePool` with `max_connections(1)` (single writer), `create_if_missing(true)`.

**Key differences from Neon:**
- `supported_encryption` stored as JSON text (serialized/deserialized manually)
- `cleanup_old_metrics` uses `chrono::Utc::now()` for timestamp calculation
- SQLite `UPSERT` uses `excluded.` prefix (lowercase) vs PostgreSQL `EXCLUDED.` (uppercase)

---

### `migrator.rs` — Versioned SQL Migrations

Applies embedded, versioned SQL migrations on startup.

- SQL files are embedded at compile time via `include_str!` from `migrations/{postgres,sqlite}/`, each with an integer `version` and a name.
- A `schema_migrations` table records applied versions; `run_postgres` / `run_sqlite` skip applied migrations and apply pending ones **in order**.
- `split_sql` is a hand-rolled multi-statement splitter that handles `--`/`/* */` comments, single- and double-quoted strings, and PostgreSQL dollar-quoted bodies (`$$ ... $$`).
- **Transactions:** each migration applies atomically. A genuine failure aborts, rolls back, and leaves the migration **unrecorded** so the next boot retries it.
- **SQLite idempotency:** because SQLite lacks `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, only statements failing with `duplicate column name` are tolerated (legacy column backfills in migrations 002/003); any other error aborts the run.

**Upgrade rule:** never edit an already-shipped migration file — add a new numbered file. A broken migration is retried on the next boot, never silently skipped.

---

### `bidding.rs` — Marketplace Bidding

Sealed-bid price discovery: a consumer opens an auction with a bandwidth
requirement + budget cap; providers submit sealed bids; the 2 s deadline
sweeper awards the highest quality-adjusted bid and locks the price.

- **Scoring** — `score = (BID_W_REPUTATION·reputation + BID_W_LATENCY·latency_factor) / price`,
  latency factor halved at `BID_REF_LATENCY_MS`.
- **Pricing** — `BID_PRICING=vickrey` (winner paid `max(own, runner-up)`)
  or `first` (pay-your-bid); awards flow into settlement via
  `settle_receipt_at_price`.
- **Guards** — one bid per provider, bid ≤ auction budget cap,
  `MAX_BIDS_PER_AUCTION` / `MAX_AUCTION_WINDOW_SECS` / `MAX_REGION_LEN`
  caps, requester-only close/cancel, blocked peers can neither open nor bid.
- **Signed remote bids (G6)** — remote providers submit ed25519 signatures
  over `bid:{auction_id}:{price.to_bits()}`; the host verifies the signature
  maps to the claimed PeerId.
- **Wire types** — `AuctionRecord` + auction-index encoding for DHT
  records (`MAX_AUCTION_RECORD_BYTES` = 2 KiB, `MAX_AUCTION_INDEX_IDS` = 32).
- **`AuctionBook`** — in-memory auction state (local + foreign/remote),
  swept every 2 s; persistence methods on `MetricsBackend` exist for
  crash CRUD, but there is **no startup rehydration** into the engine
  (known gap — auctions are seconds-long, so windows are not restored).
- **`BID_AUTO`** (provider mode) — poller over local `GET /auctions` and
  DHT-discovered remote auctions (exact `NODE_REGION` match).

### `ledger.rs` — Canonical Audit Ledger

Append-only, hash-chained audit log of every market/settlement/reputation/
security/tunnel action:

- `LedgerEvent<T>` — versioned envelope: ULID `event_id`, per-node
  `sequence`, `event_version` (per-type definition) + `schema_version`,
  `ts`, `ECHO_NETWORK` tag, actor/correlation/parent ids, `sha256` `hash`
  chained via `prev_hash`, and the JSON payload; ed25519-signed.
- `LedgerWriter::try_emit` — non-blocking draft submit into a **single FIFO
  writer task** that advances the per-node sequence/prev/root chain and
  persists `ledger_events` (spawned before every other task in both modes).
- `load_ledger_chain_state` — resumes the chain across restarts.

---

### `telemetry.rs` — Dashboard Bridge

Optional, off-by-default push bridge from the daemon to a dashboard
ingestion endpoint. Enabled only when **`DASHBOARD_TELEMETRY_URL` +
`ECHO_API_KEY`** are both set; auth is `Authorization: Bearer <ECHO_API_KEY>`
(see `DASHBOARD.md` for the full integration picture).

**Key pieces:**
- `DashboardConfig::from_env()` — reads the `DASHBOARD_TELEMETRY_*` env vars
  (spool path, max queue, backoff base/max); the node UUID is generated fresh
  per startup and every payload carries `peer_id` for row correlation.
- `DashboardReporter` — bounded mpsc channel (64) with a background posting
  task. `try_push` / `try_push_session` are **non-blocking** (the heartbeat
  never waits on HTTP); 10s HTTP timeout; failures back off exponentially
  (± jitter) and unsent frames spill to a durable disk spool
  (`DASHBOARD_TELEMETRY_SPOOL`) that is replayed on the next boot.
- `ReporterMessage::{Telemetry, Session}` — heartbeat frames (posted as the
  raw `head.payload` JSON: peer_id + NodeRow + metrics + 32-dim intelligence)
  and `kind:"session"` session-close events share one queue.
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

### `main.rs` — Orchestrator

The entry point. Wires together all services, manages the daemon lifecycle,
hosts the axum REST API, and spawns the background task set.

**Data structures:**
- `DaemonMetrics` — struct serialized as JSON for `/metrics` (includes the identity tail `peer_id`, `region`, `ip_address` — the latter omitted from JSON until public-IP detection lands)
- `DaemonNode` — struct for `/nodes`; `DaemonStatus` for `/status` (embeds the `SecurityCounters` snapshot); `HealthResponse` for `/health` (same security block)
- `MetricsState` — shared mutable state behind `Arc<RwLock<_>>`
- `Config` — env-driven runtime configuration (adds `node_mode`, `consumer_listen_addr`, `provider_addr`, `provider_peer_id`, `provider_noise_pubkey` on top of the provider settings)
- `AppState` — `{ metrics, backend, settlement, unhealthy_threshold_secs, provider_match_limit, security: Arc<SecurityCounters>, local_peer_id, blocked_peers, match_max_stale_secs, bid_engine: Arc<BidEngine>, ledger: LedgerWriter }`

**Mode gating (`NODE_MODE`):**
- **`provider`** (default) — spawns the incoming tunnel listener (`accept_incoming`), the capability publisher, and the receipt settlement task.
- **`consumer`** — validates `PROVIDER_ADDR` + `PROVIDER_PEER_ID` + `PROVIDER_NOISE_PUBKEY` (base64, must decode to 32 bytes), decodes the provider's X25519 key, and spawns `run_consumer_listener` instead. The tunnel listener, capability publisher, and receipt settlement task are **not** started (a consumer advertises nothing and never settles its own forwarded traffic).
- `TunnelService` is wrapped in `Arc` so both the provider listener and the consumer forward listener can share one instance.

**API authentication:** delegated to `auth.rs` — scoped `API_KEYS` (JSON,
`control`/`settlements` scopes) or legacy `API_KEY` (admin); token comparison
uses `constant_time_eq` (`lib.rs`, XOR-based) and `require_scope` is axum
middleware. Neither var set → auth disabled.

**Background tasks** (spawn order; full table in README "Background Tasks"):
ledger writer first (both modes), then — depending on mode/config — tunnel
event handler, tunnel listener, discovery event loop + handler, heartbeat
(3-phase lock discipline below), metric cleanup, capability publisher,
receipt settlement / consumer listener, auction sweeper, market DHT sync
(G6), one-shot public IP detection, optional `BID_AUTO` auto-bid poller,
and the opt-in telemetry reporter. REST API + graceful shutdown run in one
`axum::serve(...).with_graceful_shutdown(...)`.

**Heartbeat flow (3 phases, no nested locks):**
1. Collect system stats + compute available capacity (hold `availability` lock only)
2. Check running state + clone ping targets (hold `state` lock briefly)
3. Spawn blocking ping + upsert metrics to DB (no locks held)

---

### `cli/` — Operator CLI (binary-side module)

`main.rs` declares `mod cli;` — a clap-based subcommand CLI layered on top
of the daemon. The daemon subcommand itself is hidden (running with no args
just calls `main`); everything else operates on an XDG-style home directory
(`EchoHome`: `config.toml`, identity, DB, logs, pid file).

Top-level commands: `onboard` (resumable state machine), `start` / `stop` /
`restart` (background daemon via pid file + SIGTERM), `status`, `doctor`
(install/runtime diagnostics), `marketplace` (list auctions, submit bids,
show awards), `wallet` (earnings/settlement totals), `identity`, `logs`,
`config`.

Submodules: `config` (NodeConfig/EchoHome), `config_cmd`, `daemon`
(pid/process management), `identity_cmd`, `logs`, `market`, `onboarding`,
`status`, `wallet`.

---

## Cross-Node Marketplace (G6)

Auction state started node-local: a provider could only bid on auctions it
polled directly on the host, and a locked award price never reached the
winning provider across hosts. G6 propagates auctions over the existing
Kademlia DHT (`discovery.rs` command channel + events; `main.rs`
`market_dht_sync` task).

- **Auction propagation** — the auction host publishes `auction/{id}`
  (`AuctionRecord` JSON, `host_api` URL included) and maintains
  `auctions_by_host/{host}` id-index records every 10 s (45 s record TTL);
  providers announce interest via `start_providing("auctions/{region}")`.
  Remote pollers discover via `GetProviders` → per-host index `GetRecord` →
  per-auction `GetRecord`. Auto-bidding providers only bid on exact
  `NODE_REGION` matches.
- **Remote bids over HTTP** — Kademlia records are one-per-key and cannot
  collect bid sets, so remote providers submit sealed bids via plain HTTP
  `POST /auctions/{id}/bid` against the record's `host_api`, with an
  ed25519-signed payload (`bid:{auction_id}:{price.to_bits()}`); the host
  verifies the signature → PeerId and attributes reputation/latency
  correctly. Unsigned legacy local bids still work.
- **Award propagation** — at close the host publishes
  `award/{consumer}/{winner}` (2 s catch-up cursor); every node watches
  award records where `provider == local_peer_id` and imports them into the
  local `BidEngine` so **subsequent** receipts from that counterparty settle
  at the locked market price (120 s award-lookup cooldown). Already-settled
  receipts are never repriced (settlement inserts are idempotent).
- **Forced `kad::Mode::Server`** — libp2p's default auto-mode only serves
  inbound Kademlia streams once a *confirmed external address* exists;
  without the override, NAT'd/local-only nodes silently reject every
  inbound record. Covered by `tests/dht_market_e2e.rs` (two in-process
  swarms).
- Publishing waits until an advertise API address exists
  (`API_ADVERTISE_ADDR`, else detected public IP + `LISTEN_ADDR` port).

**Known limits (by design for this slice):**
- `host_api` must be reachable by remote bidders — DHT propagates
  *discovery*; bids ride HTTP with no NAT traversal.
- Award records are self-attested (signed by the host, not the winner);
  accepted only when `provider == local_peer_id`, bounds-checked, deduped —
  trust boundary is "only affects my own earnings accounting".
- Only receipts settling **after** award import reprice (≤ ~120 s window
  for in-flight sessions).
- Region routing is exact-match only; cross-region bidding is not routed.
- Foreign auctions are only discovered/bid by `BID_AUTO=true` pollers.
- Records expire (45 s TTL / 10 s republish): a host that dies mid-auction
  leaves stale records that remote bidders discover only to get HTTP
  timeouts — acceptable for seconds-long windows.

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
// both modes, first:
tokio::spawn(ledger_writer_task(...))    // drains LedgerWriter::try_emit (FIFO)
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
| Ledger hash chain | SHA-256 | `sha2` 0.10 |

---

## Database Schema

13 business tables + `schema_migrations`. All DDL lives in versioned SQL
migrations (`migrations/{postgres,sqlite}/*.sql`, numbered 001–007), embedded
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
| `ledger_events` | `event_id TEXT` | Canonical audit chain (migration 005) | 1 per emitted event |
| `session_tickets` | `session_id TEXT` | Resumable-session tickets (migration 006) | 1 per active ticket |
| `auctions` | `auction_id TEXT` | Marketplace auction persistence (migration 006/007) | 1 per auction (bounded by cleanup) |
| `auction_bids` | `bid_id TEXT` | Sealed bids per auction (migration 006) | ≤ 256 per auction |
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
