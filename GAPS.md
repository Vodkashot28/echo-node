# GAPS — Daemon ↔ Dashboard Integration Analysis

Status of the integration between the **echo-node** Rust daemon (`/root/echo-node`)
and the **echomesh** dashboard (`/root/echomesh`, React + Supabase).

Last updated: 2026-10-04 · Slices 1 (heartbeat ingestion) and 2 (session
federation) implemented; **live sync path** (`scripts/echo-sync.sh` →
`daemon-push`) deployed, with `peer_id`/`region` identity fields flowing.

---

## The core finding

The two halves of the product are architecturally **disconnected — there is no
data path between them today**:

- The dashboard talks **only** to Supabase (Postgres + edge functions + realtime).
- The daemon talks **only** to its own database and its own REST API on
  `LISTEN_ADDR` (`:3001`).
- Nothing in the frontend calls the daemon; nothing in the daemon calls Supabase.

The dashboard migration comment says *"mirroring Rust EchoMesh node model"* —
but the mirror is static. No production code writes real daemon data into it.

> **Slice 1 fixes this for the heartbeat metrics path.** See
> [Slice 1 — Real telemetry ingestion](#slice-1--real-telemetry-ingestion-implemented)
> below.

> **Update 2026-10-04 — a push path now exists.** `scripts/echo-sync.sh`
> (both repos) polls the daemon's local `GET /metrics` and POSTs it to the
> `daemon-push` edge function every ~30s, authenticated by `x-node-token` =
> `nodes.ingest_token` (drizzle `0000_node_ingest_token.sql`). `daemon-push`
> upserts the metrics **and** persists the identity tail (`peer_id` via
> migration `20261004000000_add_nodes_peer_id`, `region`; `ip_address`
> deliberately left to the scrape path). The daemon still never calls
> Supabase itself — `report-telemetry` (Slice 1) remains dormant — but for
> *metrics*, the "no data path" statement below is no longer true.

## The gaps (ranked)

### G0 — "Telemetry pipeline" was theater (now a real scraper + real push)
Historically `ingest-telemetry` read existing Supabase rows, applied a random
walk (`Math.random()` ~20 places), and wrote fabricated results back — the
dashboard's "Pipeline Control" card was a fake-data generator. It has since
been **rewritten as a Prometheus scraper** (parses `…/metrics` text from a
node's `node_exporter` endpoint, driven by `nodes.ip_address` +
`metrics_port`), and the daemon's heartbeat reaches the dashboard through the
sync path above. The random walk is gone — the function computes real samples
from the scrape (and sets `last_seen_at`); its remaining job is external
node_exporter scraping. *(2026-10-04: the Pipeline Control card — the
scraper's only UI trigger, an obsolete browser polling loop — was removed
along with `useTelemetryPipeline`; `ingest-telemetry` now needs a
manual/scheduled invoke until the target row below lands, while anomaly
scoring moved to an automatic 5-minute background tick in
`useNodeIntelligence`. The frontend also lost all four demo-seed buttons.)*

> ⚠️ **Pending:** the `nodes.ip_address` (tunnel hostname) + `metrics_port=80`
> UPDATE for the scraper target has not been applied yet (needs service-role
> access) — until then the scraper has no target row and returns no samples.

### G1 — Shared table names, incompatible contracts
`nodes` / `node_metrics` / `network_metrics` / `node_intelligence` exist on both
sides but do not interop:

| Dimension | Daemon | Dashboard |
|---|---|---|
| `nodes.id` | `varchar` = `node-{epoch}` | `UUID` PK, FK target |
| `user_id` | env string, default `anonymous` (not a UUID) | `auth.uid()` UUID, RLS-enforced |
| `feature_vector` | 11 dims (was, pre-Slice-1) | 32 dims (schema comment + `FEATURE_LABELS`, "32-Dim Feature Vector" UI) |
| `cluster_id` | always 0 | k-means clusters 0–2 |
| `active_nodes` | `max(active_sessions, 1)` | real network-wide count |
| `uptime_pct` | percent of *this day* (`secs/86400`) | percent-of-time uptime |

Pointing the daemon at the Supabase Postgres directly therefore fails: varchar
into UUID PK, FK violations, RLS rejection (`anonymous ∉ auth.uid()`).

### G2 — Sessions page has no real source
The dashboard's `sessions` table (status `active/completed/failed/timeout`,
`session_type proxy/relay/tunnel/cdn/storage`, protocol, packets, per-session
earnings) is **never written by the daemon**. The daemon records the same
activity as `connection_history` in its own DB (`local_peer_id`,
`remote_peer_id`, `bytes_sent/received`, `duration_secs`, `avg_latency_ms`,
`exit_reason`) plus signed `state_receipts`. In production the Sessions page
only shows seeded demo rows.

### G3 — Three unrelated earnings economies
1. Heartbeat: `uptime_secs × RU_rate × earnings_per_ru` (`main.rs`) — uptime-based.
2. Settlement engine: verified receipts × `SETTLEMENT_RATE_USD_PER_GB` (`settlement.rs`) — the verifiable product economy, in daemon-only tables.
3. Dashboard: `sessions.earnings_usd` / `network_metrics.earnings_usd` — synthetic.

The dashboard cannot distinguish real earnings from fabricated ones.

### G4 — No auth/identity bridge (partially addressed)
Dashboard = Supabase Auth (JWT, RLS by `auth.uid()`). Daemon = no account
concept. There is no claim/registration flow to bind a daemon's identity
(peer_id, keys) to a dashboard user UUID.

**Progress (2026-10-04):**
- Every node row carries a unique `ingest_token`
  (drizzle `0000_node_ingest_token.sql`, DB-generated 24 random bytes hex)
  — this is the push-path credential (`x-node-token` on `daemon-push`).
- `daemon-push` now persists the daemon's `peer_id`
  (migration `20261004000000_add_nodes_peer_id`) + `region`, written
  best-effort/non-fatal so a missing column can't break the metrics path.
- Still open: user-facing claim/pairing UX, and persisting
  `DASHBOARD_NODE_ID` into `identity.json` (Slice 3).

### G5 — Feature drift in both directions
Dashboard-only: `sessions`, IPFS `artifacts` / `generate-cid` / `ipfs-resolve`,
`copilot-chat`, Supabase realtime.
Daemon-only: `peer_reputation`, `state_receipts`, `settlements`,
`capability_descriptors`, availability engine, consumer/provider modes,
live tunnel sessions.

---

## Slice 1 — Real telemetry ingestion (implemented)

The daemon now pushes each heartbeat's **real** metrics to a new Supabase edge
function `report-telemetry`, opt-in via env vars. On by nothing, off by default.

### Daemon side (`echo-node`)

New module `src/telemetry.rs`:
- `DashboardConfig::from_env()` — reads `DASHBOARD_TELEMETRY_URL`,
  `DASHBOARD_TELEMETRY_TOKEN`, `DASHBOARD_USER_ID` (required) and
  `DASHBOARD_NODE_ID` (optional; v4 UUID generated at startup if absent).
- `DashboardReporter` — bounded channel (64) + background posting task;
  `try_push` never blocks the heartbeat; 10s HTTP timeout; best-effort with
  error logging.
- `build_feature_vector()` — computes the **32-dim** vector the dashboard
  expects, in the exact dimension order from the `node_intelligence` migration:
  real signal where the daemon has it (throughput, latency avg/p99, jitter,
  packet loss, uptime, bw util, conn density, earnings, CPU/mem/IO entropy,
  trends, volatility), stable neutral baselines where it doesn't yet
  (documents each inline). No random noise.

Wiring (`main.rs`):
- `heartbeat()` builds a `DashboardTelemetry` frame from `NodeRow`,
  `NodeMetricsRow`, `NetworkMetricsRow`, `NodeIntelligenceRow` (with the new
  32-dim vector) and hands it to the reporter after the normal local DB writes.
- Changed `MetricsState::new` to take `max_sessions` (used for
  connection-density feature) and added `#[allow(clippy::too_many_arguments)]`.

### Dashboard side (`echomesh`)

New `supabase/functions/report-telemetry/index.ts`:
- `verify_jwt = false` (daemon is headless; no browser JWT).
- Daemon authenticates with `Authorization: Bearer DASHBOARD_TELEMETRY_TOKEN`
  (shared secret stored in Supabase function secrets).
- Writes via service-role key (server side only; never exposed to the browser),
  with every row scoped by the frame's `user_id` so dashboard RLS still applies.
- Validates: `version == 1`, `node_id`/`user_id` are UUIDs, timestamp,
  status within the `nodes.status` CHECK constraint.
- Upserts the `nodes` row (conflict target `id`), inserts `node_metrics`,
  `network_metrics`, `node_intelligence`.

### How to run it

```bash
# Daemon (echo-node):
DASHBOARD_TELEMETRY_URL="https://<project>.supabase.co/functions/v1/report-telemetry" \
DASHBOARD_TELEMETRY_TOKEN="<secret, must equal function secret>" \
DASHBOARD_USER_ID="<supabase auth user UUID>" \
DASHBOARD_NODE_ID="<stable UUID, recommended>" \
cargo run

# Dashboard (echomesh):
supabase secrets set DASHBOARD_TELEMETRY_TOKEN="<secret>"   # one-time
supabase functions deploy report-telemetry
```

### Known in-flight risks (Slice 1)
- `DASHBOARD_NODE_ID` unset → a fresh node row (new UUID) on every restart.
  Set it to reuse one row. Persisting the generated id to `identity.json` is a
  follow-up (Slice 3).
- Dimensions 9/11–15/29–31 of the feature vector are neutral baselines until
  consumer-side measurement and multi-peer federation exist.

---

## Slice 2 — Session federation (implemented)

Provider-side relay sessions now flow from the daemon's
`connection_history`/relay accounting into the dashboard's `sessions` table —
one real row per completed tunnel, no more seeds-only. Closes **G2**; the
Pipeline card's fabrication over live nodes is neutralized (**G0** partial).

### Daemon side (`echo-node`)

`src/tunnel.rs`:
- New `SessionCloseDetails` — full lifetime accounting captured at provider
  teardown: remote peer/ip/port, direction, started/ended (RFC3339), duration,
  avg latency, `exit_reason`.
- `TunnelEvent::SessionClosed` now carries `details: Option<SessionCloseDetails>`
  (`Some` on the provider side, where the accounting is authoritative; `None`
  reserved for future consumer-side closes).

`src/telemetry.rs`:
- New `SessionEvent::from_close(...)` — provably idle mapping of the teardown
  into the dashboard frame: `kind="session"`, `action="closed"`,
  `status="completed"`, `session_type="tunnel"`, `protocol="tcp"`,
  `bytes_relayed`, `avg_latency_ms`, and an **estimated** per-session
  `earnings_usd = bytes_relayed/1e9 × SETTLEMENT_RATE_USD_PER_GB`.
- `DashboardReporter` channel now carries `ReporterMessage::{Telemetry, Session}`
  through one bounded queue; `try_push_session` is non-blocking like `try_push`.

`src/main.rs`:
- Reporter is created before the tunnel event handler (was after heartbeat
  wiring) and cloned into both tasks.
- `SessionClosed` handler federates when bridge enabled + details present.

### Dashboard side (`echomesh`)

- `report-telemetry` now routes `kind: "session"` to an idempotent
  `sessions` upsert keyed on the daemon session id (`onConflict: "session_id"`).
  Validates version/action/UUIDs/status/type CHECKs; computes
  `bandwidth_avg_mbps` from bytes + duration; maps the exit reason into
  `error_message` for failed/timeout rows.
- New migration `20260925000000_*.sql`: adds nullable `sessions.session_id`
  + unique index (seeded rows unaffected; retries stay idempotent).
- `ingest-telemetry` (synthetic) now **skips any node whose `last_seen_at` is
  younger than 15 minutes** — i.e. every node being driven by a live daemon —
  so the demo pipeline can no longer fabricate samples over real ones
  (returns `skipped` count). Its remaining role is seeding demo deployments.

### Known in-flight risks (Slice 2)
- Session rows are created only on **close**; `sessions.status` is always
  `completed` (active-session streaming needs provider-side `SessionEstablished`
  events — future work).
- Sessions that fail *before* relay teardown (e.g. target connect failure) do
  not federate yet — the early-error path returns before the close event.
- Consumer-side sessions are not federated (the consumer deliberately persists
  nothing; provider-side accounting is authoritative).
- Per-session `earnings_usd` is an estimate at the configured rate, not the
  receipt-verified `settlements` total. The settlement engine remains the
  authoritative economy (G3); wiring `sessions.earnings_usd` from verified
  receipts is a follow-up.

---

## The bridging roadmap

| Slice | Scope | Status |
|---|---|---|
| 0 | **Live sync path**: `echo-sync.sh` → `daemon-push` (ingest_token auth) + identity columns (`peer_id`/`region`) | ✅ Live (2026-10-03/04) |
| 1 | Real heartbeat ingestion: `telemetry.rs` + `report-telemetry` edge function + 32-dim vector + UUID node registration | ✅ Implemented (dormant — no `DASHBOARD_*` env set) |
| 2 | **Session federation**: provider relay accounting → `sessions` (idempotent upsert); synthetic `ingest-telemetry` replaced by Prometheus scraper (G2, G0 partial) | ✅ Done |
| 3 | **Claim/registration UX**: pairing flow binding a daemon (peer_id + keys) to a dashboard account; `DASHBOARD_NODE_ID` persistence in `identity.json` (G4) — note `peer_id`/`region` persistence already landed | 🔜 Next |
| 4 | **Match/capacity surfacing**: expose `capability_descriptors` + availability via edge function or RLS-view so the dashboard can show advertised capacity / provider matchmaking (G5 daemon-only) | ⏳ |
| 5 | **Decision**: IPFS `artifacts` + `copilot-chat` — either wire `cdn`/`storage` session types to them or park them (G5 dashboard-only) | ⏳ |

## Related files

- **Live sync path:** `scripts/echo-sync.sh` (both repos, forwarder),
  `echomesh/supabase/functions/daemon-push/index.ts` (ingest_token auth,
  metrics upsert + identity write),
  `echomesh/drizzle/migrations/0000_node_ingest_token.sql`,
  `echomesh/supabase/migrations/20261004000000_add_nodes_peer_id.sql`
- Daemon: `src/telemetry.rs` (frames + reporter), `src/tunnel.rs`
  (`SessionCloseDetails`), `src/main.rs` (heartbeat, session federation,
  startup), `Cargo.toml`
- Dashboard: `supabase/functions/report-telemetry/index.ts` (heartbeat +
  session routing), `supabase/functions/ingest-telemetry/index.ts`
  (Prometheus scraper), `supabase/config.toml`,
  `supabase/migrations/20260925000000_*.sql` (sessions.session_id)
- Contract: `echomesh/supabase/migrations/20260212024450_*.sql` (`sessions`
  table), `20260211084823_*.sql` (32-dim `node_intelligence`),
  `20260211084304_*.sql` (`nodes`)