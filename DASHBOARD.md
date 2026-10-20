# Echo Node ↔ EchoMesh Dashboard Integration

**Merged doc** — replaces `GAPS.md` (integration analysis + roadmap) and
`DASHBOARD_INTEGRATION.md` (ops guide). Claims verified against
`src/telemetry.rs` and `scripts/echo-sync.sh`, 2026-10-19.

The daemon and the dashboard (echomesh: React + Supabase) are separate
systems: the daemon talks only to its own DB + local REST API, the dashboard
talks only to Supabase. Two paths bridge them — the daemon never talks to
Supabase except through the **opt-in** native push (Path B).

## Paths at a glance

| | Path A — External forwarder | Path B — Native push (in-daemon) |
|---|---|---|
| **Status** | ✅ **Live / deployed** since 2026-10-04 | Implemented, **off by default** (dormant) |
| **Mechanism** | `scripts/echo-sync.sh` polls local `GET /metrics` every ~30 s, POSTs to the `daemon-push` edge function | `telemetry.rs` `DashboardReporter` pushes from inside the daemon |
| **Auth** | header `x-node-token: <nodes.ingest_token>` | header `Authorization: Bearer <ECHO_API_KEY>` |
| **Data** | heartbeat metrics only | heartbeat frames **+** session-close events |
| **Reliability** | supervisor loop restart | bounded queue → exponential backoff → durable disk spool |
| **Endpoint** | Supabase edge fn `daemon-push` | any ingest accepting Bearer (e.g. `report-telemetry`-style) |

> ⚠️ **They are not interchangeable on auth.** The daemon's native push
> sends `Bearer <ECHO_API_KEY>` and never sends `x-node-token`, while
> `daemon-push` resolves nodes by `x-node-token`. Native push therefore
> targets a Bearer-authenticated ingest (its original `report-telemetry`
> design); Path A is what feeds `daemon-push` today.

---

## Path A — External forwarder (live path)

```bash
# value = the node's rows.ingest_token (DB-generated 24-byte hex)
ECHO_NODE_TOKEN=<ingest_token> ./scripts/echo-sync.sh

# run under a supervisor so transient outages self-heal
while true; do ECHO_NODE_TOKEN=<token> ./scripts/echo-sync.sh; sleep 5; done
```

- Polls local `GET /metrics` and POSTs the JSON to the `daemon-push` edge
  function every ~30 s (header `x-node-token`).
- `daemon-push` resolves the dashboard row by `ingest_token`
  (`encode(gen_random_bytes(24),'hex')`, unique per node) and upserts the
  metrics; it also persists `peer_id` + `region` from the payload.
  `ip_address` is intentionally **not** written — the node-exporter scrape
  path owns that column.
- No session events (the script cannot see tunnel teardowns).

Token SQL:

```sql
-- read a node's token
SELECT id, name, ingest_token FROM nodes WHERE user_id = '<uuid>';

-- rotate
UPDATE nodes SET ingest_token = encode(gen_random_bytes(24),'hex')
WHERE id = '<node-id>';
```

---

## Path B — Native push (opt-in, dormant)

Enabled only when **both** are set; otherwise the bridge is fully off and the
daemon behaves exactly as before (reporting never blocks the heartbeat).

```bash
# required
DASHBOARD_TELEMETRY_URL="https://<project>.supabase.co/functions/v1/<ingest>"
ECHO_API_KEY="<key sent as Authorization: Bearer>"

# optional (defaults shown)
DASHBOARD_TELEMETRY_SPOOL="data/telemetry_spool.jsonl"
DASHBOARD_TELEMETRY_MAX_QUEUE=256
DASHBOARD_TELEMETRY_BACKOFF_BASE_MS=500
DASHBOARD_TELEMETRY_BACKOFF_MAX_MS=30000
```

> **Stale-env warning:** earlier revisions documented
> `DASHBOARD_TELEMETRY_TOKEN`, `DASHBOARD_USER_ID`, and `DASHBOARD_NODE_ID`.
> Current code reads **none** of them — auth is `ECHO_API_KEY`, and row
> correlation is the `peer_id` carried in every payload. Remove those from
> `.env` if present.

### What is sent

**Heartbeat frame** — one per heartbeat cycle (10 s default), posted as raw
JSON:

```jsonc
{
  "version": 1,
  "timestamp": "2026-10-19T12:00:00Z",
  "peer_id": "12D3KooW…",          // correlation key
  "node":        { /* NodeRow: id, region, status, uptime, latency, … */ },
  "node_metrics":     { /* per-sample latency/bandwidth/loss/earnings */ },
  "network_metrics":  { /* network-wide aggregates */ },
  "intelligence":     { /* anomaly fields + 32-dim feature_vector */ }
}
```

**Session-close event** — one per completed provider-side relay session
(consumers never federate; provider accounting is authoritative):

```jsonc
{
  "kind": "session",               // receiver discriminates on this
  "action": "closed",
  "version": 1,
  "peer_id": "12D3KooW…",
  "session_id": "…",
  "remote_peer_id": "…", "remote_ip": "…", "remote_port": 52311,
  "direction": "inbound", "status": "completed",
  "session_type": "tunnel", "protocol": "tcp",
  "started_at": "…", "ended_at": "…", "duration_secs": 60.0,
  "bytes_sent": 0, "bytes_received": 0, "bytes_relayed": 2000000000,
  "avg_latency_ms": 0.42, "exit_reason": "completed",
  "settlement_rate_usd_per_gb": 0.5,
  "earnings_usd": 1.0               // bytes/1e9 × rate — an estimate;
                                     // `settlements` remains authoritative
}
```

### Reliability model

1. Bounded in-memory FIFO (drop-oldest on overflow → fresh state wins).
2. Failed POST → exponential backoff (base 500 ms → max 30 s, ±20 % jitter),
   frame kept at queue head.
3. On shutdown, unsent frames flush to the JSONL spool; on next boot the
   spool replays.

```bash
cat data/telemetry_spool.jsonl | jq   # inspect
rm data/telemetry_spool.jsonl         # clear (daemon stopped)
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| log: `dashboard telemetry reporting disabled (set DASHBOARD_TELEMETRY_URL and ECHO_API_KEY)` | Path B env missing | set both vars (or ignore if you use Path A) |
| `push failed: … 401` | wrong/rotated key or token | check `ECHO_API_KEY` (Path B) / `nodes.ingest_token` (Path A) |
| `push failed: … 403` | RLS rejection | endpoint-side: row `user_id` must match the writing principal |
| dashboard stuck on "waiting" | forwarder not running / firewall | run `echo-sync.sh` under supervisor; verify outbound HTTPS |
| spool growing | ingest unreachable for long | fix URL/key, or clear spool if the endpoint is gone |

Connectivity probe (Path A auth):

```bash
curl -X POST -H "x-node-token: <ingest_token>" \
  -H "Content-Type: application/json" \
  -d '{"version":1,"kind":"telemetry","test":true}' \
  https://<project>.supabase.co/functions/v1/daemon-push
# 200/400 = reachable; 401 = bad token; 403 = RLS
```

---

## Integration gaps (formerly GAPS.md G0–G5)

- **G0 — telemetry theater (fixed):** the dashboard's old Pipeline Control
  fed `Math.random()`-generated rows back into Supabase. It was rewritten as
  a Prometheus scraper (`ingest-telemetry` parses a node's node_exporter
  endpoint from `nodes.ip_address` + `metrics_port`) and the heartbeat reaches
  the dashboard via Path A. Synthetic seeding now **skips** any node seen in
  the last 15 minutes, so demo data can no longer overwrite live nodes.
  *(Pending: the `ip_address`/`metrics_port` target row UPDATE still needs
  service-role access.)*
- **G1 — incompatible `nodes` contracts:** daemon `nodes.id` is
  `varchar node-{epoch}` vs dashboard UUID PK; `user_id` is an env string vs
  `auth.uid()`; feature vectors are 32-dim on both sides but `cluster_id`/
  `uptime_pct` semantics differ. Pointing the daemon straight at the
  dashboard Postgres fails (PK/FK/RLS) — hence the edge-function bridge.
- **G2 — sessions (fixed):** dashboard `sessions` was seeded-only; Slice 2
  federates provider `connection_history` teardowns into it (idempotent
  upsert on session id). Active/streaming rows and pre-relay failures are
  still out of scope.
- **G3 — three earnings economies:** heartbeat uptime-RU estimate,
  receipt-verified `settlements` (authoritative), dashboard synthetic
  `sessions.earnings_usd` (estimate at configured rate). Treat
  `settlements` as the only verified number.
- **G4 — identity bridge (partial):** `ingest_token` + `peer_id`/`region`
  persist on the push path; a user-facing claim/pairing UX binding a daemon
  to a dashboard account is still open.
- **G5 — feature drift:** dashboard-only (`copilot-chat`, IPFS artifacts,
  realtime) vs daemon-only (`peer_reputation`, receipts, settlements,
  capability descriptors, live tunnels).
- **G6 — cross-node auction propagation: ✅ done (2026-10-16)** — this gap
  is marketplace, not dashboard; its full record now lives in
  [ARCHITECTURE.md](ARCHITECTURE.md#cross-node-marketplace-g6).

## Bridging roadmap

| Slice | Scope | Status |
|---|---|---|
| 0 | Live sync path: `echo-sync.sh` → `daemon-push` (ingest_token) + `peer_id`/`region` columns | ✅ live |
| 1 | Native push: `telemetry.rs` + 32-dim vector + Bearer ingest | ✅ implemented (dormant — no `DASHBOARD_*` env set) |
| 2 | Session federation: provider teardown → `sessions` upsert; synthetic pipeline replaced by scraper | ✅ done |
| 3 | Claim/pairing UX binding peer_id+keys to a dashboard account (G4) | 🔜 next |
| 4 | Surface `capability_descriptors` + availability to the dashboard (G5) | ⏳ |
| 5 | Decide IPFS `artifacts`/`copilot-chat` fate (G5) | ⏳ |

## Related files

- **Forwarder:** `scripts/echo-sync.sh` (both repos)
- **Daemon:** `src/telemetry.rs` (frames, spool, backoff), `src/tunnel.rs`
  (`SessionCloseDetails`), `src/main.rs` (heartbeat + session federation wiring)
- **Dashboard:** `supabase/functions/daemon-push/`, `supabase/functions/
  ingest-telemetry/`, `supabase/functions/report-telemetry/`
