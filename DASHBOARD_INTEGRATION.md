# Echo Node → EchoMesh Dashboard Integration Guide

**Updated:** 2026-10-17  
**Status:** ✅ Native Push + Session Receipts Working

---

## Overview

There are **two paths** to get echo-node data flowing to the echomesh dashboard:

### Path 1: Native Push (daemon → daemon-push) ✅ RECOMMENDED
- **Direct integration:** Daemon sends data directly to `daemon-push` edge function
- **Authentication:** `x-node-token` header (node's `ingest_token`)
- **Data flow:** Heartbeat metrics + session receipts
- **Latency:** Real-time (~30s for heartbeat, instant for sessions)

### Path 2: External Forwarder (echo-sync.sh → daemon-push)
- **Polling:** Script polls local `/metrics` every 30s
- **Authentication:** `x-node-token` header
- **Data flow:** Heartbeat metrics only (no sessions)
- **Use case:** When daemon can't reach dashboard directly (firewall, air-gap)

---

## Path 1: Native Push Configuration (RECOMMENDED)

### Step 1: Get Your Node's Ingest Token

```sql
-- In Supabase SQL Editor
SELECT id, name, ingest_token FROM nodes WHERE user_id = '<your-user-id>';

-- Or create a new node
INSERT INTO nodes (name, user_id, status, services, metadata)
VALUES ('MyEchoNode', '<your-user-id>', 'online', '{}', '{}')
RETURNING id, ingest_token;
```

Copy the `ingest_token` value (24-character hex string).

### Step 2: Configure Echo Node Daemon

Edit your `.env` file or set environment variables:

```bash
# Required for native push
DASHBOARD_TELEMETRY_URL="https://<project-id>.supabase.co/functions/v1/daemon-push"
DASHBOARD_TELEMETRY_TOKEN="<your-ingest-token-from-step1>"
DASHBOARD_USER_ID="<your-supabase-auth-user-uuid>"

# Optional: Pin a stable node UUID (recommended)
DASHBOARD_NODE_ID="<stable-uuid>"  # If omitted, generates new UUID per restart

# Optional: Configure spool and backoff
DASHBOARD_TELEMETRY_SPOOL="data/telemetry_spool.jsonl"  # Default
DASHBOARD_TELEMETRY_MAX_QUEUE=256                        # Default
DASHBOARD_TELEMETRY_BACKOFF_BASE_MS=500                  # Default
DASHBOARD_TELEMETRY_BACKOFF_MAX_MS=30000                 # Default
```

### Step 3: Restart Daemon

```bash
# If running as service
systemctl restart echo-daemon

# Or if running directly
./echo-daemon
```

### Step 4: Verify Data Flow

Check daemon logs for:
```
INFO dashboard telemetry push successful (heartbeat)
INFO dashboard telemetry push successful (session closed)
```

Check dashboard:
1. Navigate to **Live Daemon Card** - should show your node syncing
2. Navigate to **Nodes** page - should list your node with `peer_id`, `region`
3. Navigate to **Sessions** page - should populate after tunnel activity

---

## What Gets Sent?

### Heartbeat Frames (every 10 seconds)
```json
{
  "version": 1,
  "kind": "telemetry",
  "node_id": "<DASHBOARD_NODE_ID>",
  "user_id": "<DASHBOARD_USER_ID>",
  "timestamp": 1697500000,
  "nodes": {
    "id": "node-<epoch>",
    "name": "EchoNode",
    "peer_id": "12D3KooW...",
    "region": "auto",
    "status": "online",
    "uptime": 3600,
    "latency": 42.5,
    "quality_score": 0.85,
    "trust_score": 0.90,
    "available_upload_mbps": 95.0,
    "available_download_mbps": 95.0,
    "active_sessions": 3
  },
  "node_metrics": {
    "node_id": "node-<epoch>",
    "latency_ms": 42.5,
    "bandwidth_down": 95.0,
    "bandwidth_up": 95.0,
    "packet_loss_pct": 0.1,
    "quality_score": 0.85,
    "earnings_usd": 125.50
  },
  "network_metrics": {
    "active_nodes": 1,
    "avg_latency": 42.5,
    "bandwidth_egress": 1000.0,
    "bandwidth_ingress": 1000.0,
    "packet_loss_pct": 0.1,
    "uptime_pct": 99.9
  },
  "node_intelligence": {
    "quality_score": 0.85,
    "trust_score": 0.90,
    "anomaly_score": 0.05,
    "is_anomalous": false,
    "cluster_id": 0,
    "feature_vector": [/* 32 floats */]
  }
}
```

### Session Close Events (on tunnel teardown)
```json
{
  "version": 1,
  "kind": "session",
  "action": "closed",
  "node_id": "<DASHBOARD_NODE_ID>",
  "user_id": "<DASHBOARD_USER_ID>",
  "session_id": "tun_<session-id>",
  "status": "completed",
  "session_type": "tunnel",
  "protocol": "tcp",
  "remote_peer_id": "12D3KooW...",
  "remote_ip": "1.2.3.4",
  "remote_port": 52311,
  "bytes_relayed": 2000000000,
  "duration_secs": 60.0,
  "avg_latency_ms": 0.42,
  "earnings_usd": 1.0,
  "exit_reason": "completed",
  "started_at": "2026-01-01T00:00:00Z",
  "ended_at": "2026-01-01T00:01:00Z"
}
```

---

## Path 2: External Forwarder (Fallback)

### When to Use
- Daemon can't reach dashboard (firewall, air-gap)
- Want to separate concerns (daemon doesn't call external services)
- Testing without modifying daemon config

### Setup

1. **Get your ingest token** (same as Step 1 above)

2. **Run the forwarder:**
   ```bash
   # From echo-node/ or echomesh/ directory
   export ECHO_NODE_TOKEN="<your-ingest-token>"
   ./scripts/echo-sync.sh
   ```

3. **Run under supervisor (production):**
   ```bash
   while true; do
     ECHO_NODE_TOKEN="<token>" ./scripts/echo-sync.sh
     sleep 5
   done
   ```

### What Gets Sent
- **Only heartbeat metrics** (polls `GET /metrics` every 30s)
- **No session events** (script doesn't listen to tunnel events)

---

## Comparison: Native Push vs External Forwarder

| Feature | Native Push | External Forwarder |
|---------|-------------|-------------------|
| **Heartbeat metrics** | ✅ Real-time (10s) | ✅ Polling (30s) |
| **Session receipts** | ✅ Instant | ❌ Not supported |
| **Setup complexity** | Low (env vars) | Medium (script + supervisor) |
| **Resource usage** | Minimal | +1 process |
| **Failure handling** | Durable spool + backoff | Script restart |
| **Firewall friendly** | Requires outbound HTTPS | Requires outbound HTTPS |
| **Air-gap compatible** | ❌ No | ❌ No (both need internet) |

---

## Durable Spool (Native Push Only)

The native push path includes a durable spool for reliability:

### How It Works
1. **In-memory queue** (256 frames by default)
2. **Overflow → disk** (`data/telemetry_spool.jsonl`)
3. **On startup → replay** spool contents
4. **On shutdown → flush** remaining frames to spool

### Configuration
```bash
DASHBOARD_TELEMETRY_SPOOL="data/telemetry_spool.jsonl"
DASHBOARD_TELEMETRY_MAX_QUEUE=256
```

### Manual Inspection
```bash
# View spooled frames
cat data/telemetry_spool.jsonl | jq

# Clear spool (daemon must be stopped)
rm data/telemetry_spool.jsonl
```

---

## Error Handling

### Native Push
```
ERROR dashboard telemetry push failed: connection refused
INFO will retry with exponential backoff (attempt 2)
```

**Automatic retry with exponential backoff:**
- Base: 500ms
- Max: 30s
- Jitter: ±20%

**No data loss:** Failed frames go to durable spool.

### External Forwarder
```
curl: (7) Failed to connect to ... Connection refused
```

**Manual retry:** Supervisor loop restarts script after 5s.

---

## Authentication Details

### x-node-token Header
```http
POST /functions/v1/daemon-push HTTP/1.1
Host: <project-id>.supabase.co
Content-Type: application/json
x-node-token: <24-char-hex-ingest-token>

{
  "version": 1,
  "kind": "telemetry",
  ...
}
```

### How daemon-push Validates
```typescript
// 1. Extract token from header
const token = req.headers.get('x-node-token');

// 2. Query database
const { data: node } = await supabase
  .from('nodes')
  .select('id, user_id')
  .eq('ingest_token', token)
  .single();

// 3. Reject if not found
if (!node) {
  return new Response('Unauthorized', { status: 401 });
}

// 4. Write data scoped to node's user_id (RLS applies)
```

### Security Notes
- **Ingest tokens are node-specific** (1 token per node row)
- **Rotate via SQL:** `UPDATE nodes SET ingest_token = encode(gen_random_bytes(24),'hex') WHERE id = '...'`
- **Never commit tokens to git**
- **Use different tokens per environment** (dev/staging/prod)

---

## Troubleshooting

### Daemon logs "telemetry disabled (no config)"
**Cause:** Missing required env vars.  
**Fix:** Set `DASHBOARD_TELEMETRY_URL`, `DASHBOARD_TELEMETRY_TOKEN`, `DASHBOARD_USER_ID`.

### Daemon logs "dashboard telemetry push failed: 401 Unauthorized"
**Cause:** Invalid or expired `ingest_token`.  
**Fix:** 
1. Verify token in database: `SELECT ingest_token FROM nodes WHERE id = '...'`
2. Update env var to match
3. Restart daemon

### Daemon logs "dashboard telemetry push failed: 403 Forbidden"
**Cause:** RLS policy rejection (user_id mismatch).  
**Fix:**
1. Verify `DASHBOARD_USER_ID` matches the node's `user_id` in database
2. Check RLS policies on `nodes`, `node_metrics`, etc.

### Dashboard shows "Waiting for telemetry"
**Possible causes:**
1. Daemon not configured (check logs)
2. Network issue (firewall blocking outbound HTTPS)
3. Wrong Supabase project URL
4. Token expired or revoked

**Debug:**
```bash
# Test daemon → dashboard connectivity
curl -X POST \
  -H "x-node-token: <token>" \
  -H "Content-Type: application/json" \
  -d '{"version":1,"kind":"telemetry","test":true}' \
  https://<project>.supabase.co/functions/v1/daemon-push

# Should return 200 OK or 400 (validation error)
# 401 = bad token, 403 = RLS rejection
```

### Spool file growing indefinitely
**Cause:** Dashboard unreachable for extended period.  
**Fix:**
1. Check dashboard URL and token
2. Verify network connectivity
3. If dashboard is permanently down, clear spool: `rm data/telemetry_spool.jsonl`

---

## Migration from echo-sync.sh to Native Push

### Step-by-step
1. **Stop echo-sync.sh supervisor loop**
2. **Configure native push env vars** (see Step 2 above)
3. **Restart daemon**
4. **Verify logs** show "dashboard telemetry push successful"
5. **Optionally remove echo-sync.sh from startup scripts**

### Rollback
If issues arise, revert to forwarder:
1. Unset telemetry env vars
2. Restart daemon
3. Start echo-sync.sh again

**No data loss:** Both paths are idempotent (duplicate data is safe).

---

## Advanced: report-telemetry vs daemon-push

### report-telemetry (Legacy/Alternative)
- **Auth:** `Authorization: Bearer <shared-secret>`
- **Scope:** All nodes share one secret
- **Use case:** Original opt-in bridge design

### daemon-push (Current/Recommended)
- **Auth:** `x-node-token <node-ingest-token>`
- **Scope:** Each node has unique token
- **Use case:** Production deployment with per-node auth

### Can I use report-telemetry instead?
**Yes**, but you'd need to:
1. Change telemetry.rs back to `.bearer_auth(token)` (revert my change)
2. Set `DASHBOARD_TELEMETRY_URL` to `report-telemetry` endpoint
3. Set `DASHBOARD_TELEMETRY_TOKEN` to shared secret
4. Configure secret in Supabase: `supabase secrets set DASHBOARD_TELEMETRY_TOKEN="<secret>"`

**Why daemon-push is better:**
- Per-node authentication (more secure)
- No shared secrets
- Token rotation per node
- Simpler deployment (no secrets config needed)

---

## Quick Reference

### Environment Variables
```bash
# Native push (recommended)
DASHBOARD_TELEMETRY_URL=https://<project>.supabase.co/functions/v1/daemon-push
DASHBOARD_TELEMETRY_TOKEN=<24-char-hex-ingest-token>
DASHBOARD_USER_ID=<your-supabase-auth-uuid>
DASHBOARD_NODE_ID=<optional-stable-uuid>

# External forwarder (fallback)
ECHO_NODE_TOKEN=<24-char-hex-ingest-token>
```

### SQL Queries
```sql
-- Get ingest token
SELECT id, name, ingest_token FROM nodes WHERE user_id = '<uuid>';

-- Rotate token
UPDATE nodes SET ingest_token = encode(gen_random_bytes(24),'hex') WHERE id = '<node-id>';

-- Verify recent telemetry
SELECT node_id, timestamp, status FROM node_metrics ORDER BY timestamp DESC LIMIT 10;

-- Verify session receipts
SELECT session_id, status, earnings_usd FROM sessions WHERE node_id = '<node-id>' ORDER BY created_at DESC LIMIT 10;
```

### Curl Test
```bash
# Test daemon → dashboard connection
curl -X POST \
  -H "x-node-token: <token>" \
  -H "Content-Type: application/json" \
  -d '{"version":1,"kind":"telemetry","node_id":"test","user_id":"<uuid>","timestamp":1234567890}' \
  https://<project>.supabase.co/functions/v1/daemon-push
```

---

## Summary

✅ **Native push is now the recommended integration path**  
✅ **Changed telemetry.rs to use `x-node-token` header**  
✅ **Session receipts flow automatically with native push**  
✅ **Durable spool prevents data loss**  
✅ **External forwarder remains as fallback option**

**Next steps:**
1. Get your node's `ingest_token` from database
2. Set `DASHBOARD_TELEMETRY_*` env vars
3. Restart daemon
4. Watch dashboard populate with real-time data!

---

**Document Version:** 1.0  
**Last Updated:** 2026-10-17  
**Change:** Modified telemetry.rs to use x-node-token header for daemon-push compatibility
