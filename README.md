# echo-node

Rust daemon for the EchoMesh decentralized infrastructure network. Collects real system metrics and syncs to Supabase.

## Features

- Real CPU, memory, and network metrics via `sysinfo`
- Real latency and packet loss via ICMP ping
- Bandwidth rate calculation (bytes/sec over sliding window)
- Quality score and trust score computation
- Supabase backend (remote) or SQLite (local dev)
- REST API on port 3001

## Quick Start

```bash
# Local development (SQLite)
cargo run

# Production (Supabase)
cp .env.example .env
# Fill in SUPABASE_URL, SUPABASE_ANON_KEY, USER_ID
cargo run --release
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics` | Latest metrics snapshot |
| GET | `/nodes` | Node info with current stats |
| GET | `/history` | Last 60 metric samples |
| GET | `/status` | Daemon status |
| GET | `/health` | Health check |
| POST | `/control` | Start/stop/restart daemon |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | No | Supabase project URL (uses SQLite if unset) |
| `SUPABASE_ANON_KEY` | No | Supabase publishable/anon key |
| `USER_ID` | No | User identifier (default: anonymous) |
| `NODE_ID` | No | Node identifier (auto-generated if unset) |
| `NODE_NAME` | No | Display name (default: EchoNode) |
| `NODE_REGION` | No | Region tag (default: auto) |
| `LISTEN_ADDR` | No | Bind address (default: 0.0.0.0:3001) |
| `SQLITE_DB` | No | SQLite path (default: sqlite:echo_node.db) |

## Architecture

```
daemon
├── main.rs          # Heartbeat loop, REST API, metrics collection
├── supabase.rs      # Supabase REST client (remote backend)
└── sqlite_store.rs  # SQLite client (local backend)
```

Every 10 seconds the daemon:
1. Reads CPU, memory, network from sysinfo
2. Pings 1.1.1.1, 8.8.8.8, 208.67.222.222 for latency/loss
3. Computes bandwidth rate from cumulative counters
4. Calculates quality_score and trust_score
5. Upserts to Supabase or SQLite
