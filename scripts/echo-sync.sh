#!/usr/bin/env bash
# Forwards echo-node daemon metrics (GET /metrics) to the EchoMesh dashboard.
# Usage: ECHO_NODE_TOKEN=<token> ./echo-sync.sh
set -u
DAEMON_URL="${DAEMON_URL:-http://127.0.0.1:3001/metrics}"
PUSH_URL="${PUSH_URL:-https://xhxlgyonitgyylsdkknj.supabase.co/functions/v1/daemon-push}"
INTERVAL="${INTERVAL:-30}"
: "${ECHO_NODE_TOKEN:?set ECHO_NODE_TOKEN}"
while true; do
  if body=$(curl -fsS --max-time 5 "$DAEMON_URL"); then
    curl -sS --max-time 10 -X POST "$PUSH_URL" \
      -H "Content-Type: application/json" -H "x-node-token: $ECHO_NODE_TOKEN" \
      -d "$body"; echo
  else
    echo "$(date -u +%FT%TZ) daemon not reachable at $DAEMON_URL" >&2
  fi
  sleep "$INTERVAL"
done
