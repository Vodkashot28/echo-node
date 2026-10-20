#!/bin/sh
# Render injects $PORT dynamically. Map it to LISTEN_ADDR so the daemon
# binds on the right port regardless of what Render assigns.
if [ -n "$PORT" ] && [ -z "$LISTEN_ADDR" ]; then
    export LISTEN_ADDR="0.0.0.0:${PORT}"
elif [ -z "$LISTEN_ADDR" ]; then
    export LISTEN_ADDR="0.0.0.0:10000"
fi

exec "$@"
