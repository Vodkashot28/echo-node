# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM rust:1.81-slim AS builder

# Install build dependencies (libp2p needs OpenSSL, sqlx needs pkg-config)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies: copy manifests first, build a stub, then replace with real source
COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY migrations ./migrations

# Touch main.rs so Cargo detects the change
RUN touch src/main.rs \
    && cargo build --release

# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -m -u 1000 echomesh
WORKDIR /home/echomesh

COPY --from=builder /app/target/release/echo-node /usr/local/bin/echo-node

RUN mkdir -p /home/echomesh/data/tickets \
    && chown -R echomesh:echomesh /home/echomesh

# Entrypoint script: Render injects $PORT; map it to LISTEN_ADDR if not already set.
COPY --chown=echomesh:echomesh docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER echomesh

# REST API (Render routes HTTP here via $PORT)
EXPOSE 10000
# Tunnel (provider-to-consumer; not HTTP-routed by Render)
EXPOSE 3002

ENV TUNNEL_ADDR=0.0.0.0:3002 \
    IDENTITY_PATH=/home/echomesh/data/identity.json \
    SQLITE_DB=sqlite:/home/echomesh/data/echo_node.db \
    RUST_LOG=echo_daemon=info \
    DHT_LOCAL_ONLY=false \
    NODE_MODE=provider

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["echo-node"]
