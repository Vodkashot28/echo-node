# ── Stage 1: Build ─────────────────────────────────────────────────────────────
# Toolchain MUST satisfy Cargo.lock: 47 locked crates declare rust-version > 1.81
# (30 are edition 2024; max = 1.88 via sqlx-postgres → home; clap is a direct dep).
# `1-slim` tracks the latest stable 1.x so future lockfile bumps never outrun it.
FROM rust:1-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency cache: compile the heavy dependency graph once against stub
# sources, then swap in the real tree so only echo-daemon recompiles.
# (Cargo.toml declares no explicit [lib], so a stub main.rs alone is enough.)
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY migrations ./migrations
RUN touch src/main.rs src/lib.rs && cargo build --release

# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
# trixie matches the builder's Debian (glibc) — keep the two stages in sync or
# the copied binary dies with GLIBC_2.xx not found.
FROM debian:trixie-slim AS runtime

# No libssl here on purpose: reqwest uses rustls-tls and sqlx uses
# runtime-tokio-rustls (verified: the binary links no libssl/libcrypto).
# iputils-ping is required — the heartbeat shells out to ping for RTT probes.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -m -u 1000 echomesh
WORKDIR /home/echomesh

COPY --from=builder /app/target/release/echo-node /usr/local/bin/echo-node

RUN mkdir -p /home/echomesh/data/tickets \
    && chown -R echomesh:echomesh /home/echomesh

# Entrypoint: Render injects $PORT; map it to LISTEN_ADDR if not already set.
# (The daemon itself also falls back to $PORT when LISTEN_ADDR is unset/empty,
# so this is belt-and-suspenders.)
COPY --chown=echomesh:echomesh docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER echomesh

# REST API (Render routes HTTP here via $PORT / LISTEN_ADDR)
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
