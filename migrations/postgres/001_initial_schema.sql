-- Echo Node: Initial schema (PostgreSQL)
-- Applied once on first startup.

CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    user_id TEXT NOT NULL,
    peer_id TEXT,
    public_key TEXT,
    ip_address TEXT,
    region TEXT,
    status TEXT NOT NULL DEFAULT 'offline',
    last_seen_at TEXT,
    uptime DOUBLE PRECISION NOT NULL DEFAULT 0,
    latency DOUBLE PRECISION NOT NULL DEFAULT 0,
    bandwidth_used DOUBLE PRECISION NOT NULL DEFAULT 0,
    quality_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    earnings DOUBLE PRECISION NOT NULL DEFAULT 0,
    packet_loss DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    last_ip TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    services JSONB NOT NULL DEFAULT '{}'::jsonb,
    uptime_pct DOUBLE PRECISION DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION DEFAULT 0,
    bandwidth_down_mbps DOUBLE PRECISION DEFAULT 0,
    bandwidth_up_mbps DOUBLE PRECISION DEFAULT 0,
    packet_loss_pct DOUBLE PRECISION DEFAULT 0,
    trust_score DOUBLE PRECISION DEFAULT 0,
    sessions_count BIGINT DEFAULT 0,
    earnings_usd DOUBLE PRECISION DEFAULT 0,
    reported_upload_cap_mbps DOUBLE PRECISION DEFAULT 0,
    reported_download_cap_mbps DOUBLE PRECISION DEFAULT 0,
    supported_encryption TEXT DEFAULT 'noise-xx',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS node_metrics (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    latency_ms DOUBLE PRECISION DEFAULT 0,
    bandwidth_down_mbps DOUBLE PRECISION DEFAULT 0,
    bandwidth_up_mbps DOUBLE PRECISION DEFAULT 0,
    packet_loss_pct DOUBLE PRECISION DEFAULT 0,
    quality_score DOUBLE PRECISION DEFAULT 0,
    earnings_usd DOUBLE PRECISION DEFAULT 0,
    recorded_at TEXT DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS network_metrics (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    active_nodes BIGINT DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION DEFAULT 0,
    bandwidth_egress_mb DOUBLE PRECISION DEFAULT 0,
    bandwidth_ingress_mb DOUBLE PRECISION DEFAULT 0,
    packet_loss_pct DOUBLE PRECISION DEFAULT 0,
    uptime_pct DOUBLE PRECISION DEFAULT 0,
    earnings_usd DOUBLE PRECISION DEFAULT 0,
    recorded_at TEXT DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS node_intelligence (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    quality_score DOUBLE PRECISION DEFAULT 0,
    trust_score DOUBLE PRECISION DEFAULT 0,
    anomaly_score DOUBLE PRECISION DEFAULT 0,
    is_anomalous BOOLEAN DEFAULT FALSE,
    cluster_id BIGINT DEFAULT 0,
    feature_vector JSONB,
    recorded_at TEXT DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS peer_reputation (
    peer_id TEXT PRIMARY KEY,
    reputation_score DOUBLE PRECISION DEFAULT 0.5,
    total_bytes_relayed BIGINT DEFAULT 0,
    successful_sessions BIGINT DEFAULT 0,
    failed_sessions BIGINT DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION DEFAULT 0,
    last_active_at TEXT,
    recorded_at TEXT DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS connection_history (
    id BIGSERIAL PRIMARY KEY,
    local_peer_id TEXT NOT NULL,
    remote_peer_id TEXT NOT NULL,
    remote_ip TEXT,
    remote_port INTEGER,
    direction TEXT NOT NULL,
    bytes_sent BIGINT DEFAULT 0,
    bytes_received BIGINT DEFAULT 0,
    duration_secs DOUBLE PRECISION DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION DEFAULT 0,
    exit_reason TEXT,
    started_at TEXT DEFAULT (now() AT TIME ZONE 'utc'),
    ended_at TEXT
);

CREATE TABLE IF NOT EXISTS state_receipts (
    receipt_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    signer_peer_id TEXT NOT NULL,
    counterparty_peer_id TEXT NOT NULL,
    bytes_transferred BIGINT DEFAULT 0,
    direction TEXT NOT NULL,
    sequence_number BIGINT DEFAULT 0,
    timestamp_secs BIGINT DEFAULT 0,
    signature TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capability_descriptors (
    peer_id TEXT PRIMARY KEY,
    public_key BYTEA NOT NULL,
    region TEXT NOT NULL,
    upload_cap_mbps DOUBLE PRECISION DEFAULT 0,
    download_cap_mbps DOUBLE PRECISION DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION DEFAULT 0,
    reputation_score DOUBLE PRECISION DEFAULT 0.5,
    supported_encryption TEXT DEFAULT '["noise-xx"]',
    max_sessions INTEGER DEFAULT 1,
    active_sessions INTEGER DEFAULT 0,
    last_updated BIGINT DEFAULT 0,
    noise_public_key BYTEA DEFAULT E'\\x00'
);

CREATE INDEX IF NOT EXISTS idx_conn_history_local ON connection_history(local_peer_id);
CREATE INDEX IF NOT EXISTS idx_conn_history_remote ON connection_history(remote_peer_id);
CREATE INDEX IF NOT EXISTS idx_receipts_session ON state_receipts(session_id);
CREATE INDEX IF NOT EXISTS idx_caps_region ON capability_descriptors(region);
