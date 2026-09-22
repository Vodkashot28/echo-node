-- Echo Node: Initial schema (SQLite)
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
    uptime REAL NOT NULL DEFAULT 0,
    latency REAL NOT NULL DEFAULT 0,
    bandwidth_used REAL NOT NULL DEFAULT 0,
    quality_score REAL NOT NULL DEFAULT 0,
    trust_score REAL DEFAULT 0,
    earnings REAL NOT NULL DEFAULT 0,
    packet_loss REAL NOT NULL DEFAULT 0,
    last_updated TEXT,
    last_ip TEXT,
    metadata TEXT DEFAULT '{}',
    services TEXT NOT NULL DEFAULT '{}',
    uptime_pct REAL DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0,
    bandwidth_down_mbps REAL DEFAULT 0,
    bandwidth_up_mbps REAL DEFAULT 0,
    packet_loss_pct REAL DEFAULT 0,
    sessions_count INTEGER DEFAULT 0,
    earnings_usd REAL DEFAULT 0,
    reported_upload_cap_mbps REAL DEFAULT 0,
    reported_download_cap_mbps REAL DEFAULT 0,
    supported_encryption TEXT DEFAULT 'noise-xx',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS node_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    latency_ms REAL DEFAULT 0,
    bandwidth_down_mbps REAL DEFAULT 0,
    bandwidth_up_mbps REAL DEFAULT 0,
    packet_loss_pct REAL DEFAULT 0,
    quality_score REAL DEFAULT 0,
    earnings_usd REAL DEFAULT 0,
    recorded_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS network_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    active_nodes INTEGER DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0,
    bandwidth_egress_mb REAL DEFAULT 0,
    bandwidth_ingress_mb REAL DEFAULT 0,
    packet_loss_pct REAL DEFAULT 0,
    uptime_pct REAL DEFAULT 0,
    earnings_usd REAL DEFAULT 0,
    recorded_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS node_intelligence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    quality_score REAL DEFAULT 0,
    trust_score REAL DEFAULT 0,
    anomaly_score REAL DEFAULT 0,
    is_anomalous INTEGER DEFAULT 0,
    cluster_id INTEGER DEFAULT 0,
    feature_vector TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS peer_reputation (
    peer_id TEXT PRIMARY KEY,
    reputation_score REAL DEFAULT 0.5,
    total_bytes_relayed INTEGER DEFAULT 0,
    successful_sessions INTEGER DEFAULT 0,
    failed_sessions INTEGER DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0,
    last_active_at TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS connection_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    local_peer_id TEXT NOT NULL,
    remote_peer_id TEXT NOT NULL,
    remote_ip TEXT,
    remote_port INTEGER,
    direction TEXT NOT NULL,
    bytes_sent INTEGER DEFAULT 0,
    bytes_received INTEGER DEFAULT 0,
    duration_secs REAL DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0,
    exit_reason TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    ended_at TEXT
);

CREATE TABLE IF NOT EXISTS state_receipts (
    receipt_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    signer_peer_id TEXT NOT NULL,
    counterparty_peer_id TEXT NOT NULL,
    bytes_transferred INTEGER DEFAULT 0,
    direction TEXT NOT NULL,
    sequence_number INTEGER DEFAULT 0,
    timestamp_secs INTEGER DEFAULT 0,
    signature TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capability_descriptors (
    peer_id TEXT PRIMARY KEY,
    public_key BLOB NOT NULL,
    region TEXT NOT NULL,
    upload_cap_mbps REAL DEFAULT 0,
    download_cap_mbps REAL DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0,
    reputation_score REAL DEFAULT 0.5,
    supported_encryption TEXT DEFAULT '["noise-xx"]',
    max_sessions INTEGER DEFAULT 1,
    active_sessions INTEGER DEFAULT 0,
    last_updated INTEGER DEFAULT 0,
    noise_public_key BLOB DEFAULT X'00'
);

CREATE INDEX IF NOT EXISTS idx_conn_history_local ON connection_history(local_peer_id);
CREATE INDEX IF NOT EXISTS idx_conn_history_remote ON connection_history(remote_peer_id);
CREATE INDEX IF NOT EXISTS idx_receipts_session ON state_receipts(session_id);
CREATE INDEX IF NOT EXISTS idx_caps_region ON capability_descriptors(region);
