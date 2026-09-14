use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

#[derive(Debug)]
pub struct NodeRow {
    pub id: String,
    pub name: String,
    pub user_id: String,
    pub ip_address: Option<String>,
    pub region: Option<String>,
    pub status: Option<String>,
    pub last_seen_at: Option<String>,
    pub uptime_pct: Option<f64>,
    pub avg_latency_ms: Option<f64>,
    pub bandwidth_down_mbps: Option<f64>,
    pub bandwidth_up_mbps: Option<f64>,
    pub packet_loss_pct: Option<f64>,
    pub quality_score: Option<f64>,
    pub trust_score: Option<f64>,
    pub sessions_count: Option<i64>,
    pub earnings_usd: Option<f64>,
}

#[derive(Debug)]
pub struct NodeMetricsRow {
    pub node_id: String,
    pub user_id: String,
    pub latency_ms: Option<f64>,
    pub bandwidth_down_mbps: Option<f64>,
    pub bandwidth_up_mbps: Option<f64>,
    pub packet_loss_pct: Option<f64>,
    pub quality_score: Option<f64>,
    pub earnings_usd: Option<f64>,
    pub recorded_at: Option<String>,
}

#[derive(Debug)]
pub struct NetworkMetricsRow {
    pub user_id: String,
    pub active_nodes: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub bandwidth_egress_mb: Option<f64>,
    pub bandwidth_ingress_mb: Option<f64>,
    pub packet_loss_pct: Option<f64>,
    pub uptime_pct: Option<f64>,
    pub earnings_usd: Option<f64>,
    pub recorded_at: Option<String>,
}

#[derive(Debug)]
pub struct NodeIntelligenceRow {
    pub node_id: String,
    pub user_id: String,
    pub quality_score: Option<f64>,
    pub trust_score: Option<f64>,
    pub anomaly_score: Option<f64>,
    pub is_anomalous: Option<bool>,
    pub cluster_id: Option<i64>,
    pub feature_vector: Option<serde_json::Value>,
    pub recorded_at: Option<String>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    user_id TEXT NOT NULL,
    ip_address TEXT,
    region TEXT,
    status TEXT DEFAULT 'offline',
    last_seen_at TEXT,
    uptime_pct REAL DEFAULT 0,
    avg_latency_ms REAL DEFAULT 0,
    bandwidth_down_mbps REAL DEFAULT 0,
    bandwidth_up_mbps REAL DEFAULT 0,
    packet_loss_pct REAL DEFAULT 0,
    quality_score REAL DEFAULT 0,
    trust_score REAL DEFAULT 0,
    sessions_count INTEGER DEFAULT 0,
    earnings_usd REAL DEFAULT 0,
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
"#;

impl SqliteStore {
    pub async fn new(db_path: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(db_path)
            .context("invalid db path")?
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("failed to connect sqlite")?;

        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .context("failed to create schema")?;

        Ok(Self { pool })
    }

    pub async fn upsert_node(&self, row: &NodeRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO nodes (id, name, user_id, ip_address, region, status, last_seen_at,
                uptime_pct, avg_latency_ms, bandwidth_down_mbps, bandwidth_up_mbps,
                packet_loss_pct, quality_score, trust_score, sessions_count, earnings_usd, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, user_id=excluded.user_id, ip_address=excluded.ip_address,
                region=excluded.region, status=excluded.status, last_seen_at=excluded.last_seen_at,
                uptime_pct=excluded.uptime_pct, avg_latency_ms=excluded.avg_latency_ms,
                bandwidth_down_mbps=excluded.bandwidth_down_mbps, bandwidth_up_mbps=excluded.bandwidth_up_mbps,
                packet_loss_pct=excluded.packet_loss_pct, quality_score=excluded.quality_score,
                trust_score=excluded.trust_score, sessions_count=excluded.sessions_count,
                earnings_usd=excluded.earnings_usd, updated_at=datetime('now')"#,
        )
        .bind(&row.id)
        .bind(&row.name)
        .bind(&row.user_id)
        .bind(&row.ip_address)
        .bind(&row.region)
        .bind(&row.status)
        .bind(&row.last_seen_at)
        .bind(row.uptime_pct)
        .bind(row.avg_latency_ms)
        .bind(row.bandwidth_down_mbps)
        .bind(row.bandwidth_up_mbps)
        .bind(row.packet_loss_pct)
        .bind(row.quality_score)
        .bind(row.trust_score)
        .bind(row.sessions_count)
        .bind(row.earnings_usd)
        .execute(&self.pool)
        .await
        .context("sqlite upsert node")?;
        Ok(())
    }

    pub async fn insert_node_metrics(&self, row: &NodeMetricsRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO node_metrics (node_id, user_id, latency_ms, bandwidth_down_mbps,
                bandwidth_up_mbps, packet_loss_pct, quality_score, earnings_usd, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&row.node_id)
        .bind(&row.user_id)
        .bind(row.latency_ms)
        .bind(row.bandwidth_down_mbps)
        .bind(row.bandwidth_up_mbps)
        .bind(row.packet_loss_pct)
        .bind(row.quality_score)
        .bind(row.earnings_usd)
        .bind(&row.recorded_at)
        .execute(&self.pool)
        .await
        .context("sqlite insert node_metrics")?;
        Ok(())
    }

    pub async fn insert_network_metrics(&self, row: &NetworkMetricsRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO network_metrics (user_id, active_nodes, avg_latency_ms, bandwidth_egress_mb,
                bandwidth_ingress_mb, packet_loss_pct, uptime_pct, earnings_usd, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&row.user_id)
        .bind(row.active_nodes)
        .bind(row.avg_latency_ms)
        .bind(row.bandwidth_egress_mb)
        .bind(row.bandwidth_ingress_mb)
        .bind(row.packet_loss_pct)
        .bind(row.uptime_pct)
        .bind(row.earnings_usd)
        .bind(&row.recorded_at)
        .execute(&self.pool)
        .await
        .context("sqlite insert network_metrics")?;
        Ok(())
    }

    pub async fn insert_node_intelligence(&self, row: &NodeIntelligenceRow) -> Result<()> {
        let fv = row
            .feature_vector
            .as_ref()
            .map(|v| v.to_string());
        sqlx::query(
            r#"INSERT INTO node_intelligence (node_id, user_id, quality_score, trust_score,
                anomaly_score, is_anomalous, cluster_id, feature_vector, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&row.node_id)
        .bind(&row.user_id)
        .bind(row.quality_score)
        .bind(row.trust_score)
        .bind(row.anomaly_score)
        .bind(row.is_anomalous.map(|b| b as i32))
        .bind(row.cluster_id)
        .bind(&fv)
        .bind(&row.recorded_at)
        .execute(&self.pool)
        .await
        .context("sqlite insert node_intelligence")?;
        Ok(())
    }
}
