use crate::models::*;
use crate::MetricsBackend;
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone)]
pub struct NeonStore {
    pool: PgPool,
}

const SCHEMA: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS nodes (
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
    )"#,
    r#"CREATE TABLE IF NOT EXISTS node_metrics (
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
    )"#,
    r#"CREATE TABLE IF NOT EXISTS network_metrics (
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
    )"#,
    r#"CREATE TABLE IF NOT EXISTS node_intelligence (
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
    )"#,
    r#"CREATE TABLE IF NOT EXISTS peer_reputation (
        peer_id TEXT PRIMARY KEY,
        reputation_score DOUBLE PRECISION DEFAULT 0.5,
        total_bytes_relayed BIGINT DEFAULT 0,
        successful_sessions BIGINT DEFAULT 0,
        failed_sessions BIGINT DEFAULT 0,
        avg_latency_ms DOUBLE PRECISION DEFAULT 0,
        last_active_at TEXT,
        recorded_at TEXT DEFAULT (now() AT TIME ZONE 'utc')
    )"#,
    r#"CREATE TABLE IF NOT EXISTS connection_history (
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
    )"#,
    r#"CREATE TABLE IF NOT EXISTS state_receipts (
        receipt_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        signer_peer_id TEXT NOT NULL,
        counterparty_peer_id TEXT NOT NULL,
        bytes_transferred BIGINT DEFAULT 0,
        direction TEXT NOT NULL,
        sequence_number BIGINT DEFAULT 0,
        timestamp_secs BIGINT DEFAULT 0,
        signature TEXT NOT NULL
    )"#,
    r#"CREATE TABLE IF NOT EXISTS capability_descriptors (
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
    )"#,
    "CREATE INDEX IF NOT EXISTS idx_conn_history_local ON connection_history(local_peer_id)",
    "CREATE INDEX IF NOT EXISTS idx_conn_history_remote ON connection_history(remote_peer_id)",
    "CREATE INDEX IF NOT EXISTS idx_receipts_session ON state_receipts(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_caps_region ON capability_descriptors(region)",
];

/// Inline migrations: each entry is a single, complete SQL statement.
/// Using a slice avoids split-on-semicolon fragility with literals.
const MIGRATIONS: &[&str] = &[
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'EchoNode'",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS user_id TEXT NOT NULL DEFAULT 'anonymous'",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS peer_id TEXT",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS public_key TEXT",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS ip_address TEXT",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS region TEXT",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'offline'",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_seen_at TEXT",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS uptime DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS latency DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS bandwidth_used DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS earnings DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS packet_loss DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS last_ip TEXT",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS services JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS uptime_pct DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS avg_latency_ms DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS bandwidth_down_mbps DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS bandwidth_up_mbps DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS packet_loss_pct DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS trust_score DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS sessions_count BIGINT DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS earnings_usd DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS reported_upload_cap_mbps DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS reported_download_cap_mbps DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS supported_encryption TEXT DEFAULT 'noise-xx'",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now()",
    "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now()",
    "ALTER TABLE capability_descriptors ADD COLUMN IF NOT EXISTS noise_public_key BYTEA DEFAULT E'\\x00'",
    // Convert legacy TEXT[] → TEXT (JSON) for supported_encryption; no-op if already TEXT
    "ALTER TABLE capability_descriptors ALTER COLUMN supported_encryption TYPE TEXT USING to_json(supported_encryption)::text",
    // Upgrade created_at/updated_at from TEXT to TIMESTAMPTZ; no-op if already correct type
    "ALTER TABLE nodes ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at::TIMESTAMPTZ",
    "ALTER TABLE nodes ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at::TIMESTAMPTZ",
];

impl NeonStore {
    pub async fn new(database_url: &str) -> Result<Self> {
        let options =
            PgConnectOptions::from_str(database_url).context("invalid Neon/Postgres database URL")?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .min_connections(1)
            // Neon serverless computes can have cold-start latency; give enough
            // time for the compute to wake up before reporting a connection error.
            .acquire_timeout(Duration::from_secs(10))
            // Recycle idle connections before Neon's 5-minute inactivity suspension
            // would make them stale.
            .idle_timeout(Duration::from_secs(240))
            // Hard ceiling on connection age to avoid using connections that were
            // established before a Neon compute restart.
            .max_lifetime(Duration::from_secs(1800))
            // Ping the connection before handing it out so stale post-suspend
            // connections are detected and replaced immediately.
            .test_before_acquire(true)
            .connect_with(options)
            .await
            .context("failed to connect to Neon/Postgres")?;

        for &sql in SCHEMA {
            sqlx::query(sql)
                .execute(&pool)
                .await
                .context("failed to create schema")?;
        }

        for &sql in MIGRATIONS {
            // Each migration is idempotent (IF NOT EXISTS / IF EXISTS guards).
            // Errors from no-op migrations (e.g. column already correct type) are
            // intentionally ignored to allow re-runs on already-migrated databases.
            let _ = sqlx::query(sql).execute(&pool).await;
        }

        Ok(Self { pool })
    }

    pub async fn upsert_node(&self, row: &NodeRow) -> Result<()> {
        let services = row
            .services
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "{}".to_string());
        sqlx::query(
            r#"INSERT INTO nodes (id, name, user_id, peer_id, public_key, region, status,
                uptime, latency, bandwidth_used, quality_score, earnings, packet_loss,
                trust_score, reported_upload_cap_mbps, reported_download_cap_mbps,
                last_seen_at, uptime_pct, avg_latency_ms, bandwidth_down_mbps,
                bandwidth_up_mbps, packet_loss_pct, sessions_count, earnings_usd,
                supported_encryption, services)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26::jsonb)
            ON CONFLICT(id) DO UPDATE SET
                name = EXCLUDED.name,
                user_id = EXCLUDED.user_id,
                peer_id = EXCLUDED.peer_id,
                public_key = EXCLUDED.public_key,
                region = EXCLUDED.region,
                status = EXCLUDED.status,
                uptime = EXCLUDED.uptime,
                latency = EXCLUDED.latency,
                bandwidth_used = EXCLUDED.bandwidth_used,
                quality_score = EXCLUDED.quality_score,
                earnings = EXCLUDED.earnings,
                packet_loss = EXCLUDED.packet_loss,
                trust_score = EXCLUDED.trust_score,
                reported_upload_cap_mbps = EXCLUDED.reported_upload_cap_mbps,
                reported_download_cap_mbps = EXCLUDED.reported_download_cap_mbps,
                last_seen_at = EXCLUDED.last_seen_at,
                uptime_pct = EXCLUDED.uptime_pct,
                avg_latency_ms = EXCLUDED.avg_latency_ms,
                bandwidth_down_mbps = EXCLUDED.bandwidth_down_mbps,
                bandwidth_up_mbps = EXCLUDED.bandwidth_up_mbps,
                packet_loss_pct = EXCLUDED.packet_loss_pct,
                sessions_count = EXCLUDED.sessions_count,
                earnings_usd = EXCLUDED.earnings_usd,
                supported_encryption = EXCLUDED.supported_encryption,
                services = EXCLUDED.services,
                updated_at = now()"#,
        )
        .bind(&row.id)
        .bind(&row.name)
        .bind(&row.user_id)
        .bind(&row.peer_id)
        .bind(&row.public_key)
        .bind(&row.region)
        .bind(&row.status)
        .bind(row.uptime)
        // `latency` is the legacy aggregate column; use avg_latency_ms as the best proxy.
        .bind(row.avg_latency_ms)
        .bind(row.bandwidth_up_mbps.unwrap_or(0.0) + row.bandwidth_down_mbps.unwrap_or(0.0))
        .bind(row.quality_score)
        .bind(row.earnings_usd)
        .bind(row.packet_loss_pct)
        .bind(row.trust_score)
        .bind(row.reported_upload_cap_mbps)
        .bind(row.reported_download_cap_mbps)
        .bind(&row.last_seen_at)
        .bind(row.uptime_pct)
        .bind(row.avg_latency_ms)
        .bind(row.bandwidth_down_mbps)
        .bind(row.bandwidth_up_mbps)
        .bind(row.packet_loss_pct)
        .bind(row.sessions_count)
        .bind(row.earnings_usd)
        .bind(&row.supported_encryption)
        .bind(&services)
        .execute(&self.pool)
        .await
        .context("neon upsert node")?;
        Ok(())
    }

    pub async fn insert_node_metrics(&self, row: &NodeMetricsRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO node_metrics (node_id, user_id, latency_ms, bandwidth_down_mbps,
                bandwidth_up_mbps, packet_loss_pct, quality_score, earnings_usd, recorded_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
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
        .context("neon insert node_metrics")?;
        Ok(())
    }

    pub async fn insert_network_metrics(&self, row: &NetworkMetricsRow) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO network_metrics (user_id, active_nodes, avg_latency_ms, bandwidth_egress_mb,
                bandwidth_ingress_mb, packet_loss_pct, uptime_pct, earnings_usd, recorded_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
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
        .context("neon insert network_metrics")?;
        Ok(())
    }

    pub async fn insert_node_intelligence(&self, row: &NodeIntelligenceRow) -> Result<()> {
        let fv = row.feature_vector.as_ref().map(|v| v.to_string());
        sqlx::query(
            r#"INSERT INTO node_intelligence (node_id, user_id, quality_score, trust_score,
                anomaly_score, is_anomalous, cluster_id, feature_vector, recorded_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)"#,
        )
        .bind(&row.node_id)
        .bind(&row.user_id)
        .bind(row.quality_score)
        .bind(row.trust_score)
        .bind(row.anomaly_score)
        .bind(row.is_anomalous)
        .bind(row.cluster_id)
        .bind(&fv)
        .bind(&row.recorded_at)
        .execute(&self.pool)
        .await
        .context("neon insert node_intelligence")?;
        Ok(())
    }
}

#[async_trait]
impl MetricsBackend for NeonStore {
    async fn upsert_node(&self, row: &NodeRow) -> Result<()> {
        self.upsert_node(row).await
    }
    async fn insert_node_metrics(&self, row: &NodeMetricsRow) -> Result<()> {
        self.insert_node_metrics(row).await
    }
    async fn insert_network_metrics(&self, row: &NetworkMetricsRow) -> Result<()> {
        self.insert_network_metrics(row).await
    }
    async fn insert_node_intelligence(&self, row: &NodeIntelligenceRow) -> Result<()> {
        self.insert_node_intelligence(row).await
    }

    async fn cleanup_old_metrics(&self, days: i64) -> Result<u64> {
        let r1 = sqlx::query(
            "DELETE FROM node_metrics WHERE recorded_at < (now() AT TIME ZONE 'utc') - INTERVAL '1 day' * $1",
        )
        .bind(days)
        .execute(&self.pool)
        .await
        .context("neon cleanup node_metrics")?;
        let r2 = sqlx::query(
            "DELETE FROM network_metrics WHERE recorded_at < (now() AT TIME ZONE 'utc') - INTERVAL '1 day' * $1",
        )
        .bind(days)
        .execute(&self.pool)
        .await
        .context("neon cleanup network_metrics")?;
        let r3 = sqlx::query(
            "DELETE FROM node_intelligence WHERE recorded_at < (now() AT TIME ZONE 'utc') - INTERVAL '1 day' * $1",
        )
        .bind(days)
        .execute(&self.pool)
        .await
        .context("neon cleanup node_intelligence")?;
        let r4 = sqlx::query(
            "DELETE FROM connection_history WHERE ended_at < (now() AT TIME ZONE 'utc') - INTERVAL '1 day' * $1 \
             OR (ended_at IS NULL AND started_at < (now() AT TIME ZONE 'utc') - INTERVAL '1 day' * $1)",
        )
        .bind(days)
        .execute(&self.pool)
        .await
        .context("neon cleanup connection_history")?;
        Ok(r1.rows_affected() + r2.rows_affected() + r3.rows_affected() + r4.rows_affected())
    }

    async fn upsert_peer_reputation(&self, row: &PeerReputation) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO peer_reputation (peer_id, reputation_score, total_bytes_relayed,
                successful_sessions, failed_sessions, avg_latency_ms, last_active_at, recorded_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT(peer_id) DO UPDATE SET
                total_bytes_relayed = peer_reputation.total_bytes_relayed + EXCLUDED.total_bytes_relayed,
                successful_sessions = peer_reputation.successful_sessions + EXCLUDED.successful_sessions,
                failed_sessions = peer_reputation.failed_sessions + EXCLUDED.failed_sessions,
                avg_latency_ms = EXCLUDED.avg_latency_ms,
                last_active_at = EXCLUDED.last_active_at,
                recorded_at = EXCLUDED.recorded_at"#,
        )
        .bind(&row.peer_id)
        .bind(row.reputation_score)
        .bind(row.total_bytes_relayed as i64)
        .bind(row.successful_sessions as i64)
        .bind(row.failed_sessions as i64)
        .bind(row.avg_latency_ms)
        .bind(&row.last_active_at)
        .bind(&row.recorded_at)
        .execute(&self.pool)
        .await
        .context("neon upsert peer_reputation")?;
        Ok(())
    }

    async fn get_peer_reputation(&self, peer_id: &str) -> Result<Option<PeerReputation>> {
        let row: Option<(String, f64, i64, i64, i64, f64, Option<String>, Option<String>)> =
            sqlx::query_as(
                "SELECT peer_id, reputation_score, total_bytes_relayed, successful_sessions,
                 failed_sessions, avg_latency_ms, last_active_at, recorded_at
                 FROM peer_reputation WHERE peer_id = $1",
            )
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await
            .context("neon get peer_reputation")?;
        Ok(row.map(|r| PeerReputation {
            peer_id: r.0,
            reputation_score: r.1,
            total_bytes_relayed: r.2 as u64,
            successful_sessions: r.3 as u64,
            failed_sessions: r.4 as u64,
            avg_latency_ms: r.5,
            last_active_at: r.6,
            recorded_at: r.7,
        }))
    }

    async fn insert_connection(&self, record: &ConnectionRecord) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO connection_history (local_peer_id, remote_peer_id, remote_ip, remote_port,
                direction, bytes_sent, bytes_received, duration_secs, avg_latency_ms,
                exit_reason, started_at, ended_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"#,
        )
        .bind(&record.local_peer_id)
        .bind(&record.remote_peer_id)
        .bind(&record.remote_ip)
        .bind(record.remote_port.map(|p| p as i32))
        .bind(&record.direction)
        .bind(record.bytes_sent as i64)
        .bind(record.bytes_received as i64)
        .bind(record.duration_secs)
        .bind(record.avg_latency_ms)
        .bind(&record.exit_reason)
        .bind(&record.started_at)
        .bind(&record.ended_at)
        .execute(&self.pool)
        .await
        .context("neon insert connection")?;
        Ok(())
    }

    async fn update_connection_end(
        &self,
        local_peer_id: &str,
        remote_peer_id: &str,
        bytes_sent: u64,
        bytes_received: u64,
        duration_secs: f64,
        avg_latency_ms: f64,
        exit_reason: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE connection_history SET
                bytes_sent=$3, bytes_received=$4, duration_secs=$5,
                avg_latency_ms=$6, exit_reason=$7, ended_at=now() AT TIME ZONE 'utc'
            WHERE id = (
                SELECT id FROM connection_history
                WHERE local_peer_id=$1 AND remote_peer_id=$2 AND ended_at IS NULL
                ORDER BY id DESC LIMIT 1
            )"#,
        )
        .bind(local_peer_id)
        .bind(remote_peer_id)
        .bind(bytes_sent as i64)
        .bind(bytes_received as i64)
        .bind(duration_secs)
        .bind(avg_latency_ms)
        .bind(exit_reason)
        .execute(&self.pool)
        .await
        .context("neon update connection_end")?;
        Ok(())
    }

    async fn insert_receipt(&self, receipt: &StateReceipt) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO state_receipts (receipt_id, session_id, signer_peer_id,
                counterparty_peer_id, bytes_transferred, direction, sequence_number,
                timestamp_secs, signature)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"#,
        )
        .bind(&receipt.receipt_id)
        .bind(&receipt.session_id)
        .bind(&receipt.signer_peer_id)
        .bind(&receipt.counterparty_peer_id)
        .bind(receipt.bytes_transferred as i64)
        .bind(&receipt.direction)
        .bind(receipt.sequence_number as i64)
        .bind(receipt.timestamp_secs as i64)
        .bind(&receipt.signature)
        .execute(&self.pool)
        .await
        .context("neon insert receipt")?;
        Ok(())
    }

    async fn get_receipts_for_session(&self, session_id: &str) -> Result<Vec<StateReceipt>> {
        let rows: Vec<(String, String, String, String, i64, String, i64, i64, String)> =
            sqlx::query_as(
                "SELECT receipt_id, session_id, signer_peer_id, counterparty_peer_id,
                 bytes_transferred, direction, sequence_number, timestamp_secs, signature
                 FROM state_receipts WHERE session_id = $1 ORDER BY sequence_number ASC",
            )
            .bind(session_id)
            .fetch_all(&self.pool)
            .await
            .context("neon get receipts")?;
        Ok(rows
            .into_iter()
            .map(|r| StateReceipt {
                receipt_id: r.0,
                session_id: r.1,
                signer_peer_id: r.2,
                counterparty_peer_id: r.3,
                bytes_transferred: r.4 as u64,
                direction: r.5,
                sequence_number: r.6 as u64,
                timestamp_secs: r.7 as u64,
                signature: r.8,
            })
            .collect())
    }

    async fn upsert_capability(&self, desc: &CapabilityDescriptor) -> Result<()> {
        let enc_json = serde_json::to_string(&desc.supported_encryption).unwrap_or_default();
        sqlx::query(
            r#"INSERT INTO capability_descriptors (peer_id, public_key, region,
                upload_cap_mbps, download_cap_mbps, avg_latency_ms, reputation_score,
                supported_encryption, max_sessions, active_sessions, last_updated,
                noise_public_key)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT(peer_id) DO UPDATE SET
                public_key=EXCLUDED.public_key, region=EXCLUDED.region,
                upload_cap_mbps=EXCLUDED.upload_cap_mbps, download_cap_mbps=EXCLUDED.download_cap_mbps,
                avg_latency_ms=EXCLUDED.avg_latency_ms, reputation_score=EXCLUDED.reputation_score,
                supported_encryption=EXCLUDED.supported_encryption, max_sessions=EXCLUDED.max_sessions,
                active_sessions=EXCLUDED.active_sessions, last_updated=EXCLUDED.last_updated,
                noise_public_key=EXCLUDED.noise_public_key"#,
        )
        .bind(&desc.peer_id)
        .bind(&desc.public_key)
        .bind(&desc.region)
        .bind(desc.upload_cap_mbps)
        .bind(desc.download_cap_mbps)
        .bind(desc.avg_latency_ms)
        .bind(desc.reputation_score)
        .bind(&enc_json)
        .bind(desc.max_sessions as i32)
        .bind(desc.active_sessions as i32)
        .bind(desc.last_updated as i64)
        .bind(&desc.noise_public_key)
        .execute(&self.pool)
        .await
        .context("neon upsert capability")?;
        Ok(())
    }

    async fn get_capabilities_in_region(
        &self,
        region: &str,
        min_upload_mbps: f64,
        min_download_mbps: f64,
        limit: i64,
    ) -> Result<Vec<CapabilityDescriptor>> {
        let rows: Vec<(
            String, Vec<u8>, String, f64, f64, f64, f64, String, i32, i32, i64, Vec<u8>,
        )> = sqlx::query_as(
            "SELECT peer_id, public_key, region, upload_cap_mbps, download_cap_mbps,
             avg_latency_ms, reputation_score, supported_encryption, max_sessions,
             active_sessions, last_updated, noise_public_key
             FROM capability_descriptors
             WHERE region = $1 AND upload_cap_mbps >= $2 AND download_cap_mbps >= $3
             AND active_sessions < max_sessions
             ORDER BY reputation_score DESC, upload_cap_mbps DESC
             LIMIT $4",
        )
        .bind(region)
        .bind(min_upload_mbps)
        .bind(min_download_mbps)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("neon get capabilities")?;
        Ok(rows
            .into_iter()
            .map(|r| CapabilityDescriptor {
                peer_id: r.0,
                public_key: r.1,
                region: r.2,
                upload_cap_mbps: r.3,
                download_cap_mbps: r.4,
                avg_latency_ms: r.5,
                reputation_score: r.6,
                supported_encryption: serde_json::from_str(&r.7).unwrap_or_default(),
                max_sessions: r.8 as u32,
                active_sessions: r.9 as u32,
                last_updated: r.10 as u64,
                noise_public_key: r.11,
            })
            .collect())
    }
}
