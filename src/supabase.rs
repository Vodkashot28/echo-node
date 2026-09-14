use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct SupabaseClient {
    client: Client,
    url: String,
    anon_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeRow {
    pub id: String,
    pub name: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_down_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_up_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub packet_loss_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sessions_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earnings_usd: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeMetricsRow {
    pub node_id: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_down_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_up_mbps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub packet_loss_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earnings_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkMetricsRow {
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_nodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_egress_mb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth_ingress_mb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub packet_loss_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earnings_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeIntelligenceRow {
    pub node_id: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anomaly_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_anomalous: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature_vector: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PostgrestResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct PostgrestError {
    message: String,
    code: Option<String>,
    details: Option<String>,
}

impl SupabaseClient {
    pub fn new(url: &str, anon_key: &str) -> Self {
        Self {
            client: Client::new(),
            url: url.trim_end_matches('/').to_string(),
            anon_key: anon_key.to_string(),
        }
    }

    fn headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "apikey",
            self.anon_key.parse().expect("invalid anon key"),
        );
        headers.insert(
            "Authorization",
            format!("Bearer {}", self.anon_key)
                .parse()
                .expect("invalid auth header"),
        );
        headers.insert(
            "Content-Type",
            "application/json".parse().expect("invalid content type"),
        );
        headers.insert("Prefer", "return=minimal".parse().expect("invalid prefer"));
        headers
    }

    async fn upsert<T: Serialize>(&self, table: &str, rows: &[T]) -> Result<()> {
        let url = format!("{}/rest/v1/{}", self.url, table);
        let resp = self
            .client
            .post(&url)
            .headers(self.headers())
            .header("Prefer", "resolution=merge-duplicates,return=minimal")
            .json(rows)
            .send()
            .await
            .context("Failed to send upsert request to Supabase")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Supabase upsert to {} failed ({}): {}",
                table,
                status,
                body
            );
        }
        Ok(())
    }

    pub async fn upsert_node(&self, node: &NodeRow) -> Result<()> {
        self.upsert("nodes", &[node]).await
    }

    pub async fn upsert_node_metrics(&self, metrics: &[NodeMetricsRow]) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }
        self.upsert("node_metrics", metrics).await
    }

    pub async fn upsert_network_metrics(&self, metrics: &NetworkMetricsRow) -> Result<()> {
        self.upsert("network_metrics", &[metrics]).await
    }

    pub async fn upsert_node_intelligence(&self, intel: &NodeIntelligenceRow) -> Result<()> {
        self.upsert("node_intelligence", &[intel]).await
    }

    pub async fn health_check(&self) -> Result<bool> {
        let url = format!("{}/rest/v1/nodes?select=id&limit=1", self.url);
        let resp = self
            .client
            .get(&url)
            .headers(self.headers())
            .send()
            .await;
        match resp {
            Ok(r) => Ok(r.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}
