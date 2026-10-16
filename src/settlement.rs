use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::identity::NodeIdentity;
use crate::models::{SettlementRecord, StateReceipt};
use crate::MetricsBackend;

/// Payout engine: converts verified, signed receipts into per-receipt
/// settlement entries (bytes + USD earnings) and exposes settlement
/// summaries for a compensation/payout flow.
///
/// Settlement is a one-way accumulator: each row is keyed by `receipt_id`
/// in the `settlements` table (see migrations 004), so replaying an already
/// settled receipt is a no-op and can never double-count earnings.
#[derive(Clone)]
pub struct SettlementEngine {
    /// The local node's identity — its ed25519 key is used to
    /// cryptographically verify receipts the local node issued.
    identity: NodeIdentity,
    backend: Arc<dyn MetricsBackend>,
    /// Conversion rate from bytes relayed to USD (per GB). 0.0 = disabled.
    usd_per_gb: f64,
}

impl SettlementEngine {
    pub fn new(identity: NodeIdentity, backend: Arc<dyn MetricsBackend>, usd_per_gb: f64) -> Self {
        Self {
            identity,
            backend,
            usd_per_gb,
        }
    }

    /// Verify a signed receipt and record it as a settlement entry at the
    /// engine's default flat rate (`SETTLEMENT_RATE_USD_PER_GB`).
    pub async fn settle_receipt(&self, receipt: StateReceipt) -> Result<Option<SettlementRecord>> {
        self.settle_receipt_at_price(receipt, None).await
    }

    /// Verify a signed receipt and record it as a settlement entry,
    /// optionally overriding the USD/GB rate with a market-discovered
    /// price (the locked price from a [`crate::bidding`] award). An
    /// override that is non-finite or non-positive falls back to the
    /// default rate.
    pub async fn settle_receipt_at_price(
        &self,
        receipt: StateReceipt,
        price_usd_per_gb: Option<f64>,
    ) -> Result<Option<SettlementRecord>> {
        let usd_per_gb = match price_usd_per_gb {
            Some(p) if p.is_finite() && p > 0.0 => p,
            _ => self.usd_per_gb,
        };
        if receipt.signer_peer_id != self.identity.peer_id_str() {
            debug!(
                receipt = %receipt.receipt_id,
                signer = %receipt.signer_peer_id,
                local = self.identity.peer_id_str(),
                "skipping receipt: not signed by this node"
            );
            return Ok(None);
        }

        match receipt.verify_signature(self.identity.public_key_bytes.as_slice()) {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    receipt = %receipt.receipt_id,
                    session = %receipt.session_id,
                    "receipt signature verification failed, not settling"
                );
                return Ok(None);
            }
            Err(e) => {
                warn!(
                    receipt = %receipt.receipt_id,
                    error = %e,
                    "receipt malformed, not settling"
                );
                return Ok(None);
            }
        }

        let record = SettlementRecord {
            receipt_id: receipt.receipt_id.clone(),
            signer_peer_id: receipt.signer_peer_id.clone(),
            counterparty_peer_id: receipt.counterparty_peer_id.clone(),
            bytes_settled: receipt.bytes_transferred,
            earnings_usd: earnings_at_rate(receipt.bytes_transferred, usd_per_gb),
            settled_at: chrono::Utc::now().to_rfc3339(),
        };

        // Idempotent by receipt_id: replaying an already-settled receipt is
        // a no-op (`false` = duplicate skipped, nothing double-counted).
        let inserted = self.backend.record_settlement(&record).await?;
        if !inserted {
            debug!(
                receipt = %record.receipt_id,
                "receipt already settled, skipping duplicate"
            );
            return Ok(None);
        }

        info!(
            receipt = %record.receipt_id,
            counterparty = %record.counterparty_peer_id,
            bytes = record.bytes_settled,
            earnings_usd = record.earnings_usd,
            "receipt settled for payout"
        );

        Ok(Some(record))
    }

    /// Compute the USD payout value for a byte count at the engine's
    /// default flat rate.
    pub fn earnings_for_bytes(&self, bytes: u64) -> f64 {
        earnings_at_rate(bytes, self.usd_per_gb)
    }

    /// The engine's default flat rate (USD/GB), used when no market price
    /// is attached to a receipt.
    pub fn default_rate_usd_per_gb(&self) -> f64 {
        self.usd_per_gb
    }

    /// List the most recently settled receipts.
    pub async fn settlements(&self, limit: i64) -> Result<Vec<SettlementRecord>> {
        self.backend.get_settlements(limit).await
    }

    /// Aggregate payout totals across all settled receipts.
    pub async fn summary(&self) -> Result<crate::models::SettlementSummary> {
        self.backend.settlement_summary().await
    }
}

/// USD payout for a byte count at an arbitrary rate (flat default or a
/// market-discovered award price).
fn earnings_at_rate(bytes: u64, usd_per_gb: f64) -> f64 {
    (bytes as f64 / 1_000_000_000.0) * usd_per_gb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earnings_at_rate_scales_bytes() {
        assert_eq!(earnings_at_rate(1_000_000_000, 0.50), 0.50);
        assert_eq!(earnings_at_rate(2_000_000_000, 0.25), 0.50);
        assert_eq!(earnings_at_rate(0, 10.0), 0.0);
    }
}