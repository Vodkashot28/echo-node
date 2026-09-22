-- Echo Node: Settlement table (PostgreSQL)
-- One row per verified, settled receipt. Payout accumulation for
-- a future compensation engine; receipt_id is the natural PK so
-- duplicate receipts are never double-counted.

CREATE TABLE IF NOT EXISTS settlements (
    receipt_id TEXT PRIMARY KEY,
    signer_peer_id TEXT NOT NULL,
    counterparty_peer_id TEXT NOT NULL,
    bytes_settled BIGINT NOT NULL DEFAULT 0,
    earnings_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    settled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_settlements_counterparty ON settlements(counterparty_peer_id);