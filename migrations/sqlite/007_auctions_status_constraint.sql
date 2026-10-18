-- Migration 007: Expand auctions status constraint to include awarded and expired
-- The original constraint only included open/closed/cancelled but the engine also
-- emits awarded (finalized with a winner) and expired (no bids at deadline).

-- SQLite does not support ALTER TABLE DROP CONSTRAINT.
-- Recreate the auctions table with the updated constraint using the rename-copy pattern.

CREATE TABLE IF NOT EXISTS auctions_new (
    auction_id TEXT PRIMARY KEY,
    requester_peer_id TEXT NOT NULL,
    region TEXT NOT NULL,
    bandwidth_mbps REAL NOT NULL,
    budget_usd_per_gb REAL NOT NULL,
    window_secs INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    closes_at INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'awarded', 'expired', 'cancelled')),
    winning_peer_id TEXT,
    winning_price REAL,
    awarded_at INTEGER,
    CHECK (budget_usd_per_gb > 0),
    CHECK (bandwidth_mbps > 0),
    CHECK (window_secs > 0 AND window_secs <= 3600)
);

INSERT INTO auctions_new SELECT * FROM auctions;

DROP TABLE auctions;

ALTER TABLE auctions_new RENAME TO auctions;

CREATE INDEX IF NOT EXISTS idx_auctions_status ON auctions(status);
CREATE INDEX IF NOT EXISTS idx_auctions_closes_at ON auctions(closes_at);
CREATE INDEX IF NOT EXISTS idx_auctions_region ON auctions(region);
CREATE INDEX IF NOT EXISTS idx_auctions_requester ON auctions(requester_peer_id);
