-- Migration 006: Session tickets and persistent auctions
-- Session tickets enable tunnel resumption across provider restarts
-- Persistent auctions survive crashes and enable market recovery

-- Session tickets table (provider stores issued tickets)
CREATE TABLE IF NOT EXISTS session_tickets (
    session_id TEXT PRIMARY KEY,
    consumer_peer_id TEXT NOT NULL,
    ticket_data BYTEA NOT NULL,
    expires_at BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_session_tickets_expires ON session_tickets(expires_at);
CREATE INDEX IF NOT EXISTS idx_session_tickets_consumer ON session_tickets(consumer_peer_id);

-- Persistent auctions table (marketplace state)
CREATE TABLE IF NOT EXISTS auctions (
    auction_id TEXT PRIMARY KEY,
    requester_peer_id TEXT NOT NULL,
    region TEXT NOT NULL,
    bandwidth_mbps DOUBLE PRECISION NOT NULL,
    budget_usd_per_gb DOUBLE PRECISION NOT NULL,
    window_secs INTEGER NOT NULL,
    created_at BIGINT NOT NULL,
    closes_at BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'closed', 'cancelled')),
    winning_peer_id TEXT,
    winning_price DOUBLE PRECISION,
    awarded_at BIGINT,
    CONSTRAINT valid_budget CHECK (budget_usd_per_gb > 0),
    CONSTRAINT valid_bandwidth CHECK (bandwidth_mbps > 0),
    CONSTRAINT valid_window CHECK (window_secs > 0 AND window_secs <= 3600)
);

CREATE INDEX IF NOT EXISTS idx_auctions_status ON auctions(status);
CREATE INDEX IF NOT EXISTS idx_auctions_closes_at ON auctions(closes_at);
CREATE INDEX IF NOT EXISTS idx_auctions_region ON auctions(region);
CREATE INDEX IF NOT EXISTS idx_auctions_requester ON auctions(requester_peer_id);

-- Auction bids table (sealed bids for each auction)
CREATE TABLE IF NOT EXISTS auction_bids (
    bid_id TEXT PRIMARY KEY,
    auction_id TEXT NOT NULL REFERENCES auctions(auction_id) ON DELETE CASCADE,
    provider_peer_id TEXT NOT NULL,
    price_usd_per_gb DOUBLE PRECISION NOT NULL,
    reputation_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    avg_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    submitted_at BIGINT NOT NULL,
    CONSTRAINT valid_price CHECK (price_usd_per_gb > 0),
    CONSTRAINT unique_auction_provider UNIQUE (auction_id, provider_peer_id)
);

CREATE INDEX IF NOT EXISTS idx_auction_bids_auction ON auction_bids(auction_id);
CREATE INDEX IF NOT EXISTS idx_auction_bids_provider ON auction_bids(provider_peer_id);
