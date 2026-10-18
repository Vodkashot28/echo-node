-- Migration 007: Expand auctions status constraint to include awarded and expired
-- The original constraint only included open/closed/cancelled but the engine also
-- emits awarded (finalized with a winner) and expired (no bids at deadline).

ALTER TABLE auctions DROP CONSTRAINT IF EXISTS auctions_status_check;

ALTER TABLE auctions ADD CONSTRAINT auctions_status_check
    CHECK (status IN ('open', 'awarded', 'expired', 'cancelled'));
