-- Canonical ledger events (append-only, hash-chained audit log).
-- Full canonical event JSON lives in event_json; the other columns are
-- extracted for indexing/queries. Never UPDATE or DELETE rows here —
-- the hash chain (prev_hash) assumes strict append-only semantics.
CREATE TABLE IF NOT EXISTS ledger_events (
    event_id         TEXT PRIMARY KEY,
    sequence         BIGINT NOT NULL,
    event_type       TEXT NOT NULL,
    event_version    INTEGER NOT NULL,
    schema_version   INTEGER NOT NULL,
    ts               TEXT NOT NULL,
    network          TEXT NOT NULL,
    node_id          TEXT NOT NULL,
    actor_id         TEXT NOT NULL,
    correlation_id   TEXT,
    parent_event_id  TEXT,
    hash             TEXT NOT NULL,
    prev_hash        TEXT,
    event_json       TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_events_sequence
    ON ledger_events (node_id, sequence);
CREATE INDEX IF NOT EXISTS idx_ledger_events_type
    ON ledger_events (event_type);
CREATE INDEX IF NOT EXISTS idx_ledger_events_correlation
    ON ledger_events (correlation_id);
