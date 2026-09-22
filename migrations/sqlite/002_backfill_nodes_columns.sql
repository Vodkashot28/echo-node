-- Echo Node: Backfill nodes table columns (SQLite)
-- Idempotent migrations for nodes table columns that may not exist
-- in databases created by earlier versions.
-- Note: SQLite does not support IF NOT EXISTS on ALTER TABLE ADD COLUMN,
-- so individual statement errors are expected and ignored.

ALTER TABLE nodes ADD COLUMN uptime REAL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN services TEXT NOT NULL DEFAULT '{}';
