-- Echo Node: Add Noise public key to capability_descriptors (PostgreSQL)

ALTER TABLE capability_descriptors ADD COLUMN IF NOT EXISTS noise_public_key BYTEA DEFAULT E'\\x00';

-- Convert legacy TEXT[] -> TEXT (JSON) for supported_encryption; no-op if already TEXT
ALTER TABLE capability_descriptors ALTER COLUMN supported_encryption TYPE TEXT USING to_json(supported_encryption)::text;

-- Upgrade created_at/updated_at from TEXT to TIMESTAMPTZ; no-op if already correct type
ALTER TABLE nodes ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at::TIMESTAMPTZ;
ALTER TABLE nodes ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at::TIMESTAMPTZ;
