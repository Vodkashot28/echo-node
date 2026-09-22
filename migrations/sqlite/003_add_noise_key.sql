-- Echo Node: Add Noise public key to capability_descriptors (SQLite)
-- Note: Individual statement errors are expected and ignored (column may already exist).

ALTER TABLE capability_descriptors ADD COLUMN noise_public_key BLOB DEFAULT X'00';
