-- 010_sm_is_binary.sql
-- Add is_binary flag to secret versions so binary secrets round-trip correctly.
ALTER TABLE jc_sm_versions ADD COLUMN IF NOT EXISTS is_binary BOOLEAN NOT NULL DEFAULT FALSE;
