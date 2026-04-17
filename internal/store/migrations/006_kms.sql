-- 006_kms.sql
-- KMS key metadata, aliases, grants, and the DEK bootstrap table.
-- All tables live in the cloud schema (set as search_path at pool creation time).

-- jc_kms_keys stores KMS key metadata and the wrapped key material.
CREATE TABLE IF NOT EXISTS jc_kms_keys (
    key_id         TEXT        NOT NULL PRIMARY KEY,
    key_data       JSONB       NOT NULL DEFAULT '{}',
    key_material   BYTEA,                          -- AES-GCM ciphertext (nil for external keys)
    enabled        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- jc_kms_aliases maps alias names to key IDs.
CREATE TABLE IF NOT EXISTS jc_kms_aliases (
    alias_name     TEXT        NOT NULL PRIMARY KEY,
    target_key_id  TEXT        NOT NULL REFERENCES jc_kms_keys(key_id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- jc_kms_grants stores grant tokens issued for a key.
CREATE TABLE IF NOT EXISTS jc_kms_grants (
    grant_id       TEXT        NOT NULL PRIMARY KEY,
    key_id         TEXT        NOT NULL REFERENCES jc_kms_keys(key_id) ON DELETE CASCADE,
    grant_data     JSONB       NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- jc_kms_dek stores the single data-encryption key for envelope encryption.
-- VERSION 0x00 = plaintext, 0x01 = AES-GCM wrapped by KEK.
CREATE TABLE IF NOT EXISTS jc_kms_dek (
    id             INT         NOT NULL PRIMARY KEY DEFAULT 1,
    dek_blob       BYTEA       NOT NULL,           -- VERSION || IV || TAG || CIPHERTEXT (or raw if 0x00)
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);

CREATE INDEX IF NOT EXISTS idx_kms_aliases_key ON jc_kms_aliases (target_key_id);
CREATE INDEX IF NOT EXISTS idx_kms_grants_key  ON jc_kms_grants  (key_id);
