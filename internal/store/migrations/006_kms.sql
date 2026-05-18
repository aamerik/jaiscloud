-- 006_kms.sql
-- KMS key metadata, aliases, grants, and the DEK bootstrap table.
CREATE TABLE IF NOT EXISTS jc_kms_keys (
    account_id     TEXT        NOT NULL,
    region         TEXT        NOT NULL,
    key_id         TEXT        NOT NULL,
    key_data       JSONB       NOT NULL DEFAULT '{}',
    key_material   BYTEA,
    enabled        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, key_id)
);

CREATE TABLE IF NOT EXISTS jc_kms_aliases (
    account_id     TEXT        NOT NULL,
    region         TEXT        NOT NULL,
    alias_name     TEXT        NOT NULL,
    target_key_id  TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, alias_name),
    FOREIGN KEY (account_id, region, target_key_id)
        REFERENCES jc_kms_keys (account_id, region, key_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS jc_kms_grants (
    account_id     TEXT        NOT NULL,
    region         TEXT        NOT NULL,
    grant_id       TEXT        NOT NULL,
    key_id         TEXT        NOT NULL,
    grant_data     JSONB       NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, grant_id),
    FOREIGN KEY (account_id, region, key_id)
        REFERENCES jc_kms_keys (account_id, region, key_id) ON DELETE CASCADE
);

-- jc_kms_dek UNCHANGED — server-level singleton, NOT scoped per account.
CREATE TABLE IF NOT EXISTS jc_kms_dek (
    id             INT         NOT NULL PRIMARY KEY DEFAULT 1,
    dek_blob       BYTEA       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);

CREATE INDEX IF NOT EXISTS idx_kms_aliases_key ON jc_kms_aliases (account_id, region, target_key_id);
CREATE INDEX IF NOT EXISTS idx_kms_grants_key  ON jc_kms_grants  (account_id, region, key_id);
