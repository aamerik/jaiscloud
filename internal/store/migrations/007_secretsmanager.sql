-- 007_secretsmanager.sql
-- SecretsManager secret metadata and version table.

-- jc_sm_secrets stores secret metadata (not the secret value).
CREATE TABLE IF NOT EXISTS jc_sm_secrets (
    secret_id       TEXT        NOT NULL PRIMARY KEY,
    name            TEXT        NOT NULL UNIQUE,
    secret_data     JSONB       NOT NULL DEFAULT '{}',
    deleted_at      TIMESTAMPTZ,                        -- soft-delete timestamp
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- jc_sm_versions stores each secret version with its encrypted value.
CREATE TABLE IF NOT EXISTS jc_sm_versions (
    secret_id       TEXT        NOT NULL REFERENCES jc_sm_secrets(secret_id) ON DELETE CASCADE,
    version_id      TEXT        NOT NULL,
    secret_binary   BYTEA,                              -- encrypted secret value
    stages          TEXT[]      NOT NULL DEFAULT ARRAY['AWSCURRENT'],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (secret_id, version_id)
);

CREATE INDEX IF NOT EXISTS idx_sm_secrets_name    ON jc_sm_secrets  (name);
CREATE INDEX IF NOT EXISTS idx_sm_versions_secret ON jc_sm_versions (secret_id);
