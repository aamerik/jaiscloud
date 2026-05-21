-- 007_secretsmanager.sql
-- SecretsManager secret metadata and version table.
CREATE TABLE IF NOT EXISTS jc_sm_secrets (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    secret_id       TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    secret_data     JSONB       NOT NULL DEFAULT '{}',
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, secret_id),
    UNIQUE (account_id, region, name)
);

CREATE TABLE IF NOT EXISTS jc_sm_versions (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    secret_id       TEXT        NOT NULL,
    version_id      TEXT        NOT NULL,
    secret_binary   BYTEA,
    is_binary       BOOLEAN     NOT NULL DEFAULT FALSE,
    stages          TEXT[]      NOT NULL DEFAULT ARRAY['AWSCURRENT'],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, secret_id, version_id),
    FOREIGN KEY (account_id, region, secret_id)
        REFERENCES jc_sm_secrets (account_id, region, secret_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sm_secrets_name    ON jc_sm_secrets  (account_id, region, name);
CREATE INDEX IF NOT EXISTS idx_sm_versions_secret ON jc_sm_versions (account_id, region, secret_id);
