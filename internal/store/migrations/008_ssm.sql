-- 008_ssm.sql
-- SSM Parameter Store tables.
CREATE TABLE IF NOT EXISTS jc_ssm_parameters (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    param_data      JSONB       NOT NULL DEFAULT '{}',
    param_value     BYTEA,
    version         BIGINT      NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, name)
);

CREATE TABLE IF NOT EXISTS jc_ssm_param_history (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    version         BIGINT      NOT NULL,
    param_data      JSONB       NOT NULL DEFAULT '{}',
    param_value     BYTEA,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, name, version)
);

CREATE INDEX IF NOT EXISTS idx_ssm_params_name  ON jc_ssm_parameters    (account_id, region, name);
CREATE INDEX IF NOT EXISTS idx_ssm_history_name ON jc_ssm_param_history (account_id, region, name);
