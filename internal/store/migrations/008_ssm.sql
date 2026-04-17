-- 008_ssm.sql
-- SSM Parameter Store tables.

CREATE TABLE IF NOT EXISTS jc_ssm_parameters (
    name            TEXT        NOT NULL PRIMARY KEY,
    param_data      JSONB       NOT NULL DEFAULT '{}',
    param_value     BYTEA,                              -- encrypted value
    version         BIGINT      NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jc_ssm_param_history (
    name            TEXT        NOT NULL,
    version         BIGINT      NOT NULL,
    param_data      JSONB       NOT NULL DEFAULT '{}',
    param_value     BYTEA,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (name, version)
);

CREATE INDEX IF NOT EXISTS idx_ssm_params_name    ON jc_ssm_parameters   (name);
CREATE INDEX IF NOT EXISTS idx_ssm_history_name   ON jc_ssm_param_history (name);
