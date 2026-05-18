-- 013_ssm_labels.sql
CREATE TABLE IF NOT EXISTS jc_ssm_parameter_labels (
    account_id     TEXT   NOT NULL,
    region         TEXT   NOT NULL,
    parameter_name TEXT   NOT NULL,
    version        BIGINT NOT NULL,
    label          TEXT   NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, parameter_name, version, label),
    FOREIGN KEY (account_id, region, parameter_name)
        REFERENCES jc_ssm_parameters (account_id, region, name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ssm_labels_param_version
    ON jc_ssm_parameter_labels (account_id, region, parameter_name, version);
