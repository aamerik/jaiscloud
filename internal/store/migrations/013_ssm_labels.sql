-- 013_ssm_labels.sql
CREATE TABLE IF NOT EXISTS jc_ssm_parameter_labels (
    parameter_name TEXT   NOT NULL,
    version        BIGINT NOT NULL,
    label          TEXT   NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parameter_name, version, label)
);
CREATE INDEX IF NOT EXISTS idx_ssm_labels_param_version
    ON jc_ssm_parameter_labels(parameter_name, version);
