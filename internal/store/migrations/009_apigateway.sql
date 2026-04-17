-- 009_apigateway.sql
-- API Gateway REST API metadata tables.

CREATE TABLE IF NOT EXISTS jc_apigw_apis (
    api_id          TEXT        NOT NULL PRIMARY KEY,
    api_data        JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jc_apigw_resources (
    resource_id     TEXT        NOT NULL PRIMARY KEY,
    api_id          TEXT        NOT NULL REFERENCES jc_apigw_apis(api_id) ON DELETE CASCADE,
    resource_data   JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jc_apigw_stages (
    stage_name      TEXT        NOT NULL,
    api_id          TEXT        NOT NULL REFERENCES jc_apigw_apis(api_id) ON DELETE CASCADE,
    stage_data      JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (api_id, stage_name)
);

CREATE TABLE IF NOT EXISTS jc_apigw_deployments (
    deployment_id   TEXT        NOT NULL PRIMARY KEY,
    api_id          TEXT        NOT NULL REFERENCES jc_apigw_apis(api_id) ON DELETE CASCADE,
    deploy_data     JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_apigw_resources_api ON jc_apigw_resources (api_id);
CREATE INDEX IF NOT EXISTS idx_apigw_stages_api    ON jc_apigw_stages    (api_id);
CREATE INDEX IF NOT EXISTS idx_apigw_deploys_api   ON jc_apigw_deployments (api_id);
