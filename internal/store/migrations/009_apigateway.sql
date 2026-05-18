-- 009_apigateway.sql
-- API Gateway REST API metadata tables.
CREATE TABLE IF NOT EXISTS jc_apigw_apis (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    api_id          TEXT        NOT NULL,
    api_data        JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, api_id)
);

CREATE TABLE IF NOT EXISTS jc_apigw_resources (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    resource_id     TEXT        NOT NULL,
    api_id          TEXT        NOT NULL,
    resource_data   JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, resource_id),
    FOREIGN KEY (account_id, region, api_id)
        REFERENCES jc_apigw_apis (account_id, region, api_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS jc_apigw_stages (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    api_id          TEXT        NOT NULL,
    stage_name      TEXT        NOT NULL,
    stage_data      JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, api_id, stage_name),
    FOREIGN KEY (account_id, region, api_id)
        REFERENCES jc_apigw_apis (account_id, region, api_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS jc_apigw_deployments (
    account_id      TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    deployment_id   TEXT        NOT NULL,
    api_id          TEXT        NOT NULL,
    deploy_data     JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, deployment_id),
    FOREIGN KEY (account_id, region, api_id)
        REFERENCES jc_apigw_apis (account_id, region, api_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_apigw_resources_api ON jc_apigw_resources   (account_id, region, api_id);
CREATE INDEX IF NOT EXISTS idx_apigw_stages_api    ON jc_apigw_stages      (account_id, region, api_id);
CREATE INDEX IF NOT EXISTS idx_apigw_deploys_api   ON jc_apigw_deployments (account_id, region, api_id);
