-- Control-plane resource metadata (all services).
CREATE TABLE IF NOT EXISTS jc_resources (
    account_id    TEXT        NOT NULL,
    region        TEXT        NOT NULL,           -- '' for global types (iam_*, route53_*, cloudfront_*)
    resource_type TEXT        NOT NULL,
    id            TEXT        NOT NULL,
    data          JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, region, resource_type, id)
);
CREATE INDEX IF NOT EXISTS idx_jc_resources_scope_type
    ON jc_resources (account_id, region, resource_type);
