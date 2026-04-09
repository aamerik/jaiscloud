-- Control-plane resource metadata (all services).
CREATE TABLE IF NOT EXISTS jc_resources (
    resource_type TEXT        NOT NULL,
    id            TEXT        NOT NULL,
    data          JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_type, id)
);

CREATE INDEX IF NOT EXISTS idx_jc_resources_type ON jc_resources (resource_type);
