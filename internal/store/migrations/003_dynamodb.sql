-- DynamoDB item data plane.
CREATE TABLE IF NOT EXISTS jc_dynamodb_items (
    table_name TEXT  NOT NULL,
    pk_hash    TEXT  NOT NULL,  -- SHA-256 of canonical key JSON
    item       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, pk_hash)
);

CREATE INDEX IF NOT EXISTS idx_dynamo_items_table
    ON jc_dynamodb_items (table_name);
