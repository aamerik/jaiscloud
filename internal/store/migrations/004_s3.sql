-- S3 bucket and object metadata.
CREATE TABLE IF NOT EXISTS jc_s3_buckets (
    name       TEXT        PRIMARY KEY,
    meta       JSONB       NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jc_s3_objects (
    bucket        TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    etag          TEXT        NOT NULL,
    crc32         TEXT        NOT NULL DEFAULT '',
    size          BIGINT      NOT NULL DEFAULT 0,
    content_type  TEXT        NOT NULL DEFAULT 'application/octet-stream',
    last_modified TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata      JSONB       NOT NULL DEFAULT '{}',
    storage_class TEXT        NOT NULL DEFAULT 'STANDARD',
    version_id    TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (bucket, key)
);

CREATE INDEX IF NOT EXISTS idx_s3_objects_bucket
    ON jc_s3_objects (bucket, key);

-- Multipart upload tracking.
CREATE TABLE IF NOT EXISTS jc_s3_multipart_uploads (
    upload_id  TEXT        PRIMARY KEY,
    bucket     TEXT        NOT NULL,
    key        TEXT        NOT NULL,
    meta       JSONB       NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jc_s3_multipart_parts (
    upload_id   TEXT NOT NULL REFERENCES jc_s3_multipart_uploads (upload_id) ON DELETE CASCADE,
    part_number INT  NOT NULL,
    etag        TEXT NOT NULL,
    size        BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (upload_id, part_number)
);
