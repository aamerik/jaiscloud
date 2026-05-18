-- S3 bucket and object metadata.
CREATE TABLE IF NOT EXISTS jc_s3_buckets (
    name             TEXT        PRIMARY KEY,
    owner_account_id TEXT        NOT NULL,
    region           TEXT        NOT NULL,
    meta             JSONB       NOT NULL DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_s3_buckets_owner
    ON jc_s3_buckets (owner_account_id);

CREATE TABLE IF NOT EXISTS jc_s3_objects (
    bucket               TEXT        NOT NULL,
    key                  TEXT        NOT NULL,
    etag                 TEXT        NOT NULL,
    crc32                TEXT        NOT NULL DEFAULT '',
    size                 BIGINT      NOT NULL DEFAULT 0,
    content_type         TEXT        NOT NULL DEFAULT 'application/octet-stream',
    last_modified        TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata             JSONB       NOT NULL DEFAULT '{}',
    storage_class        TEXT        NOT NULL DEFAULT 'STANDARD',
    version_id           TEXT        NOT NULL DEFAULT '',
    tags                 JSONB       NOT NULL DEFAULT '{}',
    encryption           TEXT        NOT NULL DEFAULT '',
    kms_key_id           TEXT        NOT NULL DEFAULT '',
    ssec_key_md5         TEXT        NOT NULL DEFAULT '',
    lock_mode            TEXT        NOT NULL DEFAULT '',
    lock_retain_until    TIMESTAMPTZ,
    legal_hold_status    TEXT        NOT NULL DEFAULT '',
    acl                  TEXT        NOT NULL DEFAULT '',
    checksum_algorithm   TEXT        NOT NULL DEFAULT '',
    checksum_value       TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (bucket, key)
);

CREATE INDEX IF NOT EXISTS idx_s3_objects_bucket
    ON jc_s3_objects (bucket, key);

CREATE TABLE IF NOT EXISTS jc_s3_object_versions (
    bucket               TEXT        NOT NULL,
    key                  TEXT        NOT NULL,
    version_id           TEXT        NOT NULL,
    is_delete_marker     BOOLEAN     NOT NULL DEFAULT FALSE,
    is_latest            BOOLEAN     NOT NULL DEFAULT FALSE,
    etag                 TEXT        NOT NULL DEFAULT '',
    crc32                TEXT        NOT NULL DEFAULT '',
    size                 BIGINT      NOT NULL DEFAULT 0,
    content_type         TEXT        NOT NULL DEFAULT 'application/octet-stream',
    last_modified        TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata             JSONB       NOT NULL DEFAULT '{}',
    storage_class        TEXT        NOT NULL DEFAULT 'STANDARD',
    encryption           TEXT        NOT NULL DEFAULT '',
    kms_key_id           TEXT        NOT NULL DEFAULT '',
    ssec_key_md5         TEXT        NOT NULL DEFAULT '',
    lock_mode            TEXT        NOT NULL DEFAULT '',
    lock_retain_until    TIMESTAMPTZ,
    legal_hold_status    TEXT        NOT NULL DEFAULT '',
    acl                  TEXT        NOT NULL DEFAULT '',
    tags                 JSONB       NOT NULL DEFAULT '{}',
    checksum_algorithm   TEXT        NOT NULL DEFAULT '',
    checksum_value       TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (bucket, key, version_id)
);

CREATE INDEX IF NOT EXISTS idx_s3_versions_bucket_key
    ON jc_s3_object_versions (bucket, key, is_latest);

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
