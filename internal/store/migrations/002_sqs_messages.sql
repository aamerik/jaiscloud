-- SQS message data plane.
CREATE TABLE IF NOT EXISTS jc_sqs_messages (
    account_id        TEXT        NOT NULL,
    region            TEXT        NOT NULL,
    queue_url         TEXT        NOT NULL,
    id                TEXT        NOT NULL,
    receipt_handle    TEXT        NOT NULL DEFAULT '',
    body              TEXT        NOT NULL,
    md5_of_body       TEXT        NOT NULL,
    attributes        JSONB       NOT NULL DEFAULT '{}',
    msg_attributes    JSONB       NOT NULL DEFAULT '{}',
    group_id          TEXT        NOT NULL DEFAULT '',
    dedup_id          TEXT        NOT NULL DEFAULT '',
    sequence_number   TEXT        NOT NULL DEFAULT '',
    visible_at        TIMESTAMPTZ,
    sent_at           TIMESTAMPTZ NOT NULL,
    delay_until       TIMESTAMPTZ,
    receive_count     INT         NOT NULL DEFAULT 0,
    first_received_at TIMESTAMPTZ,
    PRIMARY KEY (account_id, region, queue_url, id)
);
CREATE INDEX IF NOT EXISTS idx_sqs_messages_queue
    ON jc_sqs_messages (account_id, region, queue_url, sent_at);

-- FIFO deduplication window (5 minutes).
CREATE TABLE IF NOT EXISTS jc_sqs_dedup (
    account_id TEXT        NOT NULL,
    region     TEXT        NOT NULL,
    dedup_key  TEXT        NOT NULL,
    message_id TEXT        NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, region, dedup_key)
);
