-- Pub/Sub message-ID sequence: keeps message IDs monotonic across restarts
-- (mirrors SQS jc_sqs_fifo_seq). The provider allocates IDs from this sequence
-- instead of a process-local counter, so a restart can never reuse an ID and
-- silently overwrite a persisted message via ON CONFLICT (topic, message_id).
CREATE SEQUENCE IF NOT EXISTS jc_pubsub_msg_seq;

-- Seed above any pre-existing numeric message IDs so sequence-allocated IDs do
-- not collide with rows created before this migration.
SELECT setval('jc_pubsub_msg_seq',
    COALESCE((SELECT MAX(message_id::bigint) FROM jc_pubsub_messages WHERE message_id ~ '^[0-9]+$'), 0));
