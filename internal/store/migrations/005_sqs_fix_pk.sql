-- Change jc_sqs_messages primary key from (id) to (id, queue_url) so that
-- the same message_id can be delivered to multiple queues (e.g. SNS fan-out).
-- This is a no-op if the table was created after 002 was updated to use the
-- composite PK.
DO $$
BEGIN
    IF (
        SELECT count(*)
        FROM pg_constraint
        WHERE conrelid = 'jc_sqs_messages'::regclass
          AND contype = 'p'
          AND array_length(conkey, 1) = 1
    ) > 0 THEN
        ALTER TABLE jc_sqs_messages DROP CONSTRAINT jc_sqs_messages_pkey;
        ALTER TABLE jc_sqs_messages ADD PRIMARY KEY (id, queue_url);
    END IF;
END $$;
