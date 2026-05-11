package sqs

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSQSMessageStore implements SQSMessageStore against PostgreSQL.
// It expects the schema from migration 002_sqs_messages.sql to already exist.
type PostgresSQSMessageStore struct {
	pool *pgxpool.Pool
}

func NewPostgresSQSMessageStore(pool *pgxpool.Pool) *PostgresSQSMessageStore {
	return &PostgresSQSMessageStore{pool: pool}
}

func (s *PostgresSQSMessageStore) Send(ctx context.Context, msg SQSMessage) (string, error) {
	// FIFO deduplication.
	if msg.DeduplicationID != "" {
		dedupKey := msg.QueueURL + ":" + msg.DeduplicationID
		var origID string
		err := s.pool.QueryRow(ctx, `
			SELECT message_id FROM jc_sqs_dedup
			WHERE dedup_key=$1 AND expires_at > now()
		`, dedupKey).Scan(&origID)
		if err == nil {
			return origID, nil // duplicate
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("dedup check: %w", err)
		}
		// Record dedup entry (upsert).
		_, err = s.pool.Exec(ctx, `
			INSERT INTO jc_sqs_dedup (dedup_key, message_id, expires_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (dedup_key) DO UPDATE
				SET message_id=$2, expires_at=$3
		`, dedupKey, msg.MessageID, time.Now().Add(5*time.Minute))
		if err != nil {
			return "", fmt.Errorf("dedup upsert: %w", err)
		}
	}

	msg.MD5OfBody = fmt.Sprintf("%x", md5.Sum([]byte(msg.Body)))
	msg.ReceiptHandle = newHandle()

	var delayUntil *time.Time
	if !msg.DelayUntil.IsZero() {
		delayUntil = &msg.DelayUntil
	}

	attrsJSON, err := json.Marshal(msg.MessageAttributes)
	if err != nil {
		return "", fmt.Errorf("marshal message attributes: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_sqs_messages
			(id, queue_url, receipt_handle, body, md5_of_body, group_id, dedup_id,
			 sequence_number, sent_at, delay_until, receive_count, msg_attributes)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id, queue_url) DO NOTHING
	`, msg.MessageID, msg.QueueURL, msg.ReceiptHandle, msg.Body, msg.MD5OfBody,
		msg.GroupID, msg.DeduplicationID, msg.SequenceNumber,
		msg.SentAt, delayUntil, 0, attrsJSON)
	if err != nil {
		return "", fmt.Errorf("insert message: %w", err)
	}
	return "", nil
}

func (s *PostgresSQSMessageStore) Receive(ctx context.Context, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error) {
	// SELECT ... FOR UPDATE SKIP LOCKED — safe for concurrent consumers.
	rows, err := s.pool.Query(ctx, `
		SELECT id, receipt_handle, body, md5_of_body, group_id, dedup_id,
		       sequence_number, sent_at, delay_until, visible_at,
		       receive_count, first_received_at, msg_attributes
		FROM jc_sqs_messages
		WHERE queue_url = $1
		  AND (delay_until IS NULL OR delay_until <= $2)
		  AND (visible_at IS NULL OR visible_at <= $2)
		ORDER BY sent_at
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, queueURL, now, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("receive query: %w", err)
	}
	defer rows.Close()

	var msgs []SQSMessage
	for rows.Next() {
		var m SQSMessage
		var delayUntil, visibleAt *time.Time
		var firstReceivedAt *time.Time
		var attrsJSON []byte
		err := rows.Scan(&m.MessageID, &m.ReceiptHandle, &m.Body, &m.MD5OfBody,
			&m.GroupID, &m.DeduplicationID, &m.SequenceNumber,
			&m.SentAt, &delayUntil, &visibleAt,
			&m.ReceiveCount, &firstReceivedAt, &attrsJSON)
		if err != nil {
			return nil, fmt.Errorf("receive scan: %w", err)
		}
		if len(attrsJSON) > 0 && string(attrsJSON) != "null" {
			_ = json.Unmarshal(attrsJSON, &m.MessageAttributes)
		}
		if delayUntil != nil {
			m.DelayUntil = *delayUntil
		}
		if visibleAt != nil {
			m.VisibleAt = *visibleAt
		}
		m.FirstReceivedAt = firstReceivedAt
		m.QueueURL = queueURL
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Update receipt handle, receive count, visible_at for each fetched message.
	for i := range msgs {
		handle := newHandle()
		msgs[i].ReceiveCount++
		msgs[i].ReceiptHandle = handle
		visibleAt := now.Add(30 * time.Second)
		msgs[i].VisibleAt = visibleAt

		var firstRecv *time.Time
		if msgs[i].FirstReceivedAt == nil {
			firstRecv = &now
			msgs[i].FirstReceivedAt = firstRecv
		}

		_, err := s.pool.Exec(ctx, `
			UPDATE jc_sqs_messages
			SET receipt_handle=$1, receive_count=receive_count+1, visible_at=$2,
			    first_received_at=COALESCE(first_received_at,$3)
			WHERE id=$4 AND queue_url=$5
		`, handle, visibleAt, now, msgs[i].MessageID, msgs[i].QueueURL)
		if err != nil {
			return nil, fmt.Errorf("update receive state: %w", err)
		}
	}
	return msgs, nil
}

func (s *PostgresSQSMessageStore) Delete(ctx context.Context, queueURL, receiptHandle string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_sqs_messages
		WHERE queue_url=$1 AND receipt_handle=$2
	`, queueURL, receiptHandle)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("receipt handle not found")
	}
	return nil
}

func (s *PostgresSQSMessageStore) ChangeVisibility(ctx context.Context, queueURL, receiptHandle string, timeoutSec int, now time.Time) error {
	var visibleAt *time.Time
	if timeoutSec > 0 {
		t := now.Add(time.Duration(timeoutSec) * time.Second)
		visibleAt = &t
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE jc_sqs_messages
		SET visible_at=$1
		WHERE queue_url=$2 AND receipt_handle=$3
	`, visibleAt, queueURL, receiptHandle)
	return err
}

func (s *PostgresSQSMessageStore) Purge(ctx context.Context, queueURL string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_sqs_messages WHERE queue_url=$1`, queueURL)
	return err
}

func (s *PostgresSQSMessageStore) GetApproximateCounts(ctx context.Context, queueURL string, now time.Time) (visible, notVisible, delayed int, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			CASE
				WHEN delay_until IS NOT NULL AND delay_until > $2 THEN 'delayed'
				WHEN visible_at IS NOT NULL AND visible_at > $2 THEN 'not_visible'
				ELSE 'visible'
			END AS state,
			count(*) AS cnt
		FROM jc_sqs_messages
		WHERE queue_url=$1
		GROUP BY state
	`, queueURL, now)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var cnt int
		if err := rows.Scan(&state, &cnt); err != nil {
			return 0, 0, 0, err
		}
		switch state {
		case "visible":
			visible = cnt
		case "not_visible":
			notVisible = cnt
		case "delayed":
			delayed = cnt
		}
	}
	return visible, notVisible, delayed, rows.Err()
}

// SetQueueRetention is a no-op for the postgres store; TTL is enforced via DB-side
// expiry or a separate housekeeping job.
func (s *PostgresSQSMessageStore) SetQueueRetention(_ context.Context, _ string, _ int) error {
	return nil
}

func (s *PostgresSQSMessageStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_messages`)
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_dedup`)
}
