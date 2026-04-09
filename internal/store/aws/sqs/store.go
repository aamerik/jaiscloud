package sqs

import (
	"context"
	"time"
)

// SQSMessage represents a message in a queue (data plane).
type SQSMessage struct {
	MessageID         string
	ReceiptHandle     string
	QueueURL          string
	Body              string
	MD5OfBody         string
	Attributes        map[string]string          // system attributes
	MessageAttributes map[string]MessageAttribute // user-defined attributes
	GroupID           string                      // FIFO: MessageGroupId
	DeduplicationID   string                      // FIFO: MessageDeduplicationId
	VisibleAt         time.Time                   // when the message becomes visible again
	SentAt            time.Time
	DelayUntil        time.Time  // for delayed messages
	ReceiveCount      int
	FirstReceivedAt   *time.Time
	SequenceNumber    string // FIFO sequence
}

// MessageAttribute is a typed user-defined SQS message attribute.
type MessageAttribute struct {
	DataType    string // "String", "Number", "Binary"
	StringValue string
	BinaryValue []byte
}

// SQSMessageStore manages data-plane message storage for SQS.
// Phase 0: MemoryMessageStore. Phase 1: PostgresMessageStore.
type SQSMessageStore interface {
	// Send enqueues a message. For FIFO queues with deduplication, if a
	// duplicate is detected the original MessageID is returned with no error.
	// A non-empty returned messageID indicates a deduplicated send.
	Send(ctx context.Context, msg SQSMessage) (dedupMessageID string, err error)
	Receive(ctx context.Context, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error)
	Delete(ctx context.Context, queueURL, receiptHandle string) error
	ChangeVisibility(ctx context.Context, queueURL, receiptHandle string, timeoutSec int, now time.Time) error
	Purge(ctx context.Context, queueURL string) error
	GetApproximateCounts(ctx context.Context, queueURL string, now time.Time) (visible, notVisible, delayed int, err error)

	// Reset wipes all messages across all queues.
	Reset()
}
