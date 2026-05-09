package esm

import (
	"context"
	"time"

	"jaiscloud/internal/store/stream"
)

const (
	ESMStateCreating  = "Creating"
	ESMStateEnabled   = "Enabled"
	ESMStateDisabled  = "Disabled"
	ESMStateUpdating  = "Updating"
	ESMStateDisabling = "Disabling"
	ESMStateDeleting  = "Deleting"

	ESMSourceSQS            = "sqs"
	ESMSourceDynamoDBStreams = "dynamodb-streams"
)

type EventSourceMapping struct {
	UUID                           string    `json:"UUID"`
	FunctionName                   string    `json:"FunctionName"`
	FunctionArn                    string    `json:"FunctionArn"`
	EventSourceArn                 string    `json:"EventSourceArn"`
	BatchSize                      int       `json:"BatchSize"`
	MaximumBatchingWindowInSeconds int       `json:"MaximumBatchingWindowInSeconds"`
	Enabled                        bool      `json:"Enabled"`
	State                          string    `json:"State"`
	StateTransitionReason          string    `json:"StateTransitionReason"`
	LastModified                   time.Time `json:"LastModified"`
	LastProcessingResult           string    `json:"LastProcessingResult"`
	SourceType                     string    `json:"SourceType"`
	MaximumRetryAttempts           int       `json:"MaximumRetryAttempts"`
	BisectBatchOnFunctionError     bool      `json:"BisectBatchOnFunctionError"`
	Region                         string    `json:"Region"`
	Cloud                          string    `json:"Cloud"`

	ConsecutiveErrors  int    `json:"-"`
	QueueName          string `json:"-"`
	TableName          string `json:"-"`
	LastSequenceNumber int    `json:"-"`
}

// QueueInternalAPI is the SQS interface needed by ESM pollers.
type QueueInternalAPI interface {
	InternalReceive(ctx context.Context, queueName string, maxMessages int, waitTimeSec int) ([]InternalMessage, error)
	InternalDeleteBatch(ctx context.Context, queueName string, receiptHandles []string) error
}

// InternalMessage is a simplified message for internal ESM use.
type InternalMessage struct {
	MessageID         string
	ReceiptHandle     string
	Body              string
	Attributes        map[string]string
	MessageAttributes map[string]any
	MD5OfBody         string
}

// StreamStoreAPI is the DynamoDB Streams interface needed by ESM pollers.
type StreamStoreAPI interface {
	GetRecords(tableName string, afterSeq int) ([]stream.Record, int)
	GetStreamInfo(tableName string) (stream.StreamInfo, bool)
}
