// Package kinesis provides the in-memory store for Kinesis streams and records.
package kinesis

import "time"

// StreamStatus mirrors the AWS StreamStatus enum.
type StreamStatus string

const (
	StreamStatusCreating StreamStatus = "CREATING"
	StreamStatusActive   StreamStatus = "ACTIVE"
	StreamStatusDeleting StreamStatus = "DELETING"
	StreamStatusUpdating StreamStatus = "UPDATING"
)

// StreamMode mirrors the AWS StreamMode enum.
type StreamMode string

const (
	StreamModeProvisioned StreamMode = "PROVISIONED"
	StreamModeOnDemand    StreamMode = "ON_DEMAND"
)

// ShardIteratorType mirrors the AWS ShardIteratorType enum.
type ShardIteratorType string

const (
	IterTrimHorizon         ShardIteratorType = "TRIM_HORIZON"
	IterLatest              ShardIteratorType = "LATEST"
	IterAtSequenceNumber    ShardIteratorType = "AT_SEQUENCE_NUMBER"
	IterAfterSequenceNumber ShardIteratorType = "AFTER_SEQUENCE_NUMBER"
	IterAtTimestamp         ShardIteratorType = "AT_TIMESTAMP"
)

// Stream holds stream metadata.
type Stream struct {
	Name                 string
	ARN                  string
	Status               StreamStatus
	Mode                 StreamMode
	RetentionPeriodHours int
	CreatedAt            time.Time
	EncryptionType       string
	KeyID                string
	Tags                 map[string]string
	ResourcePolicy       string
}

// Shard describes a single kinesis shard.
type Shard struct {
	ShardID               string
	ParentShardID         string
	AdjacentParentShardID string
	HashKeyRange          HashKeyRange
	SequenceNumberRange   SequenceNumberRange
	IsOpen                bool
}

// HashKeyRange is the inclusive range of MD5 hash keys for a shard.
type HashKeyRange struct {
	StartingHashKey string
	EndingHashKey   string
}

// SequenceNumberRange holds the monotonic sequence number bounds for a shard.
type SequenceNumberRange struct {
	StartingSequenceNumber string
	EndingSequenceNumber   string // empty when shard is open
}

// Record is a single data record stored in a shard.
type Record struct {
	SequenceNumber         string
	ApproximateArrivalTime time.Time
	Data                   []byte
	PartitionKey           string
	EncryptionType         string
}

// Consumer is an Enhanced Fan-Out consumer registered against a stream.
type Consumer struct {
	Name      string
	ARN       string
	StreamARN string
	Status    string
	CreatedAt time.Time
}

// IteratorEntry holds server-side state for an opaque shard iterator token.
type IteratorEntry struct {
	StreamName string
	ShardID    string
	// Position is the index into the shard's Records slice for the next read.
	Position  int
	CreatedAt time.Time
}
