// Package logstream defines a neutral log event type and ingestor interface
// shared between the Lambda executor and CloudWatch Logs provider.
// Neither package imports the other; both import this package.
package logstream

import "context"

// Event is a single log line with a millisecond-epoch timestamp.
type Event struct {
	Timestamp int64
	Message   string
}

// Ingestor is implemented by the CloudWatch Logs provider and consumed by
// the Lambda log streamer.
type Ingestor interface {
	InternalPutEvents(ctx context.Context, logGroupName, logStreamName string, events []Event) error
	InternalCreateLogGroup(ctx context.Context, logGroupName string) error
}
