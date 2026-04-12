package sdk

import "context"

// Event is a typed event emitted by a plugin (e.g. job state changes).
// The host routes events to registered subscribers (SQS queues, EventBridge, etc.).
type Event struct {
	// Source identifies the plugin that emitted the event, e.g. "aws-emr-spark".
	Source string

	// Type is the event type, e.g. "EMRJobStateChange".
	Type string

	// Detail contains the event payload as a JSON-serialisable map.
	Detail map[string]any
}

// EventBus allows plugins to publish events to the host's event system.
// The host injects a concrete implementation; plugins must not assume delivery is synchronous.
type EventBus interface {
	// Publish emits an event. Returns an error only if the bus is shut down.
	// Plugins should not block waiting for consumers to process the event.
	Publish(ctx context.Context, event Event) error
}

// NoopEventBus is an EventBus that discards all events.
// Plugins may use this as a default when the host does not inject a bus
// (e.g. unit tests that don't need event verification).
type NoopEventBus struct{}

func (NoopEventBus) Publish(_ context.Context, _ Event) error { return nil }
