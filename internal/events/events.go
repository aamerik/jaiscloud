package events

import "sync"

// EventType identifies the kind of event.
type EventType string

const (
	EventMessageDLQ EventType = "message.dlq" // message moved to DLQ
)

// Event is a domain event published by a provider.
type Event struct {
	Type    EventType
	Payload any
}

// DLQEvent is emitted when a message exceeds its maxReceiveCount.
type DLQEvent struct {
	SourceQueueURL string
	DLQQueueURL    string
	MessageID      string
}

// Handler is a function that handles an event.
type Handler func(event Event)

// EventBus is a simple synchronous in-process event bus.
type EventBus struct {
	mu       sync.RWMutex
	handlers map[EventType][]Handler
}

func NewEventBus() *EventBus {
	return &EventBus{handlers: make(map[EventType][]Handler)}
}

// Subscribe registers a handler for the given event type.
func (b *EventBus) Subscribe(eventType EventType, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
}

// Publish dispatches the event to all registered handlers synchronously.
func (b *EventBus) Publish(event Event) {
	b.mu.RLock()
	handlers := b.handlers[event.Type]
	b.mu.RUnlock()
	for _, h := range handlers {
		h(event)
	}
}
