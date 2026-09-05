package pubsub

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryMessages is an in-memory Messages store.
type MemoryMessages struct {
	mu       sync.RWMutex
	messages map[string]map[string]Message // topic → messageID → message
	seq      atomic.Int64                  // monotonic message-ID counter
}

// NewMemoryMessages returns an empty in-memory message store.
func NewMemoryMessages() *MemoryMessages {
	return &MemoryMessages{messages: make(map[string]map[string]Message)}
}

// NextID returns the next monotonic message ID for this process.
func (s *MemoryMessages) NextID(_ context.Context) (string, error) {
	return strconv.FormatInt(s.seq.Add(1), 10), nil
}

func (s *MemoryMessages) Put(_ context.Context, m Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.messages[m.Topic] == nil {
		s.messages[m.Topic] = make(map[string]Message)
	}
	s.messages[m.Topic][m.MessageID] = m
	return nil
}

func (s *MemoryMessages) List(_ context.Context, topic string) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msgs := s.messages[topic]
	result := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		result = append(result, m)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PublishTime.Before(result[j].PublishTime) })
	return result, nil
}

// Pull atomically claims eligible messages (mirrors SQS Receive: skip delayed/
// in-flight, gate ordering-key groups, then claim under the mutex).
func (s *MemoryMessages) Pull(_ context.Context, topic string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.messages[topic]
	// FIFO: build the set of ordering keys that have an earlier in-flight message.
	inFlightGroups := map[string]bool{}
	for _, m := range msgs {
		if m.OrderingKey != "" && !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			inFlightGroups[m.OrderingKey] = true
		}
	}

	sorted := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		sorted = append(sorted, m)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PublishTime.Before(sorted[j].PublishTime) })

	var out []Message
	for _, m := range sorted {
		if len(out) >= maxMessages {
			break
		}
		// Retention: skip expired messages.
		if retentionSec > 0 && now.Sub(m.PublishTime) > duration(retentionSec) {
			continue
		}
		// In-flight: still within its ack deadline.
		if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			continue
		}
		// FIFO: skip if an earlier message in the same ordering-key group is in-flight.
		if m.OrderingKey != "" && inFlightGroups[m.OrderingKey] {
			continue
		}
		// Claim.
		m.VisibleAt = now.Add(duration(ackDeadlineSec))
		m.DeliveryAttempt++
		s.messages[topic][m.MessageID] = m
		if m.OrderingKey != "" {
			inFlightGroups[m.OrderingKey] = true
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *MemoryMessages) Delete(_ context.Context, topic, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msgs, ok := s.messages[topic]; ok {
		delete(msgs, messageID)
	}
	return nil
}

func (s *MemoryMessages) UpdateDeliveryAttempt(_ context.Context, topic, messageID string, attempt int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs, ok := s.messages[topic]
	if !ok {
		return nil
	}
	m, ok := msgs[messageID]
	if !ok {
		return nil
	}
	m.DeliveryAttempt = attempt
	msgs[messageID] = m
	return nil
}

// ModifyAckDeadline resets the visibility deadline for each ack ID ("topic/messageID").
func (s *MemoryMessages) ModifyAckDeadline(_ context.Context, topic string, ackIDs []string, seconds int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ackID := range ackIDs {
		msgID := ackID
		if i := lastSlash(ackID); i >= 0 {
			msgID = ackID[i+1:]
		}
		m, ok := s.messages[topic][msgID]
		if !ok {
			continue
		}
		if seconds == 0 {
			m.VisibleAt = timeZero()
		} else {
			m.VisibleAt = now.Add(duration(seconds))
		}
		s.messages[topic][msgID] = m
	}
	return nil
}

func (s *MemoryMessages) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = make(map[string]map[string]Message)
}

func duration(sec int) time.Duration { return time.Duration(sec) * time.Second }
func timeZero() time.Time            { return time.Time{} }
func lastSlash(s string) int         { return strings.LastIndex(s, "/") }
