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
	// sortedIDs caches, per topic, message IDs in ascending PublishTime order.
	// Put/Delete invalidate a topic's entry (they change membership/order);
	// claim-state mutations (Pull/UpdateDeliveryAttempt/ModifyAckDeadline only
	// touch VisibleAt/DeliveryAttempt, never PublishTime) do not, so repeated
	// polling against an unchanged topic — the common Pub/Sub access pattern —
	// avoids re-sorting the topic on every call.
	sortedIDs map[string][]string
	seq       atomic.Int64 // monotonic message-ID counter
}

// NewMemoryMessages returns an empty in-memory message store.
func NewMemoryMessages() *MemoryMessages {
	return &MemoryMessages{
		messages:  make(map[string]map[string]Message),
		sortedIDs: make(map[string][]string),
	}
}

// orderedIDsLocked returns messageIDs for topic in ascending PublishTime
// order, building and caching them if the cache was invalidated. Callers must
// hold s.mu for writing.
func (s *MemoryMessages) orderedIDsLocked(topic string) []string {
	if ids, ok := s.sortedIDs[topic]; ok {
		return ids
	}
	msgs := s.messages[topic]
	ids := make([]string, 0, len(msgs))
	for id := range msgs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return msgs[ids[i]].PublishTime.Before(msgs[ids[j]].PublishTime) })
	s.sortedIDs[topic] = ids
	return ids
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
	delete(s.sortedIDs, m.Topic)
	return nil
}

func (s *MemoryMessages) List(_ context.Context, topic string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs := s.messages[topic]
	ids := s.orderedIDsLocked(topic)
	result := make([]Message, 0, len(ids))
	for _, id := range ids {
		result = append(result, msgs[id])
	}
	return result, nil
}

// Pull atomically claims eligible messages (mirrors SQS Receive: skip delayed/
// in-flight, gate ordering-key groups, then claim under the mutex).
func (s *MemoryMessages) Pull(_ context.Context, topic string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.messages[topic]
	ids := s.orderedIDsLocked(topic)

	// FIFO: build the set of ordering keys that have an earlier in-flight message.
	inFlightGroups := map[string]bool{}
	for _, m := range msgs {
		if m.OrderingKey != "" && !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			inFlightGroups[m.OrderingKey] = true
		}
	}

	var out []Message
	for _, id := range ids {
		if len(out) >= maxMessages {
			break
		}
		m := msgs[id]
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
		if _, existed := msgs[messageID]; existed {
			delete(msgs, messageID)
			delete(s.sortedIDs, topic)
		}
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
	s.sortedIDs = make(map[string][]string)
}

func duration(sec int) time.Duration { return time.Duration(sec) * time.Second }
func timeZero() time.Time            { return time.Time{} }
func lastSlash(s string) int         { return strings.LastIndex(s, "/") }
