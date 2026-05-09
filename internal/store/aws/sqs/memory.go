package sqs

import (
	"context"
	"crypto/md5"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// dedupEntry stores deduplication metadata for a FIFO message.
type dedupEntry struct {
	expiry    time.Time
	messageID string
}

// MemoryMessageStore is an in-memory SQSMessageStore.
// Messages are stored in per-queue slices; FIFO ordering is preserved.
type MemoryMessageStore struct {
	mu     sync.Mutex
	queues map[string][]*SQSMessage // queueURL → ordered message list

	// FIFO deduplication: dedup key → entry (expiry + original messageID)
	dedup map[string]dedupEntry

	// FIFO sequence counter per queue
	seqCounter map[string]int64
}

func NewMemoryMessageStore() *MemoryMessageStore {
	return &MemoryMessageStore{
		queues:     make(map[string][]*SQSMessage),
		dedup:      make(map[string]dedupEntry),
		seqCounter: make(map[string]int64),
	}
}

func (s *MemoryMessageStore) Send(ctx context.Context, msg SQSMessage) (dedupMessageID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// FIFO deduplication: reject duplicates within 5-minute window.
	// Scope: "messageGroup" → key includes GroupID; default → queue-wide.
	if msg.DeduplicationID != "" {
		var dedupKey string
		if msg.DedupScope == "messageGroup" {
			dedupKey = msg.QueueURL + ":" + msg.GroupID + ":" + msg.DeduplicationID
		} else {
			dedupKey = msg.QueueURL + ":" + msg.DeduplicationID
		}
		if entry, ok := s.dedup[dedupKey]; ok && time.Now().Before(entry.expiry) {
			return entry.messageID, nil // return original MessageID to caller
		}
		s.dedup[dedupKey] = dedupEntry{expiry: time.Now().Add(5 * time.Minute), messageID: msg.MessageID}
	}

	// Assign FIFO sequence number
	if msg.GroupID != "" {
		s.seqCounter[msg.QueueURL]++
		msg.SequenceNumber = fmt.Sprintf("%020d", s.seqCounter[msg.QueueURL])
	}

	msg.MD5OfBody = fmt.Sprintf("%x", md5.Sum([]byte(msg.Body)))
	msg.ReceiptHandle = newHandle()

	cp := msg // copy to avoid caller mutation
	s.queues[msg.QueueURL] = append(s.queues[msg.QueueURL], &cp)
	return "", nil
}

func (s *MemoryMessageStore) Receive(ctx context.Context, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.queues[queueURL]
	var result []SQSMessage

	// FIFO: track in-flight groups to preserve ordering
	inFlightGroups := map[string]bool{}
	for _, m := range msgs {
		if m.GroupID != "" && !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			inFlightGroups[m.GroupID] = true
		}
	}

	for _, m := range msgs {
		if len(result) >= maxMessages {
			break
		}
		// Skip messages not yet visible (delayed or in-flight)
		if !m.DelayUntil.IsZero() && now.Before(m.DelayUntil) {
			continue
		}
		if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			continue
		}
		// FIFO: skip if an earlier message in the same group is in-flight
		if m.GroupID != "" && inFlightGroups[m.GroupID] {
			continue
		}

		// Assign a fresh receipt handle on each receive
		m.ReceiptHandle = newHandle()
		m.ReceiveCount++
		now2 := now
		if m.FirstReceivedAt == nil {
			m.FirstReceivedAt = &now2
		}
		// Temporarily mark as invisible (caller sets actual timeout via ChangeVisibility)
		m.VisibleAt = now.Add(30 * time.Second)

		if m.GroupID != "" {
			inFlightGroups[m.GroupID] = true
		}

		result = append(result, *m)
	}

	return result, nil
}

func (s *MemoryMessageStore) Delete(ctx context.Context, queueURL, receiptHandle string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.queues[queueURL]
	for i, m := range msgs {
		if m.ReceiptHandle == receiptHandle {
			s.queues[queueURL] = append(msgs[:i], msgs[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("receipt handle not found")
}

func (s *MemoryMessageStore) ChangeVisibility(ctx context.Context, queueURL, receiptHandle string, timeoutSec int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range s.queues[queueURL] {
		if m.ReceiptHandle == receiptHandle {
			if timeoutSec == 0 {
				m.VisibleAt = time.Time{} // immediately visible
			} else {
				m.VisibleAt = now.Add(time.Duration(timeoutSec) * time.Second)
			}
			return nil
		}
	}
	return fmt.Errorf("receipt handle not found")
}

func (s *MemoryMessageStore) Purge(ctx context.Context, queueURL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[queueURL] = nil
	return nil
}

func (s *MemoryMessageStore) GetApproximateCounts(ctx context.Context, queueURL string, now time.Time) (visible, notVisible, delayed int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range s.queues[queueURL] {
		if !m.DelayUntil.IsZero() && now.Before(m.DelayUntil) {
			delayed++
		} else if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			notVisible++
		} else {
			visible++
		}
	}
	return
}

func (s *MemoryMessageStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = make(map[string][]*SQSMessage)
	s.dedup = make(map[string]dedupEntry)
	s.seqCounter = make(map[string]int64)
}

func newHandle() string {
	return fmt.Sprintf("%x", rand.Int63())
}
