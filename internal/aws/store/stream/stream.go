// Package stream implements an in-memory DynamoDB Streams record store.
package stream

import (
	"fmt"
	"sync"
	"time"
)

// UserIdentity identifies who caused a stream event.
// For TTL-expired items: Type="Service", PrincipalId="dynamodb.amazonaws.com".
type UserIdentity struct {
	Type        string
	PrincipalId string
}

// Record represents a single DynamoDB Streams record.
type Record struct {
	SequenceNumber              int
	EventID                     string
	EventName                   string // INSERT, MODIFY, REMOVE
	ApproximateCreationDateTime time.Time
	Keys                        map[string]any
	NewImage                    map[string]any
	OldImage                    map[string]any
	UserIdentity                *UserIdentity // non-nil for service-initiated events (e.g. TTL expiry)
}

// StreamInfo holds metadata about a single stream.
type StreamInfo struct {
	StreamArn   string
	TableName   string
	StreamLabel string
	Enabled     bool
}

// MemoryStreamStore holds stream records per table.
type MemoryStreamStore struct {
	mu      sync.RWMutex
	streams map[string]*tableStream // keyed by tableName
}

type tableStream struct {
	info    StreamInfo
	records []Record
	nextSeq int
}

func NewMemoryStreamStore() *MemoryStreamStore {
	return &MemoryStreamStore{streams: make(map[string]*tableStream)}
}

func (s *MemoryStreamStore) Enable(tableName, streamArn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.streams[tableName]; !ok {
		s.streams[tableName] = &tableStream{
			info: StreamInfo{
				StreamArn:   streamArn,
				TableName:   tableName,
				StreamLabel: fmt.Sprintf("%d", time.Now().UnixNano()),
				Enabled:     true,
			},
		}
	} else {
		s.streams[tableName].info.Enabled = true
		s.streams[tableName].info.StreamArn = streamArn
	}
}

func (s *MemoryStreamStore) Disable(tableName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ts, ok := s.streams[tableName]; ok {
		ts.info.Enabled = false
	}
}

func (s *MemoryStreamStore) IsEnabled(tableName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.streams[tableName]
	return ok && ts.info.Enabled
}

func (s *MemoryStreamStore) GetStreamARN(tableName string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ts, ok := s.streams[tableName]; ok {
		return ts.info.StreamArn
	}
	return ""
}

func (s *MemoryStreamStore) GetStreamInfo(tableName string) (StreamInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.streams[tableName]
	if !ok {
		return StreamInfo{}, false
	}
	return ts.info, true
}

func (s *MemoryStreamStore) Append(tableName string, record Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.streams[tableName]
	if !ok || !ts.info.Enabled {
		return
	}
	record.SequenceNumber = ts.nextSeq
	record.ApproximateCreationDateTime = time.Now()
	ts.records = append(ts.records, record)
	ts.nextSeq++
}

// GetRecords returns records after (exclusive) the given sequence number.
// Returns records and the next sequence number to use as a cursor.
func (s *MemoryStreamStore) GetRecords(tableName string, afterSeq int) ([]Record, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.streams[tableName]
	if !ok {
		return nil, 0
	}
	var out []Record
	for _, r := range ts.records {
		if r.SequenceNumber > afterSeq {
			out = append(out, r)
		}
	}
	return out, ts.nextSeq
}

func (s *MemoryStreamStore) ListStreams() []StreamInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []StreamInfo
	for _, ts := range s.streams {
		if ts.info.Enabled {
			out = append(out, ts.info)
		}
	}
	return out
}

func (s *MemoryStreamStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = make(map[string]*tableStream)
}
