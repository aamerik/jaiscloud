package logging

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu      sync.RWMutex
	nextID  int64
	entries map[string][]LogEntry // scope → entries
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string][]LogEntry)}
}

func (s *MemoryStore) Write(_ context.Context, scope string, e LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.ID = s.nextID
	s.nextID++
	s.entries[scope] = append(s.entries[scope], e)
	return nil
}

func (s *MemoryStore) List(_ context.Context, scope string) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.entries[scope]
	result := make([]LogEntry, len(src))
	copy(result, src)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Timestamp.Equal(result[j].Timestamp) {
			return result[i].ID < result[j].ID
		}
		return result[i].Timestamp.Before(result[j].Timestamp)
	})
	return result, nil
}

func (s *MemoryStore) ListLogs(_ context.Context, scope string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[string]struct{})
	for _, e := range s.entries[scope] {
		if e.LogName != "" {
			seen[e.LogName] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func (s *MemoryStore) DeleteLog(_ context.Context, scope, logName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.entries[scope]
	kept := src[:0]
	for _, e := range src {
		if e.LogName != logName {
			kept = append(kept, e)
		}
	}
	s.entries[scope] = kept
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string][]LogEntry)
	s.nextID = 0
}
