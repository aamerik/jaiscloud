package logging

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	nextID     int64
	entries    map[string][]LogEntry              // scope → entries
	sinks      map[string]map[string]LogSink      // scope → name → sink
	exclusions map[string]map[string]LogExclusion // scope → name → exclusion
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries:    make(map[string][]LogEntry),
		sinks:      make(map[string]map[string]LogSink),
		exclusions: make(map[string]map[string]LogExclusion),
	}
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

// ─── sinks ────────────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateSink(_ context.Context, scope string, sink LogSink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sinks[scope] == nil {
		s.sinks[scope] = make(map[string]LogSink)
	}
	if _, ok := s.sinks[scope][sink.Name]; ok {
		return ErrSinkExists
	}
	s.sinks[scope][sink.Name] = sink
	return nil
}

func (s *MemoryStore) GetSink(_ context.Context, scope, name string) (LogSink, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sink, ok := s.sinks[scope][name]
	if !ok {
		return LogSink{}, ErrSinkNotFound
	}
	return sink, nil
}

func (s *MemoryStore) ListSinks(_ context.Context, scope string) ([]LogSink, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]LogSink, 0, len(s.sinks[scope]))
	for _, sink := range s.sinks[scope] {
		result = append(result, sink)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) UpdateSink(_ context.Context, scope string, sink LogSink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sinks[scope][sink.Name]; !ok {
		return ErrSinkNotFound
	}
	s.sinks[scope][sink.Name] = sink
	return nil
}

func (s *MemoryStore) DeleteSink(_ context.Context, scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sinks[scope][name]; !ok {
		return ErrSinkNotFound
	}
	delete(s.sinks[scope], name)
	return nil
}

// ─── exclusions ───────────────────────────────────────────────────────────────

func (s *MemoryStore) CreateExclusion(_ context.Context, scope string, e LogExclusion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exclusions[scope] == nil {
		s.exclusions[scope] = make(map[string]LogExclusion)
	}
	if _, ok := s.exclusions[scope][e.Name]; ok {
		return ErrExclusionExists
	}
	s.exclusions[scope][e.Name] = e
	return nil
}

func (s *MemoryStore) GetExclusion(_ context.Context, scope, name string) (LogExclusion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.exclusions[scope][name]
	if !ok {
		return LogExclusion{}, ErrExclusionNotFound
	}
	return e, nil
}

func (s *MemoryStore) ListExclusions(_ context.Context, scope string) ([]LogExclusion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]LogExclusion, 0, len(s.exclusions[scope]))
	for _, e := range s.exclusions[scope] {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *MemoryStore) UpdateExclusion(_ context.Context, scope string, e LogExclusion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.exclusions[scope][e.Name]; !ok {
		return ErrExclusionNotFound
	}
	s.exclusions[scope][e.Name] = e
	return nil
}

func (s *MemoryStore) DeleteExclusion(_ context.Context, scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.exclusions[scope][name]; !ok {
		return ErrExclusionNotFound
	}
	delete(s.exclusions[scope], name)
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string][]LogEntry)
	s.sinks = make(map[string]map[string]LogSink)
	s.exclusions = make(map[string]map[string]LogExclusion)
	s.nextID = 0
}
