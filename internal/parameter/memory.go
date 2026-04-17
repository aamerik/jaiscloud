package parameter

import (
	"context"
	"strings"
	"sync"
	"time"
)

// MemoryParameterStore is an in-process ParameterStore used in lite mode.
type MemoryParameterStore struct {
	mu         sync.RWMutex
	params     map[string]ParameterEntry
	history    map[string][]HistoryEntry
}

func NewMemoryParameterStore() *MemoryParameterStore {
	return &MemoryParameterStore{
		params:  make(map[string]ParameterEntry),
		history: make(map[string][]HistoryEntry),
	}
}

func (s *MemoryParameterStore) PutParameter(_ context.Context, e ParameterEntry, overwrite bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if existing, ok := s.params[e.Name]; ok {
		if !overwrite {
			return ErrAlreadyExists
		}
		// Record current as history before overwrite.
		s.history[e.Name] = append(s.history[e.Name], HistoryEntry{
			Name:      existing.Name,
			Version:   existing.Version,
			Type:      existing.Type,
			Value:     existing.Value,
			CreatedAt: existing.UpdatedAt,
		})
		e.Version = existing.Version + 1
		e.CreatedAt = existing.CreatedAt
	} else {
		e.Version = 1
		e.CreatedAt = now
	}
	e.UpdatedAt = now
	s.params[e.Name] = e
	return nil
}

func (s *MemoryParameterStore) GetParameter(_ context.Context, name string) (ParameterEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.params[name]
	if !ok {
		return ParameterEntry{}, ErrParameterNotFound
	}
	return e, nil
}

func (s *MemoryParameterStore) DeleteParameter(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.params[name]; !ok {
		return ErrParameterNotFound
	}
	delete(s.params, name)
	delete(s.history, name)
	return nil
}

func (s *MemoryParameterStore) ListParameters(_ context.Context, path string, recursive bool) ([]ParameterEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ParameterEntry
	for name, e := range s.params {
		if path == "" {
			out = append(out, e)
			continue
		}
		// Normalise path to always end with "/" so "/app" doesn't match "/apple/x".
		prefix := path
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		if recursive {
			if strings.HasPrefix(name, prefix) {
				out = append(out, e)
			}
		} else {
			// Non-recursive: only direct children (one level below path).
			if strings.HasPrefix(name, prefix) {
				rest := strings.TrimPrefix(name, prefix)
				if rest != "" && !strings.Contains(rest, "/") {
					out = append(out, e)
				}
			}
		}
	}
	return out, nil
}

func (s *MemoryParameterStore) GetParameterHistory(_ context.Context, name string) ([]HistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.params[name]; !ok {
		return nil, ErrParameterNotFound
	}
	h := s.history[name]
	out := make([]HistoryEntry, len(h))
	copy(out, h)
	return out, nil
}

func (s *MemoryParameterStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params = make(map[string]ParameterEntry)
	s.history = make(map[string][]HistoryEntry)
}
