package store

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// MemoryResourceStore is an in-memory ResourceStore backed by a sync.RWMutex map.
// Key: "type:id"
type MemoryResourceStore struct {
	mu      sync.RWMutex
	entries map[string]ResourceEntry
}

func NewMemoryResourceStore() *MemoryResourceStore {
	return &MemoryResourceStore{
		entries: make(map[string]ResourceEntry),
	}
}

func key(resourceType, id string) string {
	return resourceType + ":" + id
}

func (s *MemoryResourceStore) Create(ctx context.Context, entry ResourceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(entry.Type, entry.ID)
	if _, exists := s.entries[k]; exists {
		return ErrAlreadyExists
	}
	now := time.Now()
	entry.CreatedAt = now
	entry.UpdatedAt = now
	s.entries[k] = entry
	return nil
}

func (s *MemoryResourceStore) Get(ctx context.Context, resourceType, id string) (ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key(resourceType, id)]
	if !ok {
		return ResourceEntry{}, ErrNotFound
	}
	return e, nil
}

func (s *MemoryResourceStore) Update(ctx context.Context, entry ResourceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(entry.Type, entry.ID)
	existing, ok := s.entries[k]
	if !ok {
		return ErrNotFound
	}
	entry.CreatedAt = existing.CreatedAt
	entry.UpdatedAt = time.Now()
	s.entries[k] = entry
	return nil
}

func (s *MemoryResourceStore) Delete(ctx context.Context, resourceType, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(resourceType, id)
	if _, ok := s.entries[k]; !ok {
		return ErrNotFound
	}
	delete(s.entries, k)
	return nil
}

func (s *MemoryResourceStore) List(ctx context.Context, resourceType, prefix string) ([]ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var results []ResourceEntry
	typePrefix := resourceType + ":"
	for k, e := range s.entries {
		if !strings.HasPrefix(k, typePrefix) {
			continue
		}
		if prefix != "" && !strings.Contains(e.ID, prefix) {
			continue
		}
		results = append(results, e)
	}
	return results, nil
}

func (s *MemoryResourceStore) Purge(ctx context.Context, resourceType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	typePrefix := resourceType + ":"
	for k := range s.entries {
		if strings.HasPrefix(k, typePrefix) {
			delete(s.entries, k)
		}
	}
	return nil
}

func (s *MemoryResourceStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]ResourceEntry)
}

func (s *MemoryResourceStore) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.entries)
}

func (s *MemoryResourceStore) Restore(data json.RawMessage) error {
	var entries map[string]ResourceEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
	return nil
}
