package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MemoryResourceStore is an in-memory ResourceStore backed by a sync.RWMutex map.
// Key: "account:region:type:id" — global resource types (IAM, Route53) use region=GlobalRegion.
type MemoryResourceStore struct {
	mu      sync.RWMutex
	entries map[string]ResourceEntry
}

func NewMemoryResourceStore() *MemoryResourceStore {
	return &MemoryResourceStore{
		entries: make(map[string]ResourceEntry),
	}
}

func key(account, region, resourceType, id string) string {
	return account + ":" + region + ":" + resourceType + ":" + id
}

func (s *MemoryResourceStore) Create(ctx context.Context, account, region string, entry ResourceEntry) error {
	if region == "" {
		return fmt.Errorf("store: region must not be empty (type=%s id=%s); use store.GlobalRegion for global services", entry.Type, entry.ID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(account, region, entry.Type, entry.ID)
	if _, exists := s.entries[k]; exists {
		return ErrAlreadyExists
	}
	now := time.Now()
	entry.CreatedAt = now
	entry.UpdatedAt = now
	s.entries[k] = entry
	return nil
}

func (s *MemoryResourceStore) Upsert(ctx context.Context, account, region string, entry ResourceEntry) error {
	if region == "" {
		return fmt.Errorf("store: region must not be empty (type=%s id=%s); use store.GlobalRegion for global services", entry.Type, entry.ID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(account, region, entry.Type, entry.ID)
	now := time.Now()
	if existing, exists := s.entries[k]; exists {
		entry.CreatedAt = existing.CreatedAt
	} else {
		entry.CreatedAt = now
	}
	entry.UpdatedAt = now
	s.entries[k] = entry
	return nil
}

func (s *MemoryResourceStore) Get(ctx context.Context, account, region, resourceType, id string) (ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key(account, region, resourceType, id)]
	if !ok {
		return ResourceEntry{}, ErrNotFound
	}
	return e, nil
}

func (s *MemoryResourceStore) Update(ctx context.Context, account, region string, entry ResourceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(account, region, entry.Type, entry.ID)
	existing, ok := s.entries[k]
	if !ok {
		return ErrNotFound
	}
	entry.CreatedAt = existing.CreatedAt
	entry.UpdatedAt = time.Now()
	s.entries[k] = entry
	return nil
}

func (s *MemoryResourceStore) Delete(ctx context.Context, account, region, resourceType, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(account, region, resourceType, id)
	if _, ok := s.entries[k]; !ok {
		return ErrNotFound
	}
	delete(s.entries, k)
	return nil
}

func (s *MemoryResourceStore) List(ctx context.Context, account, region, resourceType, prefix string) ([]ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var results []ResourceEntry
	// When both account and region are "", scan across all scopes for this type.
	// Used by internal helpers (ESM poller, cross-service dispatchers) that don't
	// know the target account ahead of time.
	crossScope := account == "" && region == ""
	scopePrefix := account + ":" + region + ":" + resourceType + ":"
	for k, e := range s.entries {
		if crossScope {
			// Match any "account:region:type:" prefix — find the type segment.
			// Key is "account:region:type:id"; third colon-delimited segment is type.
			parts := strings.SplitN(k, ":", 4)
			if len(parts) < 4 || parts[2] != resourceType {
				continue
			}
			if prefix != "" && !strings.Contains(e.ID, prefix) {
				continue
			}
			// Populate Account/Region from key for cross-scope callers.
			entry := e
			entry.Account = parts[0]
			entry.Region = parts[1]
			results = append(results, entry)
			continue
		} else if !strings.HasPrefix(k, scopePrefix) {
			continue
		}
		if prefix != "" && !strings.Contains(e.ID, prefix) {
			continue
		}
		entry := e
		entry.Account = account
		entry.Region = region
		results = append(results, entry)
	}
	return results, nil
}

func (s *MemoryResourceStore) Purge(ctx context.Context, account, region, resourceType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	crossScope := account == "" && region == ""
	scopePrefix := account + ":" + region + ":" + resourceType + ":"
	for k := range s.entries {
		if crossScope {
			parts := strings.SplitN(k, ":", 4)
			if len(parts) >= 4 && parts[2] == resourceType {
				delete(s.entries, k)
			}
		} else if strings.HasPrefix(k, scopePrefix) {
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

// ResetScope deletes all entries for the given (account, region).
func (s *MemoryResourceStore) ResetScope(account, region string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := account + ":" + region + ":"
	for k := range s.entries {
		if strings.HasPrefix(k, prefix) {
			delete(s.entries, k)
		}
	}
}

// ResetAccount deletes all entries for the given account across all regions.
func (s *MemoryResourceStore) ResetAccount(account string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := account + ":"
	for k := range s.entries {
		if strings.HasPrefix(k, prefix) {
			delete(s.entries, k)
		}
	}
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
