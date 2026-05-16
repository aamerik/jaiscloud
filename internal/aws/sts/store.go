package sts

import (
	"encoding/json"
	"sync"
)

// Tag holds a single key/value pair with case-preserved key.
type Tag struct {
	Key   string
	Value string
}

// SessionConfig captures tagging context for an issued session credential.
type SessionConfig struct {
	// lowercase key → Tag{case-preserved key, value}
	Tags map[string]Tag `json:"tags"`
	// lowercase transitive tag keys that propagate to child assume-role calls
	TransitiveTags []string `json:"transitive_tags"`
	// IAMContext reserved for future use
	IAMContext map[string]any `json:"iam_context,omitempty"`
}

// SessionStore persists session configs keyed by AccessKeyId.
type SessionStore interface {
	StoreSession(accessKeyID string, cfg SessionConfig) error
	GetSession(accessKeyID string) (SessionConfig, bool)
	DeleteSession(accessKeyID string)

	Reset()
	Snapshot() (json.RawMessage, error)
	Restore(data json.RawMessage) error
}

// MemorySessionStore is an in-memory SessionStore.
type MemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]SessionConfig
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: make(map[string]SessionConfig)}
}

func (s *MemorySessionStore) StoreSession(accessKeyID string, cfg SessionConfig) error {
	s.mu.Lock()
	s.sessions[accessKeyID] = cfg
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionStore) GetSession(accessKeyID string) (SessionConfig, bool) {
	s.mu.RLock()
	cfg, ok := s.sessions[accessKeyID]
	s.mu.RUnlock()
	return cfg, ok
}

func (s *MemorySessionStore) DeleteSession(accessKeyID string) {
	s.mu.Lock()
	delete(s.sessions, accessKeyID)
	s.mu.Unlock()
}

func (s *MemorySessionStore) Reset() {
	s.mu.Lock()
	s.sessions = make(map[string]SessionConfig)
	s.mu.Unlock()
}

func (s *MemorySessionStore) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.sessions)
}

func (s *MemorySessionStore) Restore(data json.RawMessage) error {
	var m map[string]SessionConfig
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	s.mu.Lock()
	s.sessions = m
	s.mu.Unlock()
	return nil
}
