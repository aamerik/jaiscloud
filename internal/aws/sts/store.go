package sts

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

// Tag holds a single key/value pair with case-preserved key.
type Tag struct {
	Key   string
	Value string
}

// SessionConfig captures tagging context and identity for an issued session credential.
type SessionConfig struct {
	// lowercase key → Tag{case-preserved key, value}
	Tags map[string]Tag `json:"tags"`
	// lowercase transitive tag keys that propagate to child assume-role calls
	TransitiveTags []string `json:"transitive_tags"`
	// IAMContext reserved for future use
	IAMContext map[string]any `json:"iam_context,omitempty"`

	// Identity fields added for multi-account support (MA-5a).
	Account         string `json:"account,omitempty"`           // 12-digit target account ID
	RoleName        string `json:"role_name,omitempty"`         // role assumed (without path)
	RoleSessionName string `json:"role_session_name,omitempty"` // session name from AssumeRole
}

// SessionStore persists session configs keyed by AccessKeyId.
type SessionStore interface {
	StoreSession(accessKeyID string, cfg SessionConfig) error
	GetSession(accessKeyID string) (SessionConfig, bool)
	DeleteSession(accessKeyID string)

	Reset(ctx context.Context)
	Snapshot(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
	IsEmpty(ctx context.Context) (bool, error)
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

func (s *MemorySessionStore) Reset(ctx context.Context) {
	s.mu.Lock()
	s.sessions = make(map[string]SessionConfig)
	s.mu.Unlock()
}

func (s *MemorySessionStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions) == 0, nil
}

func (s *MemorySessionStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(s.sessions)
}

func (s *MemorySessionStore) Restore(_ context.Context, r io.Reader) error {
	var m map[string]SessionConfig
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return err
	}
	s.mu.Lock()
	s.sessions = m
	s.mu.Unlock()
	return nil
}
