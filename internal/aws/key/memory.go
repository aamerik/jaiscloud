package key

import (
	"context"
	"encoding/json"
	"sync"
)

// MemoryKeyStore is an in-process KeyStore used in lite mode.
// All state is lost on restart.
type MemoryKeyStore struct {
	mu      sync.RWMutex
	keys    map[string]KeyEntry
	aliases map[string]AliasEntry
	grants  map[string]GrantEntry
	dek     []byte // raw blob
}

func NewMemoryKeyStore() *MemoryKeyStore {
	return &MemoryKeyStore{
		keys:    make(map[string]KeyEntry),
		aliases: make(map[string]AliasEntry),
		grants:  make(map[string]GrantEntry),
	}
}

func (s *MemoryKeyStore) CreateKey(_ context.Context, e KeyEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[e.KeyID]; ok {
		return ErrAlreadyExists
	}
	s.keys[e.KeyID] = e
	return nil
}

func (s *MemoryKeyStore) GetKey(_ context.Context, keyID string) (KeyEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.keys[keyID]
	if !ok {
		return KeyEntry{}, ErrKeyNotFound
	}
	return e, nil
}

func (s *MemoryKeyStore) UpdateKey(_ context.Context, e KeyEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[e.KeyID]; !ok {
		return ErrKeyNotFound
	}
	s.keys[e.KeyID] = e
	return nil
}

func (s *MemoryKeyStore) DeleteKey(_ context.Context, keyID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[keyID]; !ok {
		return ErrKeyNotFound
	}
	delete(s.keys, keyID)
	// cascade aliases and grants
	for name, a := range s.aliases {
		if a.TargetKeyID == keyID {
			delete(s.aliases, name)
		}
	}
	for id, g := range s.grants {
		if g.KeyID == keyID {
			delete(s.grants, id)
		}
	}
	return nil
}

func (s *MemoryKeyStore) ListKeys(_ context.Context) ([]KeyEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]KeyEntry, 0, len(s.keys))
	for _, e := range s.keys {
		out = append(out, e)
	}
	return out, nil
}

func (s *MemoryKeyStore) CreateAlias(_ context.Context, e AliasEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.aliases[e.AliasName]; ok {
		return ErrAlreadyExists
	}
	s.aliases[e.AliasName] = e
	return nil
}

func (s *MemoryKeyStore) GetAlias(_ context.Context, name string) (AliasEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.aliases[name]
	if !ok {
		return AliasEntry{}, ErrAliasNotFound
	}
	return e, nil
}

func (s *MemoryKeyStore) DeleteAlias(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.aliases[name]; !ok {
		return ErrAliasNotFound
	}
	delete(s.aliases, name)
	return nil
}

func (s *MemoryKeyStore) ListAliases(_ context.Context, keyID string) ([]AliasEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []AliasEntry
	for _, a := range s.aliases {
		if keyID == "" || a.TargetKeyID == keyID {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *MemoryKeyStore) CreateGrant(_ context.Context, e GrantEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.grants[e.GrantID]; ok {
		return ErrAlreadyExists
	}
	s.grants[e.GrantID] = e
	return nil
}

func (s *MemoryKeyStore) GetGrant(_ context.Context, grantID string) (GrantEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.grants[grantID]
	if !ok {
		return GrantEntry{}, ErrGrantNotFound
	}
	return e, nil
}

func (s *MemoryKeyStore) GetGrantByToken(_ context.Context, token string) (GrantEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, g := range s.grants {
		if g.Token == token {
			return g, nil
		}
	}
	return GrantEntry{}, ErrGrantNotFound
}

func (s *MemoryKeyStore) RevokeGrant(_ context.Context, grantID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.grants[grantID]; !ok {
		return ErrGrantNotFound
	}
	delete(s.grants, grantID)
	return nil
}

func (s *MemoryKeyStore) ListGrants(_ context.Context, keyID string) ([]GrantEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []GrantEntry
	for _, g := range s.grants {
		if keyID == "" || g.KeyID == keyID {
			out = append(out, g)
		}
	}
	return out, nil
}

func (s *MemoryKeyStore) LoadDEK(_ context.Context) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.dek == nil {
		return nil, ErrKeyNotFound
	}
	cp := make([]byte, len(s.dek))
	copy(cp, s.dek)
	return cp, nil
}

func (s *MemoryKeyStore) StoreDEK(_ context.Context, blob []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dek = make([]byte, len(blob))
	copy(s.dek, blob)
	return nil
}

func (s *MemoryKeyStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = make(map[string]KeyEntry)
	s.aliases = make(map[string]AliasEntry)
	s.grants = make(map[string]GrantEntry)
	s.dek = nil
}

func (s *MemoryKeyStore) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := struct {
		Keys    map[string]KeyEntry   `json:"keys"`
		Aliases map[string]AliasEntry `json:"aliases"`
		Grants  map[string]GrantEntry `json:"grants"`
		DEK     []byte                `json:"dek"`
	}{s.keys, s.aliases, s.grants, s.dek}
	return json.Marshal(snap)
}

func (s *MemoryKeyStore) Restore(raw json.RawMessage) error {
	var snap struct {
		Keys    map[string]KeyEntry   `json:"keys"`
		Aliases map[string]AliasEntry `json:"aliases"`
		Grants  map[string]GrantEntry `json:"grants"`
		DEK     []byte                `json:"dek"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Keys != nil {
		s.keys = snap.Keys
	}
	if snap.Aliases != nil {
		s.aliases = snap.Aliases
	}
	if snap.Grants != nil {
		s.grants = snap.Grants
	}
	s.dek = snap.DEK
	return nil
}
