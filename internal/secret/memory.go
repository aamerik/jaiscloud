package secret

import (
	"context"
	"sync"
	"time"
)

// MemorySecretStore is an in-process SecretStore used in lite mode.
type MemorySecretStore struct {
	mu       sync.RWMutex
	secrets  map[string]SecretEntry  // keyed by secretID
	byName   map[string]string        // name → secretID
	versions map[string][]VersionEntry // secretID → versions
}

func NewMemorySecretStore() *MemorySecretStore {
	return &MemorySecretStore{
		secrets:  make(map[string]SecretEntry),
		byName:   make(map[string]string),
		versions: make(map[string][]VersionEntry),
	}
}

func (s *MemorySecretStore) CreateSecret(_ context.Context, e SecretEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byName[e.Name]; ok {
		return ErrAlreadyExists
	}
	now := time.Now()
	e.CreatedAt = now
	e.UpdatedAt = now
	s.secrets[e.SecretID] = e
	s.byName[e.Name] = e.SecretID
	return nil
}

func (s *MemorySecretStore) GetSecret(_ context.Context, secretID string) (SecretEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.secrets[secretID]
	if !ok {
		return SecretEntry{}, ErrSecretNotFound
	}
	return e, nil
}

func (s *MemorySecretStore) GetSecretByName(_ context.Context, name string) (SecretEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byName[name]
	if !ok {
		return SecretEntry{}, ErrSecretNotFound
	}
	return s.secrets[id], nil
}

func (s *MemorySecretStore) UpdateSecret(_ context.Context, e SecretEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.secrets[e.SecretID]; !ok {
		return ErrSecretNotFound
	}
	e.UpdatedAt = time.Now()
	s.secrets[e.SecretID] = e
	return nil
}

func (s *MemorySecretStore) DeleteSecret(_ context.Context, secretID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.secrets[secretID]
	if !ok {
		return ErrSecretNotFound
	}
	delete(s.byName, e.Name)
	delete(s.secrets, secretID)
	delete(s.versions, secretID)
	return nil
}

func (s *MemorySecretStore) ListSecrets(_ context.Context) ([]SecretEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SecretEntry, 0, len(s.secrets))
	for _, e := range s.secrets {
		out = append(out, e)
	}
	return out, nil
}

func (s *MemorySecretStore) PutVersion(_ context.Context, v VersionEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now()
	}
	// Only demote the old AWSCURRENT→AWSPREVIOUS when this version claims AWSCURRENT.
	if containsStage(v.Stages, "AWSCURRENT") {
		for i, vv := range s.versions[v.SecretID] {
			if containsStage(vv.Stages, "AWSCURRENT") && vv.VersionID != v.VersionID {
				newStages := removeStage(vv.Stages, "AWSCURRENT")
				if !containsStage(newStages, "AWSPREVIOUS") {
					newStages = append(newStages, "AWSPREVIOUS")
				}
				// Remove AWSPREVIOUS from any other version before assigning it here.
				for j, other := range s.versions[v.SecretID] {
					if other.VersionID != vv.VersionID && containsStage(other.Stages, "AWSPREVIOUS") {
						s.versions[v.SecretID][j].Stages = removeStage(other.Stages, "AWSPREVIOUS")
					}
				}
				s.versions[v.SecretID][i].Stages = newStages
			}
		}
	}
	// Replace or append.
	for i, vv := range s.versions[v.SecretID] {
		if vv.VersionID == v.VersionID {
			s.versions[v.SecretID][i] = v
			return nil
		}
	}
	s.versions[v.SecretID] = append(s.versions[v.SecretID], v)
	return nil
}

func (s *MemorySecretStore) GetVersion(_ context.Context, secretID, versionID string) (VersionEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.versions[secretID] {
		if v.VersionID == versionID {
			return v, nil
		}
	}
	return VersionEntry{}, ErrVersionNotFound
}

func (s *MemorySecretStore) GetVersionByStage(_ context.Context, secretID, stage string) (VersionEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.versions[secretID] {
		if containsStage(v.Stages, stage) {
			return v, nil
		}
	}
	return VersionEntry{}, ErrVersionNotFound
}

func (s *MemorySecretStore) ListVersions(_ context.Context, secretID string) ([]VersionEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions := s.versions[secretID]
	out := make([]VersionEntry, len(versions))
	copy(out, versions)
	return out, nil
}

func (s *MemorySecretStore) UpdateVersionStages(_ context.Context, secretID, versionID string, stages []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, v := range s.versions[secretID] {
		if v.VersionID == versionID {
			s.versions[secretID][i].Stages = stages
			return nil
		}
	}
	return ErrVersionNotFound
}

func (s *MemorySecretStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets = make(map[string]SecretEntry)
	s.byName = make(map[string]string)
	s.versions = make(map[string][]VersionEntry)
}

func containsStage(stages []string, stage string) bool {
	for _, s := range stages {
		if s == stage {
			return true
		}
	}
	return false
}

func removeStage(stages []string, stage string) []string {
	out := make([]string, 0, len(stages))
	for _, s := range stages {
		if s != stage {
			out = append(out, s)
		}
	}
	return out
}

func replaceStage(stages []string, old, newStage string) []string {
	out := make([]string, 0, len(stages))
	found := false
	for _, s := range stages {
		if s == old {
			out = append(out, newStage)
			found = true
		} else {
			out = append(out, s)
		}
	}
	if !found {
		out = append(out, newStage)
	}
	return out
}
