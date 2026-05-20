package secret

import (
	"context"
	"encoding/json"
	"io"
	"sort"
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

func secretNameKey(accountID, name string) string { return accountID + ":" + name }

func (s *MemorySecretStore) CreateSecret(_ context.Context, e SecretEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := secretNameKey(e.AccountID, e.Name)
	if _, ok := s.byName[key]; ok {
		return ErrAlreadyExists
	}
	now := time.Now()
	e.CreatedAt = now
	e.UpdatedAt = now
	s.secrets[e.SecretID] = e
	s.byName[key] = e.SecretID
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

func (s *MemorySecretStore) GetSecretByName(_ context.Context, accountID, name string) (SecretEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byName[secretNameKey(accountID, name)]
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
	delete(s.byName, secretNameKey(e.AccountID, e.Name))
	delete(s.secrets, secretID)
	delete(s.versions, secretID)
	return nil
}

func (s *MemorySecretStore) ListSecrets(_ context.Context, accountID string) ([]SecretEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SecretEntry, 0, len(s.secrets))
	for _, e := range s.secrets {
		if accountID != "" && e.AccountID != accountID {
			continue
		}
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

	// 3.7.2 — prune unlabeled versions (empty Stages) beyond 100.
	s.pruneUnlabeledVersions(v.SecretID)

	return nil
}

// pruneUnlabeledVersions deletes the oldest unlabeled versions (Stages == nil/empty)
// when there are more than 100 of them. Must be called with s.mu held.
func (s *MemorySecretStore) pruneUnlabeledVersions(secretID string) {
	const maxUnlabeled = 100
	all := s.versions[secretID]

	// Collect unlabeled versions sorted oldest-first.
	var unlabeled []VersionEntry
	for _, v := range all {
		if len(v.Stages) == 0 {
			unlabeled = append(unlabeled, v)
		}
	}
	if len(unlabeled) <= maxUnlabeled {
		return
	}
	sort.Slice(unlabeled, func(i, j int) bool {
		return unlabeled[i].CreatedAt.Before(unlabeled[j].CreatedAt)
	})
	// Mark the oldest excess for deletion.
	excess := len(unlabeled) - maxUnlabeled
	toDelete := make(map[string]struct{}, excess)
	for i := 0; i < excess; i++ {
		toDelete[unlabeled[i].VersionID] = struct{}{}
	}
	kept := all[:0]
	for _, v := range all {
		if _, remove := toDelete[v.VersionID]; !remove {
			kept = append(kept, v)
		}
	}
	s.versions[secretID] = kept
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

func (s *MemorySecretStore) DeleteVersionsByIDs(_ context.Context, secretID string, versionIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	del := make(map[string]struct{}, len(versionIDs))
	for _, id := range versionIDs {
		del[id] = struct{}{}
	}
	cur := s.versions[secretID]
	kept := cur[:0]
	for _, v := range cur {
		if _, remove := del[v.VersionID]; !remove {
			kept = append(kept, v)
		}
	}
	s.versions[secretID] = kept
	return nil
}

func (s *MemorySecretStore) Reset(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets = make(map[string]SecretEntry)
	s.byName = make(map[string]string)
	s.versions = make(map[string][]VersionEntry)
}

func (s *MemorySecretStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.secrets) == 0, nil
}

func (s *MemorySecretStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := struct {
		Secrets  map[string]SecretEntry    `json:"secrets"`
		ByName   map[string]string         `json:"by_name"`
		Versions map[string][]VersionEntry `json:"versions"`
	}{s.secrets, s.byName, s.versions}
	return json.NewEncoder(w).Encode(snap)
}

func (s *MemorySecretStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Secrets  map[string]SecretEntry    `json:"secrets"`
		Versions map[string][]VersionEntry `json:"versions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Secrets != nil {
		s.secrets = snap.Secrets
		// Rebuild byName index with account-scoped keys.
		s.byName = make(map[string]string, len(snap.Secrets))
		for _, e := range snap.Secrets {
			s.byName[secretNameKey(e.AccountID, e.Name)] = e.SecretID
		}
	}
	if snap.Versions != nil {
		s.versions = snap.Versions
	}
	return nil
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
