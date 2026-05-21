package parameter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// paramKey returns the account-scoped map key for a parameter.
func paramKey(accountID, name string) string { return accountID + ":" + name }

// labelKey is the composite key for the labels map: "account:name\x00version".
func labelKey(accountID, name string, version int64) string {
	return fmt.Sprintf("%s:%s\x00%d", accountID, name, version)
}

// MemoryParameterStore is an in-process ParameterStore used in memory mode.
type MemoryParameterStore struct {
	mu      sync.RWMutex
	params  map[string]ParameterEntry
	history map[string][]HistoryEntry
	// labels maps labelKey(accountID, name, version) → set of label strings.
	labels map[string]map[string]struct{}
}

func NewMemoryParameterStore() *MemoryParameterStore {
	return &MemoryParameterStore{
		params:  make(map[string]ParameterEntry),
		history: make(map[string][]HistoryEntry),
		labels:  make(map[string]map[string]struct{}),
	}
}

func (s *MemoryParameterStore) PutParameter(_ context.Context, e *ParameterEntry, overwrite bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	pk := paramKey(e.AccountID, e.Name)
	if existing, ok := s.params[pk]; ok {
		if !overwrite {
			return ErrAlreadyExists
		}
		hk := paramKey(e.AccountID, e.Name)
		s.history[hk] = append(s.history[hk], HistoryEntry{
			Name:      existing.Name,
			Version:   existing.Version,
			Type:      existing.Type,
			KMSKeyID:  existing.KMSKeyID,
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
	s.params[pk] = *e
	return nil
}

func (s *MemoryParameterStore) GetParameter(_ context.Context, accountID, name string) (ParameterEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.params[paramKey(accountID, name)]
	if !ok {
		return ParameterEntry{}, ErrParameterNotFound
	}
	return e, nil
}

func (s *MemoryParameterStore) DeleteParameter(_ context.Context, accountID, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pk := paramKey(accountID, name)
	if _, ok := s.params[pk]; !ok {
		return ErrParameterNotFound
	}
	delete(s.params, pk)
	delete(s.history, pk)
	return nil
}

func (s *MemoryParameterStore) ListParameters(_ context.Context, accountID, path string, recursive bool) ([]ParameterEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	acctPrefix := accountID + ":"
	var out []ParameterEntry
	for pk, e := range s.params {
		if accountID != "" && !strings.HasPrefix(pk, acctPrefix) {
			continue
		}
		name := e.Name
		if path == "" {
			out = append(out, e)
			continue
		}
		prefix := path
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		if recursive {
			if strings.HasPrefix(name, prefix) {
				out = append(out, e)
			}
		} else {
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

func (s *MemoryParameterStore) GetParameterHistory(_ context.Context, accountID, name string) ([]HistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pk := paramKey(accountID, name)
	if _, ok := s.params[pk]; !ok {
		return nil, ErrParameterNotFound
	}
	h := s.history[pk]
	out := make([]HistoryEntry, len(h))
	copy(out, h)
	return out, nil
}

func (s *MemoryParameterStore) LabelParameterVersion(_ context.Context, accountID, name string, version int64, newLabels []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.params[paramKey(accountID, name)]; !ok {
		return nil, ErrParameterNotFound
	}
	key := labelKey(accountID, name, version)
	if s.labels[key] == nil {
		s.labels[key] = make(map[string]struct{})
	}
	// A label can only be applied to one version at a time — remove from others.
	scopedPrefix := accountID + ":" + name + "\x00"
	for k, set := range s.labels {
		if strings.HasPrefix(k, scopedPrefix) && k != key {
			for _, lbl := range newLabels {
				delete(set, lbl)
			}
		}
	}
	var invalid []string
	for _, lbl := range newLabels {
		if lbl == "" || strings.HasPrefix(lbl, "aws") || strings.HasPrefix(lbl, "ssm") {
			invalid = append(invalid, lbl)
			continue
		}
		s.labels[key][lbl] = struct{}{}
	}
	return invalid, nil
}

func (s *MemoryParameterStore) UnlabelParameterVersion(_ context.Context, accountID, name string, version int64, remove []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := labelKey(accountID, name, version)
	set := s.labels[key]
	for _, lbl := range remove {
		delete(set, lbl)
	}
	return nil
}

func (s *MemoryParameterStore) GetLabelsByVersion(_ context.Context, accountID, name string, version int64) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.labels[labelKey(accountID, name, version)]
	out := make([]string, 0, len(set))
	for lbl := range set {
		out = append(out, lbl)
	}
	return out, nil
}

func (s *MemoryParameterStore) Reset(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params = make(map[string]ParameterEntry)
	s.history = make(map[string][]HistoryEntry)
	s.labels = make(map[string]map[string]struct{})
}

func (s *MemoryParameterStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.params) == 0, nil
}

func (s *MemoryParameterStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lblSnap := make(map[string][]string, len(s.labels))
	for k, set := range s.labels {
		lbls := make([]string, 0, len(set))
		for lbl := range set {
			lbls = append(lbls, lbl)
		}
		lblSnap[k] = lbls
	}
	snap := struct {
		Params  map[string]ParameterEntry `json:"params"`
		History map[string][]HistoryEntry `json:"history"`
		Labels  map[string][]string       `json:"labels"`
	}{s.params, s.history, lblSnap}
	return json.NewEncoder(w).Encode(snap)
}

func (s *MemoryParameterStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Params  map[string]ParameterEntry `json:"params"`
		History map[string][]HistoryEntry `json:"history"`
		Labels  map[string][]string       `json:"labels"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Params != nil {
		s.params = snap.Params
	}
	if snap.History != nil {
		s.history = snap.History
	}
	s.labels = make(map[string]map[string]struct{})
	for k, lbls := range snap.Labels {
		set := make(map[string]struct{}, len(lbls))
		for _, lbl := range lbls {
			set[lbl] = struct{}{}
		}
		s.labels[k] = set
	}
	return nil
}
