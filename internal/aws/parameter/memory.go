package parameter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// labelKey is the composite key for the labels map: "name\x00version".
func labelKey(name string, version int64) string {
	return fmt.Sprintf("%s\x00%d", name, version)
}

// MemoryParameterStore is an in-process ParameterStore used in lite mode.
type MemoryParameterStore struct {
	mu      sync.RWMutex
	params  map[string]ParameterEntry
	history map[string][]HistoryEntry
	// labels maps labelKey(name, version) → set of label strings.
	labels map[string]map[string]struct{}
}

func NewMemoryParameterStore() *MemoryParameterStore {
	return &MemoryParameterStore{
		params:  make(map[string]ParameterEntry),
		history: make(map[string][]HistoryEntry),
		labels:  make(map[string]map[string]struct{}),
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

func (s *MemoryParameterStore) LabelParameterVersion(_ context.Context, name string, version int64, newLabels []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.params[name]; !ok {
		return nil, ErrParameterNotFound
	}
	key := labelKey(name, version)
	if s.labels[key] == nil {
		s.labels[key] = make(map[string]struct{})
	}
	// A label can only be applied to one version at a time — remove from others.
	for k, set := range s.labels {
		if strings.HasPrefix(k, name+"\x00") && k != key {
			for _, lbl := range newLabels {
				delete(set, lbl)
			}
		}
	}
	var invalid []string
	for _, lbl := range newLabels {
		if lbl == "" {
			invalid = append(invalid, lbl)
			continue
		}
		s.labels[key][lbl] = struct{}{}
	}
	return invalid, nil
}

func (s *MemoryParameterStore) UnlabelParameterVersion(_ context.Context, name string, version int64, remove []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := labelKey(name, version)
	set := s.labels[key]
	for _, lbl := range remove {
		delete(set, lbl)
	}
	return nil
}

func (s *MemoryParameterStore) GetLabelsByVersion(_ context.Context, name string, version int64) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := s.labels[labelKey(name, version)]
	out := make([]string, 0, len(set))
	for lbl := range set {
		out = append(out, lbl)
	}
	return out, nil
}

func (s *MemoryParameterStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params = make(map[string]ParameterEntry)
	s.history = make(map[string][]HistoryEntry)
	s.labels = make(map[string]map[string]struct{})
}

func (s *MemoryParameterStore) Snapshot() (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Serialise labels as map[key][]string.
	lblSnap := make(map[string][]string, len(s.labels))
	for k, set := range s.labels {
		lbls := make([]string, 0, len(set))
		for lbl := range set {
			lbls = append(lbls, lbl)
		}
		lblSnap[k] = lbls
	}
	snap := struct {
		Params  map[string]ParameterEntry  `json:"params"`
		History map[string][]HistoryEntry  `json:"history"`
		Labels  map[string][]string        `json:"labels"`
	}{s.params, s.history, lblSnap}
	return json.Marshal(snap)
}

func (s *MemoryParameterStore) Restore(raw json.RawMessage) error {
	var snap struct {
		Params  map[string]ParameterEntry  `json:"params"`
		History map[string][]HistoryEntry  `json:"history"`
		Labels  map[string][]string        `json:"labels"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
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
