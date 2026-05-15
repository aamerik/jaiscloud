package stack

import (
	"fmt"
	"sync"
)

// ExportTable stores cross-stack exports for Fn::ImportValue resolution.
type ExportTable struct {
	mu      sync.RWMutex
	entries map[string]*exportEntry
}

type exportEntry struct {
	Value          string
	ExportingStack string
	importers      map[string]bool
}

func NewExportTable() *ExportTable {
	return &ExportTable{entries: make(map[string]*exportEntry)}
}

func (t *ExportTable) Register(name, value, stackARN string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries[name] = &exportEntry{Value: value, ExportingStack: stackARN, importers: make(map[string]bool)}
}

func (t *ExportTable) Get(name string) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e, ok := t.entries[name]; ok {
		return e.Value, true
	}
	return "", false
}

func (t *ExportTable) AddImporter(exportName, importerARN string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[exportName]; ok {
		e.importers[importerARN] = true
	}
}

func (t *ExportTable) DeleteStack(stackARN string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for name, e := range t.entries {
		if e.ExportingStack != stackARN {
			continue
		}
		if len(e.importers) > 0 {
			return fmt.Errorf("ExportInUseException: export %q is imported by another stack", name)
		}
		delete(t.entries, name)
	}
	return nil
}

func (t *ExportTable) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = make(map[string]*exportEntry)
}
