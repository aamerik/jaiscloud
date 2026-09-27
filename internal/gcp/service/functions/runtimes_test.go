package functions

import (
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

func newRuntimeService() *Service {
	return NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

func TestListRuntimesCatalog(t *testing.T) {
	s := newRuntimeService()
	rs, err := s.ListRuntimes("proj", "us-central1", "")
	if err != nil {
		t.Fatalf("ListRuntimes: %v", err)
	}
	if len(rs) == 0 {
		t.Fatal("empty runtime catalog")
	}
	var found bool
	for _, rt := range rs {
		if rt.Name == "nodejs20" {
			found = true
			if rt.Environment != "GEN_2" || rt.Stage != "GA" {
				t.Fatalf("nodejs20 = %+v, want GEN_2/GA", rt)
			}
		}
	}
	if !found {
		t.Fatal("nodejs20 missing from catalog")
	}
}

func TestListRuntimesRequiresLocation(t *testing.T) {
	s := newRuntimeService()
	if _, err := s.ListRuntimes("proj", "", ""); err == nil {
		t.Fatal("expected InvalidArgument for missing location")
	}
}

func TestListRuntimesFilter(t *testing.T) {
	s := newRuntimeService()
	all, _ := s.ListRuntimes("proj", "us-central1", "")

	// environment filter
	gen2, err := s.ListRuntimes("proj", "us-central1", `environment="GEN_2"`)
	if err != nil {
		t.Fatalf("environment filter: %v", err)
	}
	if len(gen2) != len(all) {
		t.Fatalf("environment=GEN_2 returned %d, want %d", len(gen2), len(all))
	}

	// name filter
	one, err := s.ListRuntimes("proj", "us-central1", `name="nodejs20"`)
	if err != nil {
		t.Fatalf("name filter: %v", err)
	}
	if len(one) != 1 || one[0].DisplayName != "Node.js 20" {
		t.Fatalf("name filter = %+v", one)
	}

	// two clauses joined by AND
	both, err := s.ListRuntimes("proj", "us-central1", `environment="GEN_2" AND stage="GA"`)
	if err != nil {
		t.Fatalf("AND filter: %v", err)
	}
	if len(both) == 0 {
		t.Fatal("AND filter returned nothing")
	}
	for _, rt := range both {
		if rt.Environment != "GEN_2" || rt.Stage != "GA" {
			t.Fatalf("AND filter leaked %+v", rt)
		}
	}

	// a filter that matches nothing yields an empty (non-nil) slice
	none, err := s.ListRuntimes("proj", "us-central1", `name="does-not-exist"`)
	if err != nil {
		t.Fatalf("no-match filter: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no runtimes, got %d", len(none))
	}
}

func TestListRuntimesFilterErrors(t *testing.T) {
	s := newRuntimeService()
	for _, filter := range []string{`bogus="x"`, `name`, `name="x" AND`, `name="a" OR name="b"`, `name=="x"`} {
		if _, err := s.ListRuntimes("proj", "us-central1", filter); err == nil {
			t.Fatalf("filter %q: expected InvalidArgument", filter)
		}
	}
}

func TestRuntimeJSON(t *testing.T) {
	rt := Runtime{
		Name:            "nodejs18",
		DisplayName:     "Node.js 18",
		Stage:           "DEPRECATED",
		Environment:     "GEN_2",
		Warnings:        []string{"deprecated"},
		DeprecationDate: map[string]any{"year": 2025, "month": 4, "day": 30},
	}
	m := RuntimeJSON(rt)
	if m["name"] != "nodejs18" || m["displayName"] != "Node.js 18" ||
		m["stage"] != "DEPRECATED" || m["environment"] != "GEN_2" {
		t.Fatalf("unexpected runtime JSON: %v", m)
	}
	if w, _ := m["warnings"].([]string); len(w) != 1 {
		t.Fatalf("warnings = %v", m["warnings"])
	}
	if d, _ := m["deprecationDate"].(map[string]any); d["year"] != 2025 {
		t.Fatalf("deprecationDate = %v", m["deprecationDate"])
	}
	if _, ok := m["decommissionDate"]; ok {
		t.Fatalf("decommissionDate should be omitted when nil")
	}
}
