package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/store"
)

// observedCount returns how many observed records the store holds.
func observedCount(t *testing.T, res store.ResourceStore) int {
	t.Helper()
	entries, err := res.List(context.Background(), "", "", rtObservedProject, "")
	if err != nil {
		t.Fatalf("list observed: %v", err)
	}
	return len(entries)
}

func TestObserveAccountRecordsProjectAndService(t *testing.T) {
	s, _ := newRegistryService()
	ctx := context.Background()

	s.ObserveAccount(ctx, "acme-e2e", "storage")
	s.ObserveAccount(ctx, "acme-e2e", "pubsub")
	s.ObserveAccount(ctx, "acme-e2e", "storage") // same service, counted once

	got := s.ListObservedProjects(ctx)
	if len(got) != 1 {
		t.Fatalf("observed = %d projects, want 1", len(got))
	}
	p := got[0]
	if p.ProjectID != "acme-e2e" {
		t.Fatalf("project = %q, want acme-e2e", p.ProjectID)
	}
	if p.Requests != 3 {
		t.Fatalf("requests = %d, want 3", p.Requests)
	}
	if len(p.Services) != 2 || p.Services[0] != "pubsub" || p.Services[1] != "storage" {
		t.Fatalf("services = %v, want sorted [pubsub storage]", p.Services)
	}
	if p.FirstSeen.IsZero() || p.LastSeen.IsZero() || p.LastSeen.Before(p.FirstSeen) {
		t.Fatalf("timestamps = %v..%v", p.FirstSeen, p.LastSeen)
	}
}

func TestObserveAccountSkipsEmptyAndConfiguredAndRegistered(t *testing.T) {
	s, _ := newRegistryService()
	ctx := context.Background()

	s.ObserveAccount(ctx, "", "storage")
	if got := s.ListObservedProjects(ctx); len(got) != 0 {
		t.Fatalf("empty account observed = %v, want none", got)
	}

	// A configured project always exists, so it is not "observed".
	s.ObserveAccount(ctx, "default-proj", "storage")
	s.ObserveAccount(ctx, "extra-proj1", "storage")
	// A project created through Resource Manager is registered.
	if _, _, err := s.CreateProject(ctx, CreateProjectInput{ProjectID: "created-proj"}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	s.ObserveAccount(ctx, "created-proj", "storage")

	if got := s.ListObservedProjects(ctx); len(got) != 0 {
		t.Fatalf("observed = %v, want none (configured/registered excluded)", got)
	}
}

func TestObserveAccountPersistsAcrossInstances(t *testing.T) {
	ctx := context.Background()
	res := store.NewMemoryResourceStore()
	first := NewService(res, WithKnownProjects("default-proj", nil))
	first.ObserveAccount(ctx, "acme-e2e", "storage")

	// A fresh core over the same store (a restart, or another instance on the
	// same --dsn) sees the record.
	second := NewService(res, WithKnownProjects("default-proj", nil))
	got := second.ListObservedProjects(ctx)
	if len(got) != 1 || got[0].ProjectID != "acme-e2e" {
		t.Fatalf("observed after restart = %v, want [acme-e2e]", got)
	}
}

func TestResetClearsObserved(t *testing.T) {
	ctx := context.Background()
	res := store.NewMemoryResourceStore()
	s := NewService(res, WithKnownProjects("default-proj", nil))
	s.ObserveAccount(ctx, "acme-e2e", "storage")
	if observedCount(t, res) != 1 {
		t.Fatalf("store records = %d, want 1", observedCount(t, res))
	}

	s.Reset(ctx)

	if got := s.ListObservedProjects(ctx); len(got) != 0 {
		t.Fatalf("observed after reset = %v, want none", got)
	}
	if observedCount(t, res) != 0 {
		t.Fatalf("store records after reset = %d, want 0", observedCount(t, res))
	}
}

func TestObserveAccountCapsRegistry(t *testing.T) {
	ctx := context.Background()
	res := store.NewMemoryResourceStore()
	s := NewService(res, WithKnownProjects("default-proj", nil))

	for i := 0; i < observedCap+5; i++ {
		s.ObserveAccount(ctx, projectIDForIndex(i), "storage")
	}

	got := s.ListObservedProjects(ctx)
	if len(got) != observedCap {
		t.Fatalf("observed = %d, want cap %d", len(got), observedCap)
	}
	if n := observedCount(t, res); n != observedCap {
		t.Fatalf("store records = %d, want cap %d", n, observedCap)
	}
	// The oldest entries are the ones evicted.
	for _, p := range got {
		if p.ProjectID == projectIDForIndex(0) || p.ProjectID == projectIDForIndex(4) {
			t.Fatalf("oldest entry %q survived eviction", p.ProjectID)
		}
	}
}

func TestListObservedProjectsSortsByLastSeen(t *testing.T) {
	ctx := context.Background()
	s, _ := newRegistryService()
	s.ObserveAccount(ctx, "older-proj", "storage")
	s.ObserveAccount(ctx, "newer-proj", "pubsub")

	got := s.ListObservedProjects(ctx)
	if len(got) != 2 {
		t.Fatalf("observed = %d, want 2", len(got))
	}
	if got[0].ProjectID != "newer-proj" {
		t.Fatalf("order = %v, want newest first", []string{got[0].ProjectID, got[1].ProjectID})
	}
}

// projectIDForIndex builds a distinct, grammar-valid id per index.
func projectIDForIndex(i int) string {
	return "observed-proj-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
}

// errStore wraps a real store and fails selected operations, so the registry's
// error paths (which must never fail a request) are covered.
type errStore struct {
	store.ResourceStore
	failList   bool
	failUpsert bool
	failDelete bool
}

var errInjected = errors.New("injected store failure")

func (e *errStore) List(ctx context.Context, account, region, resourceType, prefix string) ([]store.ResourceEntry, error) {
	if e.failList {
		return nil, errInjected
	}
	return e.ResourceStore.List(ctx, account, region, resourceType, prefix)
}

func (e *errStore) Upsert(ctx context.Context, account, region string, entry store.ResourceEntry) error {
	if e.failUpsert {
		return errInjected
	}
	return e.ResourceStore.Upsert(ctx, account, region, entry)
}

func (e *errStore) Delete(ctx context.Context, account, region, resourceType, id string) error {
	if e.failDelete {
		return errInjected
	}
	return e.ResourceStore.Delete(ctx, account, region, resourceType, id)
}

// TestObservedWithoutStore covers the no-store guard: observation, listing and
// reset must all stay inert instead of panicking.
func TestObservedWithoutStore(t *testing.T) {
	ctx := context.Background()
	s := NewService(nil, WithKnownProjects("default-proj", nil))

	s.ObserveAccount(ctx, "acme-e2e", "storage")
	got := s.ListObservedProjects(ctx)
	if len(got) != 1 || got[0].ProjectID != "acme-e2e" {
		t.Fatalf("observed = %v, want [acme-e2e]", got)
	}
	if err := s.ResetObserved(ctx); err != nil {
		t.Fatalf("reset without store: %v", err)
	}
	if got := s.ListObservedProjects(ctx); len(got) != 0 {
		t.Fatalf("observed after reset = %v, want none", got)
	}
}

// TestObservedListErrorRetries covers a store listing failure: the read is
// swallowed (the console must not fail over it) and the next call retries.
func TestObservedListErrorRetries(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryResourceStore()
	seed := NewService(mem, WithKnownProjects("default-proj", nil))
	seed.ObserveAccount(ctx, "acme-e2e", "storage")

	broken := &errStore{ResourceStore: mem, failList: true}
	s := NewService(broken, WithKnownProjects("default-proj", nil))
	if got := s.ListObservedProjects(ctx); len(got) != 0 {
		t.Fatalf("observed with failing store = %v, want none", got)
	}

	broken.failList = false
	got := s.ListObservedProjects(ctx)
	if len(got) != 1 || got[0].ProjectID != "acme-e2e" {
		t.Fatalf("observed after retry = %v, want [acme-e2e]", got)
	}
}

// TestObservedFlushErrorIsSwallowed covers a failing write: the in-memory
// registry still answers (discovery is best-effort), and nothing is persisted.
func TestObservedFlushErrorIsSwallowed(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryResourceStore()
	broken := &errStore{ResourceStore: mem, failUpsert: true}
	s := NewService(broken, WithKnownProjects("default-proj", nil))

	s.ObserveAccount(ctx, "acme-e2e", "storage")

	got := s.ListObservedProjects(ctx)
	if len(got) != 1 || got[0].ProjectID != "acme-e2e" {
		t.Fatalf("observed = %v, want the in-memory entry", got)
	}
	if n := observedCount(t, mem); n != 0 {
		t.Fatalf("store records = %d, want 0 (flush failed)", n)
	}
}

// TestResetObservedSurfacesStoreError covers a failing delete during reset.
func TestResetObservedSurfacesStoreError(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryResourceStore()
	seed := NewService(mem, WithKnownProjects("default-proj", nil))
	seed.ObserveAccount(ctx, "acme-e2e", "storage")

	broken := &errStore{ResourceStore: mem, failDelete: true}
	s := NewService(broken, WithKnownProjects("default-proj", nil))
	if err := s.ResetObserved(ctx); !errors.Is(err, errInjected) {
		t.Fatalf("reset err = %v, want the injected store error", err)
	}
	// The in-memory registry is cleared regardless.
	if got := s.ListObservedProjects(ctx); len(got) != 0 {
		t.Fatalf("observed after failed reset = %v, want none", got)
	}
}

// TestObservedEvictionIgnoresDeleteError covers eviction when the store copy
// cannot be deleted: the cap still holds in memory.
func TestObservedEvictionIgnoresDeleteError(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryResourceStore()
	broken := &errStore{ResourceStore: mem, failDelete: true}
	s := NewService(broken, WithKnownProjects("default-proj", nil))

	for i := 0; i < observedCap+3; i++ {
		s.ObserveAccount(ctx, projectIDForIndex(i), "storage")
	}
	if got := len(s.ListObservedProjects(ctx)); got != observedCap {
		t.Fatalf("observed = %d, want cap %d", got, observedCap)
	}
}
