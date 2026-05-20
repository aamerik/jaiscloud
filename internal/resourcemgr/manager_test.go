package resourcemgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// ─── fake ResourceStore ───────────────────────────────────────────────────────

type fakeStore struct {
	mu      sync.RWMutex
	entries map[string]ResourceEntry // "type\x00id" → entry
}

func newFakeStore(seed ...ResourceEntry) *fakeStore {
	s := &fakeStore{entries: make(map[string]ResourceEntry)}
	for _, e := range seed {
		s.entries[e.Type+"\x00"+e.ID] = e
	}
	return s
}

func (s *fakeStore) key(t, id string) string { return t + "\x00" + id }

func (s *fakeStore) Exists(ctx context.Context, account, region, resourceType, resourceID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[s.key(resourceType, resourceID)]
	return ok, nil
}

func (s *fakeStore) List(ctx context.Context, account, region, resourceType, prefix string) ([]ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []ResourceEntry
	for _, e := range s.entries {
		if e.Type == resourceType {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) Delete(ctx context.Context, account, region, resourceType, resourceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, s.key(resourceType, resourceID))
	return nil
}

func (s *fakeStore) Update(ctx context.Context, account, region string, entry ResourceEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.key(entry.Type, entry.ID)] = entry
	return nil
}

func (s *fakeStore) Get(ctx context.Context, account, region, resourceType, resourceID string) (ResourceEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[s.key(resourceType, resourceID)]
	if !ok {
		return ResourceEntry{}, fmt.Errorf("not found")
	}
	return e, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func childRule(parentType, childType string, policy DeletionPolicy) DeleteGuardRule {
	return DeleteGuardRule{
		ParentType: parentType,
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			entries, err := resources.List(ctx, account, region, childType, "")
			if err != nil {
				return nil, err
			}
			var out []ChildRef
			for _, e := range entries {
				out = append(out, ChildRef{Type: e.Type, ID: e.ID})
			}
			return out, nil
		},
		Policy: policy,
	}
}

// ─── CheckParent tests ────────────────────────────────────────────────────────

func TestCheckParent_Exists(t *testing.T) {
	store := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(store, nil)

	err := mgr.CheckParent(context.Background(), "000000000000", "", "cluster", "c1", "NotFound", "not found", 404)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestCheckParent_NotFound(t *testing.T) {
	mgr := New(newFakeStore(), nil)

	err := mgr.CheckParent(context.Background(), "000000000000", "", "cluster", "missing", "NotFound", "not found", 404)
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected *OperationError, got %T", err)
	}
	if opErr.Code != "NotFound" || opErr.HTTPStatus != 404 {
		t.Errorf("unexpected error: %+v", opErr)
	}
}

func TestCheckParent_IsDeleting(t *testing.T) {
	store := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(store, nil)

	// Acquire deletion lock — no rules so it proceeds immediately
	handle, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("AcquireDelete failed: %v", err)
	}
	defer handle.Release()

	// CheckParent should see the deletion lock
	err = mgr.CheckParent(context.Background(), "000000000000", "", "cluster", "c1", "NotFound", "not found", 404)
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected *OperationError while deleting, got %T", err)
	}
	if opErr.HTTPStatus != 404 {
		t.Errorf("expected 404, got %d", opErr.HTTPStatus)
	}
}

// ─── AcquireDelete tests ───────────────────────────────────────────────────────

func TestAcquireDelete_DoubleLock(t *testing.T) {
	store := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(store, nil)

	h1, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("first AcquireDelete failed: %v", err)
	}
	defer h1.Release()

	_, err = mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected *OperationError on double lock, got %T", err)
	}
	if opErr.HTTPStatus != 409 {
		t.Errorf("expected 409, got %d", opErr.HTTPStatus)
	}
}

func TestAcquireDelete_PolicyFail_BlocksDelete(t *testing.T) {
	fs := newFakeStore(
		ResourceEntry{Type: "cluster", ID: "c1"},
		ResourceEntry{Type: "job", ID: "j1"},
	)
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy:     PolicyFail,
		FailCode:   "DependencyViolation",
		FailStatus: 400,
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected *OperationError, got %T %v", err, err)
	}
	if opErr.Code != "DependencyViolation" {
		t.Errorf("expected DependencyViolation, got %s", opErr.Code)
	}
	if opErr.HTTPStatus != 400 {
		t.Errorf("expected 400, got %d", opErr.HTTPStatus)
	}
}

func TestAcquireDelete_PolicyFail_DefaultCodes(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy: PolicyFail,
		// FailCode and FailStatus intentionally left empty — should use defaults
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected *OperationError, got %T", err)
	}
	if opErr.Code != "ValidationException" {
		t.Errorf("expected ValidationException default, got %s", opErr.Code)
	}
	if opErr.HTTPStatus != 400 {
		t.Errorf("expected 400 default, got %d", opErr.HTTPStatus)
	}
}

func TestAcquireDelete_PolicyFail_CustomMessage(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy: PolicyFail,
		FailMessage: func(parentID string, children []ChildRef) string {
			return fmt.Sprintf("cluster %s has %d active jobs", parentID, len(children))
		},
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("expected *OperationError")
	}
	if opErr.Message != "cluster c1 has 1 active jobs" {
		t.Errorf("unexpected message: %s", opErr.Message)
	}
}

func TestAcquireDelete_PolicyFail_LockReleasedOnError(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy: PolicyFail,
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	_, _ = mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")

	// Lock should be released after PolicyFail — a new AcquireDelete (with no rules) should work
	mgr2 := New(fs, nil)
	h, err := mgr2.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("expected lock released after PolicyFail, got: %v", err)
	}
	h.Release()
}

func TestAcquireDelete_PolicyCascade_DeletesChildren(t *testing.T) {
	fs := newFakeStore(
		ResourceEntry{Type: "topic", ID: "t1"},
		ResourceEntry{Type: "subscription", ID: "s1"},
		ResourceEntry{Type: "subscription", ID: "s2"},
	)
	rule := childRule("topic", "subscription", PolicyCascade)
	mgr := New(fs, []DeleteGuardRule{rule})

	handle, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "topic", "t1")
	if err != nil {
		t.Fatalf("AcquireDelete failed: %v", err)
	}
	handle.Release()

	// Verify children were deleted
	remaining, _ := fs.List(context.Background(), "000000000000", "", "subscription", "")
	if len(remaining) != 0 {
		t.Errorf("expected subscriptions deleted, got %d remaining", len(remaining))
	}
}

func TestAcquireDelete_PolicyCascade_CustomDeleteFn(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	var cascadedIDs []string
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}, {Type: "job", ID: "j2"}}, nil
		},
		Policy: PolicyCascade,
		CascadeDelete: func(ctx context.Context, resources ResourceStore, account, region string, child ChildRef) error {
			cascadedIDs = append(cascadedIDs, child.ID)
			return nil
		},
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	handle, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("AcquireDelete failed: %v", err)
	}
	handle.Release()

	if len(cascadedIDs) != 2 {
		t.Errorf("expected 2 cascades, got %d", len(cascadedIDs))
	}
}

func TestAcquireDelete_PolicyForceTerminate(t *testing.T) {
	fs := newFakeStore(
		ResourceEntry{Type: "cluster", ID: "c1"},
		ResourceEntry{Type: "step", ID: "step1"},
	)
	var terminated []string
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "step", ID: "step1"}}, nil
		},
		Policy: PolicyForceTerminate,
		ForceTerminate: func(ctx context.Context, resources ResourceStore, account, region string, child ChildRef) error {
			terminated = append(terminated, child.ID)
			return nil
		},
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	handle, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("AcquireDelete failed: %v", err)
	}
	handle.Release()

	if len(terminated) != 1 || terminated[0] != "step1" {
		t.Errorf("expected step1 terminated, got %v", terminated)
	}
}

func TestAcquireDelete_PolicyForceTerminate_NilFn(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "step", ID: "step1"}}, nil
		},
		Policy:         PolicyForceTerminate,
		ForceTerminate: nil, // intentionally nil — should return error
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err == nil {
		t.Fatal("expected error when ForceTerminate is nil")
	}
}

func TestAcquireDelete_PolicyPriorityOrdering(t *testing.T) {
	// If both PolicyCascade and PolicyFail apply, PolicyFail must fire first
	// (no irreversible cascade should happen).
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	var cascaded bool

	cascadeRule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "log", ID: "log1"}}, nil
		},
		Policy: PolicyCascade,
		CascadeDelete: func(ctx context.Context, resources ResourceStore, account, region string, child ChildRef) error {
			cascaded = true
			return nil
		},
	}
	failRule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy: PolicyFail,
	}
	// Register cascade first, fail second — priority sort must reorder them
	mgr := New(fs, []DeleteGuardRule{cascadeRule, failRule})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err == nil {
		t.Fatal("expected PolicyFail to block delete")
	}
	if cascaded {
		t.Error("PolicyCascade must not execute when PolicyFail fires first")
	}
}

func TestAcquireDelete_NoChildren_Succeeds(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return nil, nil // no children
		},
		Policy: PolicyFail,
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	handle, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("expected success with no children, got: %v", err)
	}
	handle.Release()
}

// ─── DeletionHandle.Release tests ────────────────────────────────────────────

func TestDeletionHandle_Release_ClearsLock(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(fs, nil)

	handle, _ := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if !mgr.lock.IsDeleting("cluster", "c1") {
		t.Error("lock should be set after AcquireDelete")
	}
	handle.Release()
	if mgr.lock.IsDeleting("cluster", "c1") {
		t.Error("lock should be cleared after Release")
	}
}

// ─── RegisterRules tests ──────────────────────────────────────────────────────

func TestRegisterRules_AddedAfterConstruction(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(fs, nil)

	mgr.RegisterRules([]DeleteGuardRule{{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return []ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy: PolicyFail,
	}})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err == nil {
		t.Fatal("expected PolicyFail from dynamically registered rule")
	}
}

func TestRegisterRules_ConcurrentRegistration(t *testing.T) {
	fs := newFakeStore()
	mgr := New(fs, nil)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mgr.RegisterRules([]DeleteGuardRule{{
				ParentType: fmt.Sprintf("type%d", n),
				FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
					return nil, nil
				},
				Policy: PolicyFail,
			}})
		}(i)
	}
	wg.Wait()

	if len(mgr.rules) != 20 {
		t.Errorf("expected 20 rules, got %d", len(mgr.rules))
	}
}

// ─── Manager.Reset tests ──────────────────────────────────────────────────────

func TestManager_Reset(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(fs, nil)

	h, _ := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	_ = h // intentionally not released

	mgr.Reset(context.Background())
	if mgr.lock.IsDeleting("cluster", "c1") {
		t.Error("Reset should clear all deletion locks")
	}
}

// ─── TOCTOU: CheckParent + AcquireDelete race test ───────────────────────────

func TestCheckParent_TOCTOU_Safety(t *testing.T) {
	// Simulate a race: many goroutines call CheckParent while one calls AcquireDelete.
	// After AcquireDelete succeeds, all subsequent CheckParent calls must see IsDeleting.
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	mgr := New(fs, nil)

	handle, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("AcquireDelete: %v", err)
	}

	const readers = 50
	var wg sync.WaitGroup
	errors := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := mgr.CheckParent(context.Background(), "000000000000", "", "cluster", "c1", "NotFound", "nf", 404)
			if err == nil {
				errors <- fmt.Errorf("expected error after AcquireDelete but got nil")
			}
		}()
	}
	wg.Wait()
	close(errors)

	for e := range errors {
		t.Error(e)
	}

	handle.Release()

	// After release: parent exists again → CheckParent should succeed
	err = mgr.CheckParent(context.Background(), "000000000000", "", "cluster", "c1", "NotFound", "nf", 404)
	if err != nil {
		t.Errorf("CheckParent after Release should succeed: %v", err)
	}
}

// ─── FindChildren error propagation ──────────────────────────────────────────

func TestAcquireDelete_FindChildrenError_ReleasesLock(t *testing.T) {
	fs := newFakeStore(ResourceEntry{Type: "cluster", ID: "c1"})
	findErr := errors.New("store unavailable")
	rule := DeleteGuardRule{
		ParentType: "cluster",
		FindChildren: func(ctx context.Context, resources ResourceStore, account, region, parentID string) ([]ChildRef, error) {
			return nil, findErr
		},
		Policy: PolicyFail,
	}
	mgr := New(fs, []DeleteGuardRule{rule})

	_, err := mgr.AcquireDelete(context.Background(), "000000000000", "", "cluster", "c1")
	if !errors.Is(err, findErr) {
		t.Fatalf("expected findErr propagated, got: %v", err)
	}
	// Lock must be released on error
	if mgr.lock.IsDeleting("cluster", "c1") {
		t.Error("deletion lock should be released after FindChildren error")
	}
}
