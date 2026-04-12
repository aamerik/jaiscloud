package plugin_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sdk "github.com/jaiscloud/plugin-sdk"
	"jaiscloud/internal/plugin"
	"jaiscloud/internal/resourcemgr"
	"jaiscloud/internal/store"
)

// ─── in-memory store.ResourceStore ───────────────────────────────────────────

type memStore struct {
	entries map[string]store.ResourceEntry
}

func newMemStore() *memStore {
	return &memStore{entries: make(map[string]store.ResourceEntry)}
}

func (m *memStore) key(t, id string) string { return t + "\x00" + id }

func (m *memStore) Create(_ context.Context, e store.ResourceEntry) error {
	k := m.key(e.Type, e.ID)
	if _, ok := m.entries[k]; ok {
		return store.ErrAlreadyExists
	}
	m.entries[k] = e
	return nil
}
func (m *memStore) Get(_ context.Context, t, id string) (store.ResourceEntry, error) {
	e, ok := m.entries[m.key(t, id)]
	if !ok {
		return store.ResourceEntry{}, store.ErrNotFound
	}
	return e, nil
}
func (m *memStore) Update(_ context.Context, e store.ResourceEntry) error {
	m.entries[m.key(e.Type, e.ID)] = e
	return nil
}
func (m *memStore) Delete(_ context.Context, t, id string) error {
	delete(m.entries, m.key(t, id))
	return nil
}
func (m *memStore) List(_ context.Context, t, _ string) ([]store.ResourceEntry, error) {
	var out []store.ResourceEntry
	for _, e := range m.entries {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out, nil
}
func (m *memStore) Purge(_ context.Context, t string) error {
	for k, e := range m.entries {
		if e.Type == t {
			delete(m.entries, k)
		}
	}
	return nil
}
func (m *memStore) Reset() { m.entries = make(map[string]store.ResourceEntry) }

// ─── SDKStoreAdapter tests ────────────────────────────────────────────────────

func TestSDKStoreAdapter_Exists_True(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), store.ResourceEntry{Type: "t", ID: "id1", Data: json.RawMessage(`{}`)})
	a := plugin.NewSDKStoreAdapter(ms)

	ok, err := a.Exists(context.Background(), "t", "id1")
	if err != nil || !ok {
		t.Errorf("expected true/nil, got %v/%v", ok, err)
	}
}

func TestSDKStoreAdapter_Exists_False(t *testing.T) {
	a := plugin.NewSDKStoreAdapter(newMemStore())
	ok, err := a.Exists(context.Background(), "t", "missing")
	if err != nil || ok {
		t.Errorf("expected false/nil, got %v/%v", ok, err)
	}
}

func TestSDKStoreAdapter_Get_Found(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), store.ResourceEntry{Type: "t", ID: "id1", Data: json.RawMessage(`{"x":1}`)})
	a := plugin.NewSDKStoreAdapter(ms)

	e, err := a.Get(context.Background(), "t", "id1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.ID != "id1" || string(e.Data) != `{"x":1}` {
		t.Errorf("unexpected entry: %+v", e)
	}
}

func TestSDKStoreAdapter_Get_NotFound(t *testing.T) {
	a := plugin.NewSDKStoreAdapter(newMemStore())
	_, err := a.Get(context.Background(), "t", "missing")
	if err == nil {
		t.Fatal("expected error for missing resource")
	}
}

func TestSDKStoreAdapter_Create_Update_Delete(t *testing.T) {
	ms := newMemStore()
	a := plugin.NewSDKStoreAdapter(ms)
	ctx := context.Background()

	// Create
	if err := a.Create(ctx, sdk.ResourceEntry{Type: "t", ID: "id1", Data: []byte(`{"v":1}`)}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ok, _ := a.Exists(ctx, "t", "id1")
	if !ok {
		t.Error("should exist after Create")
	}

	// Update
	if err := a.Update(ctx, sdk.ResourceEntry{Type: "t", ID: "id1", Data: []byte(`{"v":2}`)}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	e, _ := a.Get(ctx, "t", "id1")
	if string(e.Data) != `{"v":2}` {
		t.Errorf("Update did not persist: %s", e.Data)
	}

	// Delete
	if err := a.Delete(ctx, "t", "id1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, _ = a.Exists(ctx, "t", "id1")
	if ok {
		t.Error("should not exist after Delete")
	}
}

func TestSDKStoreAdapter_List(t *testing.T) {
	ms := newMemStore()
	for _, id := range []string{"a", "b", "c"} {
		ms.Create(context.Background(), store.ResourceEntry{Type: "cluster", ID: id, Data: json.RawMessage(`{}`)})
	}
	ms.Create(context.Background(), store.ResourceEntry{Type: "job", ID: "j1", Data: json.RawMessage(`{}`)})
	a := plugin.NewSDKStoreAdapter(ms)

	entries, err := a.List(context.Background(), "cluster", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3, got %d", len(entries))
	}
}

// ─── SDKResourceManager adapter tests ────────────────────────────────────────

func newSDKRM(rules ...resourcemgr.DeleteGuardRule) sdk.ResourceManager {
	ms := newMemStore()
	ms.Create(context.Background(), store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	storeAdapter := resourcemgr.NewStoreAdapter(ms)
	rm := resourcemgr.New(storeAdapter, rules)
	return plugin.NewSDKResourceManager(rm)
}

func TestSDKResourceManager_CheckParent_Exists(t *testing.T) {
	rm := newSDKRM()
	err := rm.CheckParent(context.Background(), "cluster", "c1", "NotFound", "not found", 404)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestSDKResourceManager_CheckParent_NotFound(t *testing.T) {
	rm := newSDKRM()
	err := rm.CheckParent(context.Background(), "cluster", "missing", "NotFound", "nf", 404)
	if err == nil {
		t.Fatal("expected error for missing parent")
	}
}

func TestSDKResourceManager_AcquireDelete_ReleasesHandle(t *testing.T) {
	rm := newSDKRM()
	h, err := rm.AcquireDelete(context.Background(), "cluster", "c1")
	if err != nil {
		t.Fatalf("AcquireDelete: %v", err)
	}
	if h == nil {
		t.Fatal("expected non-nil handle")
	}
	h.Release() // must not panic
}

func TestSDKResourceManager_AcquireDelete_DoubleLock(t *testing.T) {
	rm := newSDKRM()
	h, _ := rm.AcquireDelete(context.Background(), "cluster", "c1")
	defer h.Release()

	_, err := rm.AcquireDelete(context.Background(), "cluster", "c1")
	if err == nil {
		t.Fatal("expected conflict error on double lock")
	}
}

func TestSDKResourceManager_RegisterRules_PolicyFail(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	storeAdapter := resourcemgr.NewStoreAdapter(ms)
	rm := plugin.NewSDKResourceManager(resourcemgr.New(storeAdapter, nil))

	// Register a rule that blocks deletion when there are children
	rm.RegisterRules([]sdk.DeleteGuardRule{{
		ParentType: "cluster",
		FindChildren: func(_ context.Context, _ sdk.ResourceStore, _ string) ([]sdk.ChildRef, error) {
			return []sdk.ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy:   sdk.PolicyFail,
		FailCode: "DependencyViolation",
	}})

	_, err := rm.AcquireDelete(context.Background(), "cluster", "c1")
	if err == nil {
		t.Fatal("expected PolicyFail to block deletion")
	}
}

func TestSDKResourceManager_RegisterRules_PolicyCascade(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	ms.Create(context.Background(), store.ResourceEntry{Type: "job", ID: "j1", Data: json.RawMessage(`{}`)})
	storeAdapter := resourcemgr.NewStoreAdapter(ms)
	rm := plugin.NewSDKResourceManager(resourcemgr.New(storeAdapter, nil))

	var cascadedIDs []string
	rm.RegisterRules([]sdk.DeleteGuardRule{{
		ParentType: "cluster",
		FindChildren: func(_ context.Context, _ sdk.ResourceStore, _ string) ([]sdk.ChildRef, error) {
			return []sdk.ChildRef{{Type: "job", ID: "j1"}}, nil
		},
		Policy: sdk.PolicyCascade,
		CascadeDelete: func(_ context.Context, _ sdk.ResourceStore, child sdk.ChildRef) error {
			cascadedIDs = append(cascadedIDs, child.ID)
			return nil
		},
	}})

	h, err := rm.AcquireDelete(context.Background(), "cluster", "c1")
	if err != nil {
		t.Fatalf("AcquireDelete: %v", err)
	}
	h.Release()

	if len(cascadedIDs) != 1 || cascadedIDs[0] != "j1" {
		t.Errorf("expected cascade for j1, got %v", cascadedIDs)
	}
}

func TestSDKResourceManager_RegisterRules_FindChildrenError_PropagatesAndReleasesLock(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	storeAdapter := resourcemgr.NewStoreAdapter(ms)
	rm := plugin.NewSDKResourceManager(resourcemgr.New(storeAdapter, nil))

	findErr := errors.New("store unavailable")
	rm.RegisterRules([]sdk.DeleteGuardRule{{
		ParentType: "cluster",
		FindChildren: func(_ context.Context, _ sdk.ResourceStore, _ string) ([]sdk.ChildRef, error) {
			return nil, findErr
		},
		Policy: sdk.PolicyFail,
	}})

	_, err := rm.AcquireDelete(context.Background(), "cluster", "c1")
	if !errors.Is(err, findErr) {
		t.Fatalf("expected findErr, got: %v", err)
	}

	// Lock must be released — a second attempt (with no rules) should succeed
	ms2 := newMemStore()
	ms2.Create(context.Background(), store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	rm2 := plugin.NewSDKResourceManager(resourcemgr.New(resourcemgr.NewStoreAdapter(ms2), nil))
	h, err := rm2.AcquireDelete(context.Background(), "cluster", "c1")
	if err != nil {
		t.Fatalf("lock should not persist after FindChildren error: %v", err)
	}
	h.Release()
}
