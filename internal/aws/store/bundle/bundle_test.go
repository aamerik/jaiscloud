package bundle_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"jaiscloud/internal/aws/store/bundle"
)

// ---- helpers ----------------------------------------------------------------

// simpleStore is a trivial store for testing. Data is exported so encoding/json
// can serialise it through the bundle Snapshot/Restore path.
type simpleStore struct {
	mu   sync.Mutex
	Data map[string]string `json:"data"`
}

func newSimpleStore(_, _ string) *simpleStore {
	return &simpleStore{Data: make(map[string]string)}
}

func newSimpleStoreAccount(_ string) *simpleStore {
	return &simpleStore{Data: make(map[string]string)}
}

func newSimpleStoreGlobal() *simpleStore {
	return &simpleStore{Data: make(map[string]string)}
}

func (s *simpleStore) Set(k, v string) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.Data[k] = v
}

func (s *simpleStore) Get(k string) (string, bool) {
	s.mu.Lock(); defer s.mu.Unlock()
	v, ok := s.Data[k]
	return v, ok
}

func (s *simpleStore) Reset(ctx context.Context) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.Data = make(map[string]string)
}

// ---- LocalBundle tests -------------------------------------------------------

func TestLocalBundle_Get_AutoVivify(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore)
	s1, err := b.Get("000000000000", "us-east-1")
	if err != nil || s1 == nil {
		t.Fatal("expected non-nil store on first Get")
	}
	s2, _ := b.Get("000000000000", "us-east-1")
	if s1 != s2 {
		t.Fatal("second Get should return same pointer")
	}
}

func TestLocalBundle_Get_DistinctScopes(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore)
	s1, _ := b.Get("111111111111", "us-east-1")
	s2, _ := b.Get("111111111111", "us-west-2")
	s3, _ := b.Get("222222222222", "us-east-1")
	if s1 == s2 || s1 == s3 || s2 == s3 {
		t.Fatal("distinct scopes must yield distinct stores")
	}
}

func TestLocalBundle_Reset_PreservesIdentity(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore)
	s, _ := b.Get("000000000000", "us-east-1")
	s.Set("key", "val")

	b.Reset(context.Background())

	sAfter, _ := b.Get("000000000000", "us-east-1")
	if sAfter != s {
		t.Fatal("Reset must preserve *T pointer identity")
	}
	if _, ok := sAfter.Get("key"); ok {
		t.Fatal("data must be cleared after Reset")
	}
}

func TestLocalBundle_ResetScope_Isolation(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore)
	a, _ := b.Get("111111111111", "us-east-1")
	c, _ := b.Get("111111111111", "us-west-2")
	d, _ := b.Get("222222222222", "us-east-1")
	a.Set("x", "1"); c.Set("x", "2"); d.Set("x", "3")

	b.ResetScope("111111111111", "us-east-1")

	if _, ok := a.Get("x"); ok {
		t.Error("ResetScope should have cleared (111111111111, us-east-1)")
	}
	if v, ok := c.Get("x"); !ok || v != "2" {
		t.Error("(111111111111, us-west-2) should be untouched")
	}
	if v, ok := d.Get("x"); !ok || v != "3" {
		t.Error("(222222222222, us-east-1) should be untouched")
	}
}

func TestLocalBundle_ResetAccount(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore)
	a, _ := b.Get("111111111111", "us-east-1")
	c, _ := b.Get("111111111111", "us-west-2")
	d, _ := b.Get("222222222222", "us-east-1")
	a.Set("x", "1"); c.Set("x", "2"); d.Set("x", "3")

	b.ResetAccount("111111111111")

	if _, ok := a.Get("x"); ok {
		t.Error("(111, us-east-1) should be cleared")
	}
	if _, ok := c.Get("x"); ok {
		t.Error("(111, us-west-2) should be cleared")
	}
	if v, ok := d.Get("x"); !ok || v != "3" {
		t.Error("(222, us-east-1) should be untouched")
	}
}

func TestLocalBundle_Snapshot_RoundTrip(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore)
	s, _ := b.Get("111111111111", "us-east-1")
	s.Set("color", "blue")

	snap, err := b.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	b2 := bundle.NewLocal(newSimpleStore)
	if err := b2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s2, _ := b2.Get("111111111111", "us-east-1")
	if v, ok := s2.Get("color"); !ok || v != "blue" {
		t.Errorf("restored store should have color=blue, got %q %v", v, ok)
	}
}

func TestLocalBundle_Concurrent_Get(t *testing.T) {
	var constructCount atomic.Int64
	constructor := func(account, region string) *simpleStore {
		constructCount.Add(1)
		return newSimpleStore(account, region)
	}
	b := bundle.NewLocal(constructor)

	var wg sync.WaitGroup
	const goroutines = 1000
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 4 distinct scopes across 1000 goroutines
			accounts := []string{"111111111111", "222222222222"}
			regions := []string{"us-east-1", "us-west-2"}
			_, _ = b.Get(accounts[i%2], regions[(i/2)%2])
		}(i)
	}
	wg.Wait()

	// At least 4 constructions (one per distinct scope); due to double-checked
	// locking races, more are allowed but only 4 stores are committed.
	if n := constructCount.Load(); n < 4 {
		t.Errorf("construct called %d times, want ≥4 (one per scope)", n)
	}
	// Verify exactly 4 committed stores exist in the bundle.
	count := 0
	b.Iter(func(_, _ string, _ *simpleStore) { count++ })
	if count != 4 {
		t.Errorf("bundle has %d committed stores, want 4", count)
	}
}

func TestLocalBundle_ConstructOutsideLock(t *testing.T) {
	// If construct calls back into the bundle, it must NOT deadlock.
	var b *bundle.LocalBundle[simpleStore]
	b = bundle.NewLocal(func(account, region string) *simpleStore {
		// Call Get for a DIFFERENT scope during construction to prove no deadlock.
		if region != "us-west-2" {
			_, _ = b.Get(account, "us-west-2")
		}
		return newSimpleStore(account, region)
	})
	done := make(chan struct{})
	go func() {
		_, _ = b.Get("000000000000", "us-east-1")
		close(done)
	}()
	select {
	case <-done:
	case <-waitTimeout():
		t.Fatal("deadlock: Get with reentrant construct timed out")
	}
}

func waitTimeout() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		// 2-second timeout is generous for a unit test
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); for range 2_000_000_000 { } }()
		wg.Wait()
		close(ch)
	}()
	return ch
}

func TestLocalBundle_Validate(t *testing.T) {
	b := bundle.NewLocal(newSimpleStore, bundle.LocalWithValidate[simpleStore]())
	if _, err := b.Get("bad", "us-east-1"); err == nil {
		t.Error("expected error for non-12-digit account")
	}
	if _, err := b.Get("111111111111", ""); err == nil {
		t.Error("expected error for empty region")
	}
	if _, err := b.Get("111111111111", "us-east-1"); err != nil {
		t.Errorf("valid scope should succeed: %v", err)
	}
}

// ---- CrossRegionBundle tests -------------------------------------------------

func TestCrossRegionBundle_Get_AutoVivify(t *testing.T) {
	b := bundle.NewCrossRegion(newSimpleStoreAccount)
	s1, err := b.Get("000000000000")
	if err != nil || s1 == nil {
		t.Fatal("expected non-nil store")
	}
	s2, _ := b.Get("000000000000")
	if s1 != s2 {
		t.Fatal("same account must return same store")
	}
}

func TestCrossRegionBundle_DistinctAccounts(t *testing.T) {
	b := bundle.NewCrossRegion(newSimpleStoreAccount)
	s1, _ := b.Get("111111111111")
	s2, _ := b.Get("222222222222")
	if s1 == s2 {
		t.Fatal("different accounts must produce different stores")
	}
}

func TestCrossRegionBundle_Reset_PreservesIdentity(t *testing.T) {
	b := bundle.NewCrossRegion(newSimpleStoreAccount)
	s, _ := b.Get("000000000000")
	s.Set("key", "val")
	b.Reset(context.Background())
	sAfter, _ := b.Get("000000000000")
	if sAfter != s {
		t.Fatal("Reset must preserve *T pointer")
	}
	if _, ok := sAfter.Get("key"); ok {
		t.Fatal("data must be cleared")
	}
}

func TestCrossRegionBundle_ResetAccount(t *testing.T) {
	b := bundle.NewCrossRegion(newSimpleStoreAccount)
	s1, _ := b.Get("111111111111")
	s2, _ := b.Get("222222222222")
	s1.Set("x", "1"); s2.Set("x", "2")

	b.ResetAccount("111111111111")
	if _, ok := s1.Get("x"); ok {
		t.Error("111 should be cleared")
	}
	if v, ok := s2.Get("x"); !ok || v != "2" {
		t.Error("222 should be untouched")
	}
}

func TestCrossRegionBundle_Snapshot_RoundTrip(t *testing.T) {
	b := bundle.NewCrossRegion(newSimpleStoreAccount)
	s, _ := b.Get("111111111111")
	s.Set("color", "red")

	snap, _ := b.Snapshot()
	b2 := bundle.NewCrossRegion(newSimpleStoreAccount)
	if err := b2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	s2, _ := b2.Get("111111111111")
	if v, ok := s2.Get("color"); !ok || v != "red" {
		t.Errorf("restored store should have color=red, got %q %v", v, ok)
	}
}

func TestCrossRegionBundle_Concurrent_Get(t *testing.T) {
	var count atomic.Int64
	b := bundle.NewCrossRegion(func(account string) *simpleStore {
		count.Add(1)
		return newSimpleStoreAccount(account)
	})

	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Get("000000000000")
		}()
	}
	wg.Wait()

	// Due to double-checked locking, construct may fire more than once; only
	// one store is committed. Verify exactly one store is in the bundle.
	if n := count.Load(); n < 1 {
		t.Errorf("construct called %d times, want ≥1", n)
	}
	committed := 0
	b.Iter(func(_ string, _ *simpleStore) { committed++ })
	if committed != 1 {
		t.Errorf("bundle has %d committed stores, want 1", committed)
	}
}

// ---- CrossAccountBundle tests ------------------------------------------------

func TestCrossAccountBundle_Singleton(t *testing.T) {
	var count atomic.Int64
	b := bundle.NewCrossAccount(func() *simpleStore {
		count.Add(1)
		return newSimpleStoreGlobal()
	})

	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Get()
		}()
	}
	wg.Wait()

	// CrossAccountBundle uses a mutex-guarded bool — construct is called exactly once.
	if n := count.Load(); n != 1 {
		t.Errorf("construct called %d times, want exactly 1", n)
	}
}

func TestCrossAccountBundle_Snapshot_RoundTrip(t *testing.T) {
	b := bundle.NewCrossAccount(newSimpleStoreGlobal)
	b.Get().Set("key", "global")

	snap, _ := b.Snapshot()
	b2 := bundle.NewCrossAccount(newSimpleStoreGlobal)
	if err := b2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if v, ok := b2.Get().Get("key"); !ok || v != "global" {
		t.Errorf("restored store should have key=global, got %q %v", v, ok)
	}
}

func TestCrossAccountBundle_Reset(t *testing.T) {
	b := bundle.NewCrossAccount(newSimpleStoreGlobal)
	b.Get().Set("k", "v")
	b.Reset(context.Background())
	if _, ok := b.Get().Get("k"); ok {
		t.Error("data must be cleared after Reset")
	}
}

func TestCrossAccountBundle_RestoreBeforeGet(t *testing.T) {
	var constructed atomic.Int64
	b := bundle.NewCrossAccount(func() *simpleStore {
		constructed.Add(1)
		return newSimpleStoreGlobal()
	})
	// Restore before any Get — construct must never fire.
	st := &simpleStore{Data: map[string]string{"x": "y"}}
	raw, _ := json.Marshal(map[string]any{"kind": "cross-account", "data": st})
	if err := b.Restore(raw); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if v, ok := b.Get().Get("x"); !ok || v != "y" {
		t.Errorf("got %q %v, want y true", v, ok)
	}
	if n := constructed.Load(); n != 0 {
		t.Errorf("construct called %d times after Restore, want 0", n)
	}
}
