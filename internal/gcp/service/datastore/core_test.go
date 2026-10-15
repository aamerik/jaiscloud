package datastore

import (
	"context"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// TestValueEqualNumeric pins the numeric type-strictness TD21 restored:
// integers and doubles are separate types (the documented value-type
// ordering), so equal numbers of different types are not equal, while the
// documented cross-type ordering still holds (every integer sorts before every
// double).
func TestValueEqualNumeric(t *testing.T) {
	int3 := dsstore.Value{IntegerValue: ptrInt64(3)}
	dbl3 := dsstore.Value{DoubleValue: ptrFloat64(3.0)}
	int5 := dsstore.Value{IntegerValue: ptrInt64(5)}

	if valueEqual(int3, dbl3) {
		t.Fatal("valueEqual(int 3, double 3.0) = true; want false (separate types)")
	}
	if valueEqual(dbl3, int3) {
		t.Fatal("valueEqual(double 3.0, int 3) = true; want false (separate types)")
	}
	if !valueEqual(int3, int3) {
		t.Fatal("valueEqual(int 3, int 3) = false")
	}
	if valueEqual(int3, int5) {
		t.Fatal("valueEqual(int 3, int 5) = true")
	}
	if valueEqual(int3, dsstore.Value{DoubleValue: ptrFloat64(3.5)}) {
		t.Fatal("valueEqual(int 3, double 3.5) = true")
	}

	if c, ok := valueCompare(int3, int5); !ok || c >= 0 {
		t.Fatalf("valueCompare(int 3, int 5) = %d, %v; want < 0", c, ok)
	}
	// Every integer sorts before every double, so a smaller double still sorts
	// after a larger integer (the documented `7 < 3.2`).
	if c, ok := valueCompare(int5, dbl3); !ok || c >= 0 {
		t.Fatalf("valueCompare(int 5, double 3.0) = %d, %v; want < 0", c, ok)
	}
	if c, ok := valueCompare(dbl3, int5); !ok || c <= 0 {
		t.Fatalf("valueCompare(double 3.0, int 5) = %d, %v; want > 0", c, ok)
	}
}

// TestRunQueryValidatesFilterBeforeScan pins the up-front filter validation: a
// filter real Datastore rejects is rejected before any candidate is scanned, so
// it fails loud on an empty kind — where the per-entity matcher never runs —
// rather than silently returning an empty result set. (TD21)
func TestRunQueryValidatesFilterBeforeScan(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "test")
	ctx := context.Background()

	bad := map[string]*Query{
		"IN non-array": {Kind: "Task", Filter: &Filter{Property: &PropertyFilter{
			Property: "n", Op: PropertyIn, Value: dsstore.Value{IntegerValue: ptrInt64(1)},
		}}},
		"OR composite":            {Kind: "Task", Filter: &Filter{Composite: &CompositeFilter{Op: CompositeOr}}},
		"unspecified property op": {Kind: "Task", Filter: &Filter{Property: &PropertyFilter{Property: "n"}}},
	}
	for name, q := range bad {
		_, err := s.RunQuery(ctx, "p", q, nil)
		if err == nil {
			t.Errorf("%s: RunQuery on an empty kind = nil error, want InvalidArgument", name)
			continue
		}
		if pe := asProviderError(t, err); pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("%s: error = {%q,%d}, want InvalidArgument/400", name, pe.Code, pe.HTTPStatus)
		}
	}

	// A valid filter on an empty kind still returns an empty result set.
	good := &Query{Kind: "Task", Filter: &Filter{Property: &PropertyFilter{
		Property: "n", Op: PropertyIn, Value: valArray(valInt(1)),
	}}}
	res, err := s.RunQuery(ctx, "p", good, nil)
	if err != nil {
		t.Fatalf("RunQuery with a valid IN filter: %v", err)
	}
	if len(res.Entities) != 0 {
		t.Fatalf("RunQuery on an empty kind returned %d entities, want 0", len(res.Entities))
	}
}

// TestResetClearsTransactions verifies that the core's Reset clears the
// in-memory transaction read-set registry: a handle open before Reset is no
// longer active afterwards.
func TestResetClearsTransactions(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "test")
	ctx := context.Background()

	txn, err := s.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := s.requireActive(txn); err != nil {
		t.Fatalf("transaction should be active before Reset: %v", err)
	}

	s.Reset(ctx)

	if err := s.requireActive(txn); err == nil {
		t.Fatal("transaction should be inactive after Reset")
	}
}

func ptrInt64(n int64) *int64       { return &n }
func ptrFloat64(f float64) *float64 { return &f }

// TestAllocateIDsRejectsEmptyKeyPath locks the preserved validation: an empty
// key path (no kind) cannot be completed with an allocated ID.
func TestAllocateIDsRejectsEmptyKeyPath(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "test")
	if _, err := s.AllocateIDs(context.Background(), "p", []Key{{}}); err == nil {
		t.Fatal("AllocateIDs with an empty key path should be rejected")
	}
}
