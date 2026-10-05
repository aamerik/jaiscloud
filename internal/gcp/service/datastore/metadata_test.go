package datastore

import (
	"context"
	"errors"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func strProp(name, s string) dsstore.Value { return dsstore.Value{StringValue: &s} }

// kindNames extracts the __kind__ metadata result names in order.
func kindNames(res *QueryResult) []string {
	out := make([]string, 0, len(res.Entities))
	for _, r := range res.Entities {
		if k, ok := KeyFromCanonical(r.Entity.Key); ok {
			out = append(out, k.Name)
		}
	}
	return out
}

// TestKindMetadataListsUserKinds verifies SELECT __key__ FROM __kind__ returns
// one entity per user kind, sorted ascending by kind name, with the metadata
// key shape (kind __kind__, name = kind).
func TestKindMetadataListsUserKinds(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	upsert(t, s, "p", nameKey("Task", "a"), intProp(1))
	upsert(t, s, "p", nameKey("User", "u"), intProp(2))
	upsert(t, s, "p", nameKey("Task", "b"), intProp(3))

	res, err := s.RunQuery(context.Background(), "p", &Query{Kind: metadataKindKind}, nil)
	if err != nil {
		t.Fatalf("RunQuery __kind__: %v", err)
	}
	got := kindNames(res)
	want := []string{"Task", "User"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("kinds = %v, want %v", got, want)
	}
	// The result entity's own kind is __kind__ and it has no properties.
	if res.Entities[0].Entity.Kind != metadataKindKind {
		t.Fatalf("metadata entity kind = %q", res.Entities[0].Entity.Kind)
	}
	if len(res.Entities[0].Entity.Properties) != 0 {
		t.Fatalf("__kind__ entity should have no properties: %+v", res.Entities[0].Entity.Properties)
	}
}

// TestKindMetadataRespectsNamespace verifies metadata queries are implicitly
// restricted to the query's namespace.
func TestKindMetadataRespectsNamespace(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	nsOnly := nameKey("TenantThing", "x")
	nsOnly.Namespace = "tenant-a"
	upsert(t, s, "p", nameKey("DefaultThing", "y"), intProp(1))
	upsert(t, s, "p", nsOnly, intProp(2))

	ctx := context.Background()
	def, err := s.RunQuery(ctx, "p", &Query{Kind: metadataKindKind}, nil)
	if err != nil {
		t.Fatalf("default namespace __kind__: %v", err)
	}
	if got := kindNames(def); len(got) != 1 || got[0] != "DefaultThing" {
		t.Fatalf("default kinds = %v, want [DefaultThing]", got)
	}

	ns, err := s.RunQuery(ctx, "p", &Query{Kind: metadataKindKind, Namespace: "tenant-a"}, nil)
	if err != nil {
		t.Fatalf("tenant-a __kind__: %v", err)
	}
	if got := kindNames(ns); len(got) != 1 || got[0] != "TenantThing" {
		t.Fatalf("tenant-a kinds = %v, want [TenantThing]", got)
	}
}

// TestPropertyMetadataAncestorFilter verifies a property query returns one
// entity per property of the ancestor kind, keyed [__kind__:k, __property__:p],
// with property_representation listing every value type seen (arrays flatten to
// their element types, NULL recorded when a null appears).
func TestPropertyMetadataAncestorFilter(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	taskA := dsstore.Entity{Properties: map[string]dsstore.Value{
		"name": strProp("name", "a"),
		"done": {BooleanValue: ptrBool(true)},
		"tags": {ArrayValue: &dsstore.ArrayValue{Values: []dsstore.Value{
			strProp("t", "x"), strProp("t", "y"),
		}}},
	}}
	nullStr := "NULL_VALUE"
	taskB := dsstore.Entity{Properties: map[string]dsstore.Value{
		"done": {NullValue: &nullStr},
	}}
	// A different kind's property must not leak into the ancestor query.
	upsert(t, s, "p", nameKey("Task", "a"), taskA)
	upsert(t, s, "p", nameKey("Task", "b"), taskB)
	upsert(t, s, "p", nameKey("User", "u"), dsstore.Entity{Properties: map[string]dsstore.Value{
		"email": strProp("email", "u@example.com"),
	}})

	ancestor := CanonicalKey(nameKey(metadataKindKind, "Task"))
	q := &Query{Kind: metadataKindProperty, Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName, Op: PropertyHasAncestor,
		Value: dsstore.Value{KeyValue: &ancestor},
	}}}
	res, err := s.RunQuery(ctx, "p", q, nil)
	if err != nil {
		t.Fatalf("RunQuery __property__: %v", err)
	}

	type propRow struct {
		name string
		reps []string
	}
	rows := map[string][]string{}
	for _, r := range res.Entities {
		k, ok := KeyFromCanonical(r.Entity.Key)
		if !ok {
			t.Fatalf("bad canonical key: %q", r.Entity.Key)
		}
		if k.Kind != metadataKindProperty || len(k.Ancestors) != 1 ||
			k.Ancestors[0].Kind != metadataKindKind || k.Ancestors[0].Name != "Task" {
			t.Fatalf("property key path = %+v", k)
		}
		arr := r.Entity.Properties[propertyRepresentation].ArrayValue
		if arr == nil {
			t.Fatalf("property %q missing representation", k.Name)
		}
		reps := make([]string, 0, len(arr.Values))
		for _, v := range arr.Values {
			reps = append(reps, *v.StringValue)
		}
		rows[k.Name] = reps
	}

	if len(rows) != 3 {
		t.Fatalf("properties = %v, want name/done/tags", rows)
	}
	if got := rows["name"]; len(got) != 1 || got[0] != "STRING" {
		t.Fatalf("name reps = %v", got)
	}
	if got := rows["tags"]; len(got) != 1 || got[0] != "STRING" {
		t.Fatalf("tags reps = %v (array should flatten to element type)", got)
	}
	if got := rows["done"]; len(got) != 2 || got[0] != "BOOLEAN" || got[1] != "NULL" {
		t.Fatalf("done reps = %v, want [BOOLEAN NULL]", got)
	}
}

func ptrBool(b bool) *bool { return &b }

// TestReservedWritesRejected verifies Datastore reserved names cannot be used by
// user writes: kinds beginning with "__", reserved key names, and reserved
// property names are all rejected.
func TestReservedWritesRejected(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	cases := []struct {
		name string
		mut  Mutation
	}{
		{"reserved kind", Mutation{Op: MutationUpsert, Key: nameKey("__secret__", "x"), Entity: intProp(1)}},
		{"reserved name", Mutation{Op: MutationUpsert, Key: nameKey("Task", "__id__"), Entity: intProp(1)}},
		{"reserved ancestor kind", Mutation{
			Op:     MutationUpsert,
			Key:    Key{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{{Kind: "__parent__", ID: 1, HasID: true}}},
			Entity: intProp(1),
		}},
		{"reserved property", Mutation{Op: MutationUpsert, Key: nameKey("Task", "t"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"__key__": {IntegerValue: ptrInt64(1)}}}}},
	}
	for _, tc := range cases {
		_, err := s.Commit(ctx, "p", &CommitRequest{Mutations: []Mutation{tc.mut}})
		if err == nil {
			t.Fatalf("%s: expected rejection", tc.name)
		}
		var perr interface{ Error() string }
		if !errors.As(err, &perr) {
			t.Fatalf("%s: unexpected error type %T", tc.name, err)
		}
	}

	// The metadata kinds themselves are reserved too.
	if _, err := s.Commit(ctx, "p", &CommitRequest{Mutations: []Mutation{
		{Op: MutationInsert, Key: nameKey("__kind__", "Task"), Entity: intProp(1)},
	}}); err == nil {
		t.Fatal("writing kind __kind__ should be rejected")
	}
}
