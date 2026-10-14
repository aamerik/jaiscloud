package datastore

import (
	"strings"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// ─── value constructors (one variant each) ────────────────────────────────────

func valStr(s string) dsstore.Value  { return dsstore.Value{StringValue: &s} }
func valInt(n int64) dsstore.Value   { return dsstore.Value{IntegerValue: ptrInt64(n)} }
func valDbl(f float64) dsstore.Value { return dsstore.Value{DoubleValue: ptrFloat64(f)} }
func valBool(b bool) dsstore.Value   { return dsstore.Value{BooleanValue: ptrBool(b)} }
func valTS(s string) dsstore.Value   { return dsstore.Value{TimestampValue: &s} }
func valKey(s string) dsstore.Value  { return dsstore.Value{KeyValue: &s} }
func valBlob(b []byte) dsstore.Value { return dsstore.Value{BlobValue: b} }

func valNull() dsstore.Value {
	s := "NULL_VALUE"
	return dsstore.Value{NullValue: &s}
}

func valGeo(lat, lon float64) dsstore.Value {
	return dsstore.Value{GeoPointValue: &dsstore.GeoPoint{Latitude: lat, Longitude: lon}}
}

func valEntity(e dsstore.Entity) dsstore.Value { return dsstore.Value{EntityValue: &e} }

func valArray(vs ...dsstore.Value) dsstore.Value {
	return dsstore.Value{ArrayValue: &dsstore.ArrayValue{Values: vs}}
}

func entityWith(kind, key string, props map[string]dsstore.Value) dsstore.Entity {
	return dsstore.Entity{Kind: kind, Key: key, Properties: props}
}

// TestNumericValue covers the integer/double unwrapping numericValue feeds to
// the comparisons.
func TestNumericValue(t *testing.T) {
	if v, ok := numericValue(valInt(7)); !ok || v != 7 {
		t.Fatalf("numericValue(int 7) = %v,%v; want 7,true", v, ok)
	}
	if v, ok := numericValue(valDbl(2.5)); !ok || v != 2.5 {
		t.Fatalf("numericValue(double 2.5) = %v,%v; want 2.5,true", v, ok)
	}
	if _, ok := numericValue(valStr("7")); ok {
		t.Fatal("numericValue(string) should not be numeric")
	}
	if _, ok := numericValue(dsstore.Value{}); ok {
		t.Fatal("numericValue(empty) should not be numeric")
	}
}

// TestValueEqual covers every value variant plus the cross-variant negatives.
// It pins the emulator's behaviour: an integer and a double with the same
// numeric value are equal. NOTE — that numeric-equality shortcut is an emulator
// approximation: Firestore in Datastore mode treats INTEGER and DOUBLE as
// separate types for ordering and equality (docs: "Entities, Properties, and
// Keys" → value-type ordering). The divergence is tracked as TD13-c; this test
// exists to lock the *current* behaviour against mutation, not to bless it.
func TestValueEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b dsstore.Value
		want bool
	}{
		{"int==double same", valInt(3), valDbl(3.0), true},
		{"double==int same", valDbl(3.0), valInt(3), true},
		{"int!=int", valInt(3), valInt(5), false},
		{"int!=double", valInt(3), valDbl(3.5), false},
		{"string==string", valStr("a"), valStr("a"), true},
		{"string!=string", valStr("a"), valStr("b"), false},
		{"string!=int", valStr("3"), valInt(3), false},
		{"null==null", valNull(), valNull(), true},
		{"null!=string", valNull(), valStr("x"), false},
		{"bool==bool", valBool(true), valBool(true), true},
		{"bool!=bool", valBool(true), valBool(false), false},
		{"bool!=int", valBool(true), valInt(1), false},
		{"ts==ts", valTS("2020-01-01T00:00:00Z"), valTS("2020-01-01T00:00:00Z"), true},
		{"ts!=ts", valTS("2020-01-01T00:00:00Z"), valTS("2021-01-01T00:00:00Z"), false},
		{"key==key", valKey("k1"), valKey("k1"), true},
		{"key!=key", valKey("k1"), valKey("k2"), false},
		{"key!=string", valKey("k1"), valStr("k1"), false},
		{"blob==blob", valBlob([]byte{1, 2}), valBlob([]byte{1, 2}), true},
		{"blob!=blob", valBlob([]byte{1, 2}), valBlob([]byte{1, 3}), false},
		{"blob!=string", valBlob([]byte("x")), valStr("x"), false},
		{"geo==geo", valGeo(1, 2), valGeo(1, 2), true},
		{"geo!=geo", valGeo(1, 2), valGeo(1, 3), false},
		{"geo!=string", valGeo(1, 2), valStr("geo"), false},
		{"entity==entity", valEntity(entityWith("K", "k1", nil)), valEntity(entityWith("K", "k1", nil)), true},
		{"entity!=entity", valEntity(entityWith("K", "k1", nil)), valEntity(entityWith("K", "k2", nil)), false},
		{"entity!=string", valEntity(entityWith("K", "k1", nil)), valStr("k1"), false},
		{"array==array", valArray(valInt(1), valStr("a")), valArray(valInt(1), valStr("a")), true},
		{"array len differs", valArray(valInt(1)), valArray(valInt(1), valInt(2)), false},
		{"array elem differs", valArray(valInt(1)), valArray(valInt(2)), false},
		{"array!=string", valArray(valInt(1)), valStr("x"), false},
		{"empty!=empty", dsstore.Value{}, dsstore.Value{}, false},
		// Cross-type against an absent value: these reach the later variant
		// arms (entity/array/geo/blob) and their nil guards, which the
		// same-type cases above never touch.
		{"string vs empty", valStr("x"), dsstore.Value{}, false},
		{"empty vs string", dsstore.Value{}, valStr("a"), false},
		{"empty vs bool", dsstore.Value{}, valBool(true), false},
		{"empty vs ts", dsstore.Value{}, valTS("2020-01-01T00:00:00Z"), false},
		{"empty vs key", dsstore.Value{}, valKey("a"), false},
		{"empty vs blob", dsstore.Value{}, valBlob([]byte{1}), false},
		{"empty vs geo", dsstore.Value{}, valGeo(1, 2), false},
		{"empty vs entity", dsstore.Value{}, valEntity(entityWith("K", "k1", nil)), false},
		{"empty vs array", dsstore.Value{}, valArray(valInt(1)), false},
		// A non-numeric value never equals a numeric 0 (the numeric branch is
		// entered but only compares when both are numeric).
		{"string vs int zero", valStr("x"), valInt(0), false},
		{"int zero vs string", valInt(0), valStr("x"), false},
	}
	for _, tc := range cases {
		if got := valueEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: valueEqual = %v, want %v", tc.name, got, tc.want)
		}
		if got := valueEqual(tc.b, tc.a); got != tc.want {
			t.Errorf("%s (swapped): valueEqual = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestValueCompare covers the ordering result (-1/0/1) and the ok flag for
// orderable vs incomparable pairs. Equal numerics compare 0 across int/double;
// as with TestValueEqual, cross-type pairs return ok=false, both of which are
// emulator approximations tracked as TD13-c (real Datastore has a total
// cross-type ordering over separate numeric types).
func TestValueCompare(t *testing.T) {
	cases := []struct {
		name   string
		a, b   dsstore.Value
		wantC  int
		wantOK bool
	}{
		{"int<int", valInt(3), valInt(5), -1, true},
		{"int>int", valInt(5), valInt(3), 1, true},
		{"int==int", valInt(3), valInt(3), 0, true},
		{"int<double", valInt(3), valDbl(3.5), -1, true},
		{"double<int", valDbl(3.5), valInt(3), 1, true},
		{"int==double", valInt(3), valDbl(3.0), 0, true},
		{"int vs string", valInt(3), valStr("3"), 0, false},
		{"string<int", valStr("a"), valInt(3), 0, false},
		{"str<str", valStr("a"), valStr("b"), -1, true},
		{"str>str", valStr("b"), valStr("a"), 1, true},
		{"str==str", valStr("a"), valStr("a"), 0, true},
		{"string vs int", valStr("a"), valInt(1), 0, false},
		{"bool<", valBool(false), valBool(true), -1, true},
		{"bool>", valBool(true), valBool(false), 1, true},
		{"bool==", valBool(true), valBool(true), 0, true},
		{"bool vs int", valBool(false), valInt(0), 0, false},
		{"ts<", valTS("2020-01-01T00:00:00Z"), valTS("2021-01-01T00:00:00Z"), -1, true},
		{"ts>", valTS("2021-01-01T00:00:00Z"), valTS("2020-01-01T00:00:00Z"), 1, true},
		{"ts==", valTS("2020-01-01T00:00:00Z"), valTS("2020-01-01T00:00:00Z"), 0, true},
		{"ts invalid", valTS("not-a-time"), valTS("2020-01-01T00:00:00Z"), 0, false},
		{"ts vs string", valTS("2020-01-01T00:00:00Z"), valStr("2020-01-01T00:00:00Z"), 0, false},
		{"key<", valKey("a"), valKey("b"), -1, true},
		{"key>", valKey("b"), valKey("a"), 1, true},
		{"key==", valKey("a"), valKey("a"), 0, true},
		{"key vs string", valKey("a"), valStr("a"), 0, false},
		{"blob<", valBlob([]byte{1}), valBlob([]byte{2}), -1, true},
		{"blob>", valBlob([]byte{2}), valBlob([]byte{1}), 1, true},
		{"blob==", valBlob([]byte{1}), valBlob([]byte{1}), 0, true},
		{"blob vs string", valBlob([]byte("a")), valStr("a"), 0, false},
		{"geo incomparable", valGeo(1, 2), valGeo(1, 2), 0, false},
		{"array incomparable", valArray(valInt(1)), valArray(valInt(1)), 0, false},
		{"entity incomparable", valEntity(entityWith("K", "k", nil)), valEntity(entityWith("K", "k", nil)), 0, false},
		{"null incomparable", valNull(), valNull(), 0, false},
		{"empty incomparable", dsstore.Value{}, dsstore.Value{}, 0, false},
		// An absent value on the left reaches the later variant arms and their
		// nil guards (the same-type cases stay within one variant).
		{"empty vs string", dsstore.Value{}, valStr("a"), 0, false},
		{"empty vs bool", dsstore.Value{}, valBool(true), 0, false},
		{"empty vs ts", dsstore.Value{}, valTS("2020-01-01T00:00:00Z"), 0, false},
		{"empty vs key", dsstore.Value{}, valKey("a"), 0, false},
		{"empty vs blob", dsstore.Value{}, valBlob([]byte{1}), 0, false},
	}
	for _, tc := range cases {
		c, ok := valueCompare(tc.a, tc.b)
		if ok != tc.wantOK {
			t.Errorf("%s: valueCompare ok = %v, want %v", tc.name, ok, tc.wantOK)
			continue
		}
		if ok && c != tc.wantC {
			t.Errorf("%s: valueCompare = %d, want %d", tc.name, c, tc.wantC)
		}
	}
}

// TestEntityEqual covers the key and property matching entityEqual performs for
// the entity_value variant of valueEqual (it compares the canonical Key and the
// property map; Kind is not part of the comparison).
func TestEntityEqual(t *testing.T) {
	base := entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(1)})
	if !entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(1)})) {
		t.Fatal("identical entities should be equal")
	}
	if entityEqual(base, entityWith("Kind", "k2", map[string]dsstore.Value{"a": valInt(1)})) {
		t.Fatal("entities with different keys must not be equal")
	}
	if entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(1), "b": valInt(2)})) {
		t.Fatal("entities with different property counts must not be equal")
	}
	if entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"a": valInt(2)})) {
		t.Fatal("entities with different property values must not be equal")
	}
	if entityEqual(base, entityWith("Kind", "k1", map[string]dsstore.Value{"b": valInt(1)})) {
		t.Fatal("entities with different property names must not be equal")
	}
}

// TestOperatorNames covers the error-message renderers for every operator plus
// their unspecified/default arms.
func TestOperatorNames(t *testing.T) {
	propOps := map[PropertyOp]string{
		PropertyEqual:              "EQUAL",
		PropertyNotEqual:           "NOT_EQUAL",
		PropertyIn:                 "IN",
		PropertyLessThan:           "LESS_THAN",
		PropertyLessThanOrEqual:    "LESS_THAN_OR_EQUAL",
		PropertyGreaterThan:        "GREATER_THAN",
		PropertyGreaterThanOrEqual: "GREATER_THAN_OR_EQUAL",
		PropertyHasAncestor:        "HAS_ANCESTOR",
		PropertyUnspecified:        "PROPERTY_FILTER_OP_UNSPECIFIED",
		PropertyOp(999):            "PROPERTY_FILTER_OP_UNSPECIFIED",
	}
	for op, want := range propOps {
		if got := propertyOpName(op); got != want {
			t.Errorf("propertyOpName(%d) = %q, want %q", op, got, want)
		}
	}
	compOps := map[CompositeOp]string{
		CompositeAnd:         "AND",
		CompositeOr:          "OR",
		CompositeUnspecified: "COMPOSITE_FILTER_OP_UNSPECIFIED",
		CompositeOp(999):     "COMPOSITE_FILTER_OP_UNSPECIFIED",
	}
	for op, want := range compOps {
		if got := compositeOpName(op); got != want {
			t.Errorf("compositeOpName(%d) = %q, want %q", op, got, want)
		}
	}
}

// TestUnsupportedQueryOperatorMessage locks the fail-closed error shape: an
// unsupported operator must name the operator, never match-all.
func TestUnsupportedQueryOperatorMessage(t *testing.T) {
	err := unsupportedQueryOperator("OR")
	pe := asProviderError(t, err)
	if pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("unsupportedQueryOperator = {%q,%d}, want InvalidArgument/400", pe.Code, pe.HTTPStatus)
	}
	if !strings.Contains(pe.Message, "OR") {
		t.Fatalf("message %q should name the operator", pe.Message)
	}
}

// TestMatchesFilterDispatch covers the filter dispatcher: nil matches, AND
// composites evaluate every clause, and every unsupported/incomplete shape
// fails closed with an InvalidArgument error rather than matching everything.
func TestMatchesFilterDispatch(t *testing.T) {
	e := entityWith("Kind", "k", map[string]dsstore.Value{
		"n":   valInt(5),
		"tag": valStr("x"),
	})

	if ok, err := matchesFilter(e, nil); !ok || err != nil {
		t.Fatalf("nil filter = %v,%v; want true,nil", ok, err)
	}

	eq := &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(5)}}
	neq := &Filter{Property: &PropertyFilter{Property: "tag", Op: PropertyEqual, Value: valStr("y")}}
	if ok, err := matchesFilter(e, &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{eq, eq}}}); !ok || err != nil {
		t.Fatalf("AND(all true) = %v,%v; want true,nil", ok, err)
	}
	if ok, err := matchesFilter(e, &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{eq, neq}}}); ok || err != nil {
		t.Fatalf("AND(one false) = %v,%v; want false,nil", ok, err)
	}

	errCases := []struct {
		name string
		f    *Filter
	}{
		{"composite OR", &Filter{Composite: &CompositeFilter{Op: CompositeOr, Filters: []*Filter{eq}}}},
		{"composite unspecified", &Filter{Composite: &CompositeFilter{}}},
		{"empty filter", &Filter{}},
	}
	for _, tc := range errCases {
		if _, err := matchesFilter(e, tc.f); err == nil {
			t.Errorf("%s: expected an InvalidArgument error", tc.name)
		}
	}

	// An error in a nested AND clause must propagate, not be swallowed.
	bad := &Filter{Property: &PropertyFilter{Property: "n", Op: PropertyUnspecified}}
	if _, err := matchesFilter(e, &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{bad}}}); err == nil {
		t.Fatal("AND with an unsupported clause must propagate the error")
	}
}

// TestMatchesPropertyFilterOperators covers every property operator against a
// present, absent and incomparable property.
func TestMatchesPropertyFilterOperators(t *testing.T) {
	e := entityWith("Kind", "k", map[string]dsstore.Value{
		"n":    valInt(5),
		"tags": valArray(valStr("a"), valStr("b")),
	})

	cases := []struct {
		name string
		pf   *PropertyFilter
		want bool
	}{
		{"equal match", &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(5)}, true},
		{"equal miss", &PropertyFilter{Property: "n", Op: PropertyEqual, Value: valInt(6)}, false},
		{"equal absent prop", &PropertyFilter{Property: "missing", Op: PropertyEqual, Value: valInt(5)}, false},
		{"not equal match", &PropertyFilter{Property: "n", Op: PropertyNotEqual, Value: valInt(6)}, true},
		{"not equal same", &PropertyFilter{Property: "n", Op: PropertyNotEqual, Value: valInt(5)}, false},
		{"not equal absent prop", &PropertyFilter{Property: "missing", Op: PropertyNotEqual, Value: valInt(5)}, false},
		{"in match", &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valInt(1), valInt(5))}, true},
		{"in miss", &PropertyFilter{Property: "n", Op: PropertyIn, Value: valArray(valInt(1), valInt(2))}, false},
		{"in absent prop", &PropertyFilter{Property: "missing", Op: PropertyIn, Value: valArray(valInt(1))}, false},
		{"in non-array value", &PropertyFilter{Property: "n", Op: PropertyIn, Value: valInt(5)}, false}, // emulator no-match; real GCP rejects (TD13-c)
		{"lt", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valInt(6)}, true},
		{"lt equal", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valInt(5)}, false},
		{"lte equal", &PropertyFilter{Property: "n", Op: PropertyLessThanOrEqual, Value: valInt(5)}, true},
		{"gt", &PropertyFilter{Property: "n", Op: PropertyGreaterThan, Value: valInt(4)}, true},
		{"gte equal", &PropertyFilter{Property: "n", Op: PropertyGreaterThanOrEqual, Value: valInt(5)}, true},
		{"gte miss", &PropertyFilter{Property: "n", Op: PropertyGreaterThanOrEqual, Value: valInt(6)}, false},
		{"comparison absent prop", &PropertyFilter{Property: "missing", Op: PropertyLessThan, Value: valInt(9)}, false},
		{"comparison incomparable", &PropertyFilter{Property: "n", Op: PropertyLessThan, Value: valStr("z")}, false},
	}
	for _, tc := range cases {
		got, err := matchesPropertyFilter(e, tc.pf)
		if err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: = %v, want %v", tc.name, got, tc.want)
		}
	}

	if _, err := matchesPropertyFilter(e, nil); err == nil {
		t.Fatal("a nil property filter must fail closed")
	}
	if _, err := matchesPropertyFilter(e, &PropertyFilter{Property: "n", Op: PropertyUnspecified}); err == nil {
		t.Fatal("an unspecified operator must fail closed")
	}
}

// TestHasAncestorGuards verifies HAS_ANCESTOR only accepts the reserved
// __key__ property and a key value, and that a matching ancestor key is
// satisfied end to end.
func TestHasAncestorGuards(t *testing.T) {
	root := idKey("Parent", 1)
	grand := dsstore.PathElement{Kind: "Parent", ID: 2, HasID: true}
	child := Key{Kind: "Child", Name: "c", HasName: true,
		Ancestors: []dsstore.PathElement{{Kind: "Parent", ID: 1, HasID: true}, grand}}
	e := entityWith("Child", CanonicalKey(child), nil)

	ancestor := CanonicalKey(root)
	pf := &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor, Value: dsstore.Value{KeyValue: &ancestor}}
	if ok, err := matchesPropertyFilter(e, pf); err != nil || !ok {
		t.Fatalf("HAS_ANCESTOR match = %v,%v; want true,nil", ok, err)
	}

	wrongProp := &PropertyFilter{Property: "n", Op: PropertyHasAncestor, Value: dsstore.Value{KeyValue: &ancestor}}
	if _, err := matchesPropertyFilter(e, wrongProp); err == nil {
		t.Fatal("HAS_ANCESTOR on a non-__key__ property must fail closed")
	}
	noKey := &PropertyFilter{Property: keyPropertyName, Op: PropertyHasAncestor, Value: valStr("not-a-key")}
	if _, err := matchesPropertyFilter(e, noKey); err == nil {
		t.Fatal("HAS_ANCESTOR with a non-key value must fail closed")
	}
}
