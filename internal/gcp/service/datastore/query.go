package datastore

import (
	"bytes"
	"cmp"
	"math"
	"time"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// matchesFilter evaluates a query filter against an entity. It supports
// PropertyFilter EQUAL, NOT_EQUAL, IN, the four comparison operators
// (LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL), and the
// special "__key__ HAS ANCESTOR", plus CompositeFilter AND over those property
// filters. Unsupported operators (NOT_IN, OR, unspecified/unknown) return an
// InvalidArgument error rather than matching everything.
func matchesFilter(e dsstore.Entity, f *Filter) (bool, error) {
	if f == nil {
		return true, nil
	}
	if f.Property != nil {
		return matchesPropertyFilter(e, f.Property)
	}
	if f.Composite != nil {
		cf := f.Composite
		switch cf.Op {
		case CompositeAnd:
			for _, sub := range cf.Filters {
				match, err := matchesFilter(e, sub)
				if err != nil {
					return false, err
				}
				if !match {
					return false, nil
				}
			}
			return true, nil
		default:
			return false, unsupportedQueryOperator(compositeOpName(cf.Op))
		}
	}
	return false, unsupportedQueryOperator("unknown filter")
}

// unsupportedQueryOperator returns an InvalidArgument error for a filter
// operator the emulator does not implement (fail closed, never match-all).
func unsupportedQueryOperator(op string) error {
	return invalidArgument("unsupported query operator: " + op)
}

// validateFilter checks a filter's static shape before any entity is scanned.
// matchesFilter enforces the same rules per entity, but that is reached only
// when the scan visits a candidate — so without this pass a request real
// Datastore rejects (an unsupported operator, or IN with a non-array value)
// would silently return an empty result set on an empty or fully-filtered
// collection instead of failing loud. RunQuery and RunAggregationQuery call it
// once up front.
func validateFilter(f *Filter) error {
	if f == nil {
		return nil
	}
	if f.Property != nil {
		return validatePropertyFilter(f.Property)
	}
	if f.Composite != nil {
		if f.Composite.Op != CompositeAnd {
			return unsupportedQueryOperator(compositeOpName(f.Composite.Op))
		}
		for _, sub := range f.Composite.Filters {
			if err := validateFilter(sub); err != nil {
				return err
			}
		}
		return nil
	}
	return unsupportedQueryOperator("unknown filter")
}

// validatePropertyFilter validates one property filter's static shape.
func validatePropertyFilter(pf *PropertyFilter) error {
	switch pf.Op {
	case PropertyEqual, PropertyNotEqual,
		PropertyLessThan, PropertyLessThanOrEqual,
		PropertyGreaterThan, PropertyGreaterThanOrEqual:
		return nil
	case PropertyIn:
		// Real Datastore requires the IN operand to be an array value.
		if pf.Value.ArrayValue == nil {
			return invalidArgument("IN requires an array value")
		}
		return nil
	case PropertyHasAncestor:
		if pf.Property != keyPropertyName {
			return invalidArgument("HAS_ANCESTOR requires the " + keyPropertyName + " property")
		}
		if pf.Value.KeyValue == nil {
			return invalidArgument("HAS_ANCESTOR requires a key value")
		}
		return nil
	default:
		return unsupportedQueryOperator(propertyOpName(pf.Op))
	}
}

func matchesPropertyFilter(e dsstore.Entity, pf *PropertyFilter) (bool, error) {
	if pf == nil {
		return false, unsupportedQueryOperator("missing property filter")
	}
	prop := pf.Property
	val, ok := e.Properties[prop]
	filterVal := pf.Value

	switch pf.Op {
	case PropertyHasAncestor:
		// HAS_ANCESTOR is not a property comparison: it only applies to the
		// implicit "__key__" property and its value is a key. It matches an
		// entity whose key path has that key as a proper prefix (the query's
		// partition already scopes the result set).
		if prop != keyPropertyName {
			return false, invalidArgument("HAS_ANCESTOR requires the " + keyPropertyName + " property")
		}
		if filterVal.KeyValue == nil {
			return false, invalidArgument("HAS_ANCESTOR requires a key value")
		}
		return keyHasAncestor(e.Key, *filterVal.KeyValue), nil
	case PropertyEqual:
		return ok && valueEqual(val, filterVal), nil
	case PropertyNotEqual:
		return ok && !valueEqual(val, filterVal), nil
	case PropertyIn:
		if filterVal.ArrayValue == nil {
			return false, invalidArgument("IN requires an array value")
		}
		if !ok {
			return false, nil
		}
		for _, el := range filterVal.ArrayValue.Values {
			if valueEqual(val, el) {
				return true, nil
			}
		}
		return false, nil
	case PropertyLessThan, PropertyLessThanOrEqual, PropertyGreaterThan, PropertyGreaterThanOrEqual:
		if !ok {
			return false, nil
		}
		c, comparable := valueCompare(val, filterVal)
		if !comparable {
			return false, nil
		}
		switch pf.Op {
		case PropertyLessThan:
			return c < 0, nil
		case PropertyLessThanOrEqual:
			return c <= 0, nil
		case PropertyGreaterThan:
			return c > 0, nil
		default:
			return c >= 0, nil
		}
	default:
		return false, unsupportedQueryOperator(propertyOpName(pf.Op))
	}
}

// keyPropertyName is the reserved Datastore property that exposes an entity's
// own key; the only legal operand of HAS_ANCESTOR.
const keyPropertyName = "__key__"

// keyHasAncestor reports whether ancestorKey is a proper path prefix of
// entityKey. Both arguments are canonical store keys, so the comparison is
// structural — a raw string prefix would be wrong (kind "A" is lexically a
// prefix of kind "AB").
//
// Only the paths are compared: the partition is the query's concern (the query
// already restricts results to its namespace/database), and a GQL KEY(...)
// literal — or an SDK ancestor key without an explicit namespace — carries no
// partition of its own. Requiring partition equality would wrongly reject those.
func keyHasAncestor(entityKey, ancestorKey string) bool {
	_, _, epath, eok := dsstore.ParseKey(entityKey)
	_, _, apath, aok := dsstore.ParseKey(ancestorKey)
	if !eok || !aok || len(apath) == 0 || len(apath) >= len(epath) {
		return false
	}
	for i := range apath {
		if apath[i] != epath[i] {
			return false
		}
	}
	return true
}

// Datastore value-type ranks. Firestore in Datastore mode orders a property
// whose values have mixed types by the value's *type* first, then by value
// within the type (the documented "Value type ordering" table,
// https://cloud.google.com/datastore/docs/concepts/entities#value_type_ordering):
//
//	null < integers < dates/times < booleans < byte strings
//	     < unicode strings < floats < NaN < geo points < keys
//
// Integers and floats are separate types, so every integer sorts before every
// float (the documented `7 < 3.2`).
const (
	rankNull = iota + 1
	rankInteger
	rankTimestamp
	rankBoolean
	rankBlob
	rankString
	rankDouble
	rankNaN
	rankGeoPoint
	rankKey
)

// valueRank returns v's type rank in the documented cross-type ordering. ok is
// false for the two value types with no sort order — array values and embedded
// entities — and for an absent (zero) value; such values are never orderable.
func valueRank(v dsstore.Value) (int, bool) {
	switch {
	case v.NullValue != nil:
		return rankNull, true
	case v.IntegerValue != nil:
		return rankInteger, true
	case v.TimestampValue != nil:
		return rankTimestamp, true
	case v.BooleanValue != nil:
		return rankBoolean, true
	case v.BlobValue != nil:
		return rankBlob, true
	case v.StringValue != nil:
		return rankString, true
	case v.DoubleValue != nil:
		if math.IsNaN(*v.DoubleValue) {
			return rankNaN, true
		}
		return rankDouble, true
	case v.GeoPointValue != nil:
		return rankGeoPoint, true
	case v.KeyValue != nil:
		return rankKey, true
	default:
		return 0, false
	}
}

// valueCompare compares two values for ordering, returning -1, 0, or 1. ok is
// false when either value has no sort order (an array, an embedded entity, or an
// absent value); every other cross-type pair is orderable via the documented
// type ordering (valueRank).
func valueCompare(a, b dsstore.Value) (int, bool) {
	ra, aok := valueRank(a)
	rb, bok := valueRank(b)
	if !aok || !bok {
		return 0, false
	}
	if ra != rb {
		return cmp.Compare(ra, rb), true
	}
	switch ra {
	case rankNull, rankNaN:
		// A null is the same value as any other null, and a NaN is the same
		// (NaN) value as any other NaN.
		return 0, true
	case rankInteger:
		return cmp.Compare(*a.IntegerValue, *b.IntegerValue), true
	case rankTimestamp:
		at, errA := time.Parse(time.RFC3339Nano, *a.TimestampValue)
		bt, errB := time.Parse(time.RFC3339Nano, *b.TimestampValue)
		if errA != nil || errB != nil {
			return 0, false
		}
		switch {
		case at.Before(bt):
			return -1, true
		case at.After(bt):
			return 1, true
		default:
			return 0, true
		}
	case rankBoolean:
		switch {
		case *a.BooleanValue == *b.BooleanValue:
			return 0, true
		case !*a.BooleanValue:
			return -1, true
		default:
			return 1, true
		}
	case rankBlob:
		return bytes.Compare(a.BlobValue, b.BlobValue), true
	case rankString:
		return cmp.Compare(*a.StringValue, *b.StringValue), true
	case rankDouble:
		return cmp.Compare(*a.DoubleValue, *b.DoubleValue), true
	case rankGeoPoint:
		// By latitude, then longitude (the documented geo-point sort order).
		if c := cmp.Compare(a.GeoPointValue.Latitude, b.GeoPointValue.Latitude); c != 0 {
			return c, true
		}
		return cmp.Compare(a.GeoPointValue.Longitude, b.GeoPointValue.Longitude), true
	case rankKey:
		// KeyValue holds the key's canonical string form; comparing those
		// lexically orders the path elements (an existing approximation of the
		// documented "by path elements" key ordering).
		return cmp.Compare(*a.KeyValue, *b.KeyValue), true
	}
	return 0, false
}

func valueEqual(a, b dsstore.Value) bool {
	// Integers and doubles are separate types in Datastore mode (the documented
	// value-type ordering), so `n = 3` does NOT match a stored `3.0` and vice
	// versa. A NaN equals another NaN: NaN is a single distinct value type.
	if a.IntegerValue != nil || b.IntegerValue != nil {
		return a.IntegerValue != nil && b.IntegerValue != nil && *a.IntegerValue == *b.IntegerValue
	}
	if a.DoubleValue != nil || b.DoubleValue != nil {
		if a.DoubleValue == nil || b.DoubleValue == nil {
			return false
		}
		return *a.DoubleValue == *b.DoubleValue ||
			(math.IsNaN(*a.DoubleValue) && math.IsNaN(*b.DoubleValue))
	}
	switch {
	case a.NullValue != nil || b.NullValue != nil:
		return a.NullValue != nil && b.NullValue != nil
	case a.BooleanValue != nil || b.BooleanValue != nil:
		return a.BooleanValue != nil && b.BooleanValue != nil && *a.BooleanValue == *b.BooleanValue
	case a.StringValue != nil || b.StringValue != nil:
		return a.StringValue != nil && b.StringValue != nil && *a.StringValue == *b.StringValue
	case a.TimestampValue != nil || b.TimestampValue != nil:
		return a.TimestampValue != nil && b.TimestampValue != nil && *a.TimestampValue == *b.TimestampValue
	case a.KeyValue != nil || b.KeyValue != nil:
		return a.KeyValue != nil && b.KeyValue != nil && *a.KeyValue == *b.KeyValue
	case a.BlobValue != nil || b.BlobValue != nil:
		return a.BlobValue != nil && b.BlobValue != nil && bytes.Equal(a.BlobValue, b.BlobValue)
	case a.GeoPointValue != nil || b.GeoPointValue != nil:
		return a.GeoPointValue != nil && b.GeoPointValue != nil && *a.GeoPointValue == *b.GeoPointValue
	case a.EntityValue != nil || b.EntityValue != nil:
		if a.EntityValue == nil || b.EntityValue == nil {
			return false
		}
		return entityEqual(*a.EntityValue, *b.EntityValue)
	case a.ArrayValue != nil || b.ArrayValue != nil:
		if a.ArrayValue == nil || b.ArrayValue == nil {
			return false
		}
		if len(a.ArrayValue.Values) != len(b.ArrayValue.Values) {
			return false
		}
		for i := range a.ArrayValue.Values {
			if !valueEqual(a.ArrayValue.Values[i], b.ArrayValue.Values[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func entityEqual(a, b dsstore.Entity) bool {
	if a.Key != b.Key {
		return false
	}
	if len(a.Properties) != len(b.Properties) {
		return false
	}
	for k, av := range a.Properties {
		bv, ok := b.Properties[k]
		if !ok || !valueEqual(av, bv) {
			return false
		}
	}
	return true
}

// aggregate computes one aggregation over the entities the nested query
// returned and returns its neutral Value.
func aggregate(agg Aggregation, entities []dsstore.Entity) (dsstore.Value, error) {
	switch agg.Op {
	case AggCount:
		n := int64(len(entities))
		if agg.UpTo != nil {
			if *agg.UpTo < 0 {
				return dsstore.Value{}, invalidArgument("count up_to must be non-negative")
			}
			if n > *agg.UpTo {
				n = *agg.UpTo
			}
		}
		return dsstore.Value{IntegerValue: &n}, nil
	case AggSum:
		return aggregateSum(agg.Property, entities)
	case AggAvg:
		return aggregateAvg(agg.Property, entities)
	default:
		return dsstore.Value{}, invalidArgument("aggregation has no operator")
	}
}

// aggregateSum sums the named property over entities, following the proto's
// documented Sum behavior:
//   - only integer and double values contribute; a missing property or a
//     non-numeric value (string, bool, null, key, array, ...) is skipped;
//   - an empty contributing set yields integer 0;
//   - the result is a 64-bit integer when every contributing value is an
//     integer and the sum does not overflow int64; otherwise it is a double.
func aggregateSum(name string, entities []dsstore.Entity) (dsstore.Value, error) {
	if name == "" {
		return dsstore.Value{}, invalidArgument("sum aggregation requires a property")
	}
	var (
		isum     int64
		fsum     float64
		allInt   = true
		overflow bool
	)
	for _, e := range entities {
		v, ok := e.Properties[name]
		if !ok {
			continue
		}
		switch {
		case v.IntegerValue != nil:
			iv := *v.IntegerValue
			if (iv > 0 && isum+iv < isum) || (iv < 0 && isum+iv > isum) {
				overflow = true
			}
			isum += iv
			fsum += float64(iv)
		case v.DoubleValue != nil:
			allInt = false
			fsum += *v.DoubleValue
		default:
			continue
		}
	}
	if allInt && !overflow {
		return dsstore.Value{IntegerValue: &isum}, nil
	}
	return dsstore.Value{DoubleValue: &fsum}, nil
}

// aggregateAvg averages the named property over entities, following the
// proto's documented Avg behavior: only integer and double values contribute;
// a missing property or a non-numeric value is skipped; an empty contributing
// set yields NULL; and the result is always a double.
func aggregateAvg(name string, entities []dsstore.Entity) (dsstore.Value, error) {
	if name == "" {
		return dsstore.Value{}, invalidArgument("avg aggregation requires a property")
	}
	var (
		sum float64
		n   int64
	)
	for _, e := range entities {
		v, ok := e.Properties[name]
		if !ok {
			continue
		}
		switch {
		case v.IntegerValue != nil:
			sum += float64(*v.IntegerValue)
			n++
		case v.DoubleValue != nil:
			sum += *v.DoubleValue
			n++
		default:
			continue
		}
	}
	if n == 0 {
		s := "NULL_VALUE"
		return dsstore.Value{NullValue: &s}, nil
	}
	avg := sum / float64(n)
	return dsstore.Value{DoubleValue: &avg}, nil
}

// propertyOpName renders a PropertyOp for error messages.
func propertyOpName(op PropertyOp) string {
	switch op {
	case PropertyEqual:
		return "EQUAL"
	case PropertyNotEqual:
		return "NOT_EQUAL"
	case PropertyIn:
		return "IN"
	case PropertyLessThan:
		return "LESS_THAN"
	case PropertyLessThanOrEqual:
		return "LESS_THAN_OR_EQUAL"
	case PropertyGreaterThan:
		return "GREATER_THAN"
	case PropertyGreaterThanOrEqual:
		return "GREATER_THAN_OR_EQUAL"
	case PropertyHasAncestor:
		return "HAS_ANCESTOR"
	default:
		return "PROPERTY_FILTER_OP_UNSPECIFIED"
	}
}

// compositeOpName renders a CompositeOp for error messages.
func compositeOpName(op CompositeOp) string {
	switch op {
	case CompositeAnd:
		return "AND"
	case CompositeOr:
		return "OR"
	default:
		return "COMPOSITE_FILTER_OP_UNSPECIFIED"
	}
}
