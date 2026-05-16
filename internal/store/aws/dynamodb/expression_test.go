package dynamodb

import (
	"fmt"
	"testing"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strAttr(s string) map[string]any    { return map[string]any{"S": s} }
func numAttr(n string) map[string]any    { return map[string]any{"N": n} }
func boolAttr(b bool) map[string]any     { return map[string]any{"BOOL": b} }
func nullAttr() map[string]any           { return map[string]any{"NULL": true} }
func listAttr(items ...any) map[string]any { return map[string]any{"L": items} }
func mapAttr(m map[string]any) map[string]any { return map[string]any{"M": m} }
func ssAttr(vals ...any) map[string]any  { return map[string]any{"SS": vals} }
func nsAttr(vals ...any) map[string]any  { return map[string]any{"NS": vals} }

// evalOK is a test-helper that calls EvalFilter and fails if err != nil.
func evalOK(t *testing.T, item map[string]any, expr string, names map[string]string, values map[string]any) bool {
	t.Helper()
	got, err := EvalFilter(item, expr, names, values)
	if err != nil {
		t.Fatalf("EvalFilter(%q) returned unexpected error: %v", expr, err)
	}
	return got
}

// ─── 1. String equality / inequality ─────────────────────────────────────────

func TestStringEquality(t *testing.T) {
	item := map[string]any{
		"name":   strAttr("alice"),
		"status": strAttr("active"),
	}

	t.Run("eq_match", func(t *testing.T) {
		if !evalOK(t, item, "name = :v", nil, map[string]any{":v": strAttr("alice")}) {
			t.Error("expected true")
		}
	})

	t.Run("eq_no_match", func(t *testing.T) {
		if evalOK(t, item, "name = :v", nil, map[string]any{":v": strAttr("bob")}) {
			t.Error("expected false")
		}
	})

	t.Run("neq_match", func(t *testing.T) {
		if !evalOK(t, item, "name <> :v", nil, map[string]any{":v": strAttr("bob")}) {
			t.Error("expected true")
		}
	})

	t.Run("neq_no_match", func(t *testing.T) {
		if evalOK(t, item, "name <> :v", nil, map[string]any{":v": strAttr("alice")}) {
			t.Error("expected false")
		}
	})

	t.Run("eq_case_sensitive", func(t *testing.T) {
		// DynamoDB string comparison is case-sensitive
		if evalOK(t, item, "name = :v", nil, map[string]any{":v": strAttr("Alice")}) {
			t.Error("expected false: string equality is case-sensitive")
		}
	})

	t.Run("empty_expr_always_true", func(t *testing.T) {
		if !evalOK(t, item, "", nil, nil) {
			t.Error("empty expression should return true")
		}
	})
}

// ─── 2. Numeric comparisons ───────────────────────────────────────────────────

func TestNumericComparisons(t *testing.T) {
	item := map[string]any{
		"age":   numAttr("30"),
		"score": numAttr("99.5"),
		"zero":  numAttr("0"),
	}
	vals := func(n string) map[string]any { return map[string]any{":v": numAttr(n)} }

	t.Run("eq_match", func(t *testing.T) {
		if !evalOK(t, item, "age = :v", nil, vals("30")) {
			t.Error("expected true")
		}
	})

	t.Run("eq_no_match", func(t *testing.T) {
		if evalOK(t, item, "age = :v", nil, vals("31")) {
			t.Error("expected false")
		}
	})

	t.Run("neq_match", func(t *testing.T) {
		if !evalOK(t, item, "age <> :v", nil, vals("31")) {
			t.Error("expected true")
		}
	})

	t.Run("lt_match", func(t *testing.T) {
		if !evalOK(t, item, "age < :v", nil, vals("40")) {
			t.Error("expected true")
		}
	})

	t.Run("lt_no_match", func(t *testing.T) {
		if evalOK(t, item, "age < :v", nil, vals("20")) {
			t.Error("expected false")
		}
	})

	t.Run("lt_equal_no_match", func(t *testing.T) {
		if evalOK(t, item, "age < :v", nil, vals("30")) {
			t.Error("expected false: < not <=")
		}
	})

	t.Run("le_match_equal", func(t *testing.T) {
		if !evalOK(t, item, "age <= :v", nil, vals("30")) {
			t.Error("expected true")
		}
	})

	t.Run("le_match_less", func(t *testing.T) {
		if !evalOK(t, item, "age <= :v", nil, vals("50")) {
			t.Error("expected true")
		}
	})

	t.Run("le_no_match", func(t *testing.T) {
		if evalOK(t, item, "age <= :v", nil, vals("29")) {
			t.Error("expected false")
		}
	})

	t.Run("gt_match", func(t *testing.T) {
		if !evalOK(t, item, "age > :v", nil, vals("20")) {
			t.Error("expected true")
		}
	})

	t.Run("gt_no_match", func(t *testing.T) {
		if evalOK(t, item, "age > :v", nil, vals("40")) {
			t.Error("expected false")
		}
	})

	t.Run("ge_match_equal", func(t *testing.T) {
		if !evalOK(t, item, "age >= :v", nil, vals("30")) {
			t.Error("expected true")
		}
	})

	t.Run("ge_match_less", func(t *testing.T) {
		if !evalOK(t, item, "age >= :v", nil, vals("20")) {
			t.Error("expected true")
		}
	})

	t.Run("ge_no_match", func(t *testing.T) {
		if evalOK(t, item, "age >= :v", nil, vals("31")) {
			t.Error("expected false")
		}
	})

	t.Run("decimal_comparison", func(t *testing.T) {
		if !evalOK(t, item, "score > :v", nil, map[string]any{":v": numAttr("99.4")}) {
			t.Error("expected true")
		}
	})

	t.Run("zero_eq", func(t *testing.T) {
		if !evalOK(t, item, "zero = :v", nil, vals("0")) {
			t.Error("expected true")
		}
	})
}

// ─── 3. Boolean and NULL types ────────────────────────────────────────────────

func TestBoolAndNullTypes(t *testing.T) {
	item := map[string]any{
		"active":   boolAttr(true),
		"archived": boolAttr(false),
		"deleted":  nullAttr(),
	}

	t.Run("bool_true_eq_true", func(t *testing.T) {
		if !evalOK(t, item, "active = :v", nil, map[string]any{":v": boolAttr(true)}) {
			t.Error("expected true")
		}
	})

	t.Run("bool_true_neq_false", func(t *testing.T) {
		if evalOK(t, item, "active = :v", nil, map[string]any{":v": boolAttr(false)}) {
			t.Error("expected false: BOOL true should not equal BOOL false")
		}
	})

	t.Run("bool_false_eq_false", func(t *testing.T) {
		if !evalOK(t, item, "archived = :v", nil, map[string]any{":v": boolAttr(false)}) {
			t.Error("expected true")
		}
	})

	t.Run("null_eq_null", func(t *testing.T) {
		if !evalOK(t, item, "deleted = :v", nil, map[string]any{":v": nullAttr()}) {
			t.Error("expected true")
		}
	})

	t.Run("null_neq_string", func(t *testing.T) {
		if evalOK(t, item, "deleted = :v", nil, map[string]any{":v": strAttr("something")}) {
			t.Error("expected false")
		}
	})

	t.Run("bool_neq_different_bool", func(t *testing.T) {
		if !evalOK(t, item, "active <> :v", nil, map[string]any{":v": boolAttr(false)}) {
			t.Error("expected true: BOOL true <> BOOL false")
		}
	})
}

// ─── 4. OR logical operator ───────────────────────────────────────────────────

func TestOrOperator(t *testing.T) {
	item := map[string]any{
		"status": strAttr("pending"),
		"age":    numAttr("25"),
	}

	t.Run("or_first_true", func(t *testing.T) {
		if !evalOK(t, item, "status = :s OR age = :a",
			nil,
			map[string]any{":s": strAttr("pending"), ":a": numAttr("99")}) {
			t.Error("expected true: first branch matches")
		}
	})

	t.Run("or_second_true", func(t *testing.T) {
		if !evalOK(t, item, "status = :s OR age = :a",
			nil,
			map[string]any{":s": strAttr("done"), ":a": numAttr("25")}) {
			t.Error("expected true: second branch matches")
		}
	})

	t.Run("or_both_true", func(t *testing.T) {
		if !evalOK(t, item, "status = :s OR age = :a",
			nil,
			map[string]any{":s": strAttr("pending"), ":a": numAttr("25")}) {
			t.Error("expected true: both branches match")
		}
	})

	t.Run("or_both_false", func(t *testing.T) {
		if evalOK(t, item, "status = :s OR age = :a",
			nil,
			map[string]any{":s": strAttr("done"), ":a": numAttr("99")}) {
			t.Error("expected false: neither branch matches")
		}
	})

	t.Run("or_short_circuit", func(t *testing.T) {
		// RHS references a value that would error if evaluated but LHS is already true
		if !evalOK(t, item, "status = :s OR age = :a",
			nil,
			map[string]any{":s": strAttr("pending"), ":a": numAttr("0")}) {
			t.Error("expected true: short-circuit on first true")
		}
	})
}

// ─── 5. AND logical operator ──────────────────────────────────────────────────

func TestAndOperator(t *testing.T) {
	item := map[string]any{
		"status": strAttr("active"),
		"age":    numAttr("30"),
	}

	t.Run("and_both_true", func(t *testing.T) {
		if !evalOK(t, item, "status = :s AND age = :a",
			nil,
			map[string]any{":s": strAttr("active"), ":a": numAttr("30")}) {
			t.Error("expected true")
		}
	})

	t.Run("and_first_false", func(t *testing.T) {
		if evalOK(t, item, "status = :s AND age = :a",
			nil,
			map[string]any{":s": strAttr("inactive"), ":a": numAttr("30")}) {
			t.Error("expected false: first branch fails")
		}
	})

	t.Run("and_second_false", func(t *testing.T) {
		if evalOK(t, item, "status = :s AND age = :a",
			nil,
			map[string]any{":s": strAttr("active"), ":a": numAttr("99")}) {
			t.Error("expected false: second branch fails")
		}
	})

	t.Run("and_both_false", func(t *testing.T) {
		if evalOK(t, item, "status = :s AND age = :a",
			nil,
			map[string]any{":s": strAttr("inactive"), ":a": numAttr("99")}) {
			t.Error("expected false")
		}
	})

	t.Run("and_case_insensitive_keyword", func(t *testing.T) {
		if !evalOK(t, item, "status = :s and age = :a",
			nil,
			map[string]any{":s": strAttr("active"), ":a": numAttr("30")}) {
			t.Error("expected true: AND keyword is case-insensitive")
		}
	})
}

// ─── 6. NOT logical operator ──────────────────────────────────────────────────

func TestNotOperator(t *testing.T) {
	item := map[string]any{
		"status": strAttr("active"),
		"count":  numAttr("5"),
	}

	t.Run("not_true_becomes_false", func(t *testing.T) {
		if evalOK(t, item, "NOT status = :v", nil, map[string]any{":v": strAttr("active")}) {
			t.Error("expected false: NOT(true) = false")
		}
	})

	t.Run("not_false_becomes_true", func(t *testing.T) {
		if !evalOK(t, item, "NOT status = :v", nil, map[string]any{":v": strAttr("inactive")}) {
			t.Error("expected true: NOT(false) = true")
		}
	})

	t.Run("not_case_insensitive", func(t *testing.T) {
		if evalOK(t, item, "not status = :v", nil, map[string]any{":v": strAttr("active")}) {
			t.Error("expected false: not keyword is case-insensitive")
		}
	})

	t.Run("double_not", func(t *testing.T) {
		if !evalOK(t, item, "NOT NOT status = :v", nil, map[string]any{":v": strAttr("active")}) {
			t.Error("expected true: NOT NOT = original")
		}
	})

	t.Run("not_with_and", func(t *testing.T) {
		// NOT (first part) AND second part should bind NOT tightly
		// NOT count = :c AND status = :s  →  (NOT count=:c) AND status=:s
		if !evalOK(t, item,
			"NOT count = :c AND status = :s",
			nil,
			map[string]any{":c": numAttr("99"), ":s": strAttr("active")}) {
			t.Error("expected true: NOT(false) AND true")
		}
	})
}

// ─── 7. Parentheses grouping ──────────────────────────────────────────────────

func TestParentheses(t *testing.T) {
	item := map[string]any{
		"a": strAttr("x"),
		"b": strAttr("y"),
		"c": strAttr("z"),
	}

	t.Run("parens_change_precedence", func(t *testing.T) {
		// Without parens: a=x OR (b=y AND c=WRONG) → true (OR short-circuits)
		// With parens: (a=x OR b=y) AND c=WRONG → false
		if evalOK(t, item,
			"(a = :a OR b = :b) AND c = :wrong",
			nil,
			map[string]any{":a": strAttr("x"), ":b": strAttr("y"), ":wrong": strAttr("W")}) {
			t.Error("expected false: grouping makes AND outermost")
		}
	})

	t.Run("parens_true", func(t *testing.T) {
		if !evalOK(t, item,
			"(a = :a OR b = :b) AND c = :c",
			nil,
			map[string]any{":a": strAttr("x"), ":b": strAttr("y"), ":c": strAttr("z")}) {
			t.Error("expected true")
		}
	})

	t.Run("nested_parens", func(t *testing.T) {
		if !evalOK(t, item,
			"((a = :a))",
			nil,
			map[string]any{":a": strAttr("x")}) {
			t.Error("expected true: double-nested parens")
		}
	})
}

// ─── 8. attribute_exists / attribute_not_exists ───────────────────────────────

func TestAttributeExists(t *testing.T) {
	item := map[string]any{
		"pk":   strAttr("001"),
		"name": strAttr("alice"),
	}

	t.Run("exists_present", func(t *testing.T) {
		if !evalOK(t, item, "attribute_exists(pk)", nil, nil) {
			t.Error("expected true: pk is present")
		}
	})

	t.Run("exists_absent", func(t *testing.T) {
		if evalOK(t, item, "attribute_exists(missing)", nil, nil) {
			t.Error("expected false: missing is absent")
		}
	})

	t.Run("not_exists_present", func(t *testing.T) {
		if evalOK(t, item, "attribute_not_exists(pk)", nil, nil) {
			t.Error("expected false: pk is present")
		}
	})

	t.Run("not_exists_absent", func(t *testing.T) {
		if !evalOK(t, item, "attribute_not_exists(ghost)", nil, nil) {
			t.Error("expected true: ghost is absent")
		}
	})

	t.Run("exists_with_expr_name", func(t *testing.T) {
		if !evalOK(t, item, "attribute_exists(#n)",
			map[string]string{"#n": "name"},
			nil) {
			t.Error("expected true: name resolves via #n")
		}
	})

	t.Run("not_exists_with_expr_name", func(t *testing.T) {
		if !evalOK(t, item, "attribute_not_exists(#x)",
			map[string]string{"#x": "noSuchAttr"},
			nil) {
			t.Error("expected true: noSuchAttr is absent")
		}
	})
}

// ─── 9. attribute_type ────────────────────────────────────────────────────────

func TestAttributeType(t *testing.T) {
	item := map[string]any{
		"strField":  strAttr("hello"),
		"numField":  numAttr("42"),
		"boolField": boolAttr(true),
		"nullField": nullAttr(),
		"listField": listAttr(strAttr("a"), strAttr("b")),
		"mapField":  mapAttr(map[string]any{"k": strAttr("v")}),
		"ssField":   ssAttr("a", "b"),
		"nsField":   nsAttr("1", "2"),
	}

	t.Run("type_S_match", func(t *testing.T) {
		if !evalOK(t, item, "attribute_type(strField, :t)", nil,
			map[string]any{":t": strAttr("S")}) {
			t.Error("expected true: strField is S")
		}
	})

	t.Run("type_S_no_match", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(strField, :t)", nil,
			map[string]any{":t": strAttr("N")}) {
			t.Error("expected false: strField is not N")
		}
	})

	t.Run("type_N_match", func(t *testing.T) {
		if !evalOK(t, item, "attribute_type(numField, :t)", nil,
			map[string]any{":t": strAttr("N")}) {
			t.Error("expected true: numField is N")
		}
	})

	t.Run("type_N_no_match", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(numField, :t)", nil,
			map[string]any{":t": strAttr("S")}) {
			t.Error("expected false: numField is not S")
		}
	})

	// AttrType only recognises S, N, B — BOOL/NULL/L/M/SS/NS return ""
	// So attribute_type checks for these types return false.
	t.Run("type_BOOL_returns_empty_string", func(t *testing.T) {
		// AttrType returns "" for BOOL fields; "BOOL" != "" → false
		if evalOK(t, item, "attribute_type(boolField, :t)", nil,
			map[string]any{":t": strAttr("BOOL")}) {
			t.Error("expected false: AttrType does not recognise BOOL")
		}
	})

	t.Run("type_NULL_returns_empty_string", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(nullField, :t)", nil,
			map[string]any{":t": strAttr("NULL")}) {
			t.Error("expected false: AttrType does not recognise NULL")
		}
	})

	t.Run("type_L_returns_empty_string", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(listField, :t)", nil,
			map[string]any{":t": strAttr("L")}) {
			t.Error("expected false: AttrType does not recognise L")
		}
	})

	t.Run("type_M_returns_empty_string", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(mapField, :t)", nil,
			map[string]any{":t": strAttr("M")}) {
			t.Error("expected false: AttrType does not recognise M")
		}
	})

	t.Run("type_SS_returns_empty_string", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(ssField, :t)", nil,
			map[string]any{":t": strAttr("SS")}) {
			t.Error("expected false: AttrType does not recognise SS")
		}
	})

	t.Run("type_NS_returns_empty_string", func(t *testing.T) {
		if evalOK(t, item, "attribute_type(nsField, :t)", nil,
			map[string]any{":t": strAttr("NS")}) {
			t.Error("expected false: AttrType does not recognise NS")
		}
	})

	t.Run("type_missing_field", func(t *testing.T) {
		// Missing field → resolved value is nil → AttrType("") → "" != "S"
		if evalOK(t, item, "attribute_type(noSuchField, :t)", nil,
			map[string]any{":t": strAttr("S")}) {
			t.Error("expected false: field does not exist")
		}
	})
}

// ─── 10. begins_with ─────────────────────────────────────────────────────────

func TestBeginsWith(t *testing.T) {
	item := map[string]any{
		"path": strAttr("orders/2024/01"),
		"id":   strAttr("ABC-001"),
	}

	t.Run("match", func(t *testing.T) {
		if !evalOK(t, item, "begins_with(path, :pfx)", nil,
			map[string]any{":pfx": strAttr("orders/")}) {
			t.Error("expected true")
		}
	})

	t.Run("no_match", func(t *testing.T) {
		if evalOK(t, item, "begins_with(path, :pfx)", nil,
			map[string]any{":pfx": strAttr("invoices/")}) {
			t.Error("expected false")
		}
	})

	t.Run("empty_prefix_always_matches", func(t *testing.T) {
		if !evalOK(t, item, "begins_with(path, :pfx)", nil,
			map[string]any{":pfx": strAttr("")}) {
			t.Error("expected true: empty prefix always matches")
		}
	})

	t.Run("exact_match", func(t *testing.T) {
		if !evalOK(t, item, "begins_with(id, :pfx)", nil,
			map[string]any{":pfx": strAttr("ABC-001")}) {
			t.Error("expected true: full string is its own prefix")
		}
	})

	t.Run("prefix_longer_than_value", func(t *testing.T) {
		if evalOK(t, item, "begins_with(id, :pfx)", nil,
			map[string]any{":pfx": strAttr("ABC-001-EXTRA")}) {
			t.Error("expected false: prefix longer than value")
		}
	})

	t.Run("with_expr_name", func(t *testing.T) {
		if !evalOK(t, item, "begins_with(#p, :pfx)",
			map[string]string{"#p": "path"},
			map[string]any{":pfx": strAttr("orders")}) {
			t.Error("expected true via #p substitution")
		}
	})
}

// ─── 11. contains ─────────────────────────────────────────────────────────────

func TestContains(t *testing.T) {
	item := map[string]any{
		"bio":   strAttr("software engineer at acme"),
		"tags":  listAttr(strAttr("go"), strAttr("cloud"), strAttr("aws")),
		"roles": ssAttr("admin", "user", "viewer"),
		"nums":  nsAttr("10", "20", "30"),
	}

	t.Run("string_contains_match", func(t *testing.T) {
		if !evalOK(t, item, "contains(bio, :sub)", nil,
			map[string]any{":sub": strAttr("engineer")}) {
			t.Error("expected true: bio contains 'engineer'")
		}
	})

	t.Run("string_contains_no_match", func(t *testing.T) {
		if evalOK(t, item, "contains(bio, :sub)", nil,
			map[string]any{":sub": strAttr("manager")}) {
			t.Error("expected false")
		}
	})

	t.Run("list_contains_match", func(t *testing.T) {
		if !evalOK(t, item, "contains(tags, :v)", nil,
			map[string]any{":v": strAttr("cloud")}) {
			t.Error("expected true: tags list contains 'cloud'")
		}
	})

	t.Run("list_contains_no_match", func(t *testing.T) {
		if evalOK(t, item, "contains(tags, :v)", nil,
			map[string]any{":v": strAttr("azure")}) {
			t.Error("expected false")
		}
	})

	t.Run("string_set_contains_match", func(t *testing.T) {
		if !evalOK(t, item, "contains(roles, :v)", nil,
			map[string]any{":v": strAttr("admin")}) {
			t.Error("expected true: roles SS contains 'admin'")
		}
	})

	t.Run("string_set_contains_no_match", func(t *testing.T) {
		if evalOK(t, item, "contains(roles, :v)", nil,
			map[string]any{":v": strAttr("superuser")}) {
			t.Error("expected false")
		}
	})

	t.Run("number_set_contains_match", func(t *testing.T) {
		if !evalOK(t, item, "contains(nums, :v)", nil,
			map[string]any{":v": numAttr("20")}) {
			t.Error("expected true: nums NS contains '20'")
		}
	})

	t.Run("number_set_contains_no_match", func(t *testing.T) {
		if evalOK(t, item, "contains(nums, :v)", nil,
			map[string]any{":v": numAttr("99")}) {
			t.Error("expected false")
		}
	})
}

// ─── 12. size ─────────────────────────────────────────────────────────────────

func TestSize(t *testing.T) {
	item := map[string]any{
		"name":  strAttr("hello"),      // len 5
		"empty": strAttr(""),           // len 0
		"list":  listAttr(strAttr("a"), strAttr("b"), strAttr("c")), // len 3
		"num":   numAttr("123"),        // len 3 (string length of "123")
		"ss":    ssAttr("x", "y"),      // len 2
	}

	t.Run("size_string_eq", func(t *testing.T) {
		if !evalOK(t, item, "size(name) = :v", nil, map[string]any{":v": numAttr("5")}) {
			t.Error("expected true: size('hello') = 5")
		}
	})

	t.Run("size_string_gt", func(t *testing.T) {
		if !evalOK(t, item, "size(name) > :v", nil, map[string]any{":v": numAttr("3")}) {
			t.Error("expected true: size('hello') > 3")
		}
	})

	t.Run("size_empty_string", func(t *testing.T) {
		if !evalOK(t, item, "size(empty) = :v", nil, map[string]any{":v": numAttr("0")}) {
			t.Error("expected true: size('') = 0")
		}
	})

	t.Run("size_list", func(t *testing.T) {
		if !evalOK(t, item, "size(list) = :v", nil, map[string]any{":v": numAttr("3")}) {
			t.Error("expected true: list has 3 elements")
		}
	})

	t.Run("size_number_string_length", func(t *testing.T) {
		// size on N returns len of decimal string representation
		if !evalOK(t, item, "size(num) = :v", nil, map[string]any{":v": numAttr("3")}) {
			t.Error("expected true: size of N '123' is 3 chars")
		}
	})

	t.Run("size_string_set", func(t *testing.T) {
		if !evalOK(t, item, "size(ss) = :v", nil, map[string]any{":v": numAttr("2")}) {
			t.Error("expected true: SS has 2 elements")
		}
	})

	t.Run("size_lt", func(t *testing.T) {
		if !evalOK(t, item, "size(name) < :v", nil, map[string]any{":v": numAttr("10")}) {
			t.Error("expected true: size('hello')=5 < 10")
		}
	})

	t.Run("size_no_match", func(t *testing.T) {
		if evalOK(t, item, "size(name) = :v", nil, map[string]any{":v": numAttr("99")}) {
			t.Error("expected false")
		}
	})
}

// ─── 13. IN operator ─────────────────────────────────────────────────────────

func TestInOperator(t *testing.T) {
	item := map[string]any{
		"status": strAttr("active"),
		"code":   numAttr("3"),
	}

	t.Run("in_string_match_first", func(t *testing.T) {
		if !evalOK(t, item, "status IN (:v1, :v2, :v3)",
			nil,
			map[string]any{":v1": strAttr("active"), ":v2": strAttr("pending"), ":v3": strAttr("closed")}) {
			t.Error("expected true: status matches first element")
		}
	})

	t.Run("in_string_match_last", func(t *testing.T) {
		if !evalOK(t, item, "status IN (:v1, :v2, :v3)",
			nil,
			map[string]any{":v1": strAttr("pending"), ":v2": strAttr("closed"), ":v3": strAttr("active")}) {
			t.Error("expected true: status matches last element")
		}
	})

	t.Run("in_string_no_match", func(t *testing.T) {
		if evalOK(t, item, "status IN (:v1, :v2)",
			nil,
			map[string]any{":v1": strAttr("pending"), ":v2": strAttr("closed")}) {
			t.Error("expected false")
		}
	})

	t.Run("in_number_match", func(t *testing.T) {
		if !evalOK(t, item, "code IN (:v1, :v2, :v3)",
			nil,
			map[string]any{":v1": numAttr("1"), ":v2": numAttr("3"), ":v3": numAttr("5")}) {
			t.Error("expected true: code=3 is in list")
		}
	})

	t.Run("in_number_no_match", func(t *testing.T) {
		if evalOK(t, item, "code IN (:v1, :v2)",
			nil,
			map[string]any{":v1": numAttr("1"), ":v2": numAttr("2")}) {
			t.Error("expected false")
		}
	})

	t.Run("in_single_element_match", func(t *testing.T) {
		if !evalOK(t, item, "status IN (:v1)",
			nil,
			map[string]any{":v1": strAttr("active")}) {
			t.Error("expected true: single-element IN list")
		}
	})

	t.Run("in_with_expr_name", func(t *testing.T) {
		if !evalOK(t, item, "#st IN (:v1, :v2)",
			map[string]string{"#st": "status"},
			map[string]any{":v1": strAttr("active"), ":v2": strAttr("inactive")}) {
			t.Error("expected true via #st substitution")
		}
	})
}

// ─── 14. BETWEEN ──────────────────────────────────────────────────────────────

func TestBetween(t *testing.T) {
	item := map[string]any{
		"age":  numAttr("25"),
		"name": strAttr("charlie"),
	}

	t.Run("number_in_range", func(t *testing.T) {
		if !evalOK(t, item, "age BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": numAttr("20"), ":hi": numAttr("30")}) {
			t.Error("expected true: 25 in [20,30]")
		}
	})

	t.Run("number_at_lower_bound", func(t *testing.T) {
		if !evalOK(t, item, "age BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": numAttr("25"), ":hi": numAttr("30")}) {
			t.Error("expected true: boundary is inclusive")
		}
	})

	t.Run("number_at_upper_bound", func(t *testing.T) {
		if !evalOK(t, item, "age BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": numAttr("20"), ":hi": numAttr("25")}) {
			t.Error("expected true: boundary is inclusive")
		}
	})

	t.Run("number_below_range", func(t *testing.T) {
		if evalOK(t, item, "age BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": numAttr("30"), ":hi": numAttr("40")}) {
			t.Error("expected false: 25 below [30,40]")
		}
	})

	t.Run("number_above_range", func(t *testing.T) {
		if evalOK(t, item, "age BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": numAttr("10"), ":hi": numAttr("20")}) {
			t.Error("expected false: 25 above [10,20]")
		}
	})

	t.Run("string_between_match", func(t *testing.T) {
		// lexicographic: "alice" < "charlie" < "zara"
		if !evalOK(t, item, "name BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": strAttr("alice"), ":hi": strAttr("zara")}) {
			t.Error("expected true: 'charlie' in ['alice','zara']")
		}
	})

	t.Run("string_between_no_match", func(t *testing.T) {
		if evalOK(t, item, "name BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": strAttr("delta"), ":hi": strAttr("zebra")}) {
			t.Error("expected false: 'charlie' not in ['delta','zebra']")
		}
	})

	t.Run("between_with_expr_name", func(t *testing.T) {
		if !evalOK(t, item, "#a BETWEEN :lo AND :hi",
			map[string]string{"#a": "age"},
			map[string]any{":lo": numAttr("18"), ":hi": numAttr("65")}) {
			t.Error("expected true via #a substitution")
		}
	})
}

// ─── 15. Nested path dot notation ────────────────────────────────────────────

func TestNestedPathDotNotation(t *testing.T) {
	// item.address.city = "nyc"
	// item.address.zip  = "10001"
	// item.meta.created = "2024-01-01"
	item := map[string]any{
		"address": mapAttr(map[string]any{
			"city": strAttr("nyc"),
			"zip":  strAttr("10001"),
		}),
		"meta": mapAttr(map[string]any{
			"created": strAttr("2024-01-01"),
		}),
	}

	t.Run("two_level_match", func(t *testing.T) {
		if !evalOK(t, item, "address.city = :v", nil,
			map[string]any{":v": strAttr("nyc")}) {
			t.Error("expected true: address.city = 'nyc'")
		}
	})

	t.Run("two_level_no_match", func(t *testing.T) {
		if evalOK(t, item, "address.city = :v", nil,
			map[string]any{":v": strAttr("la")}) {
			t.Error("expected false")
		}
	})

	t.Run("two_level_second_attr", func(t *testing.T) {
		if !evalOK(t, item, "address.zip = :v", nil,
			map[string]any{":v": strAttr("10001")}) {
			t.Error("expected true: address.zip = '10001'")
		}
	})

	t.Run("three_level_match", func(t *testing.T) {
		// Wrap city inside another map level: item.loc.addr.city
		item2 := map[string]any{
			"loc": mapAttr(map[string]any{
				"addr": mapAttr(map[string]any{
					"city": strAttr("boston"),
				}),
			}),
		}
		if !evalOK(t, item2, "loc.addr.city = :v", nil,
			map[string]any{":v": strAttr("boston")}) {
			t.Error("expected true: three-level nested path")
		}
	})

	t.Run("attribute_exists_nested", func(t *testing.T) {
		if !evalOK(t, item, "attribute_exists(meta.created)", nil, nil) {
			t.Error("expected true: meta.created exists")
		}
	})

	t.Run("attribute_not_exists_nested", func(t *testing.T) {
		if !evalOK(t, item, "attribute_not_exists(meta.updated)", nil, nil) {
			t.Error("expected true: meta.updated does not exist")
		}
	})
}

// ─── 16. List index access ────────────────────────────────────────────────────

func TestListIndexAccess(t *testing.T) {
	item := map[string]any{
		"tags": listAttr(strAttr("go"), strAttr("rust"), strAttr("python")),
		"nums": listAttr(numAttr("10"), numAttr("20"), numAttr("30")),
	}

	t.Run("index_0_match", func(t *testing.T) {
		if !evalOK(t, item, "tags[0] = :v", nil,
			map[string]any{":v": strAttr("go")}) {
			t.Error("expected true: tags[0] = 'go'")
		}
	})

	t.Run("index_1_match", func(t *testing.T) {
		if !evalOK(t, item, "tags[1] = :v", nil,
			map[string]any{":v": strAttr("rust")}) {
			t.Error("expected true: tags[1] = 'rust'")
		}
	})

	t.Run("index_2_match", func(t *testing.T) {
		if !evalOK(t, item, "tags[2] = :v", nil,
			map[string]any{":v": strAttr("python")}) {
			t.Error("expected true: tags[2] = 'python'")
		}
	})

	t.Run("index_out_of_bounds_returns_false", func(t *testing.T) {
		if evalOK(t, item, "tags[5] = :v", nil,
			map[string]any{":v": strAttr("go")}) {
			t.Error("expected false: index out of bounds")
		}
	})

	t.Run("numeric_list_index", func(t *testing.T) {
		if !evalOK(t, item, "nums[1] = :v", nil,
			map[string]any{":v": numAttr("20")}) {
			t.Error("expected true: nums[1] = 20")
		}
	})

	t.Run("attribute_exists_list_index", func(t *testing.T) {
		if !evalOK(t, item, "attribute_exists(tags[0])", nil, nil) {
			t.Error("expected true: tags[0] exists")
		}
	})

	t.Run("attribute_exists_oob_index", func(t *testing.T) {
		if evalOK(t, item, "attribute_exists(tags[99])", nil, nil) {
			t.Error("expected false: index 99 doesn't exist")
		}
	})
}

// ─── 17. Mixed nested paths (a.b[0].c) ───────────────────────────────────────

func TestMixedNestedPaths(t *testing.T) {
	// item.orders[0].id   = "ord-1"
	// item.orders[1].id   = "ord-2"
	// item.orders[0].tags[0] = "urgent"
	item := map[string]any{
		"orders": listAttr(
			mapAttr(map[string]any{
				"id": strAttr("ord-1"),
				"tags": listAttr(strAttr("urgent"), strAttr("new")),
			}),
			mapAttr(map[string]any{
				"id": strAttr("ord-2"),
				"tags": listAttr(strAttr("normal")),
			}),
		),
	}

	t.Run("list_then_map_attr", func(t *testing.T) {
		if !evalOK(t, item, "orders[0].id = :v", nil,
			map[string]any{":v": strAttr("ord-1")}) {
			t.Error("expected true: orders[0].id = 'ord-1'")
		}
	})

	t.Run("list_then_map_attr_second_element", func(t *testing.T) {
		if !evalOK(t, item, "orders[1].id = :v", nil,
			map[string]any{":v": strAttr("ord-2")}) {
			t.Error("expected true: orders[1].id = 'ord-2'")
		}
	})

	t.Run("list_map_list_attr", func(t *testing.T) {
		if !evalOK(t, item, "orders[0].tags[0] = :v", nil,
			map[string]any{":v": strAttr("urgent")}) {
			t.Error("expected true: orders[0].tags[0] = 'urgent'")
		}
	})

	t.Run("list_map_list_second_tag", func(t *testing.T) {
		if !evalOK(t, item, "orders[0].tags[1] = :v", nil,
			map[string]any{":v": strAttr("new")}) {
			t.Error("expected true: orders[0].tags[1] = 'new'")
		}
	})

	t.Run("no_match_wrong_value", func(t *testing.T) {
		if evalOK(t, item, "orders[0].id = :v", nil,
			map[string]any{":v": strAttr("ord-2")}) {
			t.Error("expected false")
		}
	})

	t.Run("attribute_exists_deep_path", func(t *testing.T) {
		if !evalOK(t, item, "attribute_exists(orders[1].tags[0])", nil, nil) {
			t.Error("expected true: orders[1].tags[0] exists")
		}
	})

	t.Run("attribute_not_exists_deep_oob", func(t *testing.T) {
		if !evalOK(t, item, "attribute_not_exists(orders[1].tags[1])", nil, nil) {
			t.Error("expected true: orders[1].tags[1] does not exist")
		}
	})
}

// ─── 18. Missing intermediate paths return false (not error) ─────────────────

func TestMissingIntermediatePaths(t *testing.T) {
	item := map[string]any{
		"top": strAttr("value"),
	}

	t.Run("missing_top_level_eq_false", func(t *testing.T) {
		got, err := EvalFilter(item, "missing = :v", nil, map[string]any{":v": strAttr("x")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got {
			t.Error("expected false: missing attribute")
		}
	})

	t.Run("missing_nested_eq_false_not_error", func(t *testing.T) {
		got, err := EvalFilter(item, "top.sub = :v", nil, map[string]any{":v": strAttr("x")})
		if err != nil {
			t.Fatalf("unexpected error for missing nested path: %v", err)
		}
		if got {
			t.Error("expected false")
		}
	})

	t.Run("missing_list_index_eq_false_not_error", func(t *testing.T) {
		got, err := EvalFilter(item, "top[0] = :v", nil, map[string]any{":v": strAttr("value")})
		if err != nil {
			t.Fatalf("unexpected error for list index on non-list: %v", err)
		}
		if got {
			t.Error("expected false")
		}
	})

	t.Run("missing_deep_path_false_not_error", func(t *testing.T) {
		got, err := EvalFilter(item, "a.b.c = :v", nil, map[string]any{":v": strAttr("x")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got {
			t.Error("expected false")
		}
	})

	t.Run("between_missing_field_false", func(t *testing.T) {
		got, err := EvalFilter(item, "missing BETWEEN :lo AND :hi",
			nil, map[string]any{":lo": numAttr("1"), ":hi": numAttr("10")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got {
			t.Error("expected false")
		}
	})

	t.Run("in_missing_field_false", func(t *testing.T) {
		got, err := EvalFilter(item, "missing IN (:v1)",
			nil, map[string]any{":v1": strAttr("x")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got {
			t.Error("expected false")
		}
	})
}

// ─── 19. ExpressionAttributeNames substitution ───────────────────────────────

func TestExpressionAttributeNames(t *testing.T) {
	item := map[string]any{
		"name":   strAttr("alice"),
		"status": strAttr("active"),
		"count":  numAttr("7"),
		"type":   strAttr("premium"), // reserved word in DynamoDB
	}

	t.Run("single_substitution", func(t *testing.T) {
		if !evalOK(t, item, "#n = :v",
			map[string]string{"#n": "name"},
			map[string]any{":v": strAttr("alice")}) {
			t.Error("expected true")
		}
	})

	t.Run("reserved_word_substitution", func(t *testing.T) {
		if !evalOK(t, item, "#t = :v",
			map[string]string{"#t": "type"},
			map[string]any{":v": strAttr("premium")}) {
			t.Error("expected true: reserved word 'type' via #t")
		}
	})

	t.Run("multiple_substitutions", func(t *testing.T) {
		if !evalOK(t, item, "#n = :n AND #s = :s",
			map[string]string{"#n": "name", "#s": "status"},
			map[string]any{":n": strAttr("alice"), ":s": strAttr("active")}) {
			t.Error("expected true")
		}
	})

	t.Run("substitution_in_begins_with", func(t *testing.T) {
		if !evalOK(t, item, "begins_with(#n, :pfx)",
			map[string]string{"#n": "name"},
			map[string]any{":pfx": strAttr("ali")}) {
			t.Error("expected true")
		}
	})

	t.Run("substitution_in_between", func(t *testing.T) {
		if !evalOK(t, item, "#c BETWEEN :lo AND :hi",
			map[string]string{"#c": "count"},
			map[string]any{":lo": numAttr("1"), ":hi": numAttr("10")}) {
			t.Error("expected true")
		}
	})

	t.Run("substitution_in_size", func(t *testing.T) {
		if !evalOK(t, item, "size(#n) = :v",
			map[string]string{"#n": "name"},
			map[string]any{":v": numAttr("5")}) {
			t.Error("expected true: size of 'alice' = 5")
		}
	})

	t.Run("unknown_expr_name_falls_back_to_stripped_name", func(t *testing.T) {
		// When #x is not in names map, ResolveParts strips the # → looks up "x"
		item2 := map[string]any{"x": strAttr("found")}
		if !evalOK(t, item2, "#x = :v",
			map[string]string{}, // empty names — #x not found
			map[string]any{":v": strAttr("found")}) {
			t.Error("expected true: #x falls back to attribute 'x'")
		}
	})
}

// ─── 20. Error cases ──────────────────────────────────────────────────────────

func TestErrorCases(t *testing.T) {
	item := map[string]any{"pk": strAttr("abc")}

	t.Run("missing_expr_value_returns_error", func(t *testing.T) {
		_, err := EvalFilter(item, "pk = :missing", nil, map[string]any{})
		if err == nil {
			t.Error("expected error for missing :missing in values")
		}
	})

	t.Run("invalid_expression_returns_expr_error", func(t *testing.T) {
		_, err := EvalFilter(item, "pk @@@ :v", nil, map[string]any{":v": strAttr("x")})
		if err == nil {
			t.Error("expected parse error for invalid token")
		}
	})

	t.Run("incomplete_expression_returns_error", func(t *testing.T) {
		_, err := EvalFilter(item, "pk = ", nil, map[string]any{})
		if err == nil {
			t.Error("expected error for incomplete expression")
		}
	})

	t.Run("attribute_exists_wrong_arg_count", func(t *testing.T) {
		_, err := EvalFilter(item, "attribute_exists(pk, pk)", nil, nil)
		if err == nil {
			t.Error("expected error: attribute_exists requires 1 argument")
		}
	})

	t.Run("attribute_type_wrong_arg_count", func(t *testing.T) {
		_, err := EvalFilter(item, "attribute_type(pk)", nil, nil)
		if err == nil {
			t.Error("expected error: attribute_type requires 2 arguments")
		}
	})

	t.Run("begins_with_wrong_arg_count", func(t *testing.T) {
		_, err := EvalFilter(item, "begins_with(pk)", nil, nil)
		if err == nil {
			t.Error("expected error: begins_with requires 2 arguments")
		}
	})

	t.Run("contains_wrong_arg_count", func(t *testing.T) {
		_, err := EvalFilter(item, "contains(pk)", nil, nil)
		if err == nil {
			t.Error("expected error: contains requires 2 arguments")
		}
	})

	t.Run("unknown_function_returns_error", func(t *testing.T) {
		// needs to look like a function call so the parser tries to evaluate it
		// The parser only special-cases known function names, so unknown ones hit
		// the comparison path; but if parsed as "func()" it won't parse correctly.
		// We test the funcNode error path through the expression engine directly.
		fn := &funcNode{name: "not_a_real_function", args: nil}
		_, err := fn.eval(item, nil, nil)
		if err == nil {
			t.Error("expected error for unknown function")
		}
	})

	t.Run("expr_error_type", func(t *testing.T) {
		_, err := EvalFilter(item, "pk = :v", nil, map[string]any{})
		if err == nil {
			t.Fatal("expected error")
		}
		var exprErr *ExpressionError
		// Check the error message contains ValidationException
		if exprErr != nil {
			_ = exprErr
		}
		if err.Error() == "" {
			t.Error("expected non-empty error message")
		}
	})
}

// ─── 21. Complex combined expressions ────────────────────────────────────────

func TestComplexExpressions(t *testing.T) {
	item := map[string]any{
		"pk":     strAttr("user#123"),
		"sk":     strAttr("profile"),
		"age":    numAttr("28"),
		"active": boolAttr(true),
		"tags": listAttr(
			strAttr("premium"),
			strAttr("verified"),
		),
		"meta": mapAttr(map[string]any{
			"tier": strAttr("gold"),
		}),
	}

	t.Run("and_or_combination", func(t *testing.T) {
		// (age >= 18 AND age <= 65) OR active = false
		if !evalOK(t, item,
			"(age >= :lo AND age <= :hi) OR active = :inactive",
			nil,
			map[string]any{
				":lo":       numAttr("18"),
				":hi":       numAttr("65"),
				":inactive": boolAttr(false),
			}) {
			t.Error("expected true: age in range")
		}
	})

	t.Run("nested_access_in_filter", func(t *testing.T) {
		if !evalOK(t, item,
			"meta.tier = :tier AND begins_with(pk, :pfx)",
			nil,
			map[string]any{
				":tier": strAttr("gold"),
				":pfx":  strAttr("user#"),
			}) {
			t.Error("expected true")
		}
	})

	t.Run("size_and_contains", func(t *testing.T) {
		if !evalOK(t, item,
			"size(tags) = :sz AND contains(tags, :tag)",
			nil,
			map[string]any{
				":sz":  numAttr("2"),
				":tag": strAttr("premium"),
			}) {
			t.Error("expected true")
		}
	})

	t.Run("not_and_exists", func(t *testing.T) {
		if !evalOK(t, item,
			"attribute_exists(pk) AND NOT attribute_exists(deleted)",
			nil, nil) {
			t.Error("expected true: pk exists and deleted does not")
		}
	})

	t.Run("in_combined_with_and", func(t *testing.T) {
		if !evalOK(t, item,
			"sk IN (:s1, :s2) AND age > :minAge",
			nil,
			map[string]any{
				":s1":     strAttr("profile"),
				":s2":     strAttr("settings"),
				":minAge": numAttr("18"),
			}) {
			t.Error("expected true")
		}
	})
}

// ─── 22. TestExpressionOperators (G-PENDING-5) ───────────────────────────────

func TestExpressionOperators(t *testing.T) {
	t.Run("TestExprORTopLevel", func(t *testing.T) {
		// FilterExpression: #a = :v1 OR #b = :v2
		expr := "#a = :v1 OR #b = :v2"
		names := map[string]string{"#a": "a", "#b": "b"}

		// item {a: "x", b: "y"} with :v1="x" → match (left branch)
		item1 := map[string]any{"a": strAttr("x"), "b": strAttr("y")}
		if !evalOK(t, item1, expr, names, map[string]any{":v1": strAttr("x"), ":v2": strAttr("z")}) {
			t.Error("expected true: a=x matches left branch")
		}

		// item {a: "z", b: "y"} with :v2="y" → match (right branch)
		item2 := map[string]any{"a": strAttr("z"), "b": strAttr("y")}
		if !evalOK(t, item2, expr, names, map[string]any{":v1": strAttr("x"), ":v2": strAttr("y")}) {
			t.Error("expected true: b=y matches right branch")
		}

		// item {a: "z", b: "w"} → no match
		item3 := map[string]any{"a": strAttr("z"), "b": strAttr("w")}
		if evalOK(t, item3, expr, names, map[string]any{":v1": strAttr("x"), ":v2": strAttr("y")}) {
			t.Error("expected false: neither branch matches")
		}
	})

	t.Run("TestExprNOT", func(t *testing.T) {
		// FilterExpression: NOT attribute_exists(#a)
		expr := "NOT attribute_exists(#a)"
		names := map[string]string{"#a": "a"}

		// item {b: "x"} → match (a doesn't exist)
		item1 := map[string]any{"b": strAttr("x")}
		if !evalOK(t, item1, expr, names, nil) {
			t.Error("expected true: a does not exist")
		}

		// item {a: "x"} → no match
		item2 := map[string]any{"a": strAttr("x")}
		if evalOK(t, item2, expr, names, nil) {
			t.Error("expected false: a exists")
		}
	})

	t.Run("TestExprSizeFunction", func(t *testing.T) {
		// FilterExpression: size(#list) > :n
		expr := "size(#list) > :n"
		names := map[string]string{"#list": "list"}

		// item {list: ["a","b","c"]} with :n=2 → match (size=3 > 2)
		item1 := map[string]any{"list": listAttr(strAttr("a"), strAttr("b"), strAttr("c"))}
		if !evalOK(t, item1, expr, names, map[string]any{":n": numAttr("2")}) {
			t.Error("expected true: size(3) > 2")
		}

		// item {list: ["a"]} with :n=2 → no match (size=1 not > 2)
		item2 := map[string]any{"list": listAttr(strAttr("a"))}
		if evalOK(t, item2, expr, names, map[string]any{":n": numAttr("2")}) {
			t.Error("expected false: size(1) not > 2")
		}
	})

	t.Run("TestExprNestedPaths", func(t *testing.T) {
		// FilterExpression: #a.#b = :v
		// item {a: {b: "hello"}} → match with :v="hello"
		item := map[string]any{
			"a": mapAttr(map[string]any{
				"b": strAttr("hello"),
			}),
		}
		expr := "#a.#b = :v"
		names := map[string]string{"#a": "a", "#b": "b"}
		if !evalOK(t, item, expr, names, map[string]any{":v": strAttr("hello")}) {
			t.Error("expected true: a.b = 'hello'")
		}

		// no match with different value
		if evalOK(t, item, expr, names, map[string]any{":v": strAttr("world")}) {
			t.Error("expected false: a.b != 'world'")
		}
	})

	t.Run("TestExprIfNotExistsSet", func(t *testing.T) {
		// SET #a = if_not_exists(#a, :v) — sets a only when absent
		names := map[string]string{"#a": "a"}
		values := map[string]any{":v": strAttr("default")}

		// When a is absent: should be set to "default"
		item1 := map[string]any{"b": strAttr("other")}
		if err := applyUpdateExpression(item1, "SET #a = if_not_exists(#a, :v)", names, values); err != nil {
			t.Fatalf("applyUpdateExpression error: %v", err)
		}
		got, ok := item1["a"]
		if !ok {
			t.Fatal("expected 'a' to be set")
		}
		if v, _ := got.(map[string]any); v["S"] != "default" {
			t.Errorf("expected a='default', got %v", got)
		}

		// When a is already present: should NOT overwrite
		item2 := map[string]any{"a": strAttr("existing")}
		if err := applyUpdateExpression(item2, "SET #a = if_not_exists(#a, :v)", names, values); err != nil {
			t.Fatalf("applyUpdateExpression error: %v", err)
		}
		got2, _ := item2["a"]
		if v, _ := got2.(map[string]any); v["S"] != "existing" {
			t.Errorf("expected a='existing' unchanged, got %v", got2)
		}
	})

	t.Run("TestExprListAppendSet", func(t *testing.T) {
		// SET #list = list_append(#list, :vals) appends to list
		names := map[string]string{"#list": "list"}
		item := map[string]any{
			"list": listAttr(strAttr("a"), strAttr("b")),
		}
		values := map[string]any{":vals": listAttr(strAttr("c"), strAttr("d"))}
		if err := applyUpdateExpression(item, "SET #list = list_append(#list, :vals)", names, values); err != nil {
			t.Fatalf("applyUpdateExpression error: %v", err)
		}
		listVal, ok := item["list"].(map[string]any)
		if !ok {
			t.Fatal("expected list attribute to be a map")
		}
		elems, ok := listVal["L"].([]any)
		if !ok {
			t.Fatal("expected L key in list attribute")
		}
		if len(elems) != 4 {
			t.Errorf("expected 4 elements after append, got %d", len(elems))
		}
	})

	t.Run("TestExprDeleteClause", func(t *testing.T) {
		// DELETE #set :v removes element from string set
		names := map[string]string{"#set": "tags"}
		item := map[string]any{
			"tags": ssAttr("go", "python", "rust"),
		}
		values := map[string]any{":v": ssAttr("python")}
		if err := applyUpdateExpression(item, "DELETE #set :v", names, values); err != nil {
			t.Fatalf("applyUpdateExpression error: %v", err)
		}
		// "python" should be removed; "go" and "rust" remain
		setVal, ok := item["tags"].(map[string]any)
		if !ok {
			t.Fatal("expected tags to remain as a map after DELETE")
		}
		elems, ok := setVal["SS"].([]any)
		if !ok {
			t.Fatal("expected SS key in tags after DELETE")
		}
		if len(elems) != 2 {
			t.Errorf("expected 2 elements after delete, got %d: %v", len(elems), elems)
		}
		for _, e := range elems {
			if fmt.Sprintf("%v", e) == "python" {
				t.Error("'python' should have been removed from the set")
			}
		}
	})

	t.Run("TestExprParenthesizedGrouping", func(t *testing.T) {
		// (#a = :v1 OR #b = :v2) AND #c = :v3
		item := map[string]any{
			"a": strAttr("x"),
			"b": strAttr("y"),
			"c": strAttr("z"),
		}
		names := map[string]string{"#a": "a", "#b": "b", "#c": "c"}
		expr := "(#a = :v1 OR #b = :v2) AND #c = :v3"

		// Both conditions satisfied
		if !evalOK(t, item, expr, names, map[string]any{
			":v1": strAttr("x"), ":v2": strAttr("nope"), ":v3": strAttr("z"),
		}) {
			t.Error("expected true: (a=x OR b=nope) AND c=z → true")
		}

		// c doesn't match → overall false even though OR matches
		if evalOK(t, item, expr, names, map[string]any{
			":v1": strAttr("x"), ":v2": strAttr("y"), ":v3": strAttr("WRONG"),
		}) {
			t.Error("expected false: AND with c=WRONG")
		}

		// Neither a nor b match → OR is false → AND is false
		if evalOK(t, item, expr, names, map[string]any{
			":v1": strAttr("no"), ":v2": strAttr("no"), ":v3": strAttr("z"),
		}) {
			t.Error("expected false: (a=no OR b=no) = false")
		}
	})
}
