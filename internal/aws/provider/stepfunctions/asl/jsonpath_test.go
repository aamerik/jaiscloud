package asl_test

import (
	"testing"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

func TestJSONPath_Root(t *testing.T) {
	doc := map[string]any{"a": 1}
	v, err := asl.EvalPath(doc, "$")
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok || m["a"] != 1 {
		t.Errorf("unexpected result: %v", v)
	}
}

func TestJSONPath_FieldAccess(t *testing.T) {
	doc := map[string]any{"foo": "bar"}
	v, err := asl.EvalPath(doc, "$.foo")
	if err != nil {
		t.Fatal(err)
	}
	if v != "bar" {
		t.Errorf("got %v, want bar", v)
	}
}

func TestJSONPath_NestedAccess(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": 42.0}}
	v, err := asl.EvalPath(doc, "$.a.b")
	if err != nil {
		t.Fatal(err)
	}
	if v != 42.0 {
		t.Errorf("got %v, want 42", v)
	}
}

func TestJSONPath_ArrayIndex(t *testing.T) {
	doc := map[string]any{"arr": []any{"x", "y", "z"}}
	v, err := asl.EvalPath(doc, "$.arr[1]")
	if err != nil {
		t.Fatal(err)
	}
	if v != "y" {
		t.Errorf("got %v, want y", v)
	}
}

func TestJSONPath_ArraySlice(t *testing.T) {
	doc := map[string]any{"arr": []any{1.0, 2.0, 3.0, 4.0}}
	v, err := asl.EvalPath(doc, "$.arr[1:3]")
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		t.Errorf("got %v, want [2, 3]", v)
	}
}

func TestJSONPath_Wildcard(t *testing.T) {
	doc := map[string]any{"a": 1.0, "b": 2.0}
	v, err := asl.EvalPath(doc, "$.*")
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		t.Errorf("got %v, want 2 values", v)
	}
}

func TestJSONPath_RecursiveDescent(t *testing.T) {
	doc := map[string]any{
		"a": map[string]any{"name": "alice"},
		"b": map[string]any{"name": "bob"},
	}
	v, err := asl.EvalPath(doc, "$..name")
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		t.Errorf("got %v, want 2 names", v)
	}
}

func TestJSONPath_MissingField(t *testing.T) {
	doc := map[string]any{"a": 1}
	v, err := asl.EvalPath(doc, "$.missing")
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("got %v, want nil", v)
	}
}

func TestEvalParameters_DotDollarSyntax(t *testing.T) {
	input := map[string]any{"bar": 42.0}
	params := map[string]any{"foo.$": "$.bar"}
	result, err := asl.EvalParameters(params, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["foo"] != 42.0 {
		t.Errorf("foo = %v, want 42", m["foo"])
	}
}

func TestEvalParameters_LiteralAndPath(t *testing.T) {
	input := map[string]any{"x": 1.0}
	params := map[string]any{"a": "static", "b.$": "$.x"}
	result, err := asl.EvalParameters(params, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["a"] != "static" {
		t.Errorf("a = %v, want static", m["a"])
	}
	if m["b"] != 1.0 {
		t.Errorf("b = %v, want 1", m["b"])
	}
}

func TestSetPath_FieldUpdate(t *testing.T) {
	doc := map[string]any{"a": 1}
	result, err := asl.SetPath(doc, "$.b", 99)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["b"] != 99 {
		t.Errorf("b = %v, want 99", m["b"])
	}
	if m["a"] != 1 {
		t.Errorf("a = %v, want 1 (should be preserved)", m["a"])
	}
}

func TestSetPath_CreatesIntermediates(t *testing.T) {
	doc := map[string]any{}
	result, err := asl.SetPath(doc, "$.a.b.c", "deep")
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	a, ok := m["a"].(map[string]any)
	if !ok {
		t.Fatal("a should be map")
	}
	b, ok := a["b"].(map[string]any)
	if !ok {
		t.Fatal("b should be map")
	}
	if b["c"] != "deep" {
		t.Errorf("c = %v, want deep", b["c"])
	}
}

func TestSetPath_RootReplaces(t *testing.T) {
	doc := map[string]any{"old": true}
	result, err := asl.SetPath(doc, "$", map[string]any{"new": true})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok || m["new"] != true {
		t.Errorf("unexpected result: %v", result)
	}
}
