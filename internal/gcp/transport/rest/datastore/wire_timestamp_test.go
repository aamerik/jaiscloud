package datastore

import (
	"testing"

	"jaiscloud/internal/model"
)

// timestampCommitBody builds a NON_TRANSACTIONAL commit upsert of one Task with
// the given properties.
func timestampCommitBody(name string, props map[string]any) map[string]any {
	return map[string]any{
		"mode": "NON_TRANSACTIONAL",
		"mutations": []any{map[string]any{"upsert": map[string]any{
			"key":        nameKey("Task", name),
			"properties": props,
		}}},
	}
}

// lookupProperty returns the wire value of one property of a stored Task.
func lookupProperty(t *testing.T, c *Codec, p *Provider, name, prop string) map[string]any {
	t.Helper()
	resp, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", name)}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	found, _ := resp.Data["found"].([]any)
	if len(found) != 1 {
		t.Fatalf("found = %v, want 1", resp.Data["found"])
	}
	ent := found[0].(map[string]any)["entity"].(map[string]any)
	props := ent["properties"].(map[string]any)
	pv, _ := props[prop].(map[string]any)
	return pv
}

// TestRESTTimestampValueNormalizedOnWrite pins TD41: a timestampValue with a
// non-UTC offset is stored and echoed in canonical UTC (the gRPC transport's
// form), including nested inside an arrayValue.
func TestRESTTimestampValueNormalizedOnWrite(t *testing.T) {
	c, p := newTestProvider(t)
	_, err := call(t, c, p, "test", "commit", timestampCommitBody("ts", map[string]any{
		"t": map[string]any{"timestampValue": "2019-12-31T19:00:00-05:00"},
		"arr": map[string]any{"arrayValue": map[string]any{"values": []any{
			map[string]any{"timestampValue": "2020-01-01T05:30:00+05:30"},
		}}},
	}))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	tv := lookupProperty(t, c, p, "ts", "t")
	if got := tv["timestampValue"]; got != "2020-01-01T00:00:00Z" {
		t.Fatalf("timestampValue = %v, want 2020-01-01T00:00:00Z", got)
	}

	av := lookupProperty(t, c, p, "ts", "arr")
	values, _ := av["arrayValue"].(map[string]any)["values"].([]any)
	if len(values) != 1 {
		t.Fatalf("arrayValue.values = %v", av)
	}
	if got := values[0].(map[string]any)["timestampValue"]; got != "2020-01-01T00:00:00Z" {
		t.Fatalf("nested timestampValue = %v, want 2020-01-01T00:00:00Z", got)
	}
}

// TestRESTTimestampValueRejectsMalformed pins that an unparseable (or
// non-string) timestampValue is INVALID_ARGUMENT at the decode boundary rather
// than a value stored verbatim, including when nested inside an array; real GCP
// parses it as a proto Timestamp and rejects a malformed string. (TD41)
func TestRESTTimestampValueRejectsMalformed(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{"unparseable string", map[string]any{"timestampValue": "not-a-time"}},
		{"date only", map[string]any{"timestampValue": "2020-01-01"}},
		{"non-string", map[string]any{"timestampValue": 12345}},
		{"nested unparseable", map[string]any{"arrayValue": map[string]any{"values": []any{
			map[string]any{"timestampValue": "nope"},
		}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, p := newTestProvider(t)
			_, err := call(t, c, p, "test", "commit", timestampCommitBody("bad", map[string]any{"t": tc.value}))
			perr, ok := err.(*model.ProviderError)
			if !ok {
				t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
			}
			if perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
				t.Fatalf("err = %+v, want 400/InvalidArgument", perr)
			}
		})
	}
}
