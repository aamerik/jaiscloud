//go:build gcp_conformance

package gcpconformance

import "testing"

// TestValidateRequestValue proves the request-side path: present-field types and
// unknown fields are checked, while `required` is deliberately not enforced.
func TestValidateRequestValue(t *testing.T) {
	doc := &DiscoveryDoc{Schemas: map[string]*Schema{
		"Req": {
			Type:     "object",
			Required: []string{"name"},
			Properties: map[string]*Schema{
				"name":  {Type: "string"},
				"count": {Type: "integer"},
			},
		},
	}}
	ref := &Schema{Ref: "Req"}

	// Requests do NOT enforce `required` (partial/PATCH bodies are legitimate).
	if d := ValidateRequestValue(doc, ref, map[string]any{"count": float64(1)}, "Req"); len(d) != 0 {
		t.Fatalf("missing required must be allowed for requests: %+v", d)
	}
	// ...but present-field types and unknown fields still are checked.
	if d := ValidateRequestValue(doc, ref, map[string]any{"count": "nope"}, "Req"); len(d) == 0 {
		t.Fatal("want a wrong_type finding for count")
	}
	if d := ValidateRequestValue(doc, ref, map[string]any{"bogus": true}, "Req"); len(d) == 0 {
		t.Fatal("want an unknown_field finding for bogus")
	}
	// Responses DO enforce required — the contrast.
	if d := ValidateValue(doc, ref, map[string]any{"count": float64(1)}, "Req"); len(d) == 0 {
		t.Fatal("response validation should flag the missing required field")
	}
}
