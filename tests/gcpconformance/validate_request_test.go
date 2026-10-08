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

// TestValidateNullValueEnumAcceptsJSONNull proves the google.protobuf.NullValue
// carve-out: real GCP's REST transcoder (protojson) emits JSON null for the
// single-value NullValue enum the Discovery schema documents as "NULL_VALUE"
// (AUD3-13), so a nil value is conformant for exactly that schema — but not for
// an ordinary string field, and a genuinely wrong enum name is still flagged.
func TestValidateNullValueEnumAcceptsJSONNull(t *testing.T) {
	doc := &DiscoveryDoc{Schemas: map[string]*Schema{
		"Value": {
			Type: "object",
			Properties: map[string]*Schema{
				"nullValue":   {Type: "string", Enum: []string{"NULL_VALUE"}},
				"stringValue": {Type: "string"},
			},
		},
	}}
	ref := &Schema{Ref: "Value"}

	if d := ValidateValue(doc, ref, map[string]any{"nullValue": nil}, "Value"); len(d) != 0 {
		t.Fatalf("JSON null must be conformant for the NullValue enum: %+v", d)
	}
	if d := ValidateValue(doc, ref, map[string]any{"nullValue": "NOT_A_VALUE"}, "Value"); len(d) == 0 {
		t.Fatal("a wrong enum name must still be flagged")
	}
	if d := ValidateValue(doc, ref, map[string]any{"stringValue": nil}, "Value"); len(d) == 0 {
		t.Fatal("JSON null must not be accepted for an ordinary string field")
	}
}

// TestValidateErrorEnvelopeStatusForHTTP400 proves the 400 map accepts every
// google.rpc status that maps to HTTP 400, not only INVALID_ARGUMENT, so a
// FAILED_PRECONDITION body is not reported as error.status divergence.
func TestValidateErrorEnvelopeStatusForHTTP400(t *testing.T) {
	body := []byte(`{"error":{"code":400,"message":"offset out of range","status":"FAILED_PRECONDITION"}}`)
	for _, d := range ValidateErrorEnvelope(400, body) {
		if d.Path == "error.status" && (d.Severity == "high" || d.Severity == "medium") {
			t.Fatalf("FAILED_PRECONDITION must be accepted for HTTP 400: %+v", d)
		}
	}
}
