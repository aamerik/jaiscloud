//go:build gcp_conformance

package gcpconformance

import "testing"

func TestApplyAllowlist(t *testing.T) {
	divs := []Divergence{
		// Allowed: legacy GCS omits status.
		{Kind: kindBadErrorEnvelope, Path: "error.status", Service: "storage"},
		// Allowed: modern API omits errors[].
		{Kind: kindBadErrorEnvelope, Path: "error.errors", Service: "pubsub"},
		// NOT allowed: pubsub does carry status; a missing one is a real finding.
		{Kind: kindBadErrorEnvelope, Path: "error.status", Service: "pubsub"},
		// NOT allowed: wrong type is never an allowlisted shape.
		{Kind: kindWrongType, Path: "schema", Service: "storage", Severity: "high"},
	}
	kept, suppressed := ApplyAllowlist(divs)
	if len(suppressed) != 2 {
		t.Fatalf("suppressed = %d, want 2", len(suppressed))
	}
	if len(kept) != 2 {
		t.Fatalf("kept = %d, want 2", len(kept))
	}
	for _, d := range kept {
		if d.Path == "error.status" && d.Service == "storage" {
			t.Errorf("storage error.status should have been suppressed")
		}
	}
}
