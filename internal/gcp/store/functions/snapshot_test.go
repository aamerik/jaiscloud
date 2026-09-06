package functions

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := Function{ID: "a", Runtime: "nodejs20", EntryPoint: "hello",
		EnvironmentVariables: map[string]string{"K": "V"}, Labels: map[string]string{"team": "x"}}
	if err := s.CreateFunction(ctx, "proj", "us-central1", "a", f); err != nil {
		t.Fatalf("create: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := dst.GetFunction(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.Runtime != "nodejs20" || got.EntryPoint != "hello" || got.EnvironmentVariables["K"] != "V" || got.Labels["team"] != "x" {
		t.Fatalf("restored function wrong: %+v", got)
	}
}
