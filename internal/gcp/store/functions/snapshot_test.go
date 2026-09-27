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
		EnvironmentVariables: map[string]string{"K": "V"}, Labels: map[string]string{"team": "x"},
		MinInstanceCount: 2, MaxInstanceCount: 10, MaxInstanceRequestConcurrency: 80, AvailableCPU: "0.5"}
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
	if got.MinInstanceCount != 2 || got.MaxInstanceCount != 10 || got.MaxInstanceRequestConcurrency != 80 || got.AvailableCPU != "0.5" {
		t.Fatalf("instance config not restored: %+v", got)
	}
}

func TestMemoryStoreSnapshotKeepsSourceMetadata(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := Function{ID: "s", Runtime: "python312", EntryPoint: "main.handler",
		SourceSHA256: "abc123", SourceSize: 42, SourceBlobKey: "functions/p/us/s/abc123.zip"}
	if err := s.CreateFunction(ctx, "p", "us", "s", f); err != nil {
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
	got, err := dst.GetFunction(ctx, "p", "us", "s")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.SourceSHA256 != "abc123" || got.SourceSize != 42 || got.SourceBlobKey != "functions/p/us/s/abc123.zip" {
		t.Fatalf("source metadata not restored: %+v", got)
	}
}
