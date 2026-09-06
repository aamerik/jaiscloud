package functions

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreFunctionCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	// Create.
	f := Function{ID: "a", Runtime: "nodejs20", EntryPoint: "hello", Status: "ACTIVE",
		EnvironmentVariables: map[string]string{"K": "V"}, Labels: map[string]string{"team": "x"}}
	if err := s.CreateFunction(ctx, "proj", "us-central1", "a", f); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Duplicate → ErrAlreadyExists.
	if err := s.CreateFunction(ctx, "proj", "us-central1", "a", Function{ID: "a"}); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Get.
	got, err := s.GetFunction(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != "a" || got.Runtime != "nodejs20" || got.EnvironmentVariables["K"] != "V" || got.Labels["team"] != "x" {
		t.Fatalf("unexpected function: %+v", got)
	}

	// Same ID under a different location is independent.
	if err := s.CreateFunction(ctx, "proj", "europe-west1", "a", Function{ID: "a"}); err != nil {
		t.Fatalf("create other location: %v", err)
	}

	// Update.
	upd := got
	upd.Runtime = "nodejs22"
	if err := s.UpdateFunction(ctx, "proj", "us-central1", "a", upd); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetFunction(ctx, "proj", "us-central1", "a")
	if got.Runtime != "nodejs22" {
		t.Fatalf("expected nodejs22, got %s", got.Runtime)
	}

	// List (sorted, location-scoped).
	s.CreateFunction(ctx, "proj", "us-central1", "c", Function{ID: "c"})
	s.CreateFunction(ctx, "proj", "us-central1", "b", Function{ID: "b"})
	all, err := s.ListFunctions(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != "a" || all[1].ID != "b" || all[2].ID != "c" {
		t.Fatalf("unexpected list order: %+v", all)
	}
	other, _ := s.ListFunctions(ctx, "proj", "europe-west1")
	if len(other) != 1 {
		t.Fatalf("expected 1 in other location, got %d", len(other))
	}

	// Delete.
	if err := s.DeleteFunction(ctx, "proj", "us-central1", "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetFunction(ctx, "proj", "us-central1", "a"); err != ErrNoSuchFunction {
		t.Fatalf("expected ErrNoSuchFunction, got %v", err)
	}
	if err := s.DeleteFunction(ctx, "proj", "us-central1", "a"); err != ErrNoSuchFunction {
		t.Fatalf("expected ErrNoSuchFunction on delete missing, got %v", err)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateFunction(ctx, "proj", "us-central1", "a", Function{ID: "a", CreateTime: time.Now()})
	s.Reset(ctx)
	if _, err := s.GetFunction(ctx, "proj", "us-central1", "a"); err != ErrNoSuchFunction {
		t.Fatalf("expected ErrNoSuchFunction after reset, got %v", err)
	}
}
