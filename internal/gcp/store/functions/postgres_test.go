//go:build gcp_persistence

package functions

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresOperationCRUD runs the operation store contract against the
// Postgres backend so --dsn mode persists pollable function operations (and
// survives a restart via snapshot/restore).
func TestPostgresOperationCRUD(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres store test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	f := &Function{ID: "a", Runtime: "nodejs20", EntryPoint: "hello"}
	op := Operation{ID: "op1", Location: "us-central1", Done: true, Verb: "create",
		Target: "projects/proj/locations/us-central1/functions/a", Function: f, CreateTime: time.Now(), EndTime: time.Now()}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}
	got, err := s.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if got.Verb != "create" || got.Function == nil || got.Function.Runtime != "nodejs20" {
		t.Fatalf("unexpected operation: %+v", got)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "missing"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}

	all, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list ops: %v", err)
	}
	if len(all) != 1 || all[0].ID != "op1" {
		t.Fatalf("unexpected op list: %+v", all)
	}

	// Snapshot/restore round-trip (the --dsn restart path).
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got, err := s.GetOperation(ctx, "proj", "us-central1", "op1"); err != nil || got.Function == nil {
		t.Fatalf("operation not restored: %+v err=%v", got, err)
	}

	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != nil {
		t.Fatalf("delete op: %v", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation on second delete, got %v", err)
	}

	s.Reset(ctx)
}
