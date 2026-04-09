package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"jaiscloud/internal/store"
)

func TestMemoryResourceStore_CRUD(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()

	entry := store.ResourceEntry{
		Type: "sqs_queues",
		ID:   "http://localhost:4566/000000000000/test-queue",
		Data: json.RawMessage(`{"QueueName":"test-queue"}`),
	}

	// Create
	if err := s.Create(ctx, entry); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Duplicate create must fail
	if err := s.Create(ctx, entry); err != store.ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Get
	got, err := s.Get(ctx, entry.Type, entry.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Data) != string(entry.Data) {
		t.Fatalf("data mismatch: %s", got.Data)
	}

	// Update
	entry.Data = json.RawMessage(`{"QueueName":"test-queue","Updated":true}`)
	if err := s.Update(ctx, entry); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = s.Get(ctx, entry.Type, entry.ID)
	if string(got.Data) != string(entry.Data) {
		t.Fatalf("update not persisted")
	}

	// List with prefix
	s.Create(ctx, store.ResourceEntry{Type: "sqs_queues", ID: "http://localhost:4566/000000000000/other", Data: json.RawMessage(`{}`)})
	entries, err := s.List(ctx, "sqs_queues", "test-queue")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Delete
	if err := s.Delete(ctx, entry.Type, entry.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, entry.Type, entry.ID); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	// Purge
	s.Purge(ctx, "sqs_queues")
	all, _ := s.List(ctx, "sqs_queues", "")
	if len(all) != 0 {
		t.Fatalf("expected 0 after purge, got %d", len(all))
	}
}

func TestMemoryResourceStore_Reset(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()
	s.Create(ctx, store.ResourceEntry{Type: "sqs_queues", ID: "q1", Data: json.RawMessage(`{}`)})
	s.Reset()
	all, _ := s.List(ctx, "sqs_queues", "")
	if len(all) != 0 {
		t.Fatalf("expected empty after reset, got %d", len(all))
	}
}
