package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
)

// ResourceEntry is a single control-plane resource in the store.
type ResourceEntry struct {
	Type      string          // e.g. "sqs_queues"
	ID        string          // unique within Type (e.g. queue URL)
	Data      json.RawMessage // serialised resource state (JSONB in Postgres mode)
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ResourceStore manages control-plane resource metadata.
// Phase 0: MemoryResourceStore. Phase 1: PostgresResourceStore.
type ResourceStore interface {
	Create(ctx context.Context, entry ResourceEntry) error
	Get(ctx context.Context, resourceType, id string) (ResourceEntry, error)
	Update(ctx context.Context, entry ResourceEntry) error
	Delete(ctx context.Context, resourceType, id string) error
	List(ctx context.Context, resourceType, prefix string) ([]ResourceEntry, error)
	Purge(ctx context.Context, resourceType string) error

	// Reset wipes all state — used by the admin reset endpoint.
	Reset()
}
