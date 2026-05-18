package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound           = errors.New("resource not found")
	ErrAlreadyExists      = errors.New("resource already exists")
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// ResourceEntry is a single control-plane resource in the store.
type ResourceEntry struct {
	Type      string          // e.g. "sqs_queues"
	ID        string          // unique within Type (e.g. queue URL)
	Data      json.RawMessage // serialised resource state (JSONB in Postgres mode)
	CreatedAt time.Time
	UpdatedAt time.Time
	// Account and Region are populated by List when doing a cross-scope scan
	// (account="" and region=""). Callers that need to Update after a cross-scope
	// List must use these values for the subsequent Update call.
	Account string
	Region  string
}

// ResourceStore manages control-plane resource metadata.
// Phase 0: MemoryResourceStore. Phase 1: PostgresResourceStore.
type ResourceStore interface {
	Create(ctx context.Context, account, region string, entry ResourceEntry) error
	Get(ctx context.Context, account, region, resourceType, id string) (ResourceEntry, error)
	Update(ctx context.Context, account, region string, entry ResourceEntry) error
	Delete(ctx context.Context, account, region, resourceType, id string) error
	List(ctx context.Context, account, region, resourceType, prefix string) ([]ResourceEntry, error)
	Purge(ctx context.Context, account, region, resourceType string) error

	// Reset wipes all state — used by the admin reset endpoint.
	Reset()
}
