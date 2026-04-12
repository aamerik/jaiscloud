package resourcemgr

import "context"

// ResourceStore is the minimal interface the Manager needs from the host application.
// JaisCloud wires this with StoreAdapter over internal/store.ResourceStore.
type ResourceStore interface {
	Exists(ctx context.Context, resourceType, resourceID string) (bool, error)
	List(ctx context.Context, resourceType, prefix string) ([]ResourceEntry, error)
	Delete(ctx context.Context, resourceType, resourceID string) error
	Update(ctx context.Context, entry ResourceEntry) error
	Get(ctx context.Context, resourceType, resourceID string) (ResourceEntry, error)
}

// ResourceEntry is a resource record as seen by the Manager.
type ResourceEntry struct {
	Type string
	ID   string
	Data []byte // JSON-encoded resource state
}
