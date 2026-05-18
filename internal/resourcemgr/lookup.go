package resourcemgr

import "context"

// ResourceStore is the minimal interface the Manager needs from the host application.
// JaisCloud wires this with StoreAdapter over internal/store.ResourceStore.
// account is the 12-digit AWS account ID; region is "" for global resource types.
type ResourceStore interface {
	Exists(ctx context.Context, account, region, resourceType, resourceID string) (bool, error)
	List(ctx context.Context, account, region, resourceType, prefix string) ([]ResourceEntry, error)
	Delete(ctx context.Context, account, region, resourceType, resourceID string) error
	Update(ctx context.Context, account, region string, entry ResourceEntry) error
	Get(ctx context.Context, account, region, resourceType, resourceID string) (ResourceEntry, error)
}

// ResourceEntry is a resource record as seen by the Manager.
type ResourceEntry struct {
	Type string
	ID   string
	Data []byte // JSON-encoded resource state
}
