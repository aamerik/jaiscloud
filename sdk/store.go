package sdk

import "context"

// ResourceEntry is a resource record as seen by the plugin.
// Data contains the JSON-encoded resource state.
type ResourceEntry struct {
	Type string
	ID   string
	Data []byte
}

// ResourceStore is the minimal store interface plugins use to read and write
// resource metadata. The host wires this to its internal store via an adapter.
//
// Plugins must not cache store references across Reset() calls.
type ResourceStore interface {
	// Exists reports whether the resource identified by (resourceType, resourceID) is present.
	Exists(ctx context.Context, resourceType, resourceID string) (bool, error)

	// Get returns the resource entry for (resourceType, resourceID).
	// Returns a non-nil error if the resource does not exist.
	Get(ctx context.Context, resourceType, resourceID string) (ResourceEntry, error)

	// List returns all entries of the given resourceType whose ID contains prefix.
	// Pass an empty prefix to list all entries of a type.
	List(ctx context.Context, resourceType, prefix string) ([]ResourceEntry, error)

	// Create stores a new resource entry.
	// Returns an error if an entry with the same (Type, ID) already exists.
	Create(ctx context.Context, entry ResourceEntry) error

	// Update replaces the data for an existing resource entry.
	Update(ctx context.Context, entry ResourceEntry) error

	// Delete removes the resource entry for (resourceType, resourceID).
	// Succeeds silently if the resource does not exist.
	Delete(ctx context.Context, resourceType, resourceID string) error
}
