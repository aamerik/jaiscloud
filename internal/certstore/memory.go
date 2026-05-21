package certstore

import "context"

// MemoryCertStore never persists anything. Load always returns ErrNotFound so
// the server regenerates a fresh certificate on every startup. Used in memory mode.
type MemoryCertStore struct{}

func NewMemoryCertStore() *MemoryCertStore { return &MemoryCertStore{} }

func (*MemoryCertStore) Load(_ context.Context) (*StoredCert, error) {
	return nil, ErrNotFound
}

func (*MemoryCertStore) Save(_ context.Context, _ *StoredCert) error {
	return nil
}
