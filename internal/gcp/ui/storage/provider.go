package storageui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *storage.Provider used by the Cloud
// Storage UI handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	BucketsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
