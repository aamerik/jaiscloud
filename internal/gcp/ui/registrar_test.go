package ui

import (
	"context"
	"testing"

	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// fakeStorage satisfies storage.ProviderInterface without a real provider.
type fakeStorage struct{}

func (fakeStorage) BucketsList(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
func (fakeStorage) BucketsInsert(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
func (fakeStorage) BucketsDelete(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}
func (fakeStorage) ObjectsList(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func TestRegistrar_CloudIsGCP(t *testing.T) {
	reg := NewRegistrar(nil, &config.Config{})
	if got := reg.Cloud(); got != model.CloudGCP {
		t.Fatalf("Cloud() = %q, want %q", got, model.CloudGCP)
	}
}

func TestRegistrar_NoStorage_EmptyCatalog(t *testing.T) {
	reg := NewRegistrar(nil, &config.Config{})
	if services := reg.Services(); len(services) != 0 {
		t.Fatalf("Services() = %d entries, want 0", len(services))
	}
}

func TestRegistrar_StorageAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	if services[0].ID != "storage" || services[0].RootPath != "/gcp/storage/buckets" {
		t.Fatalf("unexpected descriptor: %+v", services[0])
	}
}
