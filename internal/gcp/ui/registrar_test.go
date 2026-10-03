package ui

import (
	"testing"

	"jaiscloud/internal/config"
	storageui "jaiscloud/internal/gcp/ui/storage"
	"jaiscloud/internal/model"
)

// fakeStorage satisfies storage.ProviderInterface via an embedded (nil)
// interface; the registrar tests only exercise the catalog, never the handlers.
type fakeStorage struct{ storageui.ProviderInterface }

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
