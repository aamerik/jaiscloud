package ui

import (
	"testing"

	"jaiscloud/internal/config"
	computeui "jaiscloud/internal/gcp/ui/compute"
	firestoreui "jaiscloud/internal/gcp/ui/firestore"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	storageui "jaiscloud/internal/gcp/ui/storage"
	"jaiscloud/internal/model"
)

// fakeStorage satisfies storage.ProviderInterface via an embedded (nil)
// interface; the registrar tests only exercise the catalog, never the handlers.
type fakeStorage struct{ storageui.ProviderInterface }

// fakePubSub satisfies pubsub.ProviderInterface the same way.
type fakePubSub struct{ pubsubui.ProviderInterface }

// fakeFirestore satisfies firestore.ProviderInterface the same way.
type fakeFirestore struct{ firestoreui.ProviderInterface }

// fakeCompute satisfies compute.ProviderInterface the same way.
type fakeCompute struct{ computeui.ProviderInterface }

func TestRegistrar_CloudIsGCP(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, &config.Config{})
	if got := reg.Cloud(); got != model.CloudGCP {
		t.Fatalf("Cloud() = %q, want %q", got, model.CloudGCP)
	}
}

func TestRegistrar_NoProviders_EmptyCatalog(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, &config.Config{})
	if services := reg.Services(); len(services) != 0 {
		t.Fatalf("Services() = %d entries, want 0", len(services))
	}
}

func TestRegistrar_StorageAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	if services[0].ID != "storage" || services[0].RootPath != "/gcp/storage/buckets" {
		t.Fatalf("unexpected descriptor: %+v", services[0])
	}
}

func TestRegistrar_PubSubAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, fakePubSub{}, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "pubsub" || got.RootPath != "/gcp/pubsub/topics" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 {
		t.Fatalf("children = %+v, want topics + subscriptions", got.Children)
	}
}

func TestRegistrar_FirestoreAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, fakeFirestore{}, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "firestore" || got.RootPath != "/gcp/firestore/collections" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/firestore/collections" {
		t.Fatalf("children = %+v, want collections", got.Children)
	}
}

func TestRegistrar_ComputeAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, fakeCompute{}, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "compute" || got.RootPath != "/gcp/compute/instances" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/compute/instances" {
		t.Fatalf("children = %+v, want instances", got.Children)
	}
}

func TestRegistrar_AllAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, fakePubSub{}, fakeFirestore{}, fakeCompute{}, &config.Config{})
	if services := reg.Services(); len(services) != 4 {
		t.Fatalf("Services() = %d entries, want 4", len(services))
	}
}
