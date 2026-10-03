package ui

import (
	"testing"

	"jaiscloud/internal/config"
	bigqueryui "jaiscloud/internal/gcp/ui/bigquery"
	computeui "jaiscloud/internal/gcp/ui/compute"
	firestoreui "jaiscloud/internal/gcp/ui/firestore"
	iamui "jaiscloud/internal/gcp/ui/iam"
	kmsui "jaiscloud/internal/gcp/ui/kms"
	loggingui "jaiscloud/internal/gcp/ui/logging"
	monitoringui "jaiscloud/internal/gcp/ui/monitoring"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	runui "jaiscloud/internal/gcp/ui/run"
	schedulerui "jaiscloud/internal/gcp/ui/scheduler"
	secretmanagerui "jaiscloud/internal/gcp/ui/secretmanager"
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

// fakeBigQuery satisfies bigquery.ProviderInterface the same way.
type fakeBigQuery struct{ bigqueryui.ProviderInterface }

// fakeRun satisfies run.ProviderInterface the same way.
type fakeRun struct{ runui.ProviderInterface }

// fakeScheduler satisfies scheduler.ProviderInterface the same way.
type fakeScheduler struct{ schedulerui.ProviderInterface }

// fakeIAM satisfies iam.ProviderInterface the same way.
type fakeIAM struct{ iamui.ProviderInterface }

// fakeKMS satisfies kms.ProviderInterface the same way.
type fakeKMS struct{ kmsui.ProviderInterface }

// fakeSecret satisfies secretmanager.ProviderInterface the same way.
type fakeSecret struct {
	secretmanagerui.ProviderInterface
}

// fakeLogging satisfies logging.ProviderInterface the same way.
type fakeLogging struct{ loggingui.ProviderInterface }

// fakeMonitoring satisfies monitoring.ProviderInterface the same way.
type fakeMonitoring struct{ monitoringui.ProviderInterface }

func TestRegistrar_CloudIsGCP(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	if got := reg.Cloud(); got != model.CloudGCP {
		t.Fatalf("Cloud() = %q, want %q", got, model.CloudGCP)
	}
}

func TestRegistrar_NoProviders_EmptyCatalog(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	if services := reg.Services(); len(services) != 0 {
		t.Fatalf("Services() = %d entries, want 0", len(services))
	}
}

func TestRegistrar_StorageAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	if services[0].ID != "storage" || services[0].RootPath != "/gcp/storage/buckets" {
		t.Fatalf("unexpected descriptor: %+v", services[0])
	}
}

func TestRegistrar_PubSubAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, fakePubSub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
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
	reg := NewRegistrar(nil, nil, fakeFirestore{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
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
	reg := NewRegistrar(nil, nil, nil, fakeCompute{}, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
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

func TestRegistrar_BigQueryAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, fakeBigQuery{}, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "bigquery" || got.RootPath != "/gcp/bigquery/datasets" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 {
		t.Fatalf("children = %+v, want datasets + jobs", got.Children)
	}
}

func TestRegistrar_RunAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, fakeRun{}, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "run" || got.RootPath != "/gcp/run/services" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/run/services" {
		t.Fatalf("children = %+v, want services", got.Children)
	}
}

func TestRegistrar_SchedulerAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, fakeScheduler{}, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "scheduler" || got.RootPath != "/gcp/scheduler/jobs" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/scheduler/jobs" {
		t.Fatalf("children = %+v, want jobs", got.Children)
	}
}

func TestRegistrar_IAMAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, fakeIAM{}, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "iam" || got.RootPath != "/gcp/iam/service-accounts" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/iam/service-accounts" {
		t.Fatalf("children = %+v, want service accounts", got.Children)
	}
}

func TestRegistrar_KMSAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, fakeKMS{}, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "kms" || got.RootPath != "/gcp/kms/keyrings" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/kms/keyrings" {
		t.Fatalf("children = %+v, want key rings", got.Children)
	}
}

func TestRegistrar_SecretManagerAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeSecret{}, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "secretmanager" || got.RootPath != "/gcp/secretmanager/secrets" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/secretmanager/secrets" {
		t.Fatalf("children = %+v, want secrets", got.Children)
	}
}

func TestRegistrar_LoggingAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeLogging{}, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "logging" || got.RootPath != "/gcp/logging/entries" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 4 {
		t.Fatalf("children = %+v, want entries + metrics + sinks + exclusions", got.Children)
	}
}

func TestRegistrar_MonitoringAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeMonitoring{}, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "monitoring" || got.RootPath != "/gcp/monitoring/metrics" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 3 {
		t.Fatalf("children = %+v, want metrics + alerting + channels", got.Children)
	}
}

func TestRegistrar_AllAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, fakePubSub{}, fakeFirestore{}, fakeCompute{}, fakeBigQuery{}, fakeRun{}, fakeScheduler{}, fakeIAM{}, fakeKMS{}, fakeSecret{}, fakeLogging{}, fakeMonitoring{}, &config.Config{})
	if services := reg.Services(); len(services) != 12 {
		t.Fatalf("Services() = %d entries, want 12", len(services))
	}
}
