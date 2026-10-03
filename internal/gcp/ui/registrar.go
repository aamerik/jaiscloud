// Package ui contributes the GCP service catalog and API routes to the shared
// UI core. It exposes the GCP cloud identity and mounts per-service UI APIs
// (Cloud Storage, Pub/Sub, Firestore, Compute, Cloud Run, BigQuery; the rest
// follow) over the providers wired in cmd/jaiscloud-gcp.
package ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
	bigqueryui "jaiscloud/internal/gcp/ui/bigquery"
	computeui "jaiscloud/internal/gcp/ui/compute"
	firestoreui "jaiscloud/internal/gcp/ui/firestore"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	runui "jaiscloud/internal/gcp/ui/run"
	storageui "jaiscloud/internal/gcp/ui/storage"
	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Registrar is the GCP implementation of coreui.Registrar.
type Registrar struct {
	storage   storageui.ProviderInterface
	pubsub    pubsubui.ProviderInterface
	firestore firestoreui.ProviderInterface
	compute   computeui.ProviderInterface
	bigquery  bigqueryui.ProviderInterface
	run       runui.ProviderInterface
	cfg       *config.Config
}

// NewRegistrar returns the GCP UI registrar. A nil provider leaves that
// service's pages out of the catalog.
func NewRegistrar(storageProvider storageui.ProviderInterface, pubsubProvider pubsubui.ProviderInterface, firestoreProvider firestoreui.ProviderInterface, computeProvider computeui.ProviderInterface, bigqueryProvider bigqueryui.ProviderInterface, runProvider runui.ProviderInterface, cfg *config.Config) *Registrar {
	return &Registrar{storage: storageProvider, pubsub: pubsubProvider, firestore: firestoreProvider, compute: computeProvider, bigquery: bigqueryProvider, run: runProvider, cfg: cfg}
}

// Cloud implements coreui.Registrar.
func (r *Registrar) Cloud() model.Cloud { return model.CloudGCP }

// Services implements coreui.Registrar: only services whose provider is wired
// are advertised.
func (r *Registrar) Services() []coreui.ServiceDescriptor {
	services := make([]coreui.ServiceDescriptor, 0, 6)
	if r.storage != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "storage",
			Label:    "Cloud Storage",
			Category: "Storage",
			RootPath: "/gcp/storage/buckets",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Buckets", Path: "/gcp/storage/buckets"}},
		})
	}
	if r.pubsub != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "pubsub",
			Label:    "Pub/Sub",
			Category: "Integration",
			RootPath: "/gcp/pubsub/topics",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Topics", Path: "/gcp/pubsub/topics"},
				{Label: "Subscriptions", Path: "/gcp/pubsub/subscriptions"},
			},
		})
	}
	if r.firestore != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "firestore",
			Label:    "Firestore",
			Category: "Databases",
			RootPath: "/gcp/firestore/collections",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Collections", Path: "/gcp/firestore/collections"}},
		})
	}
	if r.compute != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "compute",
			Label:    "Compute Engine",
			Category: "Compute",
			RootPath: "/gcp/compute/instances",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Instances", Path: "/gcp/compute/instances"}},
		})
	}
	if r.run != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "run",
			Label:    "Cloud Run",
			Category: "Compute",
			RootPath: "/gcp/run/services",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Services", Path: "/gcp/run/services"}},
		})
	}
	if r.bigquery != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "bigquery",
			Label:    "BigQuery",
			Category: "Analytics",
			RootPath: "/gcp/bigquery/datasets",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Datasets", Path: "/gcp/bigquery/datasets"},
				{Label: "Jobs", Path: "/gcp/bigquery/jobs"},
			},
		})
	}
	return services
}

// MountRoutes implements coreui.Registrar. Routes are mounted inside the
// authenticated group in the shared core router.
func (r *Registrar) MountRoutes(router chi.Router) {
	if r.storage != nil {
		router.Mount("/api/ui/v1/gcp/storage", storageui.BuildRouter(r.storage, r.cfg))
	}
	if r.pubsub != nil {
		router.Mount("/api/ui/v1/gcp/pubsub", pubsubui.BuildRouter(r.pubsub, r.cfg))
	}
	if r.firestore != nil {
		router.Mount("/api/ui/v1/gcp/firestore", firestoreui.BuildRouter(r.firestore, r.cfg))
	}
	if r.compute != nil {
		router.Mount("/api/ui/v1/gcp/compute", computeui.BuildRouter(r.compute, r.cfg))
	}
	if r.run != nil {
		router.Mount("/api/ui/v1/gcp/run", runui.BuildRouter(r.run, r.cfg))
	}
	if r.bigquery != nil {
		router.Mount("/api/ui/v1/gcp/bigquery", bigqueryui.BuildRouter(r.bigquery, r.cfg))
	}
}
