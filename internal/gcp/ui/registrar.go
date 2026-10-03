// Package ui contributes the GCP service catalog and API routes to the shared
// UI core. It exposes the GCP cloud identity and mounts per-service UI APIs
// (Cloud Storage, Pub/Sub; Firestore, ... follow) over the providers wired in
// cmd/jaiscloud-gcp.
package ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	storageui "jaiscloud/internal/gcp/ui/storage"
	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Registrar is the GCP implementation of coreui.Registrar.
type Registrar struct {
	storage storageui.ProviderInterface
	pubsub  pubsubui.ProviderInterface
	cfg     *config.Config
}

// NewRegistrar returns the GCP UI registrar. A nil provider leaves that
// service's pages out of the catalog.
func NewRegistrar(storageProvider storageui.ProviderInterface, pubsubProvider pubsubui.ProviderInterface, cfg *config.Config) *Registrar {
	return &Registrar{storage: storageProvider, pubsub: pubsubProvider, cfg: cfg}
}

// Cloud implements coreui.Registrar.
func (r *Registrar) Cloud() model.Cloud { return model.CloudGCP }

// Services implements coreui.Registrar: only services whose provider is wired
// are advertised.
func (r *Registrar) Services() []coreui.ServiceDescriptor {
	services := make([]coreui.ServiceDescriptor, 0, 2)
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
}
