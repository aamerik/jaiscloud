// Package ui contributes the GCP service catalog and API routes to the shared
// UI core. It exposes the GCP cloud identity and mounts per-service UI APIs
// (Cloud Storage, Pub/Sub, Firestore, Compute, Cloud Run, BigQuery, IAM, Cloud
// KMS, Secret Manager; the rest follow) over the providers wired in
// cmd/jaiscloud-gcp.
package ui

import (
	"github.com/go-chi/chi/v5"

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
	secretmanagerui "jaiscloud/internal/gcp/ui/secretmanager"
	storageui "jaiscloud/internal/gcp/ui/storage"
	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Registrar is the GCP implementation of coreui.Registrar.
type Registrar struct {
	storage       storageui.ProviderInterface
	pubsub        pubsubui.ProviderInterface
	firestore     firestoreui.ProviderInterface
	compute       computeui.ProviderInterface
	bigquery      bigqueryui.ProviderInterface
	run           runui.ProviderInterface
	iam           iamui.ProviderInterface
	kms           kmsui.ProviderInterface
	secretmanager secretmanagerui.ProviderInterface
	logging       loggingui.ProviderInterface
	monitoring    monitoringui.ProviderInterface
	cfg           *config.Config
}

// NewRegistrar returns the GCP UI registrar. A nil provider leaves that
// service's pages out of the catalog.
func NewRegistrar(storageProvider storageui.ProviderInterface, pubsubProvider pubsubui.ProviderInterface, firestoreProvider firestoreui.ProviderInterface, computeProvider computeui.ProviderInterface, bigqueryProvider bigqueryui.ProviderInterface, runProvider runui.ProviderInterface, iamProvider iamui.ProviderInterface, kmsProvider kmsui.ProviderInterface, secretProvider secretmanagerui.ProviderInterface, loggingProvider loggingui.ProviderInterface, monitoringProvider monitoringui.ProviderInterface, cfg *config.Config) *Registrar {
	return &Registrar{
		storage:       storageProvider,
		pubsub:        pubsubProvider,
		firestore:     firestoreProvider,
		compute:       computeProvider,
		bigquery:      bigqueryProvider,
		run:           runProvider,
		iam:           iamProvider,
		kms:           kmsProvider,
		secretmanager: secretProvider,
		logging:       loggingProvider,
		monitoring:    monitoringProvider,
		cfg:           cfg,
	}
}

// Cloud implements coreui.Registrar.
func (r *Registrar) Cloud() model.Cloud { return model.CloudGCP }

// Services implements coreui.Registrar: only services whose provider is wired
// are advertised.
func (r *Registrar) Services() []coreui.ServiceDescriptor {
	services := make([]coreui.ServiceDescriptor, 0, 11)
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
	if r.iam != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "iam",
			Label:    "IAM",
			Category: "Security",
			RootPath: "/gcp/iam/service-accounts",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Service accounts", Path: "/gcp/iam/service-accounts"}},
		})
	}
	if r.kms != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "kms",
			Label:    "Cloud KMS",
			Category: "Security",
			RootPath: "/gcp/kms/keyrings",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Key rings", Path: "/gcp/kms/keyrings"}},
		})
	}
	if r.secretmanager != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "secretmanager",
			Label:    "Secret Manager",
			Category: "Security",
			RootPath: "/gcp/secretmanager/secrets",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Secrets", Path: "/gcp/secretmanager/secrets"}},
		})
	}
	if r.logging != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "logging",
			Label:    "Cloud Logging",
			Category: "Operations",
			RootPath: "/gcp/logging/entries",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Logs explorer", Path: "/gcp/logging/entries"},
				{Label: "Logs-based metrics", Path: "/gcp/logging/metrics"},
				{Label: "Log router", Path: "/gcp/logging/sinks"},
				{Label: "Exclusions", Path: "/gcp/logging/exclusions"},
			},
		})
	}
	if r.monitoring != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "monitoring",
			Label:    "Cloud Monitoring",
			Category: "Operations",
			RootPath: "/gcp/monitoring/metrics",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Metrics explorer", Path: "/gcp/monitoring/metrics"},
				{Label: "Alerting", Path: "/gcp/monitoring/alerting"},
				{Label: "Notification channels", Path: "/gcp/monitoring/channels"},
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
	if r.iam != nil {
		router.Mount("/api/ui/v1/gcp/iam", iamui.BuildRouter(r.iam, r.cfg))
	}
	if r.kms != nil {
		router.Mount("/api/ui/v1/gcp/kms", kmsui.BuildRouter(r.kms, r.cfg))
	}
	if r.secretmanager != nil {
		router.Mount("/api/ui/v1/gcp/secretmanager", secretmanagerui.BuildRouter(r.secretmanager, r.cfg))
	}
	if r.logging != nil {
		router.Mount("/api/ui/v1/gcp/logging", loggingui.BuildRouter(r.logging, r.cfg))
	}
	if r.monitoring != nil {
		router.Mount("/api/ui/v1/gcp/monitoring", monitoringui.BuildRouter(r.monitoring, r.cfg))
	}
}
