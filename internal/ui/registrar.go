package ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/model"
)

// Registrar contributes one cloud's UI surface to the shared UI core.
// The AWS build supplies an AWS registrar, the GCP build a GCP registrar.
type Registrar interface {
	// Cloud is the emulated cloud identity reported by /api/ui/v1/meta.
	Cloud() model.Cloud
	// Services lists the service catalog entries to render in the navigation.
	// A service is listed only when its provider is wired.
	Services() []ServiceDescriptor
	// MountRoutes registers the cloud's service API routes. It is invoked
	// inside the authenticated route group, so handlers assume a valid
	// session token.
	MountRoutes(r chi.Router)
}
