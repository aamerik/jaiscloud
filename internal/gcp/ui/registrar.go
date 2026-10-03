// Package ui contributes the GCP service catalog and API routes to the shared
// UI core. It currently ships the console shell only: the registrar reports the
// GCP cloud identity and renders no service pages yet. Per-service backends
// (Cloud Storage, Pub/Sub, ...) will be added here and mounted in MountRoutes.
package ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Registrar is the GCP implementation of coreui.Registrar.
type Registrar struct{}

// NewRegistrar returns the GCP UI registrar.
func NewRegistrar() *Registrar { return &Registrar{} }

// Cloud implements coreui.Registrar.
func (r *Registrar) Cloud() model.Cloud { return model.CloudGCP }

// Services implements coreui.Registrar. Empty until GCP service pages ship.
func (r *Registrar) Services() []coreui.ServiceDescriptor { return []coreui.ServiceDescriptor{} }

// MountRoutes implements coreui.Registrar. No service routes are registered in
// the shell-only milestone.
func (r *Registrar) MountRoutes(router chi.Router) {}
