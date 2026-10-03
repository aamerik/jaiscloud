package firestoreui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Firestore UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/collections", h.ListCollections)

	r.Get("/collections/{collection}/documents", h.ListDocuments)
	r.Post("/collections/{collection}/documents", h.CreateDocument)
	r.Get("/collections/{collection}/documents/{document}", h.GetDocument)
	r.Patch("/collections/{collection}/documents/{document}", h.UpdateDocument)
	r.Delete("/collections/{collection}/documents/{document}", h.DeleteDocument)

	return r
}
