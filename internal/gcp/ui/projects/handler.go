// Package projectsui serves the GCP console's project-discovery API: every
// project the emulator knows about, split by how it became known — configured at
// startup, created through Resource Manager, or merely observed on the wire.
//
// It exists because the wire API synthesizes unknown projects as ACTIVE: a
// workload can write to a project the console never registered, and without this
// endpoint the console has no way to show where the workload's resources went.
package projectsui

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// ProviderInterface is the slice of the Resource Manager core this handler
// needs. A nil provider (resourcemanager disabled) yields configured projects
// only.
type ProviderInterface interface {
	ListProjects(ctx context.Context, pageSize int, pageToken string, showDeleted bool, filter string) ([]resourcemanagercore.Project, string, error)
	ListObservedProjects(ctx context.Context) []resourcemanagercore.ObservedProject
}

// Kinds a project can have in the response.
const (
	KindConfigured = "configured"
	KindRegistered = "registered"
	KindObserved   = "observed"
)

// Project is one console project row.
type Project struct {
	ID            string   `json:"id"`
	Kind          string   `json:"kind"`
	State         string   `json:"state,omitempty"`
	ProjectNumber string   `json:"projectNumber,omitempty"`
	DisplayName   string   `json:"displayName,omitempty"`
	CreateTime    string   `json:"createTime,omitempty"`
	FirstSeen     string   `json:"firstSeen,omitempty"`
	LastSeen      string   `json:"lastSeen,omitempty"`
	Requests      int64    `json:"requests,omitempty"`
	Services      []string `json:"services,omitempty"`
}

// ProjectsResponse is the GET /projects body.
type ProjectsResponse struct {
	Projects []Project `json:"projects"`
}

// Handler serves the project-discovery API.
type Handler struct {
	provider   ProviderInterface
	configured []string
}

// NewHandler builds the handler. configured is the startup project ids
// (cfg.ProjectID + cfg.ExtraAccounts), which always exist.
func NewHandler(provider ProviderInterface, cfg *config.Config) *Handler {
	configured := []string{}
	if cfg != nil {
		if cfg.ProjectID != "" {
			configured = append(configured, cfg.ProjectID)
		}
		configured = append(configured, cfg.ExtraAccounts...)
	}
	return &Handler{provider: provider, configured: configured}
}

// BuildRouter mounts the project-discovery routes.
func BuildRouter(provider ProviderInterface, cfg *config.Config) http.Handler {
	h := NewHandler(provider, cfg)
	r := chi.NewRouter()
	r.Get("/", h.List)
	return r
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// List implements GET /api/ui/v1/gcp/projects: configured ∪ registered ∪
// observed, each row tagged with its kind. Observed rows are newest-activity
// first; configured and registered are sorted by id.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	seen := map[string]bool{}
	out := []Project{}

	for _, id := range h.configured {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, Project{ID: id, Kind: KindConfigured})
	}

	if h.provider != nil {
		registered := map[string]Project{}
		token := ""
		for {
			page, next, err := h.provider.ListProjects(ctx, 0, token, false, "")
			if err != nil {
				uihelper.WriteError(w, err)
				return
			}
			for _, p := range page {
				if seen[p.ProjectID] {
					continue
				}
				registered[p.ProjectID] = Project{
					ID:            p.ProjectID,
					Kind:          KindRegistered,
					State:         p.State,
					ProjectNumber: p.ProjectNumber,
					DisplayName:   p.DisplayName,
					CreateTime:    formatTime(p.CreateTime),
				}
			}
			if next == "" {
				break
			}
			token = next
		}
		ids := make([]string, 0, len(registered))
		for id := range registered {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			seen[id] = true
			out = append(out, registered[id])
		}

		// The core already excludes configured and registered ids.
		for _, p := range h.provider.ListObservedProjects(ctx) {
			if p.ProjectID == "" || seen[p.ProjectID] {
				continue
			}
			out = append(out, Project{
				ID:        p.ProjectID,
				Kind:      KindObserved,
				FirstSeen: formatTime(p.FirstSeen),
				LastSeen:  formatTime(p.LastSeen),
				Requests:  p.Requests,
				Services:  p.Services,
			})
		}
	}

	uihelper.WriteJSON(w, ProjectsResponse{Projects: out})
}
