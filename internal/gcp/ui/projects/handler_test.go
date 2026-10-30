package projectsui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"jaiscloud/internal/config"
	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
)

// fakeProvider is a scripted Resource Manager core.
type fakeProvider struct {
	registered []resourcemanagercore.Project
	observed   []resourcemanagercore.ObservedProject
	err        error
}

func (f *fakeProvider) ListProjects(_ context.Context, _ int, pageToken string, _ bool, _ string) ([]resourcemanagercore.Project, string, error) {
	if f.err != nil {
		return nil, "", f.err
	}
	if pageToken != "" {
		return nil, "", nil
	}
	return f.registered, "", nil
}

func (f *fakeProvider) ListObservedProjects(context.Context) []resourcemanagercore.ObservedProject {
	return f.observed
}

func get(t *testing.T, h http.Handler) (int, ProjectsResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var out ProjectsResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v (body %s)", err, rec.Body.String())
		}
	}
	return rec.Code, out
}

func TestListConfiguredOnlyWithoutProvider(t *testing.T) {
	cfg := &config.Config{ProjectID: "jaiscloud-project", ExtraAccounts: []string{"extra-a"}}
	code, out := get(t, BuildRouter(nil, cfg))
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if len(out.Projects) != 2 {
		t.Fatalf("projects = %v, want 2 configured", out.Projects)
	}
	for _, p := range out.Projects {
		if p.Kind != KindConfigured {
			t.Fatalf("kind = %q, want configured", p.Kind)
		}
	}
}

func TestListKindsAndDedup(t *testing.T) {
	cfg := &config.Config{ProjectID: "jaiscloud-project"}
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	observedAt := time.Date(2026, 1, 3, 4, 5, 6, 0, time.UTC)
	provider := &fakeProvider{
		registered: []resourcemanagercore.Project{
			{ProjectID: "billing-demo", State: "ACTIVE", ProjectNumber: "123", CreateTime: created},
			// A registered project that is also configured must appear once.
			{ProjectID: "jaiscloud-project", State: "ACTIVE"},
		},
		observed: []resourcemanagercore.ObservedProject{
			{ProjectID: "acme-e2e", LastSeen: observedAt, Requests: 7, Services: []string{"pubsub", "storage"}},
			// Observed duplicates are dropped: they have their own group.
			{ProjectID: "billing-demo", LastSeen: observedAt},
		},
	}
	code, out := get(t, BuildRouter(provider, cfg))
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}

	byKind := map[string][]Project{}
	for _, p := range out.Projects {
		byKind[p.Kind] = append(byKind[p.Kind], p)
	}
	if len(out.Projects) != 3 {
		t.Fatalf("projects = %+v, want 3 rows", out.Projects)
	}
	if len(byKind[KindConfigured]) != 1 || byKind[KindConfigured][0].ID != "jaiscloud-project" {
		t.Fatalf("configured = %v", byKind[KindConfigured])
	}
	if len(byKind[KindRegistered]) != 1 || byKind[KindRegistered][0].ID != "billing-demo" {
		t.Fatalf("registered = %v", byKind[KindRegistered])
	}
	if len(byKind[KindObserved]) != 1 {
		t.Fatalf("observed = %v", byKind[KindObserved])
	}
	obs := byKind[KindObserved][0]
	if obs.ID != "acme-e2e" || obs.Requests != 7 || len(obs.Services) != 2 {
		t.Fatalf("observed row = %+v", obs)
	}
	if obs.LastSeen != "2026-01-03T04:05:06Z" || obs.FirstSeen != "" {
		t.Fatalf("observed timestamps = %q / %q", obs.FirstSeen, obs.LastSeen)
	}
	if byKind[KindRegistered][0].CreateTime != "2026-01-02T03:04:05Z" {
		t.Fatalf("registered createTime = %q", byKind[KindRegistered][0].CreateTime)
	}
}

func TestListEmptyConfig(t *testing.T) {
	code, out := get(t, BuildRouter(nil, nil))
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if len(out.Projects) != 0 {
		t.Fatalf("projects = %v, want none", out.Projects)
	}
}
