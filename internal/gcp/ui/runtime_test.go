package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDockerSocketPath(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///tmp/custom.sock")
	if got := dockerSocketPath(); got != "/tmp/custom.sock" {
		t.Errorf("dockerSocketPath = %q, want /tmp/custom.sock", got)
	}
	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:2375")
	if got := dockerSocketPath(); got != "/var/run/docker.sock" {
		t.Errorf("dockerSocketPath = %q, want default for tcp host", got)
	}
}

// The handler must always return a well-formed document with both probes, so
// the admin Runtime view can render even when an engine is unreachable.
func TestBuildRuntimeHealthHandler_Shape(t *testing.T) {
	rec := httptest.NewRecorder()
	buildRuntimeHealthHandler()(rec, httptest.NewRequest(http.MethodGet, "/api/ui/v1/gcp/runtime", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var health RuntimeHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if health.Docker.Detail == "" {
		t.Errorf("docker probe returned no detail: %+v", health.Docker)
	}
}
