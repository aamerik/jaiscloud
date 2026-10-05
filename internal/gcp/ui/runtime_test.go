package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestKubernetesConfigSource(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("JAISCLOUD_K8S_APISERVER", "")
	t.Setenv("JAISCLOUD_K8S_TOKEN", "")
	t.Setenv("JAISCLOUD_K8S_TOKEN_FILE", "")

	if got := kubernetesConfigSource(); got != "no k8s config (using default host)" {
		t.Errorf("no config: source = %q", got)
	}

	t.Setenv("JAISCLOUD_K8S_APISERVER", "https://example:6443")
	if got := kubernetesConfigSource(); got != "JAISCLOUD_K8S_* env" {
		t.Errorf("env config: source = %q", got)
	}

	t.Setenv("JAISCLOUD_K8S_APISERVER", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	if got := kubernetesConfigSource(); got != "in-cluster service account" {
		t.Errorf("in-cluster: source = %q", got)
	}
}

// A DOCKER_HOST override must be surfaced, never silently followed: the
// emulator's executors only use the local socket. Probing a nonexistent socket
// keeps the assertion independent of the host's daemon.
func TestDockerHealthNotesIgnoredDockerHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "ssh://user@remote")
	health := dockerHealthAt(context.Background(), "/nonexistent/docker.sock")
	if health.Available {
		t.Fatalf("docker health = available, want unavailable without a socket")
	}
	if !strings.Contains(health.Detail, "DOCKER_HOST=ssh://user@remote is ignored") {
		t.Errorf("detail = %q, want it to mention the ignored override", health.Detail)
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
	if health.Kubernetes.Detail == "" {
		t.Errorf("kubernetes probe returned no detail: %+v", health.Kubernetes)
	}
}
