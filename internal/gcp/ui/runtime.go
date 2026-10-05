package ui

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/k8shelpers"
)

// RuntimeEngineHealth is the liveness of one host engine the emulator can use.
// It is deliberately a probe, not a configured value: Available=true means the
// daemon/cluster answered just now.
type RuntimeEngineHealth struct {
	Available bool   `json:"available"`
	Detail    string `json:"detail,omitempty"`
}

// RuntimeHealth is the payload for GET /api/ui/v1/gcp/runtime, driving the
// read-only Docker/Kubernetes rows on the admin Runtime view.
type RuntimeHealth struct {
	Docker     RuntimeEngineHealth `json:"docker"`
	Kubernetes RuntimeEngineHealth `json:"kubernetes"`
}

// buildRuntimeHealthHandler probes the host engines on each request (the admin
// view polls, so results stay live). The probe budget is shared so a hung
// daemon cannot stall the response.
func buildRuntimeHealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		uihelper.WriteJSON(w, RuntimeHealth{
			Docker:     dockerHealth(ctx),
			Kubernetes: kubernetesHealth(ctx),
		})
	}
}

// dockerSocketPath resolves the Docker daemon socket, honouring a unix://
// DOCKER_HOST override and defaulting to the standard path.
func dockerSocketPath() string {
	if h := os.Getenv("DOCKER_HOST"); strings.HasPrefix(h, "unix://") {
		return strings.TrimPrefix(h, "unix://")
	}
	return "/var/run/docker.sock"
}

// dockerHealth pings the Docker daemon over its unix socket.
func dockerHealth(ctx context.Context) RuntimeEngineHealth {
	socket := dockerSocketPath()
	if _, err := os.Stat(socket); err != nil {
		return RuntimeEngineHealth{Available: false, Detail: "socket not found: " + socket}
	}
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/_ping", nil)
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: err.Error()}
	}
	resp, err := client.Do(req)
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: err.Error()}
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return RuntimeEngineHealth{Available: false, Detail: fmt.Sprintf("docker /_ping = %d", resp.StatusCode)}
	}
	return RuntimeEngineHealth{Available: true, Detail: "docker daemon reachable"}
}

// kubernetesHealth reports whether the same client the executors use can reach
// the API server. It reflects the emulator's configuration (in-cluster or the
// JAISCLOUD_K8S_* env), not a developer's local kubeconfig.
func kubernetesHealth(ctx context.Context) RuntimeEngineHealth {
	client, err := k8shelpers.NewClient()
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: "client: " + err.Error()}
	}
	version, err := client.Discovery().ServerVersion()
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: err.Error()}
	}
	return RuntimeEngineHealth{Available: true, Detail: "server " + version.GitVersion}
}
