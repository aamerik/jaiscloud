package runexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
)

const (
	// dockerSocketPath is the local Docker API socket the manager dials. Like
	// the Lambda/ECS executors, the emulator talks to a daemon on its own kernel
	// (a DOCKER_HOST override is ignored); the console's runtime health view
	// surfaces that.
	dockerSocketPath = "/var/run/docker.sock"
	// dockerAPIVersion is the Docker Engine API version prefix used by the
	// existing executors. Docker 24+ shares this wire surface.
	dockerAPIVersion = "v1.41"

	defaultDockerReadyTimeout = 180 * time.Second
	dockerPollInterval        = 500 * time.Millisecond

	dockerLabelService  = "jaiscloud.io/service"
	dockerLabelValue    = "cloudrun"
	dockerLabelRunSvc   = "jaiscloud.io/run-service"
	dockerLabelRevision = "jaiscloud.io/run-revision"
	dockerLabelInstance = "jaiscloud.io/instance-id"
)

// DockerConfig configures a DockerManager.
type DockerConfig struct {
	Logger *slog.Logger
	// Platform carries the TLS PEM bundle / extra volumes / extra env applied to
	// every container; may be nil.
	Platform *platform.PlatformConfig
	// InstanceID scopes container names and labels so emulator instances sharing
	// one Docker daemon do not reap each other's containers.
	InstanceID string
	// Proxy is the HTTP client used for the upstream proxy call. Nil builds a
	// client with RequestTimeout as its total timeout.
	Proxy *http.Client
	// RequestTimeout bounds an upstream proxy call; defaults to 300s.
	RequestTimeout time.Duration
	// ReadyTimeout bounds the container-port readiness wait; defaults to 180s.
	ReadyTimeout time.Duration
	// Probe overrides the readiness TCP dial; for tests.
	Probe func(addr string) bool
	// Socket overrides the Docker API unix socket; defaults to
	// /var/run/docker.sock.
	Socket string
	// Client overrides the Docker API client (tests). When nil, a client that
	// dials Socket is built.
	Client *http.Client
	// Network sets HostConfig.NetworkMode; empty uses the daemon default.
	Network string
}

// DockerManager implements run.RuntimeManager with one container per revision
// and a published loopback host port the proxy dials. It is safe for concurrent
// use.
type DockerManager struct {
	client       *http.Client
	proxy        *http.Client
	readyTimeout time.Duration
	probe        func(string) bool
	platform     *platform.PlatformConfig
	instanceID   string
	network      string
	logger       *slog.Logger

	reg *registry

	sweepOnce sync.Once
}

// Ping reports whether the Docker daemon at socket answers the Engine API ping.
// An empty socket uses the default local socket. It lets startup fall back to
// the mock runtime when no daemon is reachable, rather than failing every
// service create.
func Ping(ctx context.Context, socket string) error {
	if socket == "" {
		socket = dockerSocketPath
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker: ping %s = HTTP %d", socket, resp.StatusCode)
	}
	return nil
}

// httpProbe considers a revision ready when its published port answers an HTTP
// request (any status). Cloud Run revisions are HTTP servers, and Docker's proxy
// binds the published port as soon as the container starts — so a bare TCP dial
// can succeed before the app serves traffic, while a connection-level failure on
// the HTTP request means it is not up yet.
func httpProbe(addr string) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

// DockerManager is the docker implementation of the Cloud Run runtime seam.
var _ runcore.RuntimeManager = (*DockerManager)(nil)

// NewDocker returns a docker-backed runtime manager.
func NewDocker(cfg DockerConfig) *DockerManager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	proxy := cfg.Proxy
	if proxy == nil {
		proxy = &http.Client{Timeout: timeout}
	}
	probe := cfg.Probe
	if probe == nil {
		probe = httpProbe
	}
	ready := cfg.ReadyTimeout
	if ready == 0 {
		ready = defaultDockerReadyTimeout
	}
	socket := cfg.Socket
	if socket == "" {
		socket = dockerSocketPath
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", socket)
				},
			},
		}
	}
	return &DockerManager{
		client:       client,
		proxy:        proxy,
		readyTimeout: ready,
		probe:        probe,
		platform:     cfg.Platform,
		instanceID:   cfg.InstanceID,
		network:      cfg.Network,
		logger:       logger,
		reg:          newRegistry(),
	}
}

// EnsureRevision starts (or replaces) the revision's container and registers
// the service's invocation authority so data-plane requests resolve to it.
func (m *DockerManager) EnsureRevision(ctx context.Context, svc runstore.Service, rev runstore.Revision) error {
	// Reap containers left by a previous emulator instance before the first
	// revision of this process.
	m.sweepOnce.Do(m.sweepOrphans)

	image, command, args, env, port, err := containerTemplate(svc, rev)
	if err != nil {
		return model.NewProviderError("InvalidArgument", err.Error(), 400)
	}
	name := dockerContainerName(m.instanceID, svc.ProjectID, svc.Location, svc.ID, rev.ID)
	// Idempotent re-ensure: a prior attempt may have left the container behind.
	m.removeContainer(ctx, name)

	spec := dockerSpec{
		name:    name,
		image:   image,
		command: command,
		args:    args,
		env:     env,
		port:    port,
		memory:  memoryBytesFromResources(firstContainer(rev)["resources"]),
		labels: map[string]string{
			dockerLabelService:  dockerLabelValue,
			dockerLabelRunSvc:   svc.ID,
			dockerLabelRevision: rev.ID,
		},
	}
	if m.instanceID != "" {
		spec.labels[dockerLabelInstance] = m.instanceID
	}
	id, hostPort, err := m.startContainer(ctx, spec)
	if err != nil {
		return fmt.Errorf("cloudrun: revision %s runtime: %w", rev.ID, err)
	}

	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	host := normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID))
	m.reg.put(svcName, host, &target{
		serviceName: svcName,
		revision:    rev.ID,
		backend:     fmt.Sprintf("http://127.0.0.1:%d", hostPort),
	})
	m.logger.Info("cloudrun: revision ready", "service", svcName, "revision", rev.ID, "container", dockerShortID(id), "port", hostPort)
	return nil
}

// RemoveRevision tears down a revision's container. It is called on a
// template-changing update and on service delete.
func (m *DockerManager) RemoveRevision(ctx context.Context, rev runstore.Revision) error {
	name := dockerContainerName(m.instanceID, rev.ProjectID, rev.Location, rev.Service, rev.ID)
	err := m.removeContainer(ctx, name)
	svcName := runcore.ServiceName(rev.ProjectID, rev.Location, rev.Service)
	host := normalizeHost(serviceHost(rev.ProjectID, rev.Location, rev.Service))
	m.reg.dropRevision(svcName, host, rev.ID)
	if err != nil {
		return fmt.Errorf("cloudrun: remove revision %s: %w", rev.ID, err)
	}
	return nil
}

// RemoveService tears down every container of a service and deregisters it.
func (m *DockerManager) RemoveService(ctx context.Context, svc runstore.Service) error {
	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	n, err := m.removeByFilter(ctx, m.serviceFilter(svc.ID))
	m.reg.dropService(svcName, normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID)))
	if err != nil {
		return fmt.Errorf("cloudrun: remove service %s: %w", svc.ID, err)
	}
	if n > 0 {
		m.logger.Info("cloudrun: removed service containers", "service", svcName, "count", n)
	}
	return nil
}

// Invoke forwards a data-plane request to the target service's latest ready
// revision and returns its raw HTTP response. Missing service → 404, no ready
// runtime → 503, dial failure → 502, timeout → 504.
func (m *DockerManager) Invoke(ctx context.Context, req runcore.InvocationRequest) (runcore.Invocation, error) {
	return proxyInvoke(ctx, m.proxy, m.reg, req)
}

// Reset tears down every revision container and clears the registry
// (/_jaiscloud/reset).
func (m *DockerManager) Reset(ctx context.Context) {
	m.reg.clear()
	if _, err := m.removeByFilter(ctx, m.instanceFilter()); err != nil {
		m.logger.Warn("cloudrun: reset sweep incomplete", "err", err)
	}
}

// sweepOrphans reaps revision containers left by a previous emulator instance.
// Best-effort: a failure is logged and never blocks a revision start.
func (m *DockerManager) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), orphanSweepTimeout)
	defer cancel()
	if n, err := m.removeByFilter(ctx, m.instanceFilter()); err != nil {
		m.logger.Warn("cloudrun: orphan sweep incomplete", "err", err)
	} else if n > 0 {
		m.logger.Info("cloudrun: reaped orphan revision containers", "count", n)
	}
}

// instanceFilter selects this instance's Cloud Run containers.
func (m *DockerManager) instanceFilter() map[string][]string {
	return m.labelFilter(nil)
}

// serviceFilter selects this instance's containers for one service.
func (m *DockerManager) serviceFilter(svcID string) map[string][]string {
	return m.labelFilter([]string{dockerLabelRunSvc + "=" + svcID})
}

// labelFilter builds a Docker label filter that always scopes to this emulator
// instance, so two emulators sharing a daemon never reap each other's containers.
func (m *DockerManager) labelFilter(extra []string) map[string][]string {
	labels := []string{dockerLabelService + "=" + dockerLabelValue}
	if m.instanceID != "" {
		labels = append(labels, dockerLabelInstance+"="+m.instanceID)
	}
	labels = append(labels, extra...)
	return map[string][]string{"label": labels}
}

// dockerSpec is the resolved input for starting one revision container.
type dockerSpec struct {
	name    string
	image   string
	command []string
	args    []string
	env     []envVar
	port    int32
	memory  int64
	labels  map[string]string
}

// startContainer creates and starts the container, then waits for its published
// host port to accept TCP. It returns the container id and the assigned port.
func (m *DockerManager) startContainer(ctx context.Context, spec dockerSpec) (string, int, error) {
	portKey := fmt.Sprintf("%d/tcp", spec.port)
	env := make([]string, 0, len(spec.env))
	for _, e := range spec.env {
		env = append(env, e.name+"="+e.value)
	}
	hostCfg := map[string]any{
		"AutoRemove": false,
		// An empty HostPort lets Docker assign an ephemeral port bound to
		// loopback only, so revisions never race for a fixed port and are not
		// exposed beyond the host.
		"PortBindings": map[string]any{portKey: []map[string]any{{"HostIp": "127.0.0.1", "HostPort": ""}}},
	}
	if spec.memory > 0 {
		hostCfg["Memory"] = spec.memory
	}
	if m.network != "" {
		hostCfg["NetworkMode"] = m.network
	}
	if m.platform != nil {
		volArgs, envArgs, err := platform.ApplyDocker(m.platform)
		if err != nil {
			m.logger.Warn("cloudrun docker: platform apply failed", "err", err)
		}
		var binds []string
		for i := 1; i < len(volArgs); i += 2 {
			binds = append(binds, volArgs[i])
		}
		for i := 1; i < len(envArgs); i += 2 {
			env = append(env, envArgs[i])
		}
		if len(binds) > 0 {
			hostCfg["Binds"] = binds
		}
	}
	createBody := map[string]any{
		"Image":        spec.image,
		"Env":          env,
		"Labels":       spec.labels,
		"ExposedPorts": map[string]any{portKey: map[string]any{}},
		"HostConfig":   hostCfg,
	}
	if len(spec.command) > 0 {
		createBody["Entrypoint"] = spec.command
	}
	if len(spec.args) > 0 {
		createBody["Cmd"] = spec.args
	}
	body, err := json.Marshal(createBody)
	if err != nil {
		return "", 0, fmt.Errorf("docker create: %w", err)
	}

	id, err := m.createContainer(ctx, spec.name, body)
	if err != nil {
		return "", 0, err
	}
	// From here the container exists: never leak it on a later failure.
	fail := func(err error) (string, int, error) {
		_ = m.removeContainer(context.WithoutCancel(ctx), id)
		return "", 0, err
	}

	if _, status, err := m.dockerCall(ctx, http.MethodPost, "/containers/"+id+"/start", nil); err != nil {
		return fail(fmt.Errorf("docker start: %w", err))
	} else if status >= 300 {
		return fail(fmt.Errorf("docker start: HTTP %d", status))
	}

	hostPort, err := m.waitContainerPort(ctx, id, spec.port)
	if err != nil {
		return fail(err)
	}
	return id, hostPort, nil
}

// createContainer creates a container, retrying once after removing a same-named
// leftover when the daemon reports a name conflict (409).
func (m *DockerManager) createContainer(ctx context.Context, name string, body []byte) (string, error) {
	createURL := "/containers/create?name=" + url.QueryEscape(name)
	respBody, status, err := m.dockerCall(ctx, http.MethodPost, createURL, body)
	if err != nil {
		return "", fmt.Errorf("docker create: %w", err)
	}
	if status == http.StatusConflict {
		if rmErr := m.removeContainer(context.WithoutCancel(ctx), name); rmErr == nil {
			respBody, status, err = m.dockerCall(ctx, http.MethodPost, createURL, body)
			if err != nil {
				return "", fmt.Errorf("docker create: %w", err)
			}
		}
	}
	if status >= 300 {
		return "", fmt.Errorf("docker create: HTTP %d: %s", status, strings.TrimSpace(string(respBody)))
	}
	var createResp struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(respBody, &createResp); err != nil || createResp.ID == "" {
		return "", fmt.Errorf("docker create: malformed response: %s", strings.TrimSpace(string(respBody)))
	}
	return createResp.ID, nil
}

// waitContainerPort polls the container until Docker has assigned its published
// host port and the port accepts a TCP connection.
func (m *DockerManager) waitContainerPort(ctx context.Context, id string, containerPort int32) (int, error) {
	deadline := clock.RealNow().Add(m.readyTimeout)
	portKey := fmt.Sprintf("%d/tcp", containerPort)
	for {
		if hostPort, err := m.inspectHostPort(ctx, id, portKey); err == nil && hostPort > 0 {
			if m.probe(fmt.Sprintf("127.0.0.1:%d", hostPort)) {
				return hostPort, nil
			}
		}
		if clock.RealNow().After(deadline) {
			return 0, fmt.Errorf("cloudrun: container %s port %d not ready within %s", dockerShortID(id), containerPort, m.readyTimeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(dockerPollInterval):
		}
	}
}

// inspectHostPort reads the host port Docker bound for a container port.
func (m *DockerManager) inspectHostPort(ctx context.Context, id, portKey string) (int, error) {
	body, status, err := m.dockerCall(ctx, http.MethodGet, "/containers/"+id+"/json", nil)
	if err != nil {
		return 0, err
	}
	if status >= 300 {
		return 0, fmt.Errorf("docker inspect: HTTP %d", status)
	}
	var info struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return 0, err
	}
	for _, binding := range info.NetworkSettings.Ports[portKey] {
		if binding.HostPort == "" {
			continue
		}
		var p int
		if _, err := fmt.Sscanf(binding.HostPort, "%d", &p); err == nil && p > 0 {
			return p, nil
		}
	}
	return 0, nil
}

// removeContainer stops and force-removes a container by id or name. Removing a
// missing container is not an error.
func (m *DockerManager) removeContainer(ctx context.Context, idOrName string) error {
	if _, status, err := m.dockerCall(ctx, http.MethodPost, "/containers/"+idOrName+"/stop", nil); err != nil {
		return err
	} else if status >= 300 && status != http.StatusNotFound {
		// A container that never started returns 304/409 from stop; the delete
		// below still reaps it, so only surface unexpected failures.
		m.logger.Debug("cloudrun docker: stop returned non-2xx", "status", status, "container", idOrName)
	}
	_, status, err := m.dockerCall(ctx, http.MethodDelete, "/containers/"+idOrName+"?force=true", nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusNotFound {
		return fmt.Errorf("docker delete %s: HTTP %d", idOrName, status)
	}
	return nil
}

// removeByFilter stops and removes every container matching the label filter,
// returning how many were reaped.
func (m *DockerManager) removeByFilter(ctx context.Context, filter map[string][]string) (int, error) {
	rawFilter, err := json.Marshal(filter)
	if err != nil {
		return 0, err
	}
	body, status, err := m.dockerCall(ctx, http.MethodGet, "/containers/json?all=true&filters="+url.QueryEscape(string(rawFilter)), nil)
	if err != nil {
		return 0, err
	}
	if status >= 300 {
		return 0, fmt.Errorf("docker list: HTTP %d", status)
	}
	var items []struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return 0, err
	}
	var reaped int
	for _, item := range items {
		if err := m.removeContainer(ctx, item.ID); err != nil {
			m.logger.Warn("cloudrun docker: failed to reap container", "id", item.ID, "err", err)
			continue
		}
		reaped++
	}
	return reaped, nil
}

// dockerCall issues a Docker Engine API request over the configured socket.
func (m *DockerManager) dockerCall(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost/"+dockerAPIVersion+path, reader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return rb, resp.StatusCode, nil
}

// dockerContainerName is the deterministic, instance-scoped container name for a
// revision. Instance scoping keeps two emulators on one daemon from colliding or
// cross-reaping.
func dockerContainerName(instanceID, project, location, service, revision string) string {
	return "jc-cloudrun-" + shortInstance(instanceID) + "-" + workloadNameFor(project, location, service, revision)
}

func shortInstance(instanceID string) string {
	if len(instanceID) >= 8 {
		return instanceID[:8]
	}
	if instanceID != "" {
		return instanceID
	}
	return "local"
}

// dockerShortID truncates a container id for logging.
func dockerShortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
