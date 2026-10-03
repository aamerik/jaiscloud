// Package runexec is the k8s runtime manager behind the Cloud Run core's
// RuntimeManager seam. For each revision it launches the template image as a
// single-replica Pod plus a ClusterIP Service (via internal/k8shelpers), waits
// for the in-cluster endpoint, and reverse-proxies data-plane HTTP requests to
// it. Unlike the Lambda executor, Cloud Run revisions are always-on: there is no
// idle/keepalive reaper — a revision runtime is torn down only on service
// delete, template-changing update, /_jaiscloud/reset, or the startup orphan
// sweep. It uses client-go (not a raw-HTTP K8s client); only the upstream proxy
// target uses net/http.
package runexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
)

const (
	defaultNamespace      = "jaiscloud"
	defaultRequestTimeout = 300 * time.Second
	orphanSweepTimeout    = 30 * time.Second

	labelApp      = "app"
	labelAppValue = "jaiscloud-cloudrun"
	labelInstance = "jaiscloud.io/instance-id"
	labelService  = "jaiscloud.io/run-service"
	labelRevision = "jaiscloud.io/run-revision"

	envPort          = "PORT"
	envService       = "K_SERVICE"
	envRevision      = "K_REVISION"
	envConfiguration = "K_CONFIGURATION"
)

// Config configures a Manager.
type Config struct {
	// Client is the in-cluster Kubernetes client (required).
	Client kubernetes.Interface
	// Namespace is where revision workloads run; defaults to "jaiscloud".
	Namespace string
	Logger    *slog.Logger
	// Proxy is the HTTP client used for the upstream proxy call. Nil builds a
	// client with RequestTimeout as its total timeout.
	Proxy *http.Client
	// RequestTimeout bounds an upstream proxy call; defaults to 300s.
	RequestTimeout time.Duration
	// ReadyTimeout bounds the k8s readiness wait; defaults to the helper's 180s.
	ReadyTimeout time.Duration
	// Probe overrides the readiness TCP dial; for tests.
	Probe func(addr string) bool
}

// Manager implements run.RuntimeManager on a Kubernetes cluster. It is safe for
// concurrent use.
type Manager struct {
	client         kubernetes.Interface
	namespace      string
	logger         *slog.Logger
	proxy          *http.Client
	requestTimeout time.Duration
	readyTimeout   time.Duration
	probe          func(string) bool

	mu        sync.RWMutex
	byHost    map[string]*target // normalized authority -> latest revision target
	byService map[string]*target // canonical service name -> latest revision target

	sweepOnce sync.Once
}

type target struct {
	serviceName string
	revision    string
	host        string
	backend     string // http://<dns>:<port>
}

// Manager is the k8s implementation of the Cloud Run runtime seam.
var _ runcore.RuntimeManager = (*Manager)(nil)

// New returns a k8s-backed runtime manager.
func New(cfg Config) *Manager {
	ns := cfg.Namespace
	if ns == "" {
		ns = defaultNamespace
	}
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
		probe = tcpProbe
	}
	return &Manager{
		client:         cfg.Client,
		namespace:      ns,
		logger:         logger,
		proxy:          proxy,
		requestTimeout: timeout,
		readyTimeout:   cfg.ReadyTimeout,
		probe:          probe,
		byHost:         map[string]*target{},
		byService:      map[string]*target{},
	}
}

// EnsureRevision starts (or reuses) the revision's Pod + Service and registers
// the service's invocation authority so data-plane requests resolve to it.
func (m *Manager) EnsureRevision(ctx context.Context, svc runstore.Service, rev runstore.Revision) error {
	// Reap workloads left by a previous emulator instance before the first
	// revision of this process. Broker/revision liveness is runtime state this
	// process cannot adopt across deterministic names, so every labeled
	// resource is an orphan.
	m.sweepOnce.Do(m.sweepOrphans)

	name := workloadName(svc, rev)
	pod, port, err := buildPod(svc, rev, name, m.namespace)
	if err != nil {
		return model.NewProviderError("InvalidArgument", err.Error(), 400)
	}
	endpoint, err := k8shelpers.EnsureWorkload(ctx, m.client, k8shelpers.WorkloadSpec{
		Namespace: m.namespace,
		Pod:       *pod,
		ServiceLabels: map[string]string{
			labelApp:      labelAppValue,
			labelService:  svc.ID,
			labelRevision: rev.ID,
		},
		Selector:     map[string]string{labelApp: labelAppValue, labelService: svc.ID},
		Ports:        []k8shelpers.ServicePort{{Name: "http", Port: port}},
		ReadyTimeout: m.readyTimeout,
		Probe:        m.probe,
	})
	if err != nil {
		return fmt.Errorf("cloudrun: revision %s runtime: %w", rev.ID, err)
	}

	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	tgt := &target{
		serviceName: svcName,
		revision:    rev.ID,
		host:        normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID)),
		backend:     "http://" + endpoint,
	}
	m.mu.Lock()
	m.byHost[tgt.host] = tgt
	m.byService[svcName] = tgt
	m.mu.Unlock()
	m.logger.Info("cloudrun: revision ready", "service", svcName, "revision", rev.ID, "endpoint", endpoint)
	return nil
}

// RemoveRevision tears down a revision's runtime. It is called on a
// template-changing update and on service delete.
func (m *Manager) RemoveRevision(ctx context.Context, rev runstore.Revision) error {
	err := k8shelpers.DeleteWorkload(ctx, m.client, m.namespace, workloadNameFor(rev.ProjectID, rev.Location, rev.Service, rev.ID))
	svcName := runcore.ServiceName(rev.ProjectID, rev.Location, rev.Service)
	host := normalizeHost(serviceHost(rev.ProjectID, rev.Location, rev.Service))
	m.mu.Lock()
	if t, ok := m.byHost[host]; ok && t.revision == rev.ID {
		delete(m.byHost, host)
	}
	if t, ok := m.byService[svcName]; ok && t.revision == rev.ID {
		delete(m.byService, svcName)
	}
	m.mu.Unlock()
	if err != nil {
		return fmt.Errorf("cloudrun: remove revision %s: %w", rev.ID, err)
	}
	return nil
}

// RemoveService tears down every runtime of a service and deregisters it.
func (m *Manager) RemoveService(ctx context.Context, svc runstore.Service) error {
	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	n, err := k8shelpers.SweepWorkloads(ctx, m.client, m.namespace,
		fmt.Sprintf("%s=%s,%s=%s", labelApp, labelAppValue, labelService, svc.ID))
	m.mu.Lock()
	delete(m.byService, svcName)
	delete(m.byHost, normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID)))
	m.mu.Unlock()
	if err != nil {
		return fmt.Errorf("cloudrun: remove service %s: %w", svc.ID, err)
	}
	if n > 0 {
		m.logger.Info("cloudrun: removed service workloads", "service", svcName, "count", n)
	}
	return nil
}

// Invoke forwards a data-plane request to the target service's latest ready
// revision and returns its raw HTTP response. Missing service → 404, no ready
// runtime → 503, dial failure → 502, timeout → 504.
func (m *Manager) Invoke(ctx context.Context, req runcore.InvocationRequest) (runcore.Invocation, error) {
	tgt, err := m.resolve(req)
	if err != nil {
		return runcore.Invocation{}, err
	}
	upstream := tgt.backend + "/" + strings.TrimLeft(req.Path, "/")
	if req.Query != "" {
		upstream += "?" + req.Query
	}
	body := bytes.NewReader(req.Body)
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, upstream, body)
	if err != nil {
		return runcore.Invocation{}, model.NewProviderError("InvalidArgument", "invalid invocation request: "+err.Error(), 400)
	}
	for k, v := range req.Headers {
		if isHopByHop(k) {
			continue
		}
		httpReq.Header.Set(k, v)
	}

	resp, err := m.proxy.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
			return runcore.Invocation{}, model.NewProviderError("DeadlineExceeded", "Cloud Run runtime request timed out", 504)
		}
		return runcore.Invocation{}, model.NewProviderError("BadGateway", "Cloud Run runtime connection failed: "+err.Error(), 502)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return runcore.Invocation{}, model.NewProviderError("BadGateway", "Cloud Run runtime read failed: "+readErr.Error(), 502)
	}
	headers := make(map[string]string, len(resp.Header))
	for k, vs := range resp.Header {
		if len(vs) == 0 || isHopByHop(k) {
			continue
		}
		headers[k] = vs[0]
	}
	return runcore.Invocation{Status: resp.StatusCode, Headers: headers, Body: respBody}, nil
}

// resolve maps a request to the registered revision target: by Host first (the
// primary data-plane path), then by explicit service for the legacy path form.
func (m *Manager) resolve(req runcore.InvocationRequest) (*target, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if req.Host != "" {
		if t, ok := m.byHost[normalizeHost(req.Host)]; ok {
			return t, nil
		}
	}
	if req.Service.ID != "" {
		svcName := runcore.ServiceName(req.Service.ProjectID, req.Service.Location, req.Service.ID)
		if t, ok := m.byService[svcName]; ok {
			return t, nil
		}
		return nil, model.NewProviderError("Unavailable", "Cloud Run service has no ready runtime: "+req.Service.ID, 503)
	}
	if req.Host != "" && runcore.IsInvocationHost(req.Host) {
		return nil, model.NewProviderError("NotFound", "Cloud Run service not found for host: "+req.Host, 404)
	}
	return nil, model.NewProviderError("NotFound", "Cloud Run service not found", 404)
}

// Reset tears down every revision runtime and clears the registry
// (/_jaiscloud/reset).
func (m *Manager) Reset(ctx context.Context) {
	m.mu.Lock()
	m.byHost = map[string]*target{}
	m.byService = map[string]*target{}
	m.mu.Unlock()
	if n, err := k8shelpers.SweepWorkloads(ctx, m.client, m.namespace, labelApp+"="+labelAppValue); err != nil {
		m.logger.Warn("cloudrun: reset sweep incomplete", "deleted", n, "err", err)
	}
}

// sweepOrphans reaps revision workloads left by a previous emulator instance.
// Best-effort: a failure is logged and never blocks a revision start.
func (m *Manager) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), orphanSweepTimeout)
	defer cancel()
	if n, err := k8shelpers.SweepWorkloads(ctx, m.client, m.namespace, labelApp+"="+labelAppValue); err != nil {
		m.logger.Warn("cloudrun: orphan sweep incomplete", "deleted", n, "err", err)
	} else if n > 0 {
		m.logger.Info("cloudrun: reaped orphan revision workloads", "count", n)
	}
}

// normalizeHost lowercases a Host[:port] and drops the port so an authority
// registered with a port still matches a request that omits it (and vice versa).
func normalizeHost(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return ""
	}
	if hostOnly, _, err := net.SplitHostPort(h); err == nil {
		return hostOnly
	}
	return h
}

func isHopByHop(header string) bool {
	switch http.CanonicalHeaderKey(header) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length":
		return true
	}
	return false
}

// tcpProbe reports whether addr accepts a TCP connection.
func tcpProbe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
