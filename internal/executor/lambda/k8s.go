package lambda

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	riePort   = 8080
	riePath   = "/2015-03-31/functions/function/invocations"
	podPrefix = "jc-lambda-"
	svcPrefix = "jc-lambda-"
	labelApp  = "jaiscloud-lambda"
)

type warmPod struct {
	podName  string
	svcName  string
	endpoint string // http://<svc>.<ns>:8080; empty = sentinel (being created)
	lastUsed time.Time
}

// K8sExecutor manages warm Pods per Lambda function using the Lambda RIE HTTP protocol.
type K8sExecutor struct {
	cfg    LambdaConfig
	k8s    *http.Client // talks to K8s API server (30s timeout, custom TLS)
	invoke *http.Client // talks to Lambda RIE pods (5min timeout)
	token  string
	mu     sync.Mutex
	pods   map[string]*warmPod // functionName -> warm pod
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewK8sExecutor creates a K8sExecutor with warm-pod-per-function architecture.
func NewK8sExecutor(cfg LambdaConfig) *K8sExecutor {
	// Read bearer token.
	token := os.Getenv("JAISCLOUD_K8S_TOKEN")
	if token == "" {
		if f := os.Getenv("JAISCLOUD_K8S_TOKEN_FILE"); f != "" {
			b, _ := os.ReadFile(f)
			token = strings.TrimSpace(string(b))
		}
	}
	if token == "" {
		b, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		token = strings.TrimSpace(string(b))
	}

	if cfg.APIServer == "" {
		cfg.APIServer = "https://kubernetes.default.svc"
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "jaiscloud"
	}

	// Build TLS config.
	tlsCfg := &tls.Config{}
	caFile := os.Getenv("JAISCLOUD_K8S_CA_FILE")
	if caFile == "" {
		caFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if caBytes, err := os.ReadFile(caFile); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caBytes)
		tlsCfg.RootCAs = pool
	} else {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // dev fallback when no CA
	}
	if certFile := os.Getenv("JAISCLOUD_K8S_CLIENT_CERT_FILE"); certFile != "" {
		keyFile := os.Getenv("JAISCLOUD_K8S_CLIENT_KEY_FILE")
		if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
	}

	k8sClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}
	invokeClient := &http.Client{Timeout: 5 * time.Minute}

	e := &K8sExecutor{
		cfg:    cfg,
		k8s:    k8sClient,
		invoke: invokeClient,
		token:  token,
		pods:   make(map[string]*warmPod),
		done:   make(chan struct{}),
	}
	e.cleanupOrphans()
	e.wg.Add(1)
	go e.gcLoop()
	return e
}

// Invoke obtains a warm pod for the function (creating one if needed) and
// POSTs the payload via the Lambda RIE HTTP protocol.
func (e *K8sExecutor) Invoke(ctx context.Context, req InvokeRequest) ([]byte, error) {
	pod, err := e.getOrCreate(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("lambda k8s: get/create pod: %w", err)
	}

	url := pod.endpoint + riePath
	var result []byte
	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				if backoff < 4*time.Second {
					backoff += time.Second
				}
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(req.Payload))
		if err != nil {
			return nil, fmt.Errorf("lambda k8s: build request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := e.invoke.Do(httpReq)
		if err != nil {
			slog.Warn("lambda k8s: invoke attempt failed", "attempt", attempt+1, "err", err)
			continue
		}
		result, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			slog.Warn("lambda k8s: read response failed", "attempt", attempt+1, "err", err)
			continue
		}

		e.mu.Lock()
		if p, ok := e.pods[req.FunctionName]; ok {
			p.lastUsed = time.Now()
		}
		e.mu.Unlock()
		return result, nil
	}

	e.removePod(ctx, req.FunctionName)
	return nil, fmt.Errorf("lambda k8s: all invoke attempts failed for %s", req.FunctionName)
}

// DeleteFunction tears down the warm pod for the named function.
func (e *K8sExecutor) DeleteFunction(ctx context.Context, name string) {
	e.removePod(ctx, name)
}

// Reset destroys all warm pods (called on /_jaiscloud/reset).
func (e *K8sExecutor) Reset() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e.mu.Lock()
	names := make([]string, 0, len(e.pods))
	for name := range e.pods {
		names = append(names, name)
	}
	e.mu.Unlock()
	for _, name := range names {
		e.removePod(ctx, name)
	}
}

// Close stops the GC goroutine and destroys all warm pods.
func (e *K8sExecutor) Close() error {
	close(e.done)
	e.wg.Wait()
	e.Reset()
	return nil
}

// ─── internal ────────────────────────────────────────────────────────────────

func (e *K8sExecutor) getOrCreate(ctx context.Context, req InvokeRequest) (*warmPod, error) {
	for {
		e.mu.Lock()
		if p, ok := e.pods[req.FunctionName]; ok {
			if p.endpoint != "" {
				e.mu.Unlock()
				return p, nil
			}
			// Sentinel present — another goroutine is creating the pod.
			e.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		// Insert sentinel.
		e.pods[req.FunctionName] = &warmPod{}
		e.mu.Unlock()

		pod, err := e.createPod(ctx, req)
		if err != nil {
			e.mu.Lock()
			if e.pods[req.FunctionName] != nil && e.pods[req.FunctionName].endpoint == "" {
				delete(e.pods, req.FunctionName)
			}
			e.mu.Unlock()
			return nil, err
		}

		e.mu.Lock()
		e.pods[req.FunctionName] = pod
		e.mu.Unlock()
		return pod, nil
	}
}

func (e *K8sExecutor) createPod(ctx context.Context, req InvokeRequest) (*warmPod, error) {
	ns := e.cfg.Namespace
	image := ImageForRuntime(req, e.cfg)
	sanitized := sanitizePodName(req.FunctionName)
	podName := podPrefix + sanitized + "-" + shortID()
	svcName := svcPrefix + sanitized

	env := []map[string]string{
		{"name": "AWS_LAMBDA_FUNCTION_NAME", "value": req.FunctionName},
		{"name": "AWS_DEFAULT_REGION", "value": regionOrDefault(e.cfg.Region)},
	}
	if e.cfg.JaisCloudEndpoint != "" {
		env = append(env, map[string]string{"name": "JAISCLOUD_ENDPOINT", "value": e.cfg.JaisCloudEndpoint})
	}
	for k, v := range req.EnvVars {
		env = append(env, map[string]string{"name": k, "value": v})
	}

	var args []string
	if req.Handler != "" {
		args = []string{req.Handler}
	}

	memMB := req.MemoryMB
	if memMB < 128 {
		memMB = 128
	}

	podManifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      podName,
			"namespace": ns,
			"labels": map[string]string{
				"app":      labelApp,
				"function": sanitized,
			},
		},
		"spec": map[string]any{
			"restartPolicy": "Never",
			"containers": []map[string]any{{
				"name":            "lambda",
				"image":           image,
				"imagePullPolicy": "IfNotPresent",
				"args":            args,
				"env":             env,
				"ports":           []map[string]any{{"containerPort": riePort}},
				"resources": map[string]any{
					"requests": map[string]string{"cpu": "100m", "memory": "64Mi"},
					"limits":   map[string]string{"cpu": "1", "memory": fmt.Sprintf("%dMi", memMB)},
				},
				"readinessProbe": map[string]any{
					"tcpSocket":           map[string]any{"port": riePort},
					"initialDelaySeconds": 1,
					"periodSeconds":       1,
					"failureThreshold":    30,
				},
			}},
		},
	}

	podBody, _ := json.Marshal(podManifest)
	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods", e.cfg.APIServer, ns)
	if err := e.k8sPost(ctx, podURL, podBody); err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	slog.Info("lambda k8s: pod created", "pod", podName, "function", req.FunctionName)

	// Create ClusterIP service (idempotent — ignore errors if already exists).
	svcManifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      svcName,
			"namespace": ns,
			"labels":    map[string]string{"app": labelApp, "function": sanitized},
		},
		"spec": map[string]any{
			"type":     "ClusterIP",
			"selector": map[string]string{"function": sanitized},
			"ports":    []map[string]any{{"port": riePort, "targetPort": riePort}},
		},
	}
	svcBody, _ := json.Marshal(svcManifest)
	svcURL := fmt.Sprintf("%s/api/v1/namespaces/%s/services", e.cfg.APIServer, ns)
	e.k8sPost(ctx, svcURL, svcBody) //nolint:errcheck // may already exist

	if err := e.waitReady(ctx, ns, podName); err != nil {
		return nil, fmt.Errorf("pod not ready: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svcName, ns, riePort)
	return &warmPod{
		podName:  podName,
		svcName:  svcName,
		endpoint: endpoint,
		lastUsed: time.Now(),
	}, nil
}

func (e *K8sExecutor) waitReady(ctx context.Context, ns, podName string) error {
	deadline := time.Now().Add(90 * time.Second)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for pod %s to be ready", podName)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}

		url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.cfg.APIServer, ns, podName)
		body, status, err := e.k8sGet(ctx, url)
		if err != nil || status != 200 {
			continue
		}
		var pod struct {
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		}
		if json.Unmarshal(body, &pod) != nil {
			continue
		}
		if pod.Status.Phase != "Running" {
			continue
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				return nil
			}
		}
	}
}

func (e *K8sExecutor) removePod(ctx context.Context, functionName string) {
	e.mu.Lock()
	pod, ok := e.pods[functionName]
	if ok {
		delete(e.pods, functionName)
	}
	e.mu.Unlock()
	if !ok || pod.podName == "" {
		return
	}

	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.cfg.APIServer, e.cfg.Namespace, pod.podName)
	e.k8sDelete(ctx, podURL)

	svcURL := fmt.Sprintf("%s/api/v1/namespaces/%s/services/%s", e.cfg.APIServer, e.cfg.Namespace, pod.svcName)
	e.k8sDelete(ctx, svcURL)
	slog.Info("lambda k8s: removed pod and service", "function", functionName)
}

func (e *K8sExecutor) gcLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.gcOnce()
		}
	}
}

func (e *K8sExecutor) gcOnce() {
	keepalive := time.Duration(e.cfg.KeepaliveSecs) * time.Second
	if keepalive == 0 {
		keepalive = 300 * time.Second
	}
	now := time.Now()
	e.mu.Lock()
	var toRemove []string
	for name, p := range e.pods {
		if p.endpoint != "" && now.Sub(p.lastUsed) > keepalive {
			toRemove = append(toRemove, name)
		}
	}
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, name := range toRemove {
		slog.Info("lambda k8s: GC idle pod", "function", name)
		e.removePod(ctx, name)
	}
}

// cleanupOrphans deletes pods and services from previous runs on startup.
func (e *K8sExecutor) cleanupOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ns := e.cfg.Namespace

	// Delete orphaned pods.
	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?labelSelector=app=%s", e.cfg.APIServer, ns, labelApp)
	if body, status, err := e.k8sGet(ctx, podURL); err == nil && status == 200 {
		var list struct {
			Items []struct {
				Metadata struct{ Name string `json:"name"` } `json:"metadata"`
			} `json:"items"`
		}
		if json.Unmarshal(body, &list) == nil {
			for _, item := range list.Items {
				url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.cfg.APIServer, ns, item.Metadata.Name)
				e.k8sDelete(ctx, url)
				slog.Info("lambda k8s: cleaned up orphan pod", "pod", item.Metadata.Name)
			}
		}
	}

	// Delete orphaned services.
	svcURL := fmt.Sprintf("%s/api/v1/namespaces/%s/services?labelSelector=app=%s", e.cfg.APIServer, ns, labelApp)
	if body, status, err := e.k8sGet(ctx, svcURL); err == nil && status == 200 {
		var list struct {
			Items []struct {
				Metadata struct{ Name string `json:"name"` } `json:"metadata"`
			} `json:"items"`
		}
		if json.Unmarshal(body, &list) == nil {
			for _, item := range list.Items {
				url := fmt.Sprintf("%s/api/v1/namespaces/%s/services/%s", e.cfg.APIServer, ns, item.Metadata.Name)
				e.k8sDelete(ctx, url)
				slog.Info("lambda k8s: cleaned up orphan service", "svc", item.Metadata.Name)
			}
		}
	}
}

// ─── K8s API helpers ─────────────────────────────────────────────────────────

func (e *K8sExecutor) k8sPost(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.k8s.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, rb)
	}
	return nil
}

func (e *K8sExecutor) k8sGet(ctx context.Context, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.k8s.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func (e *K8sExecutor) k8sDelete(ctx context.Context, url string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.k8s.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// sanitizePodName converts a function name to a valid K8s name segment.
func sanitizePodName(name string) string {
	var b strings.Builder
	for _, ch := range strings.ToLower(name) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			b.WriteRune(ch)
		} else {
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}

func shortID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b)
}
