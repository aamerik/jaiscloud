package spark

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"jaiscloud/internal/k8stypes"
)

const (
	inClusterTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// k8sClient is a minimal Kubernetes API client using only stdlib.
// It supports bearer-token auth and optional custom CA certificate.
type k8sClient struct {
	httpClient  *http.Client
	apiURL      string
	namespace   string
	tokenFile   string // path to a file containing the bearer token (re-read per request)
	tokenLiteral string // literal token value (used when JAISCLOUD_K8S_TOKEN is not a file path)
}

// bearingTransport injects a Bearer token on every outgoing request.
type bearingTransport struct {
	base      http.RoundTripper
	client    *k8sClient
}

func (t *bearingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.client.readToken()
	if err != nil {
		return nil, fmt.Errorf("k8s: read bearer token: %w", err)
	}
	if token != "" {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return t.base.RoundTrip(req)
}

// readToken returns the current bearer token, re-reading the token file on each
// call to handle K8s projected service account token rotation.
func (c *k8sClient) readToken() (string, error) {
	if c.tokenLiteral != "" {
		return c.tokenLiteral, nil
	}
	if c.tokenFile == "" {
		return "", nil
	}
	data, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// newK8sClient builds a k8sClient.
//
// tokenSource is either:
//   - a file path (if it exists on disk) → token is re-read on each request
//   - a literal token string             → used as-is
//   - empty string                        → no authentication (use clientCertFile/Key instead)
//
// caFile is the path to a PEM CA certificate file. Empty string uses system roots.
// clientCertFile / clientKeyFile are paths to PEM client certificate + key for
// mutual TLS authentication (used by Docker Desktop and most self-hosted clusters).
func newK8sClient(apiURL, tokenSource, caFile, clientCertFile, clientKeyFile, namespace string) (*k8sClient, error) {
	c := &k8sClient{
		apiURL:    strings.TrimRight(apiURL, "/"),
		namespace: namespace,
	}

	// Resolve token source.
	if tokenSource != "" {
		if _, err := os.Stat(tokenSource); err == nil {
			c.tokenFile = tokenSource
		} else {
			c.tokenLiteral = tokenSource
		}
	}

	// Build TLS config.
	tlsCfg := &tls.Config{}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("k8s: read CA file %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("k8s: no valid certificates in %s", caFile)
		}
		tlsCfg.RootCAs = pool
	}

	// Client certificate authentication (Docker Desktop / self-hosted clusters).
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("k8s: load client cert: %w", err)
		}
		tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	}

	base := &http.Transport{TLSClientConfig: tlsCfg}
	c.httpClient = &http.Client{
		Transport: &bearingTransport{base: base, client: c},
	}
	return c, nil
}

// newInClusterClient auto-detects in-cluster config from the standard
// service account token mount. Falls back gracefully if not in a pod.
func newInClusterClient(namespace string) (*k8sClient, error) {
	return newK8sClient(DefaultAPIServer, inClusterTokenFile, inClusterCAFile, "", "", namespace)
}

// ── Type aliases from k8stypes (pod-spec wire types shared across executors) ─

type (
	podSpec     = k8stypes.PodSpec
	container   = k8stypes.Container
	volume      = k8stypes.Volume
	volumeMount = k8stypes.VolumeMount
	envVar      = k8stypes.EnvVar
)

// ── Batch-job types (Spark-specific) ────────────────────────────────────────

type batchJob struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   jobMeta        `json:"metadata"`
	Spec       jobSpec        `json:"spec"`
	Status     batchJobStatus `json:"status,omitempty"`
}

type jobMeta struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type jobSpec struct {
	BackoffLimit            *int        `json:"backoffLimit"`
	TTLSecondsAfterFinished *int        `json:"ttlSecondsAfterFinished,omitempty"`
	Suspend                 *bool       `json:"suspend,omitempty"`
	Template                podTemplate `json:"template"`
}

type podTemplate struct {
	Metadata podMeta `json:"metadata,omitempty"`
	Spec     podSpec `json:"spec"`
}

type podMeta struct {
	Labels map[string]string `json:"labels,omitempty"`
}

type batchJobStatus struct {
	Active     int            `json:"active"`
	Succeeded  int            `json:"succeeded"`
	Failed     int            `json:"failed"`
	StartTime  string         `json:"startTime,omitempty"`
	Conditions []jobCondition `json:"conditions,omitempty"`
}

// jobListItem is used when listing jobs (lighter than full batchJob).
type jobListItem struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Suspend *bool `json:"suspend"`
	} `json:"spec"`
	Status struct {
		Succeeded int `json:"succeeded"`
		Failed    int `json:"failed"`
		Active    int `json:"active"`
	} `json:"status"`
}

type jobCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ── API operations ──────────────────────────────────────────────────────────

func (c *k8sClient) jobsURL() string {
	return fmt.Sprintf("%s/apis/batch/v1/namespaces/%s/jobs", c.apiURL, c.namespace)
}

func (c *k8sClient) jobURL(name string) string {
	return c.jobsURL() + "/" + name
}

// createJob posts a batch/v1 Job manifest to the K8s API.
func (c *k8sClient) createJob(ctx context.Context, job batchJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("k8s: marshal job: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.jobsURL(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("k8s: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, nil)
}

// getJob fetches a batch/v1 Job by name.
func (c *k8sClient) getJob(ctx context.Context, name string) (*batchJob, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.jobURL(name), nil)
	if err != nil {
		return nil, fmt.Errorf("k8s: new request: %w", err)
	}
	var job batchJob
	if err := c.do(req, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// deleteJob deletes a batch/v1 Job with Background propagation so that
// the driver and executor Pods are also cleaned up.
func (c *k8sClient) deleteJob(ctx context.Context, name string) error {
	deleteBody := []byte(`{"propagationPolicy":"Background"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.jobURL(name), bytes.NewReader(deleteBody))
	if err != nil {
		return fmt.Errorf("k8s: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, nil)
}

// listJobs lists batch/v1 Jobs matching the given label selector.
func (c *k8sClient) listJobs(ctx context.Context, labelSelector string) ([]jobListItem, error) {
	url := c.jobsURL()
	if labelSelector != "" {
		url += "?labelSelector=" + labelSelector
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("k8s: new request: %w", err)
	}
	var result struct {
		Items []jobListItem `json:"items"`
	}
	if err := c.do(req, &result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// suspendJob patches a Job to set spec.suspend=true.
func (c *k8sClient) suspendJob(ctx context.Context, name string) error {
	return c.patchJobSuspend(ctx, name, true)
}

// unsuspendJob patches a Job to set spec.suspend=false.
func (c *k8sClient) unsuspendJob(ctx context.Context, name string) error {
	return c.patchJobSuspend(ctx, name, false)
}

func (c *k8sClient) patchJobSuspend(ctx context.Context, name string, suspend bool) error {
	patch := fmt.Sprintf(`{"spec":{"suspend":%v}}`, suspend)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.jobURL(name), bytes.NewBufferString(patch))
	if err != nil {
		return fmt.Errorf("k8s: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	return c.do(req, nil)
}

// do executes an HTTP request. If out is non-nil the response body is decoded
// into it. HTTP 4xx/5xx are returned as errors.
func (c *k8sClient) do(req *http.Request, out any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("k8s: %s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("k8s: %s %s: HTTP %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("k8s: decode response: %w", err)
		}
	}
	return nil
}
