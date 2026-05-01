package spark

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
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

// jobListMeta is the metadata subset returned when listing Jobs.
// Annotations are included so cleanupOrphans can read the raw job-ID annotation
// (jaiscloud.io/job-id-raw) added by buildJobManifest.
type jobListMeta struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// jobListSpec is the spec subset returned when listing Jobs.
type jobListSpec struct {
	Suspend *bool `json:"suspend,omitempty"`
}

// jobListItem is used when listing jobs (lighter than full batchJob).
// Status embeds batchJobStatus so mapJobStatus/jobFailureMessage work directly
// on list items without a separate get-by-name round-trip.
type jobListItem struct {
	Metadata jobListMeta    `json:"metadata"`
	Spec     jobListSpec    `json:"spec"`
	Status   batchJobStatus `json:"status"`
}

type jobCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// k8sClientInterface abstracts the K8s API operations used by K8sExecutor.
// It allows tests to inject a fakeK8sClient without a live cluster.
type k8sClientInterface interface {
	createJob(ctx context.Context, job batchJob) error
	getJob(ctx context.Context, name string) (*batchJob, error)
	deleteJob(ctx context.Context, name string) error
	listJobs(ctx context.Context, labelSelector string) ([]jobListItem, error)
	suspendJob(ctx context.Context, name string) error
	unsuspendJob(ctx context.Context, name string) error
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
// It follows K8s list continuation tokens to page through large clusters.
func (c *k8sClient) listJobs(ctx context.Context, labelSelector string) ([]jobListItem, error) {
	const maxItems = 10_000
	var all []jobListItem
	continueToken := ""
	for {
		url := c.jobsURL()
		sep := "?"
		if labelSelector != "" {
			url += sep + "labelSelector=" + labelSelector
			sep = "&"
		}
		if continueToken != "" {
			url += sep + "continue=" + continueToken
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("k8s: new request: %w", err)
		}
		var result struct {
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
			Items []jobListItem `json:"items"`
		}
		if err := c.do(req, &result); err != nil {
			return nil, err
		}
		all = append(all, result.Items...)
		if len(all) >= maxItems {
			slog.Warn("spark k8s: listJobs: safety cap reached; some jobs may be skipped",
				"cap", maxItems)
			break
		}
		if result.Metadata.Continue == "" {
			break
		}
		continueToken = result.Metadata.Continue
	}
	return all, nil
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

// ErrJobNotFound is returned by k8sclient operations when the K8s API server
// replies 404 for the named Job. Use errors.Is(err, ErrJobNotFound).
var ErrJobNotFound = fmt.Errorf("k8s job not found")

// k8sAPIError wraps a non-2xx response from the apiserver. It unwraps to
// ErrJobNotFound on 404 so callers can use errors.Is for that case.
type k8sAPIError struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}

func (e *k8sAPIError) Error() string {
	return fmt.Sprintf("k8s api: %s %s: HTTP %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

// Unwrap allows errors.Is(err, ErrJobNotFound) on 404 responses.
func (e *k8sAPIError) Unwrap() error {
	if e.StatusCode == 404 {
		return ErrJobNotFound
	}
	return nil
}

// encodeLabelSelector URL-encodes a raw K8s label selector string for use in
// query parameters. Only "=" (%3D) and "/" (%2F) are escaped — commas are
// preserved because K8s accepts literal commas in query strings.
func encodeLabelSelector(raw string) string {
	s := strings.ReplaceAll(raw, "=", "%3D")
	s = strings.ReplaceAll(s, "/", "%2F")
	return s
}

// do executes an HTTP request. If out is non-nil the response body is decoded
// into it. HTTP 4xx/5xx are returned as *k8sAPIError (use errors.Is for ErrJobNotFound).
func (c *k8sClient) do(req *http.Request, out any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("k8s: %s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &k8sAPIError{
			Method:     req.Method,
			URL:        req.URL.String(),
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("k8s: decode response: %w", err)
		}
	}
	return nil
}
