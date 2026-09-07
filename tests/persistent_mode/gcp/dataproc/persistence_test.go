//go:build gcp_persistence

// Package dataproc_test verifies that Cloud Dataproc clusters and jobs survive
// an end-to-end export → import round trip through the /_jaiscloud admin
// endpoints when jaiscloud-gcp is backed by PostgreSQL (--dsn).
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN                  — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_DATAPROC_PERSIST_PORT — port for the managed server (default 8098)
package dataproc_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

func gcpBin() string {
	if b := os.Getenv("JAISCLOUD_GCP_BIN"); b != "" {
		return b
	}
	const rel = "../../../../jaiscloud-gcp"
	if _, err := os.Stat(rel); err == nil {
		return rel
	}
	return "jaiscloud-gcp"
}

func persistPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_DATAPROC_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8098
}

type testWriter struct {
	t      *testing.T
	prefix string
	buf    string
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.buf += string(p)
	for {
		idx := strings.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.t.Log(w.prefix + w.buf[:idx])
		w.buf = w.buf[idx+1:]
	}
	return len(p), nil
}

func startGCPProcess(t *testing.T, port int, dsn, blobDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(gcpBin(),
		"start",
		"--port", strconv.Itoa(port),
		"--dsn", dsn,
		"--blob-dir", blobDir,
		"--log-level", "warn",
	)
	cmd.Stdout = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d]: ", port)}
	cmd.Stderr = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d] ERR: ", port)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start jaiscloud-gcp on port %d: %v", port, err)
	}
	return cmd
}

func waitForHealth(t *testing.T, host string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := clock.RealNow().Add(30 * time.Second)
	for clock.RealNow().Before(deadline) {
		resp, err := client.Get(host + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", host)
}

// doRequest performs an HTTP request and returns the status code and raw body
// bytes (binary-safe, unlike a string reader).
func doRequest(t *testing.T, host, method, path string, body []byte, contentType string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, host+path, rd)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s read: %v", method, path, err)
	}
	return resp.StatusCode, data
}

func stopProcess(t *testing.T, cmd *exec.Cmd, port int) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", port))
		if err != nil {
			return
		}
		c.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("jaiscloud-gcp did not release the port after kill")
}

// jsonObj decodes a JSON response body into a map.
func jsonObj(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode JSON %q: %v", body, err)
	}
	return m
}

// strField navigates a nested map path (e.g. "status", "state") returning the
// string value, or "" when any segment is absent.
func strField(m map[string]any, path ...string) string {
	var cur any = m
	for _, k := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = mm[k]
	}
	s, _ := cur.(string)
	return s
}

// TestDataprocExportImportRoundTrip creates a cluster and two jobs (a mock-mode
// DONE sparkJob and an unsupported hiveJob that errors), exports the full
// instance state, and imports it back — asserting the terminal job state and
// status.details survive byte-for-byte.
func TestDataprocExportImportRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping dataproc export/import persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)
	blobDir := t.TempDir()

	proc := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc, port)
	waitForHealth(t, host)

	// Start from a clean slate (the shared DSN may carry state from prior runs).
	if code, body := doRequest(t, host, "POST", "/_jaiscloud/reset", nil, "application/json"); code != http.StatusOK {
		t.Fatalf("reset: got HTTP %d body %s", code, body)
	}

	const project = "proj"
	const region = "us-central1"
	const clusterName = "exp-cluster"
	const doneJob = "exp-job-done"
	const errJob = "exp-job-error"
	base := "/v1/projects/" + project + "/regions/" + region

	// ── Create cluster ────────────────────────────────────────────────────────
	code, body := doRequest(t, host, "POST", base+"/clusters",
		[]byte(`{"projectId":"`+project+`","clusterName":"`+clusterName+`","config":{"gceClusterConfig":{"zoneUri":"us-central1-a"}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("create cluster: got HTTP %d body %s", code, body)
	}

	// ── Submit a mock-mode Spark job (completes synchronously → DONE) ────────
	code, body = doRequest(t, host, "POST", base+"/jobs:submit",
		[]byte(`{"job":{"reference":{"projectId":"`+project+`","jobId":"`+doneJob+`"},"placement":{"clusterName":"`+clusterName+`"},"sparkJob":{"mainJarFileUri":"gs://b/a.jar","mainClass":"Main"}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("submit spark job: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "DONE" {
		t.Fatalf("expected DONE spark job, got state %q body %s", state, body)
	}

	// ── Submit an unsupported job type → ERROR with terminal details ─────────
	code, body = doRequest(t, host, "POST", base+"/jobs:submit",
		[]byte(`{"job":{"reference":{"projectId":"`+project+`","jobId":"`+errJob+`"},"placement":{"clusterName":"`+clusterName+`"},"hiveJob":{"queryFileUri":"gs://b/q.hql"}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("submit hive job: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "ERROR" {
		t.Fatalf("expected ERROR hive job, got state %q body %s", state, body)
	}
	detailsBefore := strField(jsonObj(t, body), "status", "details")
	if detailsBefore == "" {
		t.Fatalf("expected non-empty status.details for failed job, got body %s", body)
	}

	// ── Export the full instance state ────────────────────────────────────────
	code, tarball := doRequest(t, host, "GET", "/_jaiscloud/export", nil, "")
	if code != http.StatusOK {
		t.Fatalf("export: got HTTP %d body %s", code, tarball)
	}
	if len(tarball) < 2 || tarball[0] != 0x1f || tarball[1] != 0x8b {
		t.Fatalf("export did not return a gzip tarball (first bytes %x)", tarball[:min(len(tarball), 2)])
	}

	// ── Import without reset_first: existing state → 409 non_empty_state ──────
	code, body = doRequest(t, host, "POST", "/_jaiscloud/import", tarball, "application/gzip")
	if code != http.StatusConflict {
		t.Fatalf("import without reset_first: expected 409, got HTTP %d body %s", code, body)
	}
	if !strings.Contains(string(body), "non_empty_state") {
		t.Fatalf("expected non_empty_state error, got body %s", body)
	}

	// ── Import with reset_first=true: clears state then restores ──────────────
	code, body = doRequest(t, host, "POST", "/_jaiscloud/import?reset_first=true", tarball, "application/gzip")
	if code != http.StatusOK {
		t.Fatalf("import with reset_first: got HTTP %d body %s", code, body)
	}

	// ── Verify restored cluster ───────────────────────────────────────────────
	code, body = doRequest(t, host, "GET", base+"/clusters/"+clusterName, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get cluster after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "RUNNING" {
		t.Fatalf("cluster state after import: got %q body %s", state, body)
	}

	// ── Verify restored jobs ──────────────────────────────────────────────────
	code, body = doRequest(t, host, "GET", base+"/jobs/"+doneJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get done job after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "DONE" {
		t.Fatalf("done job state after import: got %q body %s", state, body)
	}

	code, body = doRequest(t, host, "GET", base+"/jobs/"+errJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get error job after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "ERROR" {
		t.Fatalf("error job state after import: got %q body %s", state, body)
	}
	if detailsAfter := strField(jsonObj(t, body), "status", "details"); detailsAfter != detailsBefore {
		t.Fatalf("status.details lost after import: got %q want %q", detailsAfter, detailsBefore)
	}

	// ── Verify lists return the restored entries ──────────────────────────────
	code, body = doRequest(t, host, "GET", base+"/clusters", nil, "")
	if code != http.StatusOK {
		t.Fatalf("list clusters after import: got HTTP %d body %s", code, body)
	}
	clusters, _ := jsonObj(t, body)["clusters"].([]any)
	if len(clusters) != 1 || strField(clusters[0].(map[string]any), "clusterName") != clusterName {
		t.Fatalf("list clusters after import: expected [%s], got %v", clusterName, clusters)
	}

	code, body = doRequest(t, host, "GET", base+"/jobs", nil, "")
	if code != http.StatusOK {
		t.Fatalf("list jobs after import: got HTTP %d body %s", code, body)
	}
	jobs, _ := jsonObj(t, body)["jobs"].([]any)
	if len(jobs) != 2 {
		t.Fatalf("list jobs after import: expected 2 jobs, got %d", len(jobs))
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
