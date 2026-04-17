package lambda

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// K8sExecutor submits a one-shot batch/v1 Job per Lambda invocation.
// Results are retrieved by polling Job status then reading pod logs.
type K8sExecutor struct {
	cfg    LambdaConfig
	client *http.Client
	token  string // bearer token from in-cluster service account
}

// NewK8sExecutor creates a K8sExecutor.
// It reads in-cluster auth (/var/run/secrets/kubernetes.io/serviceaccount/token)
// when JAISCLOUD_K8S_TOKEN is not set.
func NewK8sExecutor(cfg LambdaConfig) *K8sExecutor {
	token := os.Getenv("JAISCLOUD_K8S_TOKEN")
	if token == "" {
		b, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		token = string(b)
	}
	if cfg.APIServer == "" {
		cfg.APIServer = "https://kubernetes.default.svc"
	}
	return &K8sExecutor{
		cfg:    cfg,
		token:  token,
		client: &http.Client{Timeout: 10 * time.Minute},
	}
}

func (e *K8sExecutor) Invoke(ctx context.Context, req InvokeRequest) ([]byte, error) {
	image := ImageForRuntime(req, e.cfg)
	jobName := fmt.Sprintf("jc-lambda-%s-%s", sanitizeName(req.FunctionName), shortID())
	ns := e.cfg.Namespace

	payloadB64 := base64.StdEncoding.EncodeToString(req.Payload)
	env := []map[string]string{
		{"name": "_LAMBDA_PAYLOAD_B64", "value": payloadB64},
		{"name": "AWS_LAMBDA_FUNCTION_NAME", "value": req.FunctionName},
		{"name": "_HANDLER", "value": req.Handler},
	}
	for k, v := range req.EnvVars {
		env = append(env, map[string]string{"name": k, "value": v})
	}

	job := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      jobName,
			"namespace": ns,
			"labels": map[string]string{
				"app":      "jaiscloud-lambda",
				"function": sanitizeName(req.FunctionName),
			},
		},
		"spec": map[string]any{
			"ttlSecondsAfterFinished": 300,
			"backoffLimit":            0,
			"activeDeadlineSeconds":   req.TimeoutSecs + 10,
			"template": map[string]any{
				"spec": map[string]any{
					"restartPolicy":      "Never",
					"serviceAccountName": e.cfg.ServiceAccount,
					"containers": []map[string]any{{
						"name":  "lambda",
						"image": image,
						"env":   env,
						"resources": map[string]any{
							"limits": map[string]string{
								"memory": fmt.Sprintf("%dMi", req.MemoryMB),
								"cpu":    "1",
							},
						},
					}},
				},
			},
		},
	}

	body, _ := json.Marshal(job)
	url := fmt.Sprintf("%s/apis/batch/v1/namespaces/%s/jobs", e.cfg.APIServer, ns)
	if err := e.k8sPost(ctx, url, body); err != nil {
		return nil, fmt.Errorf("lambda k8s: create job: %w", err)
	}
	slog.Info("lambda k8s: job created", "job", jobName, "function", req.FunctionName)

	if err := e.waitForJob(ctx, ns, jobName, req.TimeoutSecs); err != nil {
		return nil, fmt.Errorf("lambda k8s: job failed: %w", err)
	}

	logs, err := e.readPodLogs(ctx, ns, jobName)
	if err != nil {
		return nil, fmt.Errorf("lambda k8s: read logs: %w", err)
	}
	return logs, nil
}

func (e *K8sExecutor) Close() error { return nil }

// ─── internal ─────────────────────────────────────────────────────────────────

func (e *K8sExecutor) waitForJob(ctx context.Context, ns, jobName string, timeoutSecs int) error {
	deadline := time.Now().Add(time.Duration(timeoutSecs) * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for job " + jobName)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}

		url := fmt.Sprintf("%s/apis/batch/v1/namespaces/%s/jobs/%s", e.cfg.APIServer, ns, jobName)
		body, statusCode, err := e.k8sGet(ctx, url)
		if err != nil || statusCode != 200 {
			continue
		}
		var job struct {
			Status struct {
				Succeeded int `json:"succeeded"`
				Failed    int `json:"failed"`
			} `json:"status"`
		}
		if json.Unmarshal(body, &job) != nil {
			continue
		}
		if job.Status.Succeeded > 0 {
			return nil
		}
		if job.Status.Failed > 0 {
			return fmt.Errorf("job %s failed", jobName)
		}
	}
}

func (e *K8sExecutor) readPodLogs(ctx context.Context, ns, jobName string) ([]byte, error) {
	// List pods for the job.
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods?labelSelector=job-name=%s",
		e.cfg.APIServer, ns, jobName)
	body, _, err := e.k8sGet(ctx, url)
	if err != nil {
		return nil, err
	}
	var podList struct {
		Items []struct {
			Metadata struct{ Name string `json:"name"` } `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &podList); err != nil || len(podList.Items) == 0 {
		return []byte("{}"), nil
	}
	podName := podList.Items[0].Metadata.Name
	logURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/log", e.cfg.APIServer, ns, podName)
	logs, _, err := e.k8sGet(ctx, logURL)
	if err != nil {
		return nil, err
	}
	// The last line of pod stdout is treated as the Lambda response payload.
	lines := strings.Split(strings.TrimSpace(string(logs)), "\n")
	return []byte(lines[len(lines)-1]), nil
}

func (e *K8sExecutor) k8sPost(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.client.Do(req)
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
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func shortID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b)
}
