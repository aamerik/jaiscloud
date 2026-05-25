package ecs

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
	"time"

	"jaiscloud/internal/clock"
		"jaiscloud/internal/k8stypes"
)

const (
	ecsLabelApp     = "jaiscloud-ecs"
	ecsK8sDefaultNS = "jaiscloud"
	ecsK8sAPIServer = "https://kubernetes.default.svc"
)

type k8sExecutor struct {
	apiServer string
	namespace string
	token     string
	k8s       *http.Client
	logsAPI   LogsIngestor
}

func newK8sExecutor(logsAPI LogsIngestor) *k8sExecutor {
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

	apiServer := os.Getenv("JAISCLOUD_K8S_APISERVER")
	if apiServer == "" {
		apiServer = ecsK8sAPIServer
	}
	ns := os.Getenv("JAISCLOUD_K8S_NAMESPACE")
	if ns == "" {
		ns = ecsK8sDefaultNS
	}

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

	return &k8sExecutor{
		apiServer: apiServer,
		namespace: ns,
		token:     token,
		logsAPI:   logsAPI,
		k8s: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}
}

func (e *k8sExecutor) Run(ctx context.Context, spec TaskSpec) (TaskHandle, error) {
	ns := e.namespace
	podName := "jc-ecs-" + ecsShortID(spec.TaskARN)

	var containers []k8stypes.Container
	for _, cs := range spec.Containers {
		var env []k8stypes.EnvVar
		for k, v := range cs.Env {
			env = append(env, k8stypes.EnvVar{Name: k, Value: v})
		}
		var ports []k8stypes.ContainerPort
		for _, pm := range cs.PortMappings {
			ports = append(ports, k8stypes.ContainerPort{ContainerPort: pm.ContainerPort})
		}
		c := k8stypes.Container{
			Name:            sanitizeK8sName(cs.Name),
			Image:           cs.Image,
			ImagePullPolicy: "IfNotPresent",
			Args:            cs.Cmd,
			Env:             env,
			Ports:           ports,
		}
		if cs.Memory > 0 || cs.CPU > 0 {
			res := &k8stypes.Resources{
				Requests: map[string]string{},
				Limits:   map[string]string{},
			}
			if cs.Memory > 0 {
				res.Limits["memory"] = fmt.Sprintf("%dMi", cs.Memory/1024/1024)
			}
			if cs.CPU > 0 {
				res.Requests["cpu"] = "100m"
			}
			c.Resources = res
		}
		containers = append(containers, c)
	}

	podLabels := map[string]string{
		"app":               ecsLabelApp,
		"jaiscloud.io/task": spec.TaskARN,
	}

	podSpec := k8stypes.PodSpec{
		RestartPolicy: "Never",
		Containers:    containers,
	}

	podManifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      podName,
			"namespace": ns,
			"labels":    podLabels,
		},
		"spec": podSpec,
	}

	podBody, _ := json.Marshal(podManifest)
	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods", e.apiServer, ns)
	if err := e.k8sPost(ctx, podURL, podBody); err != nil {
		return TaskHandle{}, fmt.Errorf("ecs k8s: create pod: %w", err)
	}
	slog.Info("ecs k8s: pod created", "pod", podName, "task", spec.TaskARN)

	// Stream logs for each container after the pod starts.
	if e.logsAPI != nil {
		taskID := ecsShortID(spec.TaskARN)
		for _, cs := range spec.Containers {
			logCfg := cs.LogConfig
			if logCfg.LogDriver == "" {
				logCfg = spec.LogConfig
			}
			if logCfg.LogDriver == "awslogs" {
				go e.streamPodLogs(context.Background(), ns, podName, cs.Name, logCfg, taskID)
			}
		}
	}

	return TaskHandle{PodName: podName, Mode: ModeK8s}, nil
}

func (e *k8sExecutor) Wait(ctx context.Context, handle TaskHandle) error {
	ns := e.namespace
	podName := handle.PodName
	for {
		phase, err := e.podPhase(ctx, ns, podName)
		if err != nil {
			return err
		}
		if phase == "Succeeded" || phase == "Failed" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (e *k8sExecutor) Stop(ctx context.Context, handle TaskHandle) error {
	if handle.PodName == "" {
		return nil
	}
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.apiServer, e.namespace, handle.PodName)
	e.k8sDelete(ctx, url)
	return nil
}

func (e *k8sExecutor) StatusOf(ctx context.Context, handle TaskHandle) (Status, error) {
	if handle.PodName == "" {
		return Status{LastStatus: "STOPPED"}, nil
	}
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.apiServer, e.namespace, handle.PodName)
	body, status, err := e.k8sGet(ctx, url)
	if err != nil || status >= 300 {
		return Status{LastStatus: "STOPPED"}, nil
	}

	var pod struct {
		Status struct {
			Phase             string `json:"phase"`
			ContainerStatuses []struct {
				Name  string `json:"name"`
				State struct {
					Running    *struct{} `json:"running"`
					Terminated *struct {
						ExitCode int `json:"exitCode"`
					} `json:"terminated"`
				} `json:"state"`
			} `json:"containerStatuses"`
		} `json:"status"`
	}
	json.Unmarshal(body, &pod)

	last := podPhaseToECS(pod.Status.Phase)
	var containers []ContainerStatus
	for _, cs := range pod.Status.ContainerStatuses {
		cst := ContainerStatus{Name: cs.Name, LastStatus: "PENDING"}
		if cs.State.Running != nil {
			cst.LastStatus = "RUNNING"
		} else if cs.State.Terminated != nil {
			cst.LastStatus = "STOPPED"
			code := cs.State.Terminated.ExitCode
			cst.ExitCode = &code
		}
		containers = append(containers, cst)
	}

	return Status{LastStatus: last, Containers: containers}, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (e *k8sExecutor) podPhase(ctx context.Context, ns, podName string) (string, error) {
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.apiServer, ns, podName)
	body, status, err := e.k8sGet(ctx, url)
	if err != nil || status >= 300 {
		return "", fmt.Errorf("ecs k8s: get pod: HTTP %d: %w", status, err)
	}
	var pod struct {
		Status struct{ Phase string } `json:"status"`
	}
	json.Unmarshal(body, &pod)
	return pod.Status.Phase, nil
}

func (e *k8sExecutor) streamPodLogs(ctx context.Context, ns, podName, containerName string, cfg LogConfig, taskID string) {
	// Wait for container to start before streaming.
	deadline := clock.RealNow().Add(90 * time.Second)
	for clock.RealNow().Before(deadline) {
		url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.apiServer, ns, podName)
		body, status, err := e.k8sGet(ctx, url)
		if err != nil || status >= 300 {
			time.Sleep(2 * time.Second)
			continue
		}
		var pod struct {
			Status struct{ Phase string } `json:"status"`
		}
		json.Unmarshal(body, &pod)
		if pod.Status.Phase == "Running" || pod.Status.Phase == "Succeeded" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	logsURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/log?container=%s&follow=true",
		e.apiServer, ns, podName, containerName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, logsURL, nil)
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
	defer resp.Body.Close()
	StreamLogs(ctx, e.logsAPI, cfg, containerName, taskID, resp.Body)
}

func (e *k8sExecutor) k8sPost(ctx context.Context, url string, body []byte) error {
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

func (e *k8sExecutor) k8sGet(ctx context.Context, url string) ([]byte, int, error) {
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

func (e *k8sExecutor) k8sDelete(ctx context.Context, url string) {
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

func podPhaseToECS(phase string) string {
	switch phase {
	case "Pending":
		return "PENDING"
	case "Running":
		return "RUNNING"
	case "Succeeded", "Failed":
		return "STOPPED"
	default:
		return "PROVISIONING"
	}
}

func sanitizeK8sName(name string) string {
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
