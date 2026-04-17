package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	dockerSocket     = "/var/run/docker.sock"
	invocationPort   = 8080
	containerPrefix  = "jc-lambda-"
)

// warmContainer holds the state of a running Docker container for one function.
type warmContainer struct {
	id       string
	hostPort int
	lastUsed time.Time
}

// DockerExecutor manages warm Docker containers per Lambda function.
// Each distinct function name gets one container reused across invocations
// until it has been idle for cfg.KeepaliveSecs seconds.
type DockerExecutor struct {
	cfg        LambdaConfig
	client     *http.Client   // talks to Docker socket
	mu         sync.Mutex
	containers map[string]*warmContainer // functionName → container
	nextPort   int
	done       chan struct{}
	wg         sync.WaitGroup
}

// NewDockerExecutor creates a DockerExecutor and starts the GC goroutine.
func NewDockerExecutor(cfg LambdaConfig) *DockerExecutor {
	e := &DockerExecutor{
		cfg:        cfg,
		containers: make(map[string]*warmContainer),
		nextPort:   9100,
		done:       make(chan struct{}),
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", dockerSocket)
				},
			},
		},
	}
	e.wg.Add(1)
	go e.gcLoop()
	return e
}

// Invoke obtains a warm container for the function (starting one if needed),
// then POSTs the payload and returns the response body.
func (e *DockerExecutor) Invoke(ctx context.Context, req InvokeRequest) ([]byte, error) {
	c, err := e.getOrStart(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("lambda docker: start container: %w", err)
	}

	url := fmt.Sprintf("http://localhost:%d/2015-03-31/functions/function/invocations", c.hostPort)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(req.Payload))
	if err != nil {
		return nil, fmt.Errorf("lambda docker: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		// Container may have died — remove it and return the error.
		e.removeContainer(req.FunctionName)
		return nil, fmt.Errorf("lambda docker: invoke: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("lambda docker: read response: %w", err)
	}

	e.mu.Lock()
	if c, ok := e.containers[req.FunctionName]; ok {
		c.lastUsed = time.Now()
	}
	e.mu.Unlock()

	return body, nil
}

// Close stops all warm containers and the GC goroutine.
func (e *DockerExecutor) Close() error {
	close(e.done)
	e.wg.Wait()

	e.mu.Lock()
	names := make([]string, 0, len(e.containers))
	for name := range e.containers {
		names = append(names, name)
	}
	e.mu.Unlock()

	for _, name := range names {
		e.removeContainer(name)
	}
	return nil
}

// ─── internal ─────────────────────────────────────────────────────────────────

func (e *DockerExecutor) getOrStart(ctx context.Context, req InvokeRequest) (*warmContainer, error) {
	e.mu.Lock()
	if c, ok := e.containers[req.FunctionName]; ok {
		e.mu.Unlock()
		return c, nil
	}
	// Reserve a port and insert a sentinel so concurrent callers skip straight
	// to the existing entry rather than racing to start a second container.
	port := e.nextPort
	e.nextPort++
	sentinel := &warmContainer{hostPort: port}
	e.containers[req.FunctionName] = sentinel
	e.mu.Unlock()

	image := ImageForRuntime(req, e.cfg)
	id, err := e.startContainer(ctx, req, image, port)
	if err != nil {
		// Remove the sentinel so the next caller can retry.
		e.mu.Lock()
		if e.containers[req.FunctionName] == sentinel {
			delete(e.containers, req.FunctionName)
		}
		e.mu.Unlock()
		return nil, err
	}

	c := &warmContainer{id: id, hostPort: port, lastUsed: time.Now()}
	e.mu.Lock()
	e.containers[req.FunctionName] = c
	e.mu.Unlock()
	return c, nil
}

func (e *DockerExecutor) startContainer(ctx context.Context, req InvokeRequest, image string, hostPort int) (string, error) {
	name := containerPrefix + sanitizeName(req.FunctionName)

	env := []string{
		fmt.Sprintf("AWS_LAMBDA_FUNCTION_NAME=%s", req.FunctionName),
		fmt.Sprintf("AWS_DEFAULT_REGION=us-east-1"),
		fmt.Sprintf("_HANDLER=%s", req.Handler),
	}
	if e.cfg.JaisCloudEndpoint != "" {
		env = append(env, "JAISCLOUD_ENDPOINT="+e.cfg.JaisCloudEndpoint)
	}
	for k, v := range req.EnvVars {
		env = append(env, k+"="+v)
	}

	body, _ := json.Marshal(map[string]any{
		"Image": image,
		"Env":   env,
		"ExposedPorts": map[string]any{
			fmt.Sprintf("%d/tcp", invocationPort): map[string]any{},
		},
		"HostConfig": map[string]any{
			"PortBindings": map[string]any{
				fmt.Sprintf("%d/tcp", invocationPort): []map[string]any{
					{"HostPort": fmt.Sprintf("%d", hostPort)},
				},
			},
			"NetworkMode": e.cfg.Network,
			"Memory":      int64(req.MemoryMB) * 1024 * 1024,
		},
	})

	createURL := fmt.Sprintf("http://localhost/v1.41/containers/create?name=%s", name)
	respBody, statusCode, err := e.dockerCall(ctx, http.MethodPost, createURL, body)
	if err != nil {
		return "", fmt.Errorf("docker create: %w", err)
	}
	if statusCode >= 300 {
		return "", fmt.Errorf("docker create: HTTP %d: %s", statusCode, respBody)
	}

	var createResp struct{ Id string }
	json.Unmarshal(respBody, &createResp)

	startURL := fmt.Sprintf("http://localhost/v1.41/containers/%s/start", createResp.Id)
	_, statusCode, err = e.dockerCall(ctx, http.MethodPost, startURL, nil)
	if err != nil {
		return "", fmt.Errorf("docker start: %w", err)
	}
	if statusCode >= 300 {
		return "", fmt.Errorf("docker start: HTTP %d", statusCode)
	}

	// Brief readiness wait.
	time.Sleep(500 * time.Millisecond)
	slog.Info("lambda docker: started container", "function", req.FunctionName, "port", hostPort)
	return createResp.Id, nil
}

func (e *DockerExecutor) removeContainer(functionName string) {
	e.mu.Lock()
	c, ok := e.containers[functionName]
	if ok {
		delete(e.containers, functionName)
	}
	e.mu.Unlock()
	if !ok {
		return
	}

	ctx := context.Background()
	stopURL := fmt.Sprintf("http://localhost/v1.41/containers/%s/stop", c.id)
	e.dockerCall(ctx, http.MethodPost, stopURL, nil)
	rmURL := fmt.Sprintf("http://localhost/v1.41/containers/%s?force=true", c.id)
	e.dockerCall(ctx, http.MethodDelete, rmURL, nil)
	slog.Info("lambda docker: removed container", "function", functionName)
}

func (e *DockerExecutor) gcLoop() {
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

func (e *DockerExecutor) gcOnce() {
	keepalive := time.Duration(e.cfg.KeepaliveSecs) * time.Second
	now := time.Now()
	e.mu.Lock()
	var toRemove []string
	for name, c := range e.containers {
		if now.Sub(c.lastUsed) > keepalive {
			toRemove = append(toRemove, name)
		}
	}
	e.mu.Unlock()
	for _, name := range toRemove {
		slog.Info("lambda docker: GC idle container", "function", name)
		e.removeContainer(name)
	}
}

func (e *DockerExecutor) dockerCall(ctx context.Context, method, url string, body []byte) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return rb, resp.StatusCode, nil
}

func sanitizeName(name string) string {
	return strings.NewReplacer(":", "-", "/", "-", "_", "-").Replace(name)
}
