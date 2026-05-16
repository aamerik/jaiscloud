package lambda

import (
	"bytes"
	"context"
	"encoding/json"
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

	"jaiscloud/internal/platform"
)

// CodeLoader fetches function zip bytes for mounting into a container.
type CodeLoader interface {
	LoadCode(ctx context.Context, account, funcName, version string) ([]byte, error)
}

// LayerBlobLoader fetches layer zip bytes by blob key.
type LayerBlobLoader interface {
	GetLayerBlob(ctx context.Context, blobKey string) ([]byte, error)
}

const (
	dockerSocket   = "/var/run/docker.sock"
	invocationPort = 8080
)

// warmContainer holds the state of a running Docker container for one function.
type warmContainer struct {
	id       string
	hostPort int
	lastUsed time.Time
	codeDir  string // temp dir holding extracted /var/task; empty = no code mounted
	optDir   string // temp dir holding extracted /opt (layers); empty = no layers mounted
}

// DockerExecutor manages warm Docker containers per Lambda function.
// Each distinct function name gets one container reused across invocations
// until it has been idle for cfg.KeepaliveSecs seconds.
type DockerExecutor struct {
	cfg         LambdaConfig
	platform    *platform.PlatformConfig
	client      *http.Client // talks to Docker socket
	mu          sync.Mutex
	containers  map[string]*warmContainer // functionName → container
	nextPort    int
	done        chan struct{}
	wg          sync.WaitGroup
	codeLoader  CodeLoader      // optional; nil in tests
	layerLoader LayerBlobLoader // optional; nil = no layer mounting
	logsAPI     LogsIngestor    // optional; nil in tests
}

// SetCodeLoader injects the code loader used to mount /var/task into containers.
func (e *DockerExecutor) SetCodeLoader(l CodeLoader) { e.codeLoader = l }

// SetLayerBlobLoader injects the layer blob loader used to mount layers at /opt.
func (e *DockerExecutor) SetLayerBlobLoader(l LayerBlobLoader) { e.layerLoader = l }

// SetLogsAPI injects the CloudWatch Logs ingestor for container log streaming.
func (e *DockerExecutor) SetLogsAPI(l LogsIngestor) { e.logsAPI = l }

// NewDockerExecutor creates a DockerExecutor and starts the GC goroutine.
// plat may be nil.
func NewDockerExecutor(cfg LambdaConfig, plat *platform.PlatformConfig) *DockerExecutor {
	e := &DockerExecutor{
		cfg:        cfg,
		platform:   plat,
		containers: make(map[string]*warmContainer),
		nextPort:   9100,
		done:       make(chan struct{}),
		client: &http.Client{
			Timeout: 16 * time.Minute,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", dockerSocket)
				},
			},
		},
	}
	e.cleanupOrphans()
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
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
			e.removeContainer(req.FunctionName)
			return nil, ctx.Err()
		default:
			return nil, fmt.Errorf("lambda docker: invoke: %w", err)
		}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("lambda docker: read response: %w", err)
	}
	if resp.StatusCode >= 500 {
		e.removeContainer(req.FunctionName)
		return nil, fmt.Errorf("lambda docker: RIE returned HTTP %d", resp.StatusCode)
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
	id, codeDir, optDir, err := e.startContainer(ctx, req, image, port)
	if err != nil {
		// Remove the sentinel so the next caller can retry.
		e.mu.Lock()
		if e.containers[req.FunctionName] == sentinel {
			delete(e.containers, req.FunctionName)
		}
		e.mu.Unlock()
		return nil, err
	}

	c := &warmContainer{id: id, hostPort: port, lastUsed: time.Now(), codeDir: codeDir, optDir: optDir}
	e.mu.Lock()
	e.containers[req.FunctionName] = c
	e.mu.Unlock()
	return c, nil
}

func (e *DockerExecutor) startContainer(ctx context.Context, req InvokeRequest, image string, hostPort int) (id string, codeDir string, optDir string, err error) {
	pfx := instancePrefix(e.cfg.InstanceID)
	name := pfx + sanitizeName(req.FunctionName) + "-" + shortID()

	env := []string{
		fmt.Sprintf("AWS_LAMBDA_FUNCTION_NAME=%s", req.FunctionName),
		fmt.Sprintf("AWS_DEFAULT_REGION=%s", regionOrDefault(e.cfg.Region)),
		fmt.Sprintf("AWS_REGION=%s", regionOrDefault(e.cfg.Region)),
		fmt.Sprintf("_HANDLER=%s", req.Handler),
		fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", req.AccountID),
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_SESSION_TOKEN=test",
		"LAMBDA_TASK_ROOT=/var/task",
		"LAMBDA_RUNTIME_DIR=/var/runtime",
		"AWS_LAMBDA_RUNTIME_API=127.0.0.1:9001",
	}
	if e.cfg.JaisCloudEndpoint != "" {
		env = append(env, "AWS_ENDPOINT_URL="+e.cfg.JaisCloudEndpoint)
		env = append(env, "JAISCLOUD_ENDPOINT="+e.cfg.JaisCloudEndpoint)
	}
	for k, v := range req.EnvVars {
		env = append(env, k+"="+v)
	}

	// Platform layer: TLS PEM bundle + extra env for this container.
	var binds []string
	if e.platform != nil {
		volArgs, envArgs, applyErr := platform.ApplyDocker(e.platform)
		if applyErr != nil {
			slog.Warn("lambda docker: platform apply failed", "err", applyErr)
		}
		// volArgs are pairs ["-v", "src:dst:ro"]; extract bind strings.
		for i := 1; i < len(volArgs); i += 2 {
			binds = append(binds, volArgs[i])
		}
		// envArgs are pairs ["-e", "KEY=VAL"]; extract env strings.
		for i := 1; i < len(envArgs); i += 2 {
			env = append(env, envArgs[i])
		}
	}

	// Extract and mount function code into /var/task when a code loader is available.
	if e.codeLoader != nil && req.AccountID != "" && req.FunctionName != "" {
		if zipBytes, loadErr := e.codeLoader.LoadCode(context.Background(), req.AccountID, req.FunctionName, "$LATEST"); loadErr == nil && len(zipBytes) > 0 {
			if dir, mkErr := os.MkdirTemp("", "lambda-code-*"); mkErr == nil {
				if extErr := ExtractZip(zipBytes, dir); extErr == nil {
					codeDir = dir
					binds = append(binds, dir+":/var/task:ro")
				}
			}
		}
	}

	// Extract and mount each layer into /opt when a layer blob loader is available.
	if e.layerLoader != nil && len(req.Layers) > 0 {
		var mkErr error
		optDir, mkErr = os.MkdirTemp("", "lambda-opt-*")
		if mkErr == nil {
			anyLayerMounted := false
			for _, layer := range req.Layers {
				if layer.BlobKey == "" {
					continue
				}
				zipBytes, loadErr := e.layerLoader.GetLayerBlob(context.Background(), layer.BlobKey)
				if loadErr != nil || len(zipBytes) == 0 {
					slog.Warn("lambda docker: failed to load layer blob", "arn", layer.ARN, "err", loadErr)
					continue
				}
				if extErr := ExtractZip(zipBytes, optDir); extErr != nil {
					slog.Warn("lambda docker: failed to extract layer", "arn", layer.ARN, "err", extErr)
					continue
				}
				anyLayerMounted = true
			}
			if anyLayerMounted {
				binds = append(binds, optDir+":/opt:ro")
			} else {
				os.RemoveAll(optDir)
				optDir = ""
			}
		} else {
			optDir = ""
		}
	} else if len(req.Layers) > 0 {
		slog.Debug("lambda docker: layers configured but no layer blob loader set; skipping layer mount", "count", len(req.Layers))
	}

	hostConfig := map[string]any{
		"PortBindings": map[string]any{
			fmt.Sprintf("%d/tcp", invocationPort): []map[string]any{
				{"HostPort": fmt.Sprintf("%d", hostPort)},
			},
		},
		"NetworkMode": e.cfg.Network,
		"Memory":      int64(req.MemoryMB) * 1024 * 1024,
	}
	if len(binds) > 0 {
		hostConfig["Binds"] = binds
	}

	body, _ := json.Marshal(map[string]any{
		"Image": image,
		"Env":   env,
		"ExposedPorts": map[string]any{
			fmt.Sprintf("%d/tcp", invocationPort): map[string]any{},
		},
		"HostConfig": hostConfig,
	})

	createURL := fmt.Sprintf("http://localhost/v1.41/containers/create?name=%s", name)
	respBody, statusCode, createErr := e.dockerCall(ctx, http.MethodPost, createURL, body)
	if createErr != nil {
		return "", codeDir, optDir, fmt.Errorf("docker create: %w", createErr)
	}
	if statusCode >= 300 {
		return "", codeDir, optDir, fmt.Errorf("docker create: HTTP %d: %s", statusCode, respBody)
	}

	var createResp struct{ Id string }
	json.Unmarshal(respBody, &createResp)

	startURL := fmt.Sprintf("http://localhost/v1.41/containers/%s/start", createResp.Id)
	_, statusCode, startErr := e.dockerCall(ctx, http.MethodPost, startURL, nil)
	if startErr != nil {
		return "", codeDir, optDir, fmt.Errorf("docker start: %w", startErr)
	}
	if statusCode >= 300 {
		return "", codeDir, optDir, fmt.Errorf("docker start: HTTP %d", statusCode)
	}

	// Brief readiness wait.
	time.Sleep(500 * time.Millisecond)
	slog.Info("lambda docker: started container", "function", req.FunctionName, "port", hostPort)
	return createResp.Id, codeDir, optDir, nil
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
	if c.codeDir != "" {
		os.RemoveAll(c.codeDir)
	}
	if c.optDir != "" {
		os.RemoveAll(c.optDir)
	}
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
		if c.id == "" {
			continue // sentinel during cold start; skip
		}
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

// DeleteFunction tears down the warm container for the named function.
func (e *DockerExecutor) DeleteFunction(_ context.Context, name string) {
	e.removeContainer(name)
}

// Reset tears down all warm containers without stopping the GC goroutine.
// After the map pass it sweeps by instance prefix to catch containers that
// are live on the daemon but missing from the in-memory map (LG5).
func (e *DockerExecutor) Reset() {
	e.mu.Lock()
	names := make([]string, 0, len(e.containers))
	for name := range e.containers {
		names = append(names, name)
	}
	e.mu.Unlock()
	for _, name := range names {
		e.removeContainer(name)
	}
	// Best-effort sweep for escaped containers.
	e.removeContainersByPrefix(instancePrefix(e.cfg.InstanceID))
}

// cleanupOrphans stops and removes Docker containers from previous runs.
// Only containers whose names start with this instance's prefix are removed
// so multiple JaisCloud instances on the same Docker daemon don't cross-reap (LG4).
func (e *DockerExecutor) cleanupOrphans() {
	pfx := instancePrefix(e.cfg.InstanceID)
	e.removeContainersByPrefix(pfx)
}

// removeContainersByPrefix lists and removes all containers whose names match prefix.
func (e *DockerExecutor) removeContainersByPrefix(pfx string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	filterVal := fmt.Sprintf(`{"name":["%s"]}`, pfx)
	url := "http://localhost/v1.41/containers/json?all=true&filters=" + filterVal
	body, status, err := e.dockerCall(ctx, http.MethodGet, url, nil)
	if err != nil || status >= 300 {
		return
	}
	var items []struct {
		Id string `json:"Id"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return
	}
	for _, item := range items {
		e.dockerCall(ctx, http.MethodPost, fmt.Sprintf("http://localhost/v1.41/containers/%s/stop", item.Id), nil)
		e.dockerCall(ctx, http.MethodDelete, fmt.Sprintf("http://localhost/v1.41/containers/%s?force=true", item.Id), nil)
		slog.Info("lambda docker: cleaned up orphan container", "id", item.Id[:12])
	}
}

func sanitizeName(name string) string {
	return strings.NewReplacer(":", "-", "/", "-", "_", "-").Replace(name)
}
