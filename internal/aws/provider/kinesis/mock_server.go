package kinesis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

const (
	kinesismockVersion   = "0.4.7"
	kinesismockBaseURL   = "https://github.com/etspaceman/kinesis-mock/releases/download/v" + kinesismockVersion
	kinesismockShardLimit = 500
	kinesismockOnDemandLimit = 50
)

// MockServer manages a kinesis-mock subprocess for full-fidelity Kinesis emulation.
type MockServer struct {
	port      int
	tlsPort   int // allocated but not used; kinesis-mock requires it
	binaryPath string
	accountID  string
	dataDir    string
	latency    string

	mu  sync.Mutex
	cmd *exec.Cmd
}

// NewMockServer creates a MockServer. Start() must be called separately.
func NewMockServer(accountID, dataDir string) (*MockServer, error) {
	binPath, err := locateOrDownloadBinary()
	if err != nil {
		return nil, fmt.Errorf("kinesis-mock binary: %w", err)
	}
	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("kinesis-mock: find free port: %w", err)
	}
	tlsPort, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("kinesis-mock: find free tls port: %w", err)
	}
	return &MockServer{
		port:       port,
		tlsPort:    tlsPort,
		binaryPath: binPath,
		accountID:  accountID,
		dataDir:    dataDir,
		latency:    "0",
	}, nil
}

// Port returns the HTTP port kinesis-mock is bound to.
func (s *MockServer) Port() int { return s.port }

// Start launches the kinesis-mock binary and waits up to 60s for it to be ready.
func (s *MockServer) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	env := s.buildEnv()
	cmd := exec.CommandContext(ctx, s.binaryPath)
	cmd.Env = env
	cmd.Stdout = &slogWriter{level: slog.LevelDebug, prefix: "kinesis-mock"}
	cmd.Stderr = &slogWriter{level: slog.LevelWarn, prefix: "kinesis-mock"}

	slog.Info("starting kinesis-mock", "port", s.port, "binary", s.binaryPath)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("kinesis-mock start: %w", err)
	}
	s.cmd = cmd

	if err := s.waitReady(ctx, 60*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("kinesis-mock startup timeout: %w", err)
	}
	slog.Info("kinesis-mock ready", "port", s.port)
	return nil
}

// Stop sends SIGTERM and waits up to 5 seconds, then SIGKILL.
func (s *MockServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	}
	s.cmd = nil
	return nil
}

// Restart stops then starts kinesis-mock (used for reset in full mode).
func (s *MockServer) Restart(ctx context.Context) error {
	if err := s.Stop(); err != nil {
		return err
	}
	return s.Start(ctx)
}

// Healthy returns true if the subprocess is running and accepting connections.
func (s *MockServer) Healthy() bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", s.port), time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// ─── env var construction ─────────────────────────────────────────────────────

func (s *MockServer) buildEnv() []string {
	latencyParams := []string{
		"CREATE_STREAM_DURATION",
		"DELETE_STREAM_DURATION",
		"REGISTER_STREAM_CONSUMER_DURATION",
		"DEREGISTER_STREAM_CONSUMER_DURATION",
		"MERGE_SHARDS_DURATION",
		"SPLIT_SHARD_DURATION",
		"UPDATE_SHARD_COUNT_DURATION",
		"UPDATE_STREAM_MODE_DURATION",
		"START_STREAM_ENCRYPTION_DURATION",
		"STOP_STREAM_ENCRYPTION_DURATION",
	}
	env := []string{
		fmt.Sprintf("KINESIS_MOCK_PLAIN_PORT=%d", s.port),
		fmt.Sprintf("KINESIS_MOCK_TLS_PORT=%d", s.tlsPort),
		fmt.Sprintf("AWS_ACCOUNT_ID=%s", s.accountID),
		fmt.Sprintf("SHARD_LIMIT=%d", kinesismockShardLimit),
		fmt.Sprintf("ON_DEMAND_STREAM_COUNT_LIMIT=%d", kinesismockOnDemandLimit),
		"LOG_LEVEL=INFO",
	}
	for _, p := range latencyParams {
		env = append(env, fmt.Sprintf("%s=%sms", p, s.latency))
	}
	if s.dataDir != "" {
		env = append(env,
			"SHOULD_PERSIST_DATA=true",
			fmt.Sprintf("PERSIST_PATH=%s", s.dataDir),
			fmt.Sprintf("PERSIST_FILE_NAME=%s.json", s.accountID),
			"PERSIST_INTERVAL=5s",
		)
	}
	return env
}

// ─── readiness poll ───────────────────────────────────────────────────────────

func (s *MockServer) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Healthy() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return errors.New("timed out waiting for kinesis-mock to become ready")
}

// ─── binary location ──────────────────────────────────────────────────────────

func locateOrDownloadBinary() (string, error) {
	// 1. explicit override
	if p := os.Getenv("JAISCLOUD_KINESIS_MOCK_PATH"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
		return "", fmt.Errorf("JAISCLOUD_KINESIS_MOCK_PATH=%q: file not found", p)
	}

	// 2. default cache location
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("home dir: %w", err)
	}
	binDir := filepath.Join(home, ".jaiscloud", "bin")
	p := filepath.Join(binDir, "kinesis-mock")
	if _, err := os.Stat(p); err == nil {
		return p, nil
	}

	// 3. auto-download
	return downloadBinary(binDir, p)
}

func downloadBinary(dir, dest string) (string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", dir, err)
	}

	filename, err := kinesismockFilename()
	if err != nil {
		return "", err
	}
	url := kinesismockBaseURL + "/" + filename

	slog.Info("downloading kinesis-mock binary", "url", url, "dest", dest)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return "", fmt.Errorf("download kinesis-mock: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("download kinesis-mock: HTTP %d from %s", resp.StatusCode, url)
	}

	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return "", fmt.Errorf("create binary: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return "", fmt.Errorf("write binary: %w", err)
	}
	slog.Info("kinesis-mock downloaded", "path", dest)
	return dest, nil
}

// kinesismockFilename returns the platform-specific release asset filename.
func kinesismockFilename() (string, error) {
	goos := runtime.GOOS
	goarch := runtime.GOARCH

	// native binary names from GitHub releases
	switch {
	case goos == "linux" && goarch == "amd64":
		return "kinesis-mock-native-linux-amd64", nil
	case goos == "linux" && goarch == "arm64":
		return "kinesis-mock-native-linux-arm64", nil
	case goos == "darwin" && goarch == "amd64":
		return "kinesis-mock-native-macos-amd64", nil
	case goos == "darwin" && goarch == "arm64":
		return "kinesis-mock-native-macos-arm64", nil
	default:
		return "", fmt.Errorf("no kinesis-mock native binary for %s/%s; set JAISCLOUD_KINESIS_MOCK_PATH", goos, goarch)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// slogWriter adapts kinesis-mock stdout/stderr to slog.
type slogWriter struct {
	level  slog.Level
	prefix string
}

func (w *slogWriter) Write(p []byte) (int, error) {
	slog.Log(context.Background(), w.level, string(p), "source", w.prefix)
	return len(p), nil
}
