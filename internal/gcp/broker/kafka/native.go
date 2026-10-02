package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"jaiscloud/internal/clock"
)

// nativeBroker runs a Redpanda subprocess per Managed Kafka cluster, bound to
// loopback ports. It mirrors the AWS kinesis-mock lifecycle: locate a binary,
// pick free ports, poll for readiness, and reap on stop.
type nativeBroker struct {
	binary  string
	dataDir string
	logger  *slog.Logger

	mu        sync.Mutex
	instances map[ClusterKey]*nativeInstance

	// probe reports whether addr is accepting TCP connections. Overridable in
	// tests; defaults to a one-second TCP dial.
	probe func(addr string) bool
}

type nativeInstance struct {
	cmd  *exec.Cmd
	addr string
	dir  string
}

func newNativeBroker(binary, dataDir string, logger *slog.Logger) *nativeBroker {
	return &nativeBroker{
		binary:    binary,
		dataDir:   dataDir,
		logger:    logger,
		instances: make(map[ClusterKey]*nativeInstance),
		probe:     tcpProbe,
	}
}

func (b *nativeBroker) Mode() Mode { return ModeNative }

func (b *nativeBroker) Endpoint(project, location, cluster string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if inst, ok := b.instances[ClusterKey{Project: project, Location: location, Cluster: cluster}]; ok {
		return inst.addr
	}
	return ""
}

func (b *nativeBroker) EnsureCluster(ctx context.Context, project, location, cluster string) (string, error) {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}

	b.mu.Lock()
	if inst, ok := b.instances[key]; ok {
		b.mu.Unlock()
		return inst.addr, nil
	}
	b.mu.Unlock()

	port, err := freePort()
	if err != nil {
		return "", fmt.Errorf("managedkafka broker: find free port: %w", err)
	}
	rpcPort, err := freePort()
	if err != nil {
		return "", fmt.Errorf("managedkafka broker: find free rpc port: %w", err)
	}

	dir := ""
	if b.dataDir != "" {
		dir = filepath.Join(b.dataDir, "managedkafka", brokerResourceName(key))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("managedkafka broker: data dir: %w", err)
		}
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// The resolved binary is the Redpanda CLI (`rpk`); `rpk redpanda start`
	// writes the node config and launches the broker.
	args := []string{
		"redpanda",
		"start",
		"--overprovisioned",
		"--smp", "1",
		"--memory", "1G",
		"--reserve-memory", "0M",
		"--node-id", "0",
		"--check=false",
		"--kafka-addr", "PLAINTEXT://" + addr,
		"--advertise-kafka-addr", "PLAINTEXT://" + addr,
		"--rpc-addr", fmt.Sprintf("127.0.0.1:%d", rpcPort),
	}
	if dir != "" {
		args = append(args, "--data-dir", dir)
	}

	// The subprocess lifetime is owned by the broker manager (StopCluster /
	// Shutdown), not by the request context, so it is deliberately not bound to
	// ctx: a request-scoped context would kill the broker when the create call
	// returns.
	cmd := exec.Command(b.binary, args...)
	cmd.Stdout = &slogWriter{level: slog.LevelDebug, prefix: "redpanda"}
	cmd.Stderr = &slogWriter{level: slog.LevelWarn, prefix: "redpanda"}

	b.logger.Info("managedkafka broker: starting redpanda", "cluster", key.String(), "address", addr, "binary", b.binary)
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("managedkafka broker: start redpanda: %w", err)
	}

	if err := b.waitReady(ctx, addr); err != nil {
		_ = cmd.Process.Kill()
		return "", err
	}

	b.mu.Lock()
	b.instances[key] = &nativeInstance{cmd: cmd, addr: addr, dir: dir}
	b.mu.Unlock()
	b.logger.Info("managedkafka broker ready", "cluster", key.String(), "address", addr)
	return addr, nil
}

func (b *nativeBroker) StopCluster(_ context.Context, project, location, cluster string) error {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	b.mu.Lock()
	inst := b.instances[key]
	delete(b.instances, key)
	b.mu.Unlock()
	if inst == nil {
		return nil
	}
	return stopProcess(inst.cmd)
}

func (b *nativeBroker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	keys := make([]ClusterKey, 0, len(b.instances))
	for k := range b.instances {
		keys = append(keys, k)
	}
	b.mu.Unlock()

	var firstErr error
	for _, k := range keys {
		if err := b.StopCluster(ctx, k.Project, k.Location, k.Cluster); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *nativeBroker) waitReady(ctx context.Context, addr string) error {
	deadline := clock.RealNow().Add(brokerReadyTimeout)
	for clock.RealNow().Before(deadline) {
		if b.probe(addr) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("managedkafka broker: %s not ready within %s", addr, brokerReadyTimeout)
}

// stopProcess sends SIGTERM and waits up to 5 seconds, then SIGKILL.
func stopProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
	return nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// slogWriter adapts a broker subprocess's stdout/stderr to slog.
type slogWriter struct {
	level  slog.Level
	prefix string
}

func (w *slogWriter) Write(p []byte) (int, error) {
	slog.Log(context.Background(), w.level, string(p), "source", w.prefix)
	return len(p), nil
}
