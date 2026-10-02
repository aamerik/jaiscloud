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
	core "jaiscloud/internal/gcp/service/managedkafka"
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
	admins    *adminPool

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
		admins:    newAdminPool(nil),
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

// EnsureTopic provisions the topic on the live broker, or no-ops when no broker
// is running for the cluster (metadata-only topology).
func (b *nativeBroker) EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int) error {
	return ensureTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic, partitions)
}

// AddTopicPartitions raises the topic's partition count on the live broker.
func (b *nativeBroker) AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error {
	return addPartitions(ctx, b.admins, b.Endpoint(project, location, cluster), topic, totalPartitions)
}

// DeleteBrokerTopic removes the topic from the live broker.
func (b *nativeBroker) DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error {
	return deleteBrokerTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic)
}

// ListConsumerGroups returns the live broker's consumer groups, or an empty set
// when no broker is running.
func (b *nativeBroker) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error) {
	return listGroups(ctx, b.admins, b.Endpoint(project, location, cluster))
}

// ConsumerGroupOffsets returns the group's committed offsets on the live broker.
func (b *nativeBroker) ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]core.ConsumerGroupOffset, bool, error) {
	return groupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// ConsumerGroupMembers returns the group's active member count on the live broker.
func (b *nativeBroker) ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error) {
	return groupMembers(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// DeleteConsumerGroup removes the group from the live broker.
func (b *nativeBroker) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error) {
	return deleteGroup(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// CommitConsumerGroupOffsets sets the group's committed offsets on the live broker.
func (b *nativeBroker) CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []core.ConsumerGroupOffset) error {
	return commitGroupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group, offsets)
}

// ReplaceAcl mirrors an ACL's entry set onto the live broker.
func (b *nativeBroker) ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []core.AclBinding) error {
	return replaceACLs(ctx, b.admins, b.Endpoint(project, location, cluster), aclSpec{
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
		Entries:      entries,
	})
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
		// Reap the failed process and collect it so no zombie or orphaned
		// child is left behind.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
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
	// Drop the pooled admin client before the process goes away.
	b.admins.close(inst.addr)
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
	b.admins.closeAll()
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
