package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/clock"
)

const (
	// kafkaPort is the PLAINTEXT listener the broker serves and the Service
	// exposes. Real GCP omits the port from bootstrapAddress; the emulator
	// includes it so a raw client can dial the in-cluster Service directly.
	kafkaPort = 9092
	// brokerReadyTimeout bounds how long EnsureCluster waits for the Pod's
	// Service to accept a TCP connection.
	brokerReadyTimeout = 180 * time.Second
	// brokerLabelKey/Value tag every managedkafka broker resource so the K8s
	// e2e smoke can find the per-cluster Service the cluster advertises.
	brokerLabelKey   = "jaiscloud.io/broker"
	brokerLabelValue = "managedkafka"
)

// k8sBroker runs a single-node Redpanda Pod plus a ClusterIP Service per
// Managed Kafka cluster. It is safe for concurrent use.
type k8sBroker struct {
	client    kubernetes.Interface
	namespace string
	image     string
	logger    *slog.Logger

	mu        sync.Mutex
	endpoints map[ClusterKey]string // key → "<svc>.<ns>.svc.cluster.local:9092"

	// probe reports whether addr is accepting TCP connections. Overridable in
	// tests; defaults to a one-second TCP dial.
	probe func(addr string) bool
}

func newK8sBroker(client kubernetes.Interface, namespace, image string, logger *slog.Logger) *k8sBroker {
	return &k8sBroker{
		client:    client,
		namespace: namespace,
		image:     image,
		logger:    logger,
		endpoints: make(map[ClusterKey]string),
		probe:     tcpProbe,
	}
}

func (b *k8sBroker) Mode() Mode { return ModeK8s }

func (b *k8sBroker) Endpoint(project, location, cluster string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.endpoints[ClusterKey{Project: project, Location: location, Cluster: cluster}]
}

func (b *k8sBroker) EnsureCluster(ctx context.Context, project, location, cluster string) (string, error) {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	keyStr := key.String()

	b.mu.Lock()
	if ep, ok := b.endpoints[key]; ok {
		b.mu.Unlock()
		return ep, nil
	}
	b.mu.Unlock()

	name := brokerResourceName(key)
	dns := fmt.Sprintf("%s.%s.svc.cluster.local:%d", name, b.namespace, kafkaPort)

	if err := b.ensureNamespace(ctx); err != nil {
		return "", fmt.Errorf("managedkafka broker: ensure namespace: %w", err)
	}
	if err := b.ensureService(ctx, name); err != nil {
		return "", fmt.Errorf("managedkafka broker: ensure service: %w", err)
	}
	if err := b.ensurePod(ctx, name, dns); err != nil {
		return "", fmt.Errorf("managedkafka broker: ensure pod: %w", err)
	}

	b.logger.Info("managedkafka broker: waiting for redpanda pod", "cluster", keyStr, "address", dns)
	if err := b.waitReady(ctx, dns); err != nil {
		return "", err
	}

	b.mu.Lock()
	b.endpoints[key] = dns
	b.mu.Unlock()
	b.logger.Info("managedkafka broker ready", "cluster", keyStr, "address", dns)
	return dns, nil
}

func (b *k8sBroker) StopCluster(ctx context.Context, project, location, cluster string) error {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	name := brokerResourceName(key)

	if err := b.client.CoreV1().Pods(b.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("managedkafka broker: delete pod: %w", err)
	}
	if err := b.client.CoreV1().Services(b.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("managedkafka broker: delete service: %w", err)
	}
	b.mu.Lock()
	delete(b.endpoints, key)
	b.mu.Unlock()
	return nil
}

func (b *k8sBroker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	keys := make([]ClusterKey, 0, len(b.endpoints))
	for k := range b.endpoints {
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

// ─── K8s resource management ─────────────────────────────────────────────────

// ensureNamespace is best-effort: the emulator's ServiceAccount is granted only
// namespaced RBAC (namespaces are cluster-scoped), so a Forbidden Get is not an
// error — it means the namespace is managed externally (it is, in every
// deployment: the emulator runs in it). A genuinely missing namespace surfaces
// on the subsequent Pod/Service create.
func (b *k8sBroker) ensureNamespace(ctx context.Context) error {
	_, err := b.client.CoreV1().Namespaces().Get(ctx, b.namespace, metav1.GetOptions{})
	if err == nil || k8serrors.IsForbidden(err) {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	_, err = b.client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: b.namespace, Labels: map[string]string{"managed-by": "jaiscloud"}},
	}, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) || k8serrors.IsForbidden(err) {
		return nil
	}
	return err
}

func (b *k8sBroker) ensureService(ctx context.Context, name string) error {
	_, err := b.client.CoreV1().Services(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil // already provisioned
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	_, err = b.client.CoreV1().Services(b.namespace).Create(ctx, b.buildService(name), metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (b *k8sBroker) ensurePod(ctx context.Context, name, dns string) error {
	_, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil // already provisioned
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	_, err = b.client.CoreV1().Pods(b.namespace).Create(ctx, b.buildPod(name, dns), metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (b *k8sBroker) buildPod(name, dns string) *corev1.Pod {
	labels := map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "redpanda",
				Image: b.image,
				// `rpk redpanda start` writes the node config and launches the
				// broker; the bare `redpanda` binary no longer parses a `start`
				// subcommand or the --kafka-addr family (v24+).
				Command: []string{"rpk", "redpanda"},
				Args: []string{
					"start",
					"--overprovisioned",
					"--smp", "1",
					"--memory", "1G",
					"--reserve-memory", "0M",
					"--node-id", "0",
					"--check=false",
					"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", kafkaPort),
					"--advertise-kafka-addr", "PLAINTEXT://" + dns,
					"--rpc-addr", "0.0.0.0:33145",
					"--pandaproxy-addr", "0.0.0.0:8082",
					"--schema-registry-addr", "0.0.0.0:8081",
				},
				Ports: []corev1.ContainerPort{{Name: "kafka", ContainerPort: kafkaPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(kafkaPort)},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       3,
					FailureThreshold:    40,
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/redpanda/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
}

func (b *k8sBroker) buildService(name string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.namespace, Labels: map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue}},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{{
				Name:       "kafka",
				Port:       kafkaPort,
				TargetPort: intstr.FromInt(kafkaPort),
			}},
		},
	}
}

func (b *k8sBroker) waitReady(ctx context.Context, addr string) error {
	// Broker startup is real elapsed time, not the simulated clock.
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

// brokerResourceName returns a DNS-safe, per-cluster resource name derived from
// the cluster identity. The hash keeps it stable and collision-free while the
// sanitized cluster name keeps it debuggable.
func brokerResourceName(key ClusterKey) string {
	sum := sha256.Sum256([]byte(key.String()))
	hash := hex.EncodeToString(sum[:])[:10]
	sanitized := sanitizeDNSLabel(key.Cluster)
	if sanitized == "" {
		return "mkbroker-" + hash
	}
	if len(sanitized) > 40 {
		sanitized = sanitized[:40]
	}
	return "mkbroker-" + sanitized + "-" + hash
}

func sanitizeDNSLabel(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		}
	}
	return string(out)
}

// tcpProbe reports whether addr accepts a TCP connection.
func tcpProbe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
