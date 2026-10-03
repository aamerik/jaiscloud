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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	core "jaiscloud/internal/gcp/service/managedkafka"
	"jaiscloud/internal/k8shelpers"
)

const (
	// kafkaPort is the PLAINTEXT listener the broker serves and the Service
	// exposes. Real GCP omits the port from bootstrapAddress; the emulator
	// includes it so a raw client can dial the in-cluster Service directly.
	kafkaPort = 9092
	// brokerReadyTimeout bounds how long the native broker waits for its
	// listener to accept a TCP connection. The k8s broker uses the shared
	// k8shelpers workload readiness timeout.
	brokerReadyTimeout = 180 * time.Second
	// brokerLabelKey/Value tag every managedkafka broker resource so the K8s
	// e2e smoke can find the per-cluster Service the cluster advertises.
	brokerLabelKey   = "jaiscloud.io/broker"
	brokerLabelValue = "managedkafka"
	// brokerSelector matches every broker resource this package owns; the
	// startup orphan sweep and Reset use it to reap leftovers.
	brokerSelector = brokerLabelKey + "=" + brokerLabelValue
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
	admins    *adminPool
	// sweepOnce runs the startup orphan sweep exactly once, before the first
	// broker starts.
	sweepOnce sync.Once

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
		admins:    newAdminPool(nil),
		probe:     tcpProbe,
	}
}

func (b *k8sBroker) Mode() Mode { return ModeK8s }

func (b *k8sBroker) Endpoint(project, location, cluster string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.endpoints[ClusterKey{Project: project, Location: location, Cluster: cluster}]
}

// EnsureTopic provisions the topic on the live broker, or no-ops when no broker
// is running for the cluster (metadata-only topology).
func (b *k8sBroker) EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error {
	return ensureTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic, partitions, configs)
}

// AlterTopicConfigs applies an incremental topic-config change on the live
// broker, or no-ops when no broker is running for the cluster.
func (b *k8sBroker) AlterTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error {
	return alterTopicConfigs(ctx, b.admins, b.Endpoint(project, location, cluster), topic, set, remove)
}

// AddTopicPartitions raises the topic's partition count on the live broker.
func (b *k8sBroker) AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error {
	return addPartitions(ctx, b.admins, b.Endpoint(project, location, cluster), topic, totalPartitions)
}

// DeleteBrokerTopic removes the topic from the live broker.
func (b *k8sBroker) DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error {
	return deleteBrokerTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic)
}

// ListConsumerGroups returns the live broker's consumer groups, or an empty set
// when no broker is running.
func (b *k8sBroker) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error) {
	return listGroups(ctx, b.admins, b.Endpoint(project, location, cluster))
}

// ConsumerGroupOffsets returns the group's committed offsets on the live broker.
func (b *k8sBroker) ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]core.ConsumerGroupOffset, bool, error) {
	return groupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// ConsumerGroupMembers returns the group's active member count on the live broker.
func (b *k8sBroker) ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error) {
	return groupMembers(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// DeleteConsumerGroup removes the group from the live broker.
func (b *k8sBroker) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error) {
	return deleteGroup(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// CommitConsumerGroupOffsets sets the group's committed offsets on the live broker.
func (b *k8sBroker) CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []core.ConsumerGroupOffset) error {
	return commitGroupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group, offsets)
}

// ReplaceAcl mirrors an ACL's entry set onto the live broker.
func (b *k8sBroker) ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []core.AclBinding) error {
	return replaceACLs(ctx, b.admins, b.Endpoint(project, location, cluster), aclSpec{
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
		Entries:      entries,
	})
}

func (b *k8sBroker) EnsureCluster(ctx context.Context, project, location, cluster string) (string, error) {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	keyStr := key.String()

	// Reap broker resources left behind by a previous emulator instance (or a
	// failed start) before starting a new one.
	b.sweepOnce.Do(b.sweepOrphans)

	b.mu.Lock()
	if ep, ok := b.endpoints[key]; ok {
		b.mu.Unlock()
		return ep, nil
	}
	b.mu.Unlock()

	name := brokerResourceName(key)
	dns := k8shelpers.WorkloadEndpoint(name, b.namespace, kafkaPort)

	b.logger.Info("managedkafka broker: waiting for redpanda pod", "cluster", keyStr, "address", dns)
	// The workload helper owns the Pod + ClusterIP Service lifecycle and reaps
	// both if the broker never becomes ready, so a failed start cannot leak.
	if _, err := k8shelpers.EnsureWorkload(ctx, b.client, k8shelpers.WorkloadSpec{
		Namespace:     b.namespace,
		Pod:           *b.buildPod(name, dns),
		ServiceLabels: map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue},
		Selector:      map[string]string{"app": name},
		Ports:         []k8shelpers.ServicePort{{Name: "kafka", Port: kafkaPort, TargetPort: kafkaPort}},
		Probe:         b.probe,
	}); err != nil {
		return "", fmt.Errorf("managedkafka broker: ensure workload: %w", err)
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

	reapErr := k8shelpers.DeleteWorkload(ctx, b.client, b.namespace, name)

	// Always drop the endpoint and its pooled admin, even if a delete failed:
	// keeping a dead address advertised is worse than retrying a delete.
	b.mu.Lock()
	ep := b.endpoints[key]
	delete(b.endpoints, key)
	b.mu.Unlock()
	if ep != "" {
		b.admins.close(ep)
	}
	if reapErr != nil {
		return fmt.Errorf("managedkafka broker: %w", reapErr)
	}
	return nil
}

// Reset stops and reaps every tracked broker, then sweeps any broker resources
// left in the namespace that this process never tracked (a failed partial
// create, or a previous instance's leftovers). The manager stays usable: a
// later EnsureCluster starts a fresh broker.
func (b *k8sBroker) Reset(ctx context.Context) error {
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
	b.admins.closeAll()
	b.sweepOrphans()
	return firstErr
}

// Shutdown reaps every broker on emulator shutdown.
func (b *k8sBroker) Shutdown(ctx context.Context) error { return b.Reset(ctx) }

// sweepOrphans deletes broker Pods and Services left by a previous emulator
// instance or a failed start, matched by the broker label. Broker liveness is
// runtime state this process cannot adopt across the deterministic resource
// names, so every labeled resource is an orphan. Best-effort: a failure is
// logged, never fatal to cluster creation.
func (b *k8sBroker) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if n, err := k8shelpers.SweepWorkloads(ctx, b.client, b.namespace, brokerSelector); err != nil {
		b.logger.Warn("managedkafka broker: orphan sweep incomplete", "deleted", n, "err", err)
	} else if n > 0 {
		b.logger.Info("managedkafka broker: reaped orphan workloads", "count", n)
	}
}

// ─── K8s resource management ─────────────────────────────────────────────────

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
				Name: "data",
				// Broker data is ephemeral and non-portable (MK5): an emptyDir
				// is deleted with the Pod, so a reaped/reused cluster id starts
				// clean and no volume is left behind. Control-plane metadata is
				// the authoritative state; producer bytes never travel in a
				// --dsn snapshot.
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
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
