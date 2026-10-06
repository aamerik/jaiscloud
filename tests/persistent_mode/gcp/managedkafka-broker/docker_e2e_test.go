//go:build managedkafka_broker_e2e

// This file is the local-Docker data-plane gate for the Managed Kafka broker
// (W1.1/MKD1). Unlike the k3d gates it needs no cluster: it drives an emulator
// started with JAISCLOUD_KAFKA_BROKER_MODE=docker on the local Docker daemon
// and proves a host-side franz-go client can produce and consume records at the
// broker's advertised 127.0.0.1:<port>, then that deleting the cluster reaps
// the container (the published port stops accepting).
//
// Run with:
//
//	make test-managedkafka-broker-docker
//
// It skips unless MANAGEDKAFKA_DOCKER_E2E=1 (which the Makefile target sets) and
// the emulator is reachable, so `make test-managedkafka-broker-k8s` (which also
// compiles this file) is unaffected.
package managedkafkabroker_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// dockerCreateClient allows the synchronous broker startup (container create +
// Kafka-listener readiness wait) to complete within one create request.
var dockerCreateClient = &http.Client{Timeout: 10 * time.Minute}

// emulatorBase returns the reachable emulator base URL (JAISCLOUD_HOST, else
// http://localhost:8080), skipping when it is not up.
func emulatorBase(t *testing.T) string {
	t.Helper()
	base := strings.TrimRight(os.Getenv("JAISCLOUD_HOST"), "/")
	if base == "" {
		base = "http://localhost:8080"
	}
	resp, err := http.Get(base + "/_jaiscloud/health")
	if err != nil {
		t.Skipf("emulator not reachable at %s: %v", base, err)
	}
	resp.Body.Close()
	return base
}

func TestManagedKafkaBrokerDocker(t *testing.T) {
	if os.Getenv("MANAGEDKAFKA_DOCKER_E2E") != "1" {
		t.Skip("set MANAGEDKAFKA_DOCKER_E2E=1 (run via make test-managedkafka-broker-docker)")
	}
	base := emulatorBase(t)

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	cluster := "mk-docker-" + run
	t.Cleanup(func() { deleteCluster(t, base, cluster) })
	// Delete any broker left by a prior run before asserting reaping.
	deleteCluster(t, base, cluster)

	code, body := api(t, dockerCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{"labels": map[string]string{"smoke": "managedkafka-docker-broker"}})
	if code < 200 || code >= 300 {
		t.Fatalf("create cluster: HTTP %d: %v", code, body)
	}

	code, cl := api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get cluster: HTTP %d: %v", code, cl)
	}
	addr := strField(cl, "bootstrapAddress")
	if addr == "" {
		t.Fatalf("cluster has no bootstrapAddress: %v", cl)
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("bootstrapAddress %q is not the docker loopback endpoint — is the emulator running JAISCLOUD_KAFKA_BROKER_MODE=docker?", addr)
	}

	// The advertised address must accept a connection (the container is live).
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		t.Fatalf("dial broker %s: %v", addr, err)
	}
	conn.Close()

	// Create the topic through the Managed Kafka API (mirrored onto the broker),
	// then produce and consume over the real Kafka wire.
	const topic = "orders"
	code, topicBody := api(t, httpClient, http.MethodPost,
		base+clusterPath(cluster)+"/topics?topicId="+topic,
		map[string]any{"partitionCount": 1, "replicationFactor": 1})
	if code < 200 || code >= 300 {
		t.Fatalf("create topic: HTTP %d: %v", code, topicBody)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	produceConsume(t, ctx, addr, topic)

	// Delete the cluster and assert the container is reaped: the advertised
	// loopback port stops accepting connections.
	deleteCluster(t, base, cluster)
	waitPortClosed(t, addr, 60*time.Second)
}

// produceConsume writes a handful of records and reads them back over a real
// franz-go client at addr.
func produceConsume(t *testing.T, ctx context.Context, addr, topic string) {
	t.Helper()
	const records = 10

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DefaultProduceTopic(topic),
		kgo.ClientID("jaiscloud-mk-docker-e2e"),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer producer.Close()
	for i := 0; i < records; i++ {
		if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("m-%d", i))}).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("jaiscloud-mk-docker-e2e"),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consumer.Close()

	got := 0
	deadline := time.Now().Add(60 * time.Second)
	for got < records && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fetches.EachRecord(func(*kgo.Record) { got++ })
	}
	if got < records {
		t.Fatalf("consumed %d records, want %d", got, records)
	}
}

// waitPortClosed waits until addr stops accepting TCP connections.
func waitPortClosed(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("broker port %s still accepting connections after cluster delete", addr)
}
