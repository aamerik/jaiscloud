package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewModeSelection(t *testing.T) {
	// Native resolution must not accidentally find a PATH binary.
	t.Setenv("PATH", "")
	t.Setenv("JAISCLOUD_KAFKA_BROKER_BIN", "")

	cases := []struct {
		name     string
		cfg      Config
		wantMove Mode
	}{
		{"empty defaults to mock", Config{}, ModeMock},
		{"explicit mock", Config{Mode: "mock"}, ModeMock},
		{"unknown degrades to mock", Config{Mode: "docker"}, ModeMock},
		{"k8s without client degrades to mock", Config{Mode: "k8s"}, ModeMock},
		{"k8s with client", Config{Mode: "k8s", Client: fake.NewSimpleClientset()}, ModeK8s},
		{"native without binary degrades to mock", Config{Mode: "native"}, ModeMock},
		{"native with binary", Config{Mode: "native", BinaryPath: "/bin/true"}, ModeNative},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := New(tc.cfg).Mode(); got != tc.wantMove {
				t.Fatalf("Mode() = %q, want %q", got, tc.wantMove)
			}
		})
	}
}

func TestK8sBrokerEnsureEndpointAndReap(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "My_Cluster"}
	name := brokerResourceName(key)

	// Endpoint before ensure is empty and does not start anything.
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Fatalf("Endpoint before ensure = %q, want empty", ep)
	}

	got, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster)
	if err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	want := name + ".jaiscloud.svc.cluster.local:9092"
	if got != want {
		t.Fatalf("endpoint = %q, want %q", got, want)
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != want {
		t.Fatalf("Endpoint after ensure = %q, want %q", ep, want)
	}

	// Service and Pod exist with the same name as the DNS label.
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("service not created: %v", err)
	}
	pod, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod not created: %v", err)
	}
	if pod.Spec.Containers[0].Image != "redpanda:test" {
		t.Errorf("pod image = %q, want redpanda:test", pod.Spec.Containers[0].Image)
	}
	if !hasArg(pod, "--advertise-kafka-addr", "PLAINTEXT://"+want) {
		t.Errorf("pod does not advertise the service DNS: %v", pod.Spec.Containers[0].Args)
	}

	// A second ensure reuses the running broker (no new objects, same address).
	again, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster)
	if err != nil || again != want {
		t.Fatalf("second EnsureCluster = %q, %v; want %q, nil", again, err, want)
	}

	if err := b.StopCluster(ctx, key.Project, key.Location, key.Cluster); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Fatalf("Endpoint after stop = %q, want empty", ep)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("pod still present after StopCluster")
	}
	// Reaping an unknown cluster is a no-op.
	if err := b.StopCluster(ctx, "p", "l", "absent"); err != nil {
		t.Errorf("StopCluster(absent): %v", err)
	}
}

func TestK8sBrokerForbiddenNamespaceGetIsNotFatal(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "jaiscloud", errors.New("forbidden"))
	})
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster with a forbidden namespace Get: %v", err)
	}
}

func TestK8sBrokerShutdownReapsAll(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	for _, c := range []string{"c1", "c2"} {
		if _, err := b.EnsureCluster(ctx, "p", "l", c); err != nil {
			t.Fatalf("EnsureCluster(%s): %v", c, err)
		}
	}
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for _, c := range []string{"c1", "c2"} {
		if ep := b.Endpoint("p", "l", c); ep != "" {
			t.Errorf("Endpoint(%s) after shutdown = %q, want empty", c, ep)
		}
	}
}

func TestBrokerResourceName(t *testing.T) {
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "My_Cluster!!"}
	name := brokerResourceName(key)
	if name != strings.ToLower(name) {
		t.Errorf("name %q is not lowercase", name)
	}
	if strings.ContainsAny(name, "_!") {
		t.Errorf("name %q contains characters unsafe for a DNS label", name)
	}
	if len(name) > 63 {
		t.Errorf("name %q exceeds 63 chars", name)
	}
	// Stable for the same key and distinct across keys.
	if brokerResourceName(key) != name {
		t.Error("name is not stable")
	}
	other := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "other"}
	if brokerResourceName(other) == name {
		t.Error("distinct clusters produced the same name")
	}
}

func TestNativeBrokerLifecycle(t *testing.T) {
	// A fake "redpanda" that stays alive until reaped. EnsureCluster's readiness
	// probe is injected, so no real listener is needed.
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-redpanda")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	b := newNativeBroker(bin, dir, discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	got, err := b.EnsureCluster(ctx, "p", "l", "c1")
	if err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	if !strings.HasPrefix(got, "127.0.0.1:") {
		t.Fatalf("endpoint = %q, want loopback", got)
	}
	if b.Endpoint("p", "l", "c1") != got {
		t.Errorf("Endpoint = %q, want %q", b.Endpoint("p", "l", "c1"), got)
	}
	// The broker data dir is created under the configured root.
	if _, err := os.Stat(filepath.Join(dir, "managedkafka", brokerResourceName(ClusterKey{Project: "p", Location: "l", Cluster: "c1"}))); err != nil {
		t.Errorf("data dir not created: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- b.StopCluster(ctx, "p", "l", "c1") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StopCluster: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopCluster did not reap the subprocess in time")
	}
	if b.Endpoint("p", "l", "c1") != "" {
		t.Error("Endpoint not cleared after StopCluster")
	}
}

func hasArg(pod *corev1.Pod, name, value string) bool {
	args := pod.Spec.Containers[0].Args
	for i, a := range args {
		if a == name && i+1 < len(args) && args[i+1] == value {
			return true
		}
	}
	return false
}
