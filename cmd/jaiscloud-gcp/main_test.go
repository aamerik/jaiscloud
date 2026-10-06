package main

import (
	"context"
	"errors"
	"testing"
)

// TestLambdaCodeURL locks the K8s code-mount URL derivation: the admin base is
// built from a cluster-reachable emulator endpoint (with /_jaiscloud appended),
// and stays empty when none is configured so no unreachable localhost URL is
// guessed.
func TestLambdaCodeURL(t *testing.T) {
	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "")
	if got := lambdaCodeURL(); got != "" {
		t.Errorf("unset endpoint: got %q, want empty (code mount disabled)", got)
	}

	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
	want := "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080/_jaiscloud"
	if got := lambdaCodeURL(); got != want {
		t.Errorf("scheme-less endpoint: got %q, want %q", got, want)
	}

	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "https://emulator.example.com/")
	if got := lambdaCodeURL(); got != "https://emulator.example.com/_jaiscloud" {
		t.Errorf("trailing slash: got %q, want https://emulator.example.com/_jaiscloud", got)
	}
}

// TestResolveKafkaBrokerMode locks the Managed Kafka broker mode precedence:
// JAISCLOUD_KAFKA_BROKER_MODE → JAISCLOUD_EXECUTOR_MODE → mock, so a global
// executor mode covers Kafka while Kafka's own variable keeps precedence.
func TestResolveKafkaBrokerMode(t *testing.T) {
	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "") // unrelated; keep env hermetic
	cases := []struct {
		name         string
		kafkaMode    string
		executorMode string
		wantMode     string
		wantSource   string
	}{
		{"default is mock", "", "", "mock", "default"},
		{"global executor mode is the fallback", "", "docker", "docker", "JAISCLOUD_EXECUTOR_MODE"},
		{"kafka variable wins over the global", "native", "docker", "native", "JAISCLOUD_KAFKA_BROKER_MODE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JAISCLOUD_KAFKA_BROKER_MODE", tc.kafkaMode)
			t.Setenv("JAISCLOUD_EXECUTOR_MODE", tc.executorMode)
			mode, source := resolveKafkaBrokerMode()
			if mode != tc.wantMode || source != tc.wantSource {
				t.Fatalf("resolveKafkaBrokerMode() = (%q, %q), want (%q, %q)", mode, source, tc.wantMode, tc.wantSource)
			}
		})
	}
}

// TestEffectiveFunctionsMode locks the docker-mode daemon probe: a docker mode
// with no reachable daemon degrades to mock (as GCP Cloud Run/Dataproc and AWS
// Lambda do), while every other mode passes through untouched and never probes.
func TestEffectiveFunctionsMode(t *testing.T) {
	reachable := func(context.Context) error { return nil }
	unreachable := func(context.Context) error {
		return errors.New("dial unix /var/run/docker.sock: connect: no such file or directory")
	}
	probes := 0
	counting := func(context.Context) error { probes++; return nil }

	cases := []struct {
		name string
		mode string
		ping func(context.Context) error
		want string
	}{
		{"mock passes through", "mock", reachable, "mock"},
		{"empty passes through", "", reachable, ""},
		{"k8s passes through without probing", "k8s", unreachable, "k8s"},
		{"docker with a reachable daemon stays docker", "docker", reachable, "docker"},
		{"docker with no daemon degrades to mock", "docker", unreachable, "mock"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveFunctionsMode(tc.mode, tc.ping); got != tc.want {
				t.Fatalf("effectiveFunctionsMode(%q) = %q, want %q", tc.mode, got, tc.want)
			}
		})
	}

	// Only docker mode pings the daemon.
	if got := effectiveFunctionsMode("k8s", counting); got != "k8s" || probes != 0 {
		t.Fatalf("k8s mode probed the daemon (%d probes) or changed mode (%q)", probes, got)
	}
	if got := effectiveFunctionsMode("docker", counting); got != "docker" || probes != 1 {
		t.Fatalf("docker mode did not probe exactly once (probes=%d, mode=%q)", probes, got)
	}
}
