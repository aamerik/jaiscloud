package main

import "testing"

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
