package config_test

import (
	"testing"

	"jaiscloud/internal/config"
)

func TestConfig_DefaultIsNotEphemeral(t *testing.T) {
	t.Setenv("JAISCLOUD_EPHEMERAL", "")
	t.Setenv("JAISCLOUD_DSN", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Ephemeral {
		t.Error("expected Ephemeral=false by default")
	}
}

func TestConfig_EphemeralFromEnv(t *testing.T) {
	t.Setenv("JAISCLOUD_EPHEMERAL", "true")
	t.Setenv("JAISCLOUD_DSN", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Ephemeral {
		t.Error("expected Ephemeral=true when JAISCLOUD_EPHEMERAL=true")
	}
}

func TestConfig_EphemeralAndDSNRejected(t *testing.T) {
	t.Setenv("JAISCLOUD_EPHEMERAL", "true")
	t.Setenv("JAISCLOUD_DSN", "postgres://localhost/test")

	_, err := config.Load()
	if err == nil {
		t.Error("expected error when both JAISCLOUD_EPHEMERAL and JAISCLOUD_DSN are set")
	}
}
