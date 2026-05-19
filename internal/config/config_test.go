package config_test

import (
	"testing"

	"jaiscloud/internal/config"
)

// TestConfig_CIEnvOverridesDefaultMode verifies that when CI=true is set and no
// explicit JAISCLOUD_MODE is present, the loaded config uses ModeMemory.
// (The viper default is already "memory"; this guards against regressions that
// might change that default while CI environments rely on memory mode.)
func TestConfig_CIEnvOverridesDefaultMode(t *testing.T) {
	t.Setenv("CI", "true")
	t.Setenv("JAISCLOUD_MODE", "") // no explicit mode override

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Mode != config.ModeMemory {
		t.Errorf("expected Mode %q when CI=true, got %q", config.ModeMemory, cfg.Mode)
	}
}

// TestConfig_DefaultMode verifies that without any env vars, the default mode is ModeMemory.
func TestConfig_DefaultMode(t *testing.T) {
	t.Setenv("JAISCLOUD_MODE", "")
	t.Setenv("CI", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Mode != config.ModeMemory {
		t.Errorf("expected default Mode %q, got %q", config.ModeMemory, cfg.Mode)
	}
}

// TestConfig_PersistentModeFromEnv verifies that JAISCLOUD_MODE=persistent is accepted.
func TestConfig_PersistentModeFromEnv(t *testing.T) {
	t.Setenv("JAISCLOUD_MODE", "persistent")
	t.Setenv("CI", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Mode != config.ModePersistent {
		t.Errorf("expected Mode %q, got %q", config.ModePersistent, cfg.Mode)
	}
}

// TestConfig_InvalidModeRejected verifies that unknown mode strings fail Load().
func TestConfig_InvalidModeRejected(t *testing.T) {
	t.Setenv("JAISCLOUD_MODE", "lite") // old name, now rejected
	t.Setenv("CI", "")

	_, err := config.Load()
	if err == nil {
		t.Error("expected error for invalid mode 'lite', got nil")
	}
}
