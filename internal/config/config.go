package config

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/spf13/viper"
	"jaiscloud/internal/clock"
)

type Mode string

const (
	ModeLite Mode = "lite"
	ModeFull Mode = "full"
)

type Config struct {
	Port      int
	Mode      Mode
	LogLevel  string
	Region    string
	AccountID string

	// Deterministic mode
	Deterministic bool
	Seed          int64
	TimeStart     time.Time
	TimeMode      string // "frozen" or "offset"

	// Resolved at startup
	Clock      clock.Clock
	RandSource rand.Source
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("port", 4566)
	v.SetDefault("mode", "lite")
	v.SetDefault("log_level", "info")
	v.SetDefault("region", "us-east-1")
	v.SetDefault("account_id", "000000000000")
	v.SetDefault("deterministic", false)
	v.SetDefault("seed", 0)
	v.SetDefault("time_mode", "offset")

	v.SetEnvPrefix("JAISCLOUD")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	cfg := &Config{
		Port:          v.GetInt("port"),
		Mode:          Mode(v.GetString("mode")),
		LogLevel:      v.GetString("log_level"),
		Region:        v.GetString("region"),
		AccountID:     v.GetString("account_id"),
		Deterministic: v.GetBool("deterministic"),
		Seed:          v.GetInt64("seed"),
		TimeMode:      v.GetString("time_mode"),
	}

	if cfg.Mode != ModeLite && cfg.Mode != ModeFull {
		return nil, fmt.Errorf("invalid mode %q: must be lite or full", cfg.Mode)
	}

	// Parse base time for deterministic mode
	if ts := v.GetString("time"); ts != "" {
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("invalid --time %q: %w", ts, err)
		}
		cfg.TimeStart = t
	} else {
		cfg.TimeStart = time.Now()
	}

	// Resolve clock
	if cfg.Deterministic {
		cfg.RandSource = rand.NewSource(cfg.Seed)
		if cfg.TimeMode == "frozen" {
			cfg.Clock = clock.FixedClock{T: cfg.TimeStart}
		} else {
			cfg.Clock = clock.NewOffsetClock(cfg.TimeStart)
		}
	} else {
		cfg.Clock = clock.RealClock{}
	}

	return cfg, nil
}
