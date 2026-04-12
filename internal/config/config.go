package config

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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
	Cloud     string // Cloud provider to emulate: aws (default), azure, gcp
	LogLevel  string
	Region    string
	AccountID string
	DSN       string // PostgreSQL DSN (required when Mode == full)
	BlobDir   string // Directory for S3 blob bytes (full mode only; defaults to ~/.jaiscloud/blobs)
	PluginDir string // Directory to scan for plugin .so files (full mode only; empty = disabled)

	// Observability (opt-in)
	Metrics bool // expose /metrics endpoint
	Tracing bool // emit OTel traces to stdout

	// Deterministic mode
	Deterministic bool
	Seed          int64
	TimeStart     time.Time
	TimeMode      string // "frozen" or "offset"

	// Resolved at startup
	Clock      clock.Clock
	RandSource rand.Source
}

// AWSResourceID returns a ResourceID function that formats AWS ARNs.
// The returned function maps abstract provider resource types to their AWS ARN format.
// Inject this into NormalizedRequest.ResourceID at the gateway layer.
func AWSResourceID(region, accountID string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		switch resourceType {
		case "dynamodb-table":
			return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, accountID, name)
		case "dynamodb-stream":
			// name is expected to be "tableName/stream/label"
			return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, accountID, name)
		case "lambda-function":
			return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", region, accountID, name)
		case "sns-topic", "sns-subscription":
			return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, name)
		case "sqs-queue":
			return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, accountID, name)
		case "iam-role":
			return fmt.Sprintf("arn:aws:iam::%s:role/%s", accountID, name)
		case "iam-policy":
			return fmt.Sprintf("arn:aws:iam::%s:policy/%s", accountID, name)
		case "iam-user":
			return fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, name)
		case "s3-bucket":
			return fmt.Sprintf("arn:aws:s3:::%s", name)
		default:
			return name
		}
	}
}

func Load() (*Config, error) {
	viper.SetDefault("port", 4566)
	viper.SetDefault("mode", "lite")
	viper.SetDefault("cloud", "aws")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("region", "us-east-1")
	viper.SetDefault("account_id", "000000000000")
	viper.SetDefault("dsn", "")
	if home, err := os.UserHomeDir(); err == nil {
		viper.SetDefault("blob_dir", filepath.Join(home, ".jaiscloud", "blobs"))
	} else {
		viper.SetDefault("blob_dir", ".jaiscloud/blobs")
	}
	viper.SetDefault("plugin_dir", "")
	viper.SetDefault("metrics", false)
	viper.SetDefault("tracing", false)
	viper.SetDefault("deterministic", false)
	viper.SetDefault("seed", 0)
	viper.SetDefault("time_mode", "offset")

	viper.SetEnvPrefix("JAISCLOUD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	cfg := &Config{
		Port:          viper.GetInt("port"),
		Mode:          Mode(viper.GetString("mode")),
		Cloud:         viper.GetString("cloud"),
		LogLevel:      viper.GetString("log_level"),
		Region:        viper.GetString("region"),
		AccountID:     viper.GetString("account_id"),
		DSN:           viper.GetString("dsn"),
		BlobDir:       viper.GetString("blob_dir"),
		PluginDir:     viper.GetString("plugin_dir"),
		Metrics:       viper.GetBool("metrics"),
		Tracing:       viper.GetBool("tracing"),
		Deterministic: viper.GetBool("deterministic"),
		Seed:          viper.GetInt64("seed"),
		TimeMode:      viper.GetString("time_mode"),
	}

	if cfg.Mode != ModeLite && cfg.Mode != ModeFull {
		return nil, fmt.Errorf("invalid mode %q: must be lite or full", cfg.Mode)
	}

	switch cfg.Cloud {
	case "aws", "azure", "gcp":
		// valid
	default:
		return nil, fmt.Errorf("invalid cloud %q: must be aws, azure, or gcp", cfg.Cloud)
	}

	// Parse base time for deterministic mode
	if ts := viper.GetString("time"); ts != "" {
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
