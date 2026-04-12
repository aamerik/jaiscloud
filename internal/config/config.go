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

// awsARNFormatters maps abstract resource types to their AWS ARN format function.
// To add a new resource type, add one entry here — no switch statement to update.
// The function signature is func(region, accountID, name string) string;
// IAM ARNs omit region, S3 ARNs omit both, so each formatter uses only what it needs.
var awsARNFormatters = map[string]func(region, accountID, name string) string{
	// DynamoDB
	"dynamodb-table":  func(r, a, n string) string { return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", r, a, n) },
	"dynamodb-stream": func(r, a, n string) string { return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", r, a, n) }, // name is "tableName/stream/label"
	// Lambda
	"lambda-function": func(r, a, n string) string { return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", r, a, n) },
	// SNS
	"sns-topic":        func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:%s", r, a, n) },
	"sns-subscription": func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:%s", r, a, n) },
	// SQS
	"sqs-queue": func(r, a, n string) string { return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", r, a, n) },
	// IAM — no region in ARN
	"iam-role":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:role/%s", a, n) },
	"iam-policy": func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:policy/%s", a, n) },
	"iam-user":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:user/%s", a, n) },
	// S3 — no region or account in ARN
	"s3-bucket": func(_, _, n string) string { return fmt.Sprintf("arn:aws:s3:::%s", n) },
	// EventBridge
	"events-rule": func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", r, a, n) },
}

// AWSResourceID returns a ResourceID function that formats AWS ARNs.
// The returned function maps abstract provider resource types to their AWS ARN format
// using awsARNFormatters above. Adding a new resource type requires only a new entry
// in that map — this function never needs to change.
// Inject the result into NormalizedRequest.ResourceID at the gateway layer.
func AWSResourceID(region, accountID string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		if f, ok := awsARNFormatters[resourceType]; ok {
			return f(region, accountID, name)
		}
		return name
	}
}

// AzureResourceID returns a ResourceID function that formats Azure resource IDs.
// Stub implementation: returns name unchanged until Azure provider is implemented.
func AzureResourceID(region, accountID string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		// Azure resource IDs follow /subscriptions/{sub}/resourceGroups/{rg}/providers/...
		// Return name as-is until real Azure providers are implemented.
		return name
	}
}

// GCPResourceID returns a ResourceID function that formats GCP resource names.
// Stub implementation: returns name unchanged until GCP provider is implemented.
func GCPResourceID(accountID string) func(resourceType, name string) string {
	return func(resourceType, name string) string {
		// GCP resource names follow projects/{project}/locations/{loc}/...
		// Return name as-is until real GCP providers are implemented.
		return name
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
