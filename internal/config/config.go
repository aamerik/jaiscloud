package config

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"jaiscloud/internal/clock"

	"github.com/spf13/viper"
)

// hashName produces a short deterministic hash for use in ARN suffixes.
func hashName(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}

type Mode string

const (
	// ModeMemory is the default mode: all state is in-memory (no PostgreSQL required).
	ModeMemory Mode = "memory"
	// ModePersistent uses PostgreSQL for durable storage across restarts.
	ModePersistent Mode = "persistent"
)

type Config struct {
	Port      int
	Mode      Mode
	Cloud     string // Cloud provider to emulate: aws (default), azure, gcp
	LogLevel  string
	Region    string
	AccountID string
	DSN       string // PostgreSQL DSN (optional; if set with Mode==persistent, enables Postgres-backed stores)
	// BlobDir is deprecated; use DataDir instead. Kept for backward compatibility.
	BlobDir string // Deprecated: use DataDir
	DataDir string // Root data directory for persistent-file backend (default: ~/.jaiscloud/<binary>)
	// FreshStart wipes existing state on startup before initializing stores.
	FreshStart bool
	// SnapshotInterval controls how often the file-backend snapshot loop saves state (default: 30s).
	SnapshotInterval time.Duration
	// ExportSoftLimit is the size threshold (bytes) above which export logs a warning (default: 2 GiB).
	ExportSoftLimit int64

	// ExecutorMode selects the container orchestrator for all executors (Spark + Lambda).
	// Values: "" (default, mock/instant) | "mock" | "docker" | "k8s".
	// Set via --executor-mode / JAISCLOUD_EXECUTOR_MODE.
	ExecutorMode string

	// KMS
	KMSMasterKey string // 32-byte hex KEK; if unset DEK is stored plaintext (dev only)

	// Lambda executor
	LambdaImage         string // override default runtime image
	LambdaNetwork       string // Docker network for Lambda containers (default: "jaiscloud-net")
	LambdaKeepaliveSecs int    // Docker warm container idle timeout in seconds (default: 300)

	// AWS emulator wiring for Spark drivers. When set, Spark driver pods route
	// S3/IMDS/creds calls at this HTTP(S) endpoint instead of real AWS. Works
	// against any K8s cluster and any S3-compatible endpoint; s3a SSL is
	// derived from the URL scheme.
	AWSEmulatorEndpoint string
	// S3VirtualHostBases are host suffixes the S3 codec treats as virtual-hosted
	// bases (in addition to amazonaws.com). Comma-separated env var.
	S3VirtualHostBases []string

	// IMDSEnabled turns on the AWS instance-metadata emulator endpoints at the
	// gateway. Requires Cloud == "aws". Independent of executor mode so the
	// same flag works for bare-metal SDK consumers pointing their metadata
	// service endpoint at JaisCloud.
	IMDSEnabled bool

	// OIDCIssuers maps OIDC issuer URLs to their JWKS endpoint URLs.
	// Used by AssumeRoleWithWebIdentity to verify JWT signatures.
	// Env var: JAISCLOUD_OIDC_ISSUERS=issuer1=jwks_url1,issuer2=jwks_url2
	// If empty, JWT verification is skipped (back-compat for tests).
	OIDCIssuers map[string]string

	// K8s/Spark image configuration
	K8sNamespace     string
	K8sSparkImage    string
	K8sSparkSA       string
	SparkEMRImage    string
	SparkEMREKSImage string

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
	"events-rule":            func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", r, a, n) },
	"events-bus":             func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:event-bus/%s", r, a, n) },
	"events-archive":         func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:archive/%s", r, a, n) },
	"events-replay":          func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:replay/%s", r, a, n) },
	"events-connection":      func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:connection/%s", r, a, n) },
	"events-api-destination": func(r, a, n string) string { return fmt.Sprintf("arn:aws:events:%s:%s:api-destination/%s", r, a, n) },
	// EMR
	"emr-cluster": func(r, a, n string) string { return fmt.Sprintf("arn:aws:elasticmapreduce:%s:%s:cluster/%s", r, a, n) },
	// EMR Containers — name encodes composite IDs as "vcID/resourceID" where needed.
	"emr-virtual-cluster": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/%s", r, a, n)
	},
	"emr-job-run": func(r, a, n string) string {
		// n = "virtualClusterID/jobRunID"
		if vcID, runID, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/%s/jobruns/%s", r, a, vcID, runID)
		}
		return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/-/jobruns/%s", r, a, n)
	},
	"emr-managed-endpoint": func(r, a, n string) string {
		// n = "virtualClusterID/endpointID"
		if vcID, epID, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/%s/endpoints/%s", r, a, vcID, epID)
		}
		return fmt.Sprintf("arn:aws:emr-containers:%s:%s:/virtualclusters/-/endpoints/%s", r, a, n)
	},
	// IAM root
	"iam-root": func(_, a, _ string) string { return fmt.Sprintf("arn:aws:iam::%s:root", a) },
	// KMS
	"kms-key":   func(r, a, n string) string { return fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", r, a, n) },
	"kms-alias": func(r, a, n string) string { return fmt.Sprintf("arn:aws:kms:%s:%s:alias/%s", r, a, n) },
	"kms-grant": func(r, a, n string) string { return fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", r, a, n) },
	// SecretsManager
	"secretsmanager-secret": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s", r, a, n)
	},
	// SSM
	"ssm-parameter": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ssm:%s:%s:parameter/%s", r, a, n) },
	// API Gateway
	"apigateway-restapi":    func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	"apigateway-stage":      func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) }, // n = "apiID/stageName"
	"apigateway-resource":   func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	"apigateway-deployment": func(r, _, n string) string { return fmt.Sprintf("arn:aws:apigateway:%s::/restapis/%s", r, n) },
	// CloudFormation
	"cloudformation-stack": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cloudformation:%s:%s:stack/%s", r, a, n)
	},
	// CloudWatch Logs
	"logs-group":  func(r, a, n string) string { return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s:*", r, a, n) },
	"logs-stream": func(r, a, n string) string { return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s", r, a, n) },
	// Kinesis
	"kinesis-stream":   func(r, a, n string) string { return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", r, a, n) },
	"kinesis-consumer": func(r, a, n string) string { return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", r, a, n) }, // n = "streamName/consumer/name:ts"
	// ECR
	"ecr-repository": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecr:%s:%s:repository/%s", r, a, n) },
	// Step Functions
	"sfn-state-machine": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", r, a, n)
	},
	"sfn-activity": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:states:%s:%s:activity:%s", r, a, n)
	},
	// Phase 14 additions
	"eks-cluster":   func(r, a, n string) string { return fmt.Sprintf("arn:aws:eks:%s:%s:cluster/%s", r, a, n) },
	"eks-nodegroup": func(r, a, n string) string { return fmt.Sprintf("arn:aws:eks:%s:%s:nodegroup/%s", r, a, n) },
	"eks-addon":     func(r, a, n string) string { return fmt.Sprintf("arn:aws:eks:%s:%s:addon/%s", r, a, n) },
	// ECS
	"ecs-cluster":            func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", r, a, n) },
	"ecs-task":               func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:task/%s", r, a, n) },
	"ecs-task-definition":    func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:task-definition/%s", r, a, n) },
	"ecs-service":            func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:service/%s", r, a, n) },
	"ecs-container-instance": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:container-instance/%s", r, a, n) },
	// RDS
	"rds-cluster":     func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:cluster:%s", r, a, n) },
	"rds-instance":    func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", r, a, n) },
	"rds-subnetgroup": func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:subgrp:%s", r, a, n) },
	// STS / IAM additional types
	"sts-assumed-role":     func(_, a, n string) string { return fmt.Sprintf("arn:aws:sts::%s:assumed-role/%s", a, n) },
	"sts-federated-user":   func(_, a, n string) string { return fmt.Sprintf("arn:aws:sts::%s:federated-user/%s", a, n) },
	"iam-group":            func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:group/%s", a, n) },
	"iam-instance-profile": func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:instance-profile/%s", a, n) },
	"rds-snapshot":         func(r, a, n string) string { return fmt.Sprintf("arn:aws:rds:%s:%s:snapshot:%s", r, a, n) },
	"cfn-changeset": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cloudformation:%s:%s:changeSet/%s", r, a, n)
	},
	"elasticache-cluster": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:cluster:%s", r, a, n)
	},
	"elasticache-replication-group": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:replicationgroup:%s", r, a, n)
	},
	"elasticache-subnetgroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:subnetgroup:%s", r, a, n)
	},
	"elasticache-parametergroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticache:%s:%s:parametergroup:%s", r, a, n)
	},
	"rds-pg": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:rds:%s:%s:pg:%s", r, a, n)
	},
	// SNS platform
	"sns-platform-app":      func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:app/%s", r, a, n) },
	"sns-platform-endpoint": func(r, a, n string) string { return fmt.Sprintf("arn:aws:sns:%s:%s:endpoint/%s", r, a, n) },
	// Lambda code signing
	"lambda-code-signing-config": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:lambda:%s:%s:code-signing-config/%s", r, a, n)
	},
	// ECS task set and capacity provider
	"ecs-task-set":          func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:task-set/%s", r, a, n) },
	"ecs-capacity-provider": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ecs:%s:%s:capacity-provider/%s", r, a, n) },
	// Phase 15 additions
	"cognito-userpool": func(r, a, n string) string { return fmt.Sprintf("arn:aws:cognito-idp:%s:%s:userpool/%s", r, a, n) },
	"cognito-identitypool": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:cognito-identity:%s:%s:identitypool/%s", r, a, n)
	},
	"acm-certificate":         func(r, a, n string) string { return fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", r, a, n) },
	"firehose-stream":         func(r, a, n string) string { return fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/%s", r, a, n) },
	"cloudfront-distribution": func(_, a, n string) string { return fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", a, n) },
	"athena-workgroup":        func(r, a, n string) string { return fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", r, a, n) },
	"redshift-cluster":        func(r, a, n string) string { return fmt.Sprintf("arn:aws:redshift:%s:%s:cluster:%s", r, a, n) },
	"s3-accesspoint":          func(r, a, n string) string { return fmt.Sprintf("arn:aws:s3:%s:%s:accesspoint/%s", r, a, n) },
	// CloudWatch
	"cloudwatch-alarm":     func(r, a, n string) string { return fmt.Sprintf("arn:aws:cloudwatch:%s:%s:alarm:%s", r, a, n) },
	"cloudwatch-dashboard": func(_, a, n string) string { return fmt.Sprintf("arn:aws:cloudwatch::%s:dashboard/%s", a, n) },
	// IAM OIDC + additional IAM types
	"iam-oidc-provider":  func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", a, n) },
	"iam-managed-policy": func(_, a, n string) string { return fmt.Sprintf("arn:aws:iam::%s:policy/%s", a, n) },
	// SES
	"ses-identity": func(r, a, n string) string { return fmt.Sprintf("arn:aws:ses:%s:%s:identity/%s", r, a, n) },
	// Redshift additional
	"redshift-subnetgroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:redshift:%s:%s:subnetgroup:%s", r, a, n)
	},
	// Athena
	"athena-query-execution": func(r, a, n string) string { return fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", r, a, n) },
	// Step Functions executions — n encodes "sm-name/exec-name"
	"sfn-execution": func(r, a, n string) string {
		if sm, exec, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", r, a, sm, exec)
		}
		return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s", r, a, n)
	},
	"sfn-express-execution": func(r, a, n string) string {
		if sm, exec, ok := strings.Cut(n, "/"); ok {
			return fmt.Sprintf("arn:aws:states:%s:%s:express:%s:%s", r, a, sm, exec)
		}
		return fmt.Sprintf("arn:aws:states:%s:%s:express:%s", r, a, n)
	},
	// ELBv2 — n encodes the resource name; a unique hex suffix is expected to follow the name.
	"elb-loadbalancer": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/app/%s/%x", r, a, n, hashName(n))
	},
	"elb-targetgroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:targetgroup/%s/%x", r, a, n, hashName(n))
	},
	"elb-listener": func(r, a, n string) string {
		// n may be "lbArn-port" composite; embed a hash suffix.
		return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:listener/app/%s/%x", r, a, n, hashName(n))
	},
	// AWS Config
	"config-rule": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:config:%s:%s:config-rule/config-rule-%s", r, a, n)
	},
	// Resource Groups
	"resourcegroup": func(r, a, n string) string {
		return fmt.Sprintf("arn:aws:resource-groups:%s:%s:group/%s", r, a, n)
	},
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
		slog.Warn("AWSResourceID: unknown resource type, returning name as-is", "resourceType", resourceType, "name", name)
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

// splitCSV splits a comma-separated string into trimmed, non-empty tokens.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ExecutorMode returns the effective executor mode for a subsystem.
// Resolution: JAISCLOUD_{SUBSYSTEM}_EXECUTOR_MODE → JAISCLOUD_EXECUTOR_MODE → defaultMode.
func ExecutorMode(subsystem, defaultMode string) (mode, source string) {
	key := "JAISCLOUD_" + strings.ToUpper(subsystem) + "_EXECUTOR_MODE"
	if v := os.Getenv(key); v != "" {
		return v, key
	}
	if v := os.Getenv("JAISCLOUD_EXECUTOR_MODE"); v != "" {
		return v, "JAISCLOUD_EXECUTOR_MODE"
	}
	return defaultMode, "default"
}

func Load() (*Config, error) {
	viper.SetDefault("port", 4566)
	viper.SetDefault("mode", "memory")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("region", "us-east-1")
	viper.SetDefault("account_id", "000000000000")
	viper.SetDefault("dsn", "")
	if home, err := os.UserHomeDir(); err == nil {
		viper.SetDefault("blob_dir", filepath.Join(home, ".jaiscloud", "blobs"))
		viper.SetDefault("data_dir", "")
	} else {
		viper.SetDefault("blob_dir", ".jaiscloud/blobs")
		viper.SetDefault("data_dir", "")
	}
	viper.SetDefault("fresh_start", false)
	viper.SetDefault("snapshot_interval", "30s")
	viper.SetDefault("export_soft_limit", int64(2*1024*1024*1024))
	viper.SetDefault("executor_mode", "")
	viper.SetDefault("kms_master_key", "")
	viper.SetDefault("k8s_namespace", "jaiscloud")
	viper.SetDefault("k8s_spark_image", "")
	viper.SetDefault("k8s_spark_sa", "")
	viper.SetDefault("spark_emr_image", "")
	viper.SetDefault("spark_emreks_image", "")
	viper.SetDefault("aws_emulator_endpoint", "")
	viper.SetDefault("s3_virtual_host_bases", "")
	viper.SetDefault("imds_enabled", false)
	viper.SetDefault("lambda_image", "")
	viper.SetDefault("lambda_network", "jaiscloud-net")
	viper.SetDefault("lambda_keepalive_secs", 300)
	viper.SetDefault("metrics", false)
	viper.SetDefault("tracing", false)
	viper.SetDefault("deterministic", false)
	viper.SetDefault("seed", 0)
	viper.SetDefault("time_mode", "offset")
	viper.SetDefault("oidc_issuers", "")

	viper.SetEnvPrefix("JAISCLOUD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Parse snapshot interval.
	snapshotInterval := 30 * time.Second
	if s := viper.GetString("snapshot_interval"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			snapshotInterval = d
		}
	}

	cfg := &Config{
		Port:                viper.GetInt("port"),
		Mode:                Mode(viper.GetString("mode")),
		LogLevel:            viper.GetString("log_level"),
		Region:              viper.GetString("region"),
		AccountID:           viper.GetString("account_id"),
		DSN:                 viper.GetString("dsn"),
		BlobDir:             viper.GetString("blob_dir"),
		DataDir:             viper.GetString("data_dir"),
		FreshStart:          viper.GetBool("fresh_start"),
		SnapshotInterval:    snapshotInterval,
		ExportSoftLimit:     viper.GetInt64("export_soft_limit"),
		ExecutorMode:        viper.GetString("executor_mode"),
		KMSMasterKey:        viper.GetString("kms_master_key"),
		K8sNamespace:        viper.GetString("k8s_namespace"),
		K8sSparkImage:       viper.GetString("k8s_spark_image"),
		K8sSparkSA:          viper.GetString("k8s_spark_sa"),
		SparkEMRImage:       viper.GetString("spark_emr_image"),
		SparkEMREKSImage:    viper.GetString("spark_emreks_image"),
		AWSEmulatorEndpoint: viper.GetString("aws_emulator_endpoint"),
		S3VirtualHostBases:  splitCSV(viper.GetString("s3_virtual_host_bases")),
		IMDSEnabled:         viper.GetBool("imds_enabled"),
		LambdaImage:         viper.GetString("lambda_image"),
		LambdaNetwork:       viper.GetString("lambda_network"),
		LambdaKeepaliveSecs: viper.GetInt("lambda_keepalive_secs"),
		Metrics:             viper.GetBool("metrics"),
		Tracing:             viper.GetBool("tracing"),
		Deterministic:       viper.GetBool("deterministic"),
		Seed:                viper.GetInt64("seed"),
		TimeMode:            viper.GetString("time_mode"),
		OIDCIssuers:         nil, // populated below from oidc_issuers
	}

	// DSN must not be set for memory mode.
	// Detect via the env var since viper cannot distinguish between default from explicit for flags.
	if cfg.Mode == ModeMemory && cfg.DSN != "" && os.Getenv("JAISCLOUD_DSN") != string(ModeMemory) {
		return nil, fmt.Errorf("JAISCLOUD_DSN is set but mode is memory: either unset JAISCLOUD_DSN or set mode to persistent")
	}

	if cfg.Mode != ModeMemory && cfg.Mode != ModePersistent {
		return nil, fmt.Errorf("invalid mode %q: must be memory or persistent", cfg.Mode)
	}

	// Parse OIDC_ISSUERS: "issuer1=jwks_url1,issuer2=jwks_url2"
	if raw := viper.GetString("oidc_issuers"); raw != "" {
		cfg.OIDCIssuers = make(map[string]string)
		for _, pair := range strings.Split(raw, ",") {
			pair = strings.TrimSpace(pair)
			if k, v, ok := strings.Cut(pair, "="); ok {
				cfg.OIDCIssuers[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
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
