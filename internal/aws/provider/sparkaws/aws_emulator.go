package sparkaws

import (
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// AWSEmulatorConfig carries the AWS emulator endpoint wiring needed by Spark
// driver pods. All fields are optional; nil config is a no-op.
type AWSEmulatorConfig struct {
	Region          string
	AccountID       string
	S3Endpoint      string // HTTP scheme drives s3a SSL: http→false, https→true
	IMDSEndpoint    string // empty → AWS_EC2_METADATA_DISABLED=true
	AccessKeyID     string
	SecretAccessKey string
}

// DriverEnv returns env vars to inject into the spark-submit driver container.
// Returns nil when cfg is nil.
func DriverEnv(cfg *AWSEmulatorConfig) []corev1.EnvVar {
	if cfg == nil {
		return nil
	}
	akid := cfg.AccessKeyID
	if akid == "" {
		akid = "test"
	}
	sak := cfg.SecretAccessKey
	if sak == "" {
		sak = "test"
	}
	env := []corev1.EnvVar{
		{Name: "AWS_ACCESS_KEY_ID", Value: akid},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: sak},
		{Name: "AWS_REGION", Value: cfg.Region},
		{Name: "AWS_DEFAULT_REGION", Value: cfg.Region},
		{Name: "AWS_ENDPOINT_URL", Value: cfg.S3Endpoint},
		{Name: "AWS_ENDPOINT_URL_S3", Value: cfg.S3Endpoint},
	}
	if cfg.IMDSEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "AWS_EC2_METADATA_SERVICE_ENDPOINT", Value: cfg.IMDSEndpoint})
	} else {
		env = append(env, corev1.EnvVar{Name: "AWS_EC2_METADATA_DISABLED", Value: "true"})
	}
	return env
}

// DriverSparkConfsFromEnv returns spark-submit --conf tokens for s3a wiring.
// Accepts pre-computed driverEnv (from DriverEnv) so callers that already hold
// the slice avoid a second computation. Returns nil when cfg is nil.
func DriverSparkConfsFromEnv(cfg *AWSEmulatorConfig, driverEnv []corev1.EnvVar) []string {
	if cfg == nil {
		return nil
	}
	akid := cfg.AccessKeyID
	if akid == "" {
		akid = "test"
	}
	sak := cfg.SecretAccessKey
	if sak == "" {
		sak = "test"
	}
	ssl := sslEnabledFromScheme(cfg.S3Endpoint)
	confs := []string{
		"--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
		"--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.access.key=%s", akid),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.secret.key=%s", sak),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.endpoint=%s", cfg.S3Endpoint),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.connection.ssl.enabled=%t", ssl),
		"--conf", "spark.hadoop.fs.s3a.path.style.access=true",
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3a.endpoint.region=%s", cfg.Region),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3.region=%s", cfg.Region),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3.endpoint=%s", cfg.S3Endpoint),
		"--conf", fmt.Sprintf("spark.hadoop.fs.s3n.endpoint=%s", cfg.S3Endpoint),
		"--conf", "spark.hadoop.fs.s3.path.style.access=true",
		"--conf", "spark.hadoop.fs.s3n.path.style.access=true",
	}
	// Mirror every driver env var into spark.executorEnv.* so executor pods
	// inherit the same AWS wiring.
	for _, e := range driverEnv {
		confs = append(confs, "--conf", fmt.Sprintf("spark.executorEnv.%s=%s", e.Name, e.Value))
	}
	return confs
}

// DriverSparkConfs returns spark-submit --conf tokens for s3a wiring.
// Use DriverSparkConfsFromEnv when DriverEnv has already been computed to avoid
// computing it twice per job submission.
func DriverSparkConfs(cfg *AWSEmulatorConfig) []string {
	return DriverSparkConfsFromEnv(cfg, DriverEnv(cfg))
}

// sslEnabledFromScheme returns true iff the endpoint URL uses https.
func sslEnabledFromScheme(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" {
		return false
	}
	return strings.EqualFold(u.Scheme, "https")
}
