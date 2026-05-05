package sparkaws

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestSSL_FromScheme(t *testing.T) {
	cases := []struct {
		endpoint string
		want     bool
	}{
		{"", false},
		{"http://foo:4566", false},
		{"https://foo:4566", true},
		{"HTTPS://FOO", true},
		{"malformed", false},
	}
	for _, tc := range cases {
		if got := sslEnabledFromScheme(tc.endpoint); got != tc.want {
			t.Errorf("sslEnabledFromScheme(%q) = %v, want %v", tc.endpoint, got, tc.want)
		}
	}
}

func TestDriverEnv_NilConfigReturnsEmpty(t *testing.T) {
	if got := DriverEnv(nil); got != nil {
		t.Fatalf("DriverEnv(nil) = %v, want nil", got)
	}
}

func TestDriverEnv_AppliesDefaultCredentials(t *testing.T) {
	env := DriverEnv(&AWSEmulatorConfig{
		Region:     "us-east-1",
		S3Endpoint: "http://x",
	})
	m := envMap(env)
	if m["AWS_ACCESS_KEY_ID"] != "test" || m["AWS_SECRET_ACCESS_KEY"] != "test" {
		t.Fatalf("default creds not applied: %v", m)
	}
	if m["AWS_REGION"] != "us-east-1" || m["AWS_DEFAULT_REGION"] != "us-east-1" {
		t.Fatalf("region env missing: %v", m)
	}
	if m["AWS_ENDPOINT_URL"] != "http://x" || m["AWS_ENDPOINT_URL_S3"] != "http://x" {
		t.Fatalf("endpoint env missing: %v", m)
	}
}

func TestDriverEnv_IMDSEndpointSet(t *testing.T) {
	env := DriverEnv(&AWSEmulatorConfig{
		Region:       "us-west-2",
		S3Endpoint:   "http://x",
		IMDSEndpoint: "http://y",
	})
	m := envMap(env)
	if m["AWS_EC2_METADATA_SERVICE_ENDPOINT"] != "http://y" {
		t.Fatalf("IMDS endpoint not set: %v", m)
	}
	if _, disabled := m["AWS_EC2_METADATA_DISABLED"]; disabled {
		t.Fatalf("AWS_EC2_METADATA_DISABLED should not be set when IMDSEndpoint is: %v", m)
	}
}

func TestDriverEnv_IMDSEndpointEmptyDisablesMetadata(t *testing.T) {
	env := DriverEnv(&AWSEmulatorConfig{Region: "us-west-2", S3Endpoint: "http://x"})
	m := envMap(env)
	if m["AWS_EC2_METADATA_DISABLED"] != "true" {
		t.Fatalf("AWS_EC2_METADATA_DISABLED not set when IMDS endpoint missing: %v", m)
	}
}

func TestDriverSparkConfs_NilConfigReturnsEmpty(t *testing.T) {
	if got := DriverSparkConfs(nil); got != nil {
		t.Fatalf("DriverSparkConfs(nil) = %v, want nil", got)
	}
}

func TestDriverSparkConfs_SSLDerivedFromScheme(t *testing.T) {
	httpConfs := confMap(DriverSparkConfs(&AWSEmulatorConfig{
		Region: "us-east-1", S3Endpoint: "http://emu:4566",
	}))
	if got := httpConfs["spark.hadoop.fs.s3a.connection.ssl.enabled"]; got != "false" {
		t.Fatalf("http endpoint should disable ssl, got %q", got)
	}
	httpsConfs := confMap(DriverSparkConfs(&AWSEmulatorConfig{
		Region: "us-east-1", S3Endpoint: "https://s3.amazonaws.com",
	}))
	if got := httpsConfs["spark.hadoop.fs.s3a.connection.ssl.enabled"]; got != "true" {
		t.Fatalf("https endpoint should enable ssl, got %q", got)
	}
}

func TestDriverSparkConfs_RequiredKeysPresent(t *testing.T) {
	confs := confMap(DriverSparkConfs(&AWSEmulatorConfig{
		Region:     "us-east-1",
		S3Endpoint: "http://emu:4566",
	}))
	for _, key := range []string{
		"spark.hadoop.fs.s3a.aws.credentials.provider",
		"spark.hadoop.fs.s3a.access.key",
		"spark.hadoop.fs.s3a.secret.key",
		"spark.hadoop.fs.s3a.endpoint",
		"spark.hadoop.fs.s3a.path.style.access",
		"spark.hadoop.fs.s3a.endpoint.region",
		"spark.hadoop.fs.s3.region",
		"spark.hadoop.fs.s3.endpoint",
		"spark.hadoop.fs.s3n.endpoint",
		"spark.hadoop.fs.s3.path.style.access",
		"spark.hadoop.fs.s3n.path.style.access",
		"spark.executorEnv.AWS_REGION",
		"spark.executorEnv.AWS_ACCESS_KEY_ID",
	} {
		if _, ok := confs[key]; !ok {
			t.Errorf("missing conf key %q", key)
		}
	}
}

func TestDriverSparkConfs_ExecutorEnvMirrorsDriverEnv(t *testing.T) {
	cfg := &AWSEmulatorConfig{Region: "us-east-1", S3Endpoint: "http://emu"}
	driver := envMap(DriverEnv(cfg))
	confs := confMap(DriverSparkConfs(cfg))
	for k, v := range driver {
		key := "spark.executorEnv." + k
		if got, ok := confs[key]; !ok || got != v {
			t.Errorf("executorEnv mismatch for %s: got=%q want=%q", k, got, v)
		}
	}
}

func envMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

// confMap pairs adjacent tokens and splits each "key=value" on "=".
func confMap(tokens []string) map[string]string {
	out := make(map[string]string)
	for i := 0; i+1 < len(tokens); i += 2 {
		if tokens[i] != "--conf" {
			continue
		}
		parts := strings.SplitN(tokens[i+1], "=", 2)
		if len(parts) == 2 {
			out[parts[0]] = parts[1]
		}
	}
	return out
}
