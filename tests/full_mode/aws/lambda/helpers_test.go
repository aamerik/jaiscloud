//go:build lambda_e2e

package lambda_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
)

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func awsCfg(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return cfg
}

func newLambdaClient(t *testing.T) *awslambda.Client {
	t.Helper()
	return awslambda.NewFromConfig(awsCfg(t), func(o *awslambda.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudHost()+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

func requireLambdaDockerEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("LAMBDA_E2E_DOCKER_IMAGE") == "" {
		t.Skip("LAMBDA_E2E_DOCKER_IMAGE not set — skipping Lambda Docker e2e test")
	}
}

func requireLambdaK8sEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("LAMBDA_E2E_K8S_IMAGE") == "" {
		t.Skip("LAMBDA_E2E_K8S_IMAGE not set — skipping Lambda K8s e2e test")
	}
}

func invokeTimeout() time.Duration {
	if v := os.Getenv("LAMBDA_E2E_INVOKE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 2 * time.Minute
}

func pollInterval() time.Duration {
	if v := os.Getenv("LAMBDA_E2E_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 3 * time.Second
}
