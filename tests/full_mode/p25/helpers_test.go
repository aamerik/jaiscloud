//go:build lambda_e2e

package p25_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awscf "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
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

func newKMSClient(t *testing.T) *awskms.Client {
	t.Helper()
	return awskms.NewFromConfig(awsCfg(t), func(o *awskms.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func newSMClient(t *testing.T) *awssm.Client {
	t.Helper()
	return awssm.NewFromConfig(awsCfg(t), func(o *awssm.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func newSSMClient(t *testing.T) *awsssm.Client {
	t.Helper()
	return awsssm.NewFromConfig(awsCfg(t), func(o *awsssm.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func newCFClient(t *testing.T) *awscf.Client {
	t.Helper()
	return awscf.NewFromConfig(awsCfg(t), func(o *awscf.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func newSQSClient(t *testing.T) *awssqs.Client {
	t.Helper()
	return awssqs.NewFromConfig(awsCfg(t), func(o *awssqs.Options) {
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

// ─── skip guards ─────────────────────────────────────────────────────────────

// requireLambdaDockerEnv skips the test unless LAMBDA_E2E_DOCKER_IMAGE is set,
// which implies the server is running with JAISCLOUD_LAMBDA_MODE=docker and
// Docker is available. The variable should be the image URI for a test Lambda
// function (e.g. public.ecr.aws/lambda/python:3.12 with a handler that echoes
// the event).
func requireLambdaDockerEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("LAMBDA_E2E_DOCKER_IMAGE") == "" {
		t.Skip("LAMBDA_E2E_DOCKER_IMAGE not set — skipping Lambda Docker e2e test")
	}
}

// requireLambdaK8sEnv skips the test unless LAMBDA_E2E_K8S_IMAGE is set,
// which implies the server is running with JAISCLOUD_LAMBDA_MODE=k8s and a
// Kubernetes cluster is reachable.
func requireLambdaK8sEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("LAMBDA_E2E_K8S_IMAGE") == "" {
		t.Skip("LAMBDA_E2E_K8S_IMAGE not set — skipping Lambda K8s e2e test")
	}
}

// ─── timing helpers ───────────────────────────────────────────────────────────

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

// ─── cloudformation helpers ───────────────────────────────────────────────────

// pollStackStatus polls DescribeStacks until the stack reaches a terminal
// CREATE_COMPLETE / UPDATE_COMPLETE / DELETE_COMPLETE / *_FAILED state.
func pollStackStatus(t *testing.T, cfClient *awscf.Client, stackName string) string {
	t.Helper()
	deadline := time.Now().Add(invokeTimeout())
	for time.Now().Before(deadline) {
		out, err := cfClient.DescribeStacks(context.Background(), &awscf.DescribeStacksInput{
			StackName: aws.String(stackName),
		})
		if err != nil {
			// Stack not found = deleted
			return "DELETE_COMPLETE"
		}
		if len(out.Stacks) == 0 {
			return "DELETE_COMPLETE"
		}
		status := string(out.Stacks[0].StackStatus)
		t.Logf("stack %s status: %s", stackName, status)
		if isTerminalStackStatus(status) {
			return status
		}
		time.Sleep(pollInterval())
	}
	t.Fatalf("stack %s did not reach terminal status within %s", stackName, invokeTimeout())
	return ""
}

func isTerminalStackStatus(s string) bool {
	switch s {
	case "CREATE_COMPLETE", "CREATE_FAILED", "ROLLBACK_COMPLETE",
		"UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
		"DELETE_COMPLETE", "DELETE_FAILED":
		return true
	}
	return false
}
