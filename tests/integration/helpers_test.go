package integration_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
)

const jaiscloudEndpoint = "http://localhost:4566"

func newSQSClient(t *testing.T) *sqs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newIAMClient(t *testing.T) *awsiam.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsiam.NewFromConfig(cfg, func(o *awsiam.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newDynamoClient(t *testing.T) *awsdynamo.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awsdynamo.NewFromConfig(cfg, func(o *awsdynamo.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newSNSClient(t *testing.T) *awssns.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awssns.NewFromConfig(cfg, func(o *awssns.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func newS3Client(t *testing.T) *awss3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
		o.UsePathStyle = true
	})
}

func newSTSClient(t *testing.T) *awssts.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awssts.NewFromConfig(cfg, func(o *awssts.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudEndpoint+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

// host returns the host JaisCloud is expected to include in queue URLs.
func host() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "localhost"
}
