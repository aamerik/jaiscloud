//go:build kms_fullmode

package kms_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudHost()+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}
