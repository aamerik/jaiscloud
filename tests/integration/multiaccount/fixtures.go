package multiaccount

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"

	"jaiscloud/internal/aws/identity"
)

const (
	AcctA = "111111111111"
	AcctB = "222222222222"
	AcctC = "333333333333"

	endpoint = "http://localhost:4566"
	region   = "us-east-1"
)

// accessKeyFor returns an LSIA-encoded access key that encodes the given account ID.
func accessKeyFor(t *testing.T, account string) string {
	t.Helper()
	key, err := identity.EncodeLSIA(account)
	if err != nil {
		t.Fatalf("EncodeLSIA(%q): %v", account, err)
	}
	return key
}

// awsCfgFor returns an aws.Config pointed at the local emulator using account-scoped credentials.
func awsCfgFor(t *testing.T, account string) aws.Config {
	t.Helper()
	ak := accessKeyFor(t, account)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, "test", "")),
	)
	if err != nil {
		t.Fatalf("load config for account %s: %v", account, err)
	}
	return cfg
}

func newSQSFor(t *testing.T, account string) *sqs.Client {
	cfg := awsCfgFor(t, account)
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newDynamoFor(t *testing.T, account string) *awsdynamo.Client {
	cfg := awsCfgFor(t, account)
	return awsdynamo.NewFromConfig(cfg, func(o *awsdynamo.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newKMSFor(t *testing.T, account string) *awskms.Client {
	cfg := awsCfgFor(t, account)
	return awskms.NewFromConfig(cfg, func(o *awskms.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newSTSFor(t *testing.T, account string) *awssts.Client {
	cfg := awsCfgFor(t, account)
	return awssts.NewFromConfig(cfg, func(o *awssts.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newSNSFor(t *testing.T, account string) *awssns.Client {
	cfg := awsCfgFor(t, account)
	return awssns.NewFromConfig(cfg, func(o *awssns.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func newS3For(t *testing.T, account string) *awss3.Client {
	cfg := awsCfgFor(t, account)
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func newSMFor(t *testing.T, account string) *awssm.Client {
	cfg := awsCfgFor(t, account)
	return awssm.NewFromConfig(cfg, func(o *awssm.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

// resetState wipes all emulator state between tests.
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(endpoint+"/_jaiscloud/reset", "", nil)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	resp.Body.Close()
}

// resetScope wipes state for one (account, region) pair.
func resetScope(t *testing.T, account, reg string) {
	t.Helper()
	u := fmt.Sprintf("%s/_jaiscloud/reset?account=%s&region=%s", endpoint, account, reg)
	resp, err := http.Post(u, "", nil)
	if err != nil {
		t.Fatalf("resetScope: %v", err)
	}
	resp.Body.Close()
}

// resetAccount wipes all regions for one account.
func resetAccount(t *testing.T, account string) {
	t.Helper()
	u := fmt.Sprintf("%s/_jaiscloud/reset?account=%s", endpoint, account)
	resp, err := http.Post(u, "", nil)
	if err != nil {
		t.Fatalf("resetAccount: %v", err)
	}
	resp.Body.Close()
}
