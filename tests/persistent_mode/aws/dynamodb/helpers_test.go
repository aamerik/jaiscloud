//go:build dynamo_fullmode

package dynamodb_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

func newDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	return dynamodb.NewFromConfig(awsCfg(t), func(o *dynamodb.Options) {
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

// strAttr returns a DynamoDB AttributeValue with type S.
func strAttr(v string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: v}
}

// numAttr returns a DynamoDB AttributeValue with type N.
func numAttr(v string) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: v}
}
