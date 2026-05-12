package integration

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jaiscloudEndpoint() string {
	if ep := os.Getenv("JAISCLOUD_ENDPOINT"); ep != "" {
		return ep
	}
	return "http://localhost:4566"
}

func newAWSConfig(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	return cfg
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudEndpoint()+"/_jaiscloud/reset", "", nil)
	require.NoError(t, err)
	resp.Body.Close()
}

func assertAWSError(t *testing.T, err error, expectedCode string) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, expectedCode, apiErr.ErrorCode())
}

// sfnSyncCtx disables the SDK's automatic "sync-" hostname prefix for StartSyncExecution.
// Without this the SDK rewrites localhost:4566 → sync-localhost:4566, causing DNS failures.
func sfnSyncCtx(ctx context.Context) context.Context {
	return smithyhttp.DisableEndpointHostPrefix(ctx, true)
}
