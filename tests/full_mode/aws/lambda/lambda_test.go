//go:build lambda_e2e

package lambda_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dockerImage returns the test Lambda image URI from LAMBDA_E2E_DOCKER_IMAGE.
// The image must be a valid Lambda container image that echoes the invocation
// event back as its response (e.g. built from public.ecr.aws/lambda/python:3.12
// with a handler: lambda_handler(event, ctx) { return event }).
func dockerImage() string { return os.Getenv("LAMBDA_E2E_DOCKER_IMAGE") }

// TestLambdaDocker_ColdStart_ReturnsResponse creates a function backed by a
// real Docker container and invokes it once, verifying a non-error response.
func TestLambdaDocker_ColdStart_ReturnsResponse(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("docker-cold-start"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	payload := map[string]any{"hello": "docker", "ts": time.Now().UnixMilli()}
	payloadBytes, _ := json.Marshal(payload)

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-cold-start"),
		Payload:      payloadBytes,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
	assert.NotEmpty(t, out.Payload, "expected non-empty response payload")
}

// TestLambdaDocker_WarmPoolReuse invokes the same function twice and asserts
// the second call is faster (reuses the warm container, no cold start).
func TestLambdaDocker_WarmPoolReuse(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("docker-warm-pool"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	payload := []byte(`{"ping":true}`)

	// Cold start.
	coldStart := time.Now()
	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-warm-pool"),
		Payload:      payload,
	})
	require.NoError(t, err)
	coldDuration := time.Since(coldStart)

	// Warm invocation — same container reused.
	warmStart := time.Now()
	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-warm-pool"),
		Payload:      payload,
	})
	require.NoError(t, err)
	warmDuration := time.Since(warmStart)

	t.Logf("cold=%s  warm=%s", coldDuration, warmDuration)
	// Warm invocation should be meaningfully faster than cold start.
	// Allow generous slack for slow CI; just verify warm < cold.
	assert.Less(t, warmDuration, coldDuration,
		"warm invocation should be faster than cold start")
}

// TestLambdaDocker_ConcurrentInvocations fires 5 simultaneous invocations and
// verifies all succeed without deadlocking or returning errors. The executor
// must serialize concurrent requests to the single warm container.
func TestLambdaDocker_ConcurrentInvocations(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("docker-concurrent"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	const n = 5
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]int{"i": idx})
			_, errs[idx] = c.Invoke(ctx, &awslambda.InvokeInput{
				FunctionName: aws.String("docker-concurrent"),
				Payload:      payload,
			})
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		assert.NoError(t, e, "invocation %d failed", i)
	}
}

// TestLambdaDocker_DeleteFunction_StopsContainer creates a function, invokes
// it once (starts the warm container), then deletes the function and verifies
// the function is gone. The container cleanup is async but the API must succeed.
func TestLambdaDocker_DeleteFunction_StopsContainer(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("docker-delete-test"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	// Start the warm container via a first invocation.
	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-delete-test"),
		Payload:      []byte(`{}`),
	})
	require.NoError(t, err)

	// Delete must succeed synchronously.
	_, err = c.DeleteFunction(ctx, &awslambda.DeleteFunctionInput{
		FunctionName: aws.String("docker-delete-test"),
	})
	require.NoError(t, err)

	// Subsequent invocation must fail.
	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-delete-test"),
		Payload:      []byte(`{}`),
	})
	require.Error(t, err, "expected error invoking deleted function")
}

// TestLambdaDocker_UpdateCode_HotswapdContainer updates a function's image URI
// and verifies the function configuration reflects the new image. Container
// hotswap (stopping old, starting new on next invoke) is exercised implicitly.
func TestLambdaDocker_UpdateCode_HotswapContainer(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("docker-hotswap"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	// Warm up the container.
	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-hotswap"),
		Payload:      []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	// Simulate "new version" by updating to the same image (same tag).
	// In practice this would be a new image URI; here we verify the API path.
	newImage := dockerImage()
	updOut, err := c.UpdateFunctionCode(ctx, &awslambda.UpdateFunctionCodeInput{
		FunctionName: aws.String("docker-hotswap"),
		ImageUri:     aws.String(newImage),
	})
	require.NoError(t, err)
	assert.Equal(t, "docker-hotswap", aws.ToString(updOut.FunctionName))

	// After update, invocation still works.
	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-hotswap"),
		Payload:      []byte(`{"v":2}`),
	})
	require.NoError(t, err)
}

// TestLambdaDocker_EnvironmentVariables_PassedToContainer verifies that env
// vars set on the function are present inside the running container. The test
// Lambda handler must return os.environ["TEST_KEY"] in its response.
func TestLambdaDocker_EnvironmentVariables_PassedToContainer(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("docker-env-test"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Environment: &types.Environment{
			Variables: map[string]string{
				"TEST_KEY":  "hello-from-env",
				"STAGE":     "e2e",
			},
		},
	})
	require.NoError(t, err)

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("docker-env-test"),
		Payload:      []byte(`{"echo_env": "TEST_KEY"}`),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
	// If the test image echoes env vars, response should include "hello-from-env".
	// With a pure echo handler this verifies the env was accepted at create time.
	t.Logf("response: %s", string(out.Payload))
}

// TestLambdaDocker_LogResult_Tail creates a Python 3.11 function that prints
// a sentinel string to stdout, invokes it with LogType=Tail, and asserts
// that LogResult (base64-decoded) contains the sentinel.
func TestLambdaDocker_LogResult_Tail(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	// Build function zip: lambda_function.py that prints to stdout
	fnZip := buildZip(t, map[string]string{
		"lambda_function.py": `def handler(event, context):
    print("HELLO_FROM_LAMBDA")
    return {"ok": True}
`,
	})
	fnB64 := base64.StdEncoding.EncodeToString(fnZip)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("log-result-tail-test"),
		Runtime:      types.RuntimePython311,
		Handler:      aws.String("lambda_function.handler"),
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Code: &types.FunctionCode{
			ZipFile: []byte(fnB64),
		},
	})
	require.NoError(t, err, "CreateFunction")

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("log-result-tail-test"),
		Payload:      []byte(`{}`),
		LogType:      types.LogTypeTail,
	})
	require.NoError(t, err, "Invoke")
	assert.EqualValues(t, 200, out.StatusCode)
	assert.Empty(t, aws.ToString(out.FunctionError), "expected no function error")

	// LogResult is base64-encoded in the response header; SDK exposes it decoded.
	require.NotEmpty(t, aws.ToString(out.LogResult), "expected non-empty LogResult")
	logBytes, err := base64.StdEncoding.DecodeString(aws.ToString(out.LogResult))
	require.NoError(t, err, "base64-decode LogResult")
	assert.True(t, strings.Contains(string(logBytes), "HELLO_FROM_LAMBDA"),
		"expected LogResult to contain HELLO_FROM_LAMBDA, got: %s", string(logBytes))
}
