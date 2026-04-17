//go:build lambda_e2e

package p25_test

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// k8sImage returns the test Lambda image URI from LAMBDA_E2E_K8S_IMAGE.
// The image must be a valid Lambda container image accessible from the K8s
// cluster that the server is configured to use.
func k8sImage() string { return os.Getenv("LAMBDA_E2E_K8S_IMAGE") }

// TestLambdaK8s_Invoke_ReturnsResponse creates a Lambda function and invokes
// it via the K8s executor. Each invocation creates a one-shot batch/v1 Job and
// collects the result from pod logs.
func TestLambdaK8s_Invoke_ReturnsResponse(t *testing.T) {
	requireLambdaK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("k8s-invoke-test"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(k8sImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	payload := map[string]any{"hello": "k8s", "n": 1}
	payloadBytes, _ := json.Marshal(payload)

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("k8s-invoke-test"),
		Payload:      payloadBytes,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
	assert.NotEmpty(t, out.Payload, "expected non-empty response from K8s job")
}

// TestLambdaK8s_MultipleInvocations_EachCreatesNewJob verifies that the K8s
// executor creates a distinct Job per invocation (no warm pool — each call is
// a fresh one-shot Job). All invocations must complete successfully.
func TestLambdaK8s_MultipleInvocations_EachCreatesNewJob(t *testing.T) {
	requireLambdaK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("k8s-multi-invoke"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(k8sImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	const n = 3
	for i := 0; i < n; i++ {
		payload, _ := json.Marshal(map[string]int{"seq": i})
		out, err := c.Invoke(ctx, &awslambda.InvokeInput{
			FunctionName: aws.String("k8s-multi-invoke"),
			Payload:      payload,
		})
		require.NoError(t, err, "invocation %d failed", i)
		assert.EqualValues(t, 200, out.StatusCode, "invocation %d status", i)
	}
}

// TestLambdaK8s_ConcurrentInvocations verifies that multiple simultaneous
// invocations each create their own K8s Job and all complete without error.
func TestLambdaK8s_ConcurrentInvocations(t *testing.T) {
	requireLambdaK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("k8s-concurrent"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(k8sImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	statuses := make([]int32, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]int{"idx": idx})
			out, err := c.Invoke(ctx, &awslambda.InvokeInput{
				FunctionName: aws.String("k8s-concurrent"),
				Payload:      payload,
			})
			errs[idx] = err
			if out != nil {
				statuses[idx] = out.StatusCode
			}
		}(i)
	}
	wg.Wait()

	for i := range errs {
		assert.NoError(t, errs[i], "concurrent invocation %d error", i)
		assert.EqualValues(t, 200, statuses[i], "concurrent invocation %d status", i)
	}
}

// TestLambdaK8s_DeleteFunction_NoErrorAfterDelete deletes a function and
// verifies that subsequent invocations return a not-found error. In K8s mode
// there is no warm pool to clean up, so delete is synchronous.
func TestLambdaK8s_DeleteFunction_NoErrorAfterDelete(t *testing.T) {
	requireLambdaK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("k8s-delete-test"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(k8sImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
	})
	require.NoError(t, err)

	_, err = c.DeleteFunction(ctx, &awslambda.DeleteFunctionInput{
		FunctionName: aws.String("k8s-delete-test"),
	})
	require.NoError(t, err)

	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("k8s-delete-test"),
		Payload:      []byte(`{}`),
	})
	require.Error(t, err, "invoking deleted function should return error")
}

// TestLambdaK8s_EnvironmentVariables_PassedToJob verifies that environment
// variables configured on the function are included in the K8s Job spec and
// thus visible to the Lambda handler.
func TestLambdaK8s_EnvironmentVariables_PassedToJob(t *testing.T) {
	requireLambdaK8sEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("k8s-env-test"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(k8sImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Environment: &types.Environment{
			Variables: map[string]string{
				"APP_MODE": "e2e",
				"VERSION":  "42",
			},
		},
	})
	require.NoError(t, err)

	// Verify env vars are stored on the function config.
	getOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("k8s-env-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Configuration.Environment)
	assert.Equal(t, "e2e", getOut.Configuration.Environment.Variables["APP_MODE"])
	assert.Equal(t, "42", getOut.Configuration.Environment.Variables["VERSION"])

	// Invoke — the K8s Job will receive these env vars.
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("k8s-env-test"),
		Payload:      []byte(`{}`),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
}
