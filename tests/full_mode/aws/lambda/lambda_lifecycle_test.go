//go:build lambda_e2e

package lambda_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLambda_ColdStartAfterReset creates a function, invokes it, resets state,
// then verifies the function is gone and re-creation works correctly.
func TestLambda_ColdStartAfterReset(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("lifecycle-reset-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("handler.handler"),
	})
	require.NoError(t, err)

	_, err = c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("lifecycle-reset-fn"),
		Payload:      []byte(`{"hello":"reset"}`),
	})
	require.NoError(t, err)

	resetState(t)

	// After reset the function must be gone.
	_, err = c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("lifecycle-reset-fn"),
	})
	require.Error(t, err, "function must be gone after reset")

	// Re-create succeeds.
	_, err = c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("lifecycle-reset-fn"),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("handler.handler"),
	})
	require.NoError(t, err)
}

// TestLambda_DeleteAndReCreate deletes a warm function and immediately re-creates
// it, verifying there is no collision in the executor's warm pool.
func TestLambda_DeleteAndReCreate(t *testing.T) {
	requireLambdaDockerEnv(t)
	resetState(t)

	ctx := context.Background()
	c := newLambdaClient(t)

	const name = "delete-recreate-fn"

	createFn := func() {
		t.Helper()
		_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
			FunctionName: aws.String(name),
			PackageType:  types.PackageTypeImage,
			Code:         &types.FunctionCode{ImageUri: aws.String(dockerImage())},
			Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
			Handler:      aws.String("handler.handler"),
		})
		require.NoError(t, err)
	}

	createFn()

	// Invoke to warm the container.
	_, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String(name),
		Payload:      []byte(`{}`),
	})
	require.NoError(t, err)

	// Delete the function.
	_, err = c.DeleteFunction(ctx, &awslambda.DeleteFunctionInput{
		FunctionName: aws.String(name),
	})
	require.NoError(t, err)

	// Re-create and invoke must succeed.
	createFn()
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String(name),
		Payload:      []byte(`{"recreated":true}`),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
}

// TestLambda_HealthAfterOrphanCleanup verifies the server is healthy after startup
// (which triggers cleanupOrphans in the executor).
func TestLambda_HealthAfterOrphanCleanup(t *testing.T) {
	requireLambdaDockerEnv(t)

	resp, err := newLambdaClient(t).ListFunctions(context.Background(), &awslambda.ListFunctionsInput{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}
