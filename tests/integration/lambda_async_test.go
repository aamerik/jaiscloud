package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalZip is a stub zip payload that satisfies the emulator's CreateFunction
// without requiring a real deployable archive.
var minimalZip = []byte("fake-zip-payload")

// createAsyncTestFunction creates a simple Lambda function for async tests.
func createAsyncTestFunction(t *testing.T, c *awslambda.Client, name string) {
	t.Helper()
	_, err := c.CreateFunction(context.Background(), &awslambda.CreateFunctionInput{
		FunctionName: aws.String(name),
		Runtime:      lambdatypes.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: minimalZip},
	})
	require.NoError(t, err, "create function %q", name)
}

// TestLambda_InvokeEvent_Async verifies that invoking a Lambda with
// InvocationType=Event returns HTTP 202 and an empty payload.
func TestLambda_InvokeEvent_Async(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "async-event-fn")

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName:   aws.String("async-event-fn"),
		InvocationType: lambdatypes.InvocationTypeEvent,
		Payload:        []byte(`{"key":"value"}`),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 202, out.StatusCode, "InvocationType=Event must return 202")
	assert.Empty(t, out.Payload, "async invocation must return empty payload")
}

// TestLambda_EventInvokeConfig_DLQ_Stored verifies that a DestinationConfig with
// OnFailure set to a DLQ ARN is stored and retrievable via GetFunctionEventInvokeConfig.
func TestLambda_EventInvokeConfig_DLQ_Stored(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "dlq-config-fn")

	dlqARN := sqsARN("lambda-dlq")

	// Store the event invoke config with OnFailure DLQ.
	_, err := c.PutFunctionEventInvokeConfig(ctx, &awslambda.PutFunctionEventInvokeConfigInput{
		FunctionName:         aws.String("dlq-config-fn"),
		MaximumRetryAttempts: aws.Int32(2),
		DestinationConfig: &lambdatypes.DestinationConfig{
			OnFailure: &lambdatypes.OnFailure{
				Destination: aws.String(dlqARN),
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetFunctionEventInvokeConfig(ctx, &awslambda.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String("dlq-config-fn"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DestinationConfig, "DestinationConfig must be returned")
	require.NotNil(t, getOut.DestinationConfig.OnFailure, "OnFailure must be set")
	assert.Equal(t, dlqARN, aws.ToString(getOut.DestinationConfig.OnFailure.Destination))
}

// TestLambda_UpdateFunctionEventInvokeConfig_DLQ verifies that updating the event
// invoke config with a new DLQ ARN replaces the old one.
func TestLambda_UpdateFunctionEventInvokeConfig_DLQ(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "upd-dlq-fn")

	firstDLQ := sqsARN("first-dlq")
	secondDLQ := sqsARN("second-dlq")

	// Set an initial DLQ.
	_, err := c.PutFunctionEventInvokeConfig(ctx, &awslambda.PutFunctionEventInvokeConfigInput{
		FunctionName:         aws.String("upd-dlq-fn"),
		MaximumRetryAttempts: aws.Int32(1),
		DestinationConfig: &lambdatypes.DestinationConfig{
			OnFailure: &lambdatypes.OnFailure{
				Destination: aws.String(firstDLQ),
			},
		},
	})
	require.NoError(t, err)

	// Update to a new DLQ.
	_, err = c.UpdateFunctionEventInvokeConfig(ctx, &awslambda.UpdateFunctionEventInvokeConfigInput{
		FunctionName:         aws.String("upd-dlq-fn"),
		MaximumRetryAttempts: aws.Int32(3),
		DestinationConfig: &lambdatypes.DestinationConfig{
			OnFailure: &lambdatypes.OnFailure{
				Destination: aws.String(secondDLQ),
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetFunctionEventInvokeConfig(ctx, &awslambda.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String("upd-dlq-fn"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DestinationConfig)
	require.NotNil(t, getOut.DestinationConfig.OnFailure)
	assert.Equal(t, secondDLQ, aws.ToString(getOut.DestinationConfig.OnFailure.Destination))
	assert.EqualValues(t, 3, aws.ToInt32(getOut.MaximumRetryAttempts))
}

// TestLambda_InvokeEvent_NoPayloadRequired verifies that invoking with
// InvocationType=Event and no payload still returns 202.
func TestLambda_InvokeEvent_NoPayloadRequired(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "async-nopayload-fn")

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName:   aws.String("async-nopayload-fn"),
		InvocationType: lambdatypes.InvocationTypeEvent,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 202, out.StatusCode, "event invocation with no payload must return 202")
}

// TestLambda_InvokeRequest_Response_Sync verifies that invoking a Lambda with
// InvocationType=RequestResponse returns HTTP 200 and a non-empty payload.
func TestLambda_InvokeRequest_Response_Sync(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "sync-rr-fn")

	payload := []byte(`{"hello":"sync"}`)
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName:   aws.String("sync-rr-fn"),
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
		Payload:        payload,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode, "RequestResponse must return 200")
	assert.NotEmpty(t, out.Payload, "sync invocation must return a payload (echo)")
}

// TestLambda_InvokeDryRun verifies that InvocationType=DryRun returns HTTP 204
// and an empty payload.
func TestLambda_InvokeDryRun(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "dryrun-fn")

	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName:   aws.String("dryrun-fn"),
		InvocationType: lambdatypes.InvocationTypeDryRun,
		Payload:        []byte(`{"test":"dry"}`),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 204, out.StatusCode, "DryRun must return 204")
	assert.Empty(t, out.Payload, "DryRun must not return a payload")
}

// TestLambda_GetFunctionConcurrency verifies the full lifecycle of reserved
// concurrency: Put → Get → Delete → Get returns no concurrency.
func TestLambda_GetFunctionConcurrency(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createAsyncTestFunction(t, c, "concurrency-fn")

	// Set reserved concurrency to 5.
	putOut, err := c.PutFunctionConcurrency(ctx, &awslambda.PutFunctionConcurrencyInput{
		FunctionName:                 aws.String("concurrency-fn"),
		ReservedConcurrentExecutions: aws.Int32(5),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 5, aws.ToInt32(putOut.ReservedConcurrentExecutions))

	// GetFunctionConcurrency must return 5.
	getOut, err := c.GetFunctionConcurrency(ctx, &awslambda.GetFunctionConcurrencyInput{
		FunctionName: aws.String("concurrency-fn"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ReservedConcurrentExecutions, "ReservedConcurrentExecutions must be non-nil")
	assert.EqualValues(t, 5, aws.ToInt32(getOut.ReservedConcurrentExecutions))

	// DeleteFunctionConcurrency removes the reserved concurrency.
	_, err = c.DeleteFunctionConcurrency(ctx, &awslambda.DeleteFunctionConcurrencyInput{
		FunctionName: aws.String("concurrency-fn"),
	})
	require.NoError(t, err)

	// After deletion, GetFunctionConcurrency must return nil/zero concurrency.
	getAfter, err := c.GetFunctionConcurrency(ctx, &awslambda.GetFunctionConcurrencyInput{
		FunctionName: aws.String("concurrency-fn"),
	})
	require.NoError(t, err)
	assert.Nil(t, getAfter.ReservedConcurrentExecutions,
		"ReservedConcurrentExecutions must be nil after deletion")
}
