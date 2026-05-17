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

// TestESM_FilterCriteria_Stored verifies that an ESM created with FilterCriteria
// returns the criteria when retrieved.
func TestESM_FilterCriteria_Stored(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-filter-fn")

	pattern := `{"body":{"type":["order"]}}`
	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-filter-fn"),
		EventSourceArn: aws.String(sqsARN("esm-filter-queue")),
		BatchSize:      aws.Int32(10),
		FilterCriteria: &lambdatypes.FilterCriteria{
			Filters: []lambdatypes.Filter{
				{Pattern: aws.String(pattern)},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.FilterCriteria, "FilterCriteria must be returned")
	require.NotEmpty(t, getOut.FilterCriteria.Filters, "Filters list must be non-empty")
	assert.Equal(t, pattern, aws.ToString(getOut.FilterCriteria.Filters[0].Pattern))
}

// TestESM_UpdateFilterCriteria verifies that updating an ESM to add FilterCriteria
// persists the new filter and it is returned on subsequent Get.
func TestESM_UpdateFilterCriteria(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-upd-filter-fn")

	// Create without FilterCriteria.
	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-upd-filter-fn"),
		EventSourceArn: aws.String(sqsARN("esm-upd-filter-queue")),
		BatchSize:      aws.Int32(5),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	// Update to add a FilterCriteria.
	newPattern := `{"body":{"status":["pending"]}}`
	_, err = c.UpdateEventSourceMapping(ctx, &awslambda.UpdateEventSourceMappingInput{
		UUID: createOut.UUID,
		FilterCriteria: &lambdatypes.FilterCriteria{
			Filters: []lambdatypes.Filter{
				{Pattern: aws.String(newPattern)},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.FilterCriteria, "FilterCriteria must be set after update")
	require.NotEmpty(t, getOut.FilterCriteria.Filters)
	assert.Equal(t, newPattern, aws.ToString(getOut.FilterCriteria.Filters[0].Pattern))
}

// TestESM_BisectOnFunctionError_Config verifies that BisectBatchOnFunctionError=true
// is persisted and returned by GetEventSourceMapping.
func TestESM_BisectOnFunctionError_Config(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-bisect-fn")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:               aws.String("esm-bisect-fn"),
		EventSourceArn:             aws.String(sqsARN("esm-bisect-queue")),
		BatchSize:                  aws.Int32(10),
		BisectBatchOnFunctionError: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))
	assert.True(t, aws.ToBool(createOut.BisectBatchOnFunctionError),
		"BisectBatchOnFunctionError must be true in create response")

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(getOut.BisectBatchOnFunctionError),
		"BisectBatchOnFunctionError must be true after Get")
}

// TestESM_MaximumRetryAttempts_Config verifies that MaximumRetryAttempts is
// persisted and returned correctly.
func TestESM_MaximumRetryAttempts_Config(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-retry-fn")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:         aws.String("esm-retry-fn"),
		EventSourceArn:       aws.String(sqsARN("esm-retry-queue")),
		BatchSize:            aws.Int32(5),
		MaximumRetryAttempts: aws.Int32(3),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.MaximumRetryAttempts, "MaximumRetryAttempts must be non-nil")
	assert.EqualValues(t, 3, aws.ToInt32(getOut.MaximumRetryAttempts))
}

// TestESM_OnFailureDestination_Config verifies that DestinationConfig.OnFailure.Destination
// (a DLQ ARN) is persisted and returned by GetEventSourceMapping.
func TestESM_OnFailureDestination_Config(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-dlq-fn")

	dlqARN := sqsARN("esm-dlq-queue")
	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-dlq-fn"),
		EventSourceArn: aws.String(sqsARN("esm-src-queue")),
		BatchSize:      aws.Int32(10),
		DestinationConfig: &lambdatypes.DestinationConfig{
			OnFailure: &lambdatypes.OnFailure{
				Destination: aws.String(dlqARN),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DestinationConfig, "DestinationConfig must be non-nil")
	require.NotNil(t, getOut.DestinationConfig.OnFailure, "OnFailure must be non-nil")
	assert.Equal(t, dlqARN, aws.ToString(getOut.DestinationConfig.OnFailure.Destination))
}

// TestESM_MaximumBatchingWindow verifies that MaximumBatchingWindowInSeconds is
// persisted and returned correctly.
func TestESM_MaximumBatchingWindow(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-window-fn")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:                   aws.String("esm-window-fn"),
		EventSourceArn:                 aws.String(sqsARN("esm-window-queue")),
		BatchSize:                      aws.Int32(10),
		MaximumBatchingWindowInSeconds: aws.Int32(30),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.MaximumBatchingWindowInSeconds, "MaximumBatchingWindowInSeconds must be set")
	assert.EqualValues(t, 30, aws.ToInt32(getOut.MaximumBatchingWindowInSeconds))
}

// TestESM_DestinationConfig_OnSuccess_OnFailure verifies that both OnSuccess and
// OnFailure destinations are persisted and returned correctly.
func TestESM_DestinationConfig_OnSuccess_OnFailure(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-dest-fn")

	successARN := sqsARN("esm-success-queue")
	failureARN := sqsARN("esm-failure-queue")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-dest-fn"),
		EventSourceArn: aws.String(sqsARN("esm-src-dest-queue")),
		BatchSize:      aws.Int32(5),
		DestinationConfig: &lambdatypes.DestinationConfig{
			OnSuccess: &lambdatypes.OnSuccess{
				Destination: aws.String(successARN),
			},
			OnFailure: &lambdatypes.OnFailure{
				Destination: aws.String(failureARN),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DestinationConfig, "DestinationConfig must be non-nil")
	require.NotNil(t, getOut.DestinationConfig.OnSuccess, "OnSuccess must be non-nil")
	require.NotNil(t, getOut.DestinationConfig.OnFailure, "OnFailure must be non-nil")
	assert.Equal(t, successARN, aws.ToString(getOut.DestinationConfig.OnSuccess.Destination))
	assert.Equal(t, failureARN, aws.ToString(getOut.DestinationConfig.OnFailure.Destination))
}

// TestESM_FilterCriteria_MultipleFilters verifies that multiple filter patterns
// are accepted. The server stores the first pattern per our implementation
// but the create call must not error.
func TestESM_FilterCriteria_MultipleFilters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-multi-filter-fn")

	pattern1 := `{"body":{"type":["order"]}}`
	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-multi-filter-fn"),
		EventSourceArn: aws.String(sqsARN("esm-multi-filter-queue")),
		BatchSize:      aws.Int32(10),
		FilterCriteria: &lambdatypes.FilterCriteria{
			Filters: []lambdatypes.Filter{
				{Pattern: aws.String(pattern1)},
				{Pattern: aws.String(`{"body":{"type":["payment"]}}`)},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	// The ESM must be retrievable.
	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.FilterCriteria, "FilterCriteria must be set")
	require.NotEmpty(t, getOut.FilterCriteria.Filters, "at least one filter must be returned")
}
