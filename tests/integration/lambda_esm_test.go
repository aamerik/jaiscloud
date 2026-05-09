package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// createTestFunction creates a minimal Lambda function for ESM tests.
// It uses NodeJS 18.x runtime and a fake zip payload to keep setup lightweight.
func createTestFunction(t *testing.T, c *awslambda.Client, name string) {
	t.Helper()
	_, err := c.CreateFunction(context.Background(), &awslambda.CreateFunctionInput{
		FunctionName: aws.String(name),
		Runtime:      types.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("index.handler"),
		Code:         &types.FunctionCode{ZipFile: []byte("fake-zip")},
	})
	require.NoError(t, err, "create function %q", name)
}

// sqsARN builds a synthetic SQS queue ARN that matches JaisCloud's format.
func sqsARN(queueName string) string {
	return "arn:aws:sqs:us-east-1:000000000000:" + queueName
}

// dynamoStreamARN builds a synthetic DynamoDB Streams ARN.
func dynamoStreamARN(tableName string) string {
	return "arn:aws:dynamodb:us-east-1:000000000000:table/" + tableName + "/stream/2024-01-01T00:00:00.000"
}

// ─── TestESM_CreateAndGet ─────────────────────────────────────────────────────

// TestESM_CreateAndGet verifies that an ESM can be created against an existing
// function, and that Get returns the same UUID, FunctionArn and EventSourceArn.
func TestESM_CreateAndGet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-fn-1")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-fn-1"),
		EventSourceArn: aws.String(sqsARN("esm-queue-1")),
		BatchSize:      aws.Int32(5),
	})
	require.NoError(t, err)

	// CreateEventSourceMapping returns 202 — the SDK surfaces the body as a
	// successful response (non-nil output) even though the HTTP status is 202.
	require.NotNil(t, createOut)
	require.NotEmpty(t, aws.ToString(createOut.UUID), "UUID must be non-empty")
	assert.Equal(t, sqsARN("esm-queue-1"), aws.ToString(createOut.EventSourceArn))
	// FunctionArn should contain the function name
	assert.Contains(t, aws.ToString(createOut.FunctionArn), "esm-fn-1")

	// GetEventSourceMapping should return the same ESM.
	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(createOut.UUID), aws.ToString(getOut.UUID))
	assert.Equal(t, aws.ToString(createOut.EventSourceArn), aws.ToString(getOut.EventSourceArn))
	assert.Equal(t, aws.ToString(createOut.FunctionArn), aws.ToString(getOut.FunctionArn))
}

// ─── TestESM_CreateForNonExistentFunction ─────────────────────────────────────

// TestESM_CreateForNonExistentFunction verifies that creating an ESM pointing at
// a function that does not exist returns a ResourceNotFoundException (HTTP 404).
func TestESM_CreateForNonExistentFunction(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("no-such-function"),
		EventSourceArn: aws.String(sqsARN("some-queue")),
	})
	require.Error(t, err, "expected ResourceNotFoundException for missing function")
}

// ─── TestESM_ListESMs ─────────────────────────────────────────────────────────

// TestESM_ListESMs verifies that all created ESMs appear in ListEventSourceMappings
// and that the optional FunctionName filter narrows the results correctly.
func TestESM_ListESMs(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-list-fn-a")
	createTestFunction(t, c, "esm-list-fn-b")

	esmA, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-list-fn-a"),
		EventSourceArn: aws.String(sqsARN("esm-list-queue-a")),
	})
	require.NoError(t, err)

	esmB, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-list-fn-b"),
		EventSourceArn: aws.String(sqsARN("esm-list-queue-b")),
	})
	require.NoError(t, err)

	// List all ESMs — must include both.
	listAll, err := c.ListEventSourceMappings(ctx, &awslambda.ListEventSourceMappingsInput{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(listAll.EventSourceMappings), 2, "expected at least 2 ESMs in unfiltered list")

	uuids := make(map[string]bool, len(listAll.EventSourceMappings))
	for _, m := range listAll.EventSourceMappings {
		uuids[aws.ToString(m.UUID)] = true
	}
	assert.True(t, uuids[aws.ToString(esmA.UUID)], "ESM A must appear in unfiltered list")
	assert.True(t, uuids[aws.ToString(esmB.UUID)], "ESM B must appear in unfiltered list")

	// Filter by FunctionName — should return only the matching ESM.
	listFiltered, err := c.ListEventSourceMappings(ctx, &awslambda.ListEventSourceMappingsInput{
		FunctionName: aws.String("esm-list-fn-a"),
	})
	require.NoError(t, err)
	require.Len(t, listFiltered.EventSourceMappings, 1, "filtered list should contain exactly 1 ESM")
	assert.Equal(t, aws.ToString(esmA.UUID), aws.ToString(listFiltered.EventSourceMappings[0].UUID))
}

// ─── TestESM_UpdateESM ────────────────────────────────────────────────────────

// TestESM_UpdateESM verifies that UpdateEventSourceMapping changes BatchSize and
// that the new value is reflected when the mapping is retrieved afterwards.
func TestESM_UpdateESM(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-upd-fn")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-upd-fn"),
		EventSourceArn: aws.String(sqsARN("esm-upd-queue")),
		BatchSize:      aws.Int32(5),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	// Update BatchSize from 5 → 20.
	updOut, err := c.UpdateEventSourceMapping(ctx, &awslambda.UpdateEventSourceMappingInput{
		UUID:      createOut.UUID,
		BatchSize: aws.Int32(20),
	})
	require.NoError(t, err)
	require.NotNil(t, updOut)

	// Verify via GetEventSourceMapping.
	getOut, err := c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.BatchSize, "BatchSize must be set")
	assert.EqualValues(t, 20, aws.ToInt32(getOut.BatchSize))
}

// ─── TestESM_DeleteESM ────────────────────────────────────────────────────────

// TestESM_DeleteESM verifies that after deletion, GetEventSourceMapping returns
// an error (ResourceNotFoundException / not found).
func TestESM_DeleteESM(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-del-fn")

	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-del-fn"),
		EventSourceArn: aws.String(sqsARN("esm-del-queue")),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))

	// Delete the ESM.
	_, err = c.DeleteEventSourceMapping(ctx, &awslambda.DeleteEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)

	// Subsequent Get must return an error.
	_, err = c.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.Error(t, err, "GetEventSourceMapping after deletion should return an error")
}

// ─── TestESM_DuplicateCreateFails ────────────────────────────────────────────

// TestESM_DuplicateCreateFails verifies that creating a second ESM with the same
// function name and event source ARN returns a ResourceConflictException.
func TestESM_DuplicateCreateFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-dup-fn")

	input := &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-dup-fn"),
		EventSourceArn: aws.String(sqsARN("esm-dup-queue")),
	}

	_, err := c.CreateEventSourceMapping(ctx, input)
	require.NoError(t, err, "first create must succeed")

	_, err = c.CreateEventSourceMapping(ctx, input)
	require.Error(t, err, "second create with same function+source should return ResourceConflictException")
}

// ─── TestESM_CreateWithDynamoDBStreamARN ─────────────────────────────────────

// TestESM_CreateWithDynamoDBStreamARN verifies that a DynamoDB Streams ARN is
// accepted and the ESM is stored with the correct EventSourceArn.
func TestESM_CreateWithDynamoDBStreamARN(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "esm-dynamo-fn")

	streamArn := dynamoStreamARN("my-table")
	createOut, err := c.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String("esm-dynamo-fn"),
		EventSourceArn: aws.String(streamArn),
		BatchSize:      aws.Int32(100),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.UUID))
	assert.Equal(t, streamArn, aws.ToString(createOut.EventSourceArn))
}
