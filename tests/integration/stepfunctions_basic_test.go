// Package integration provides basic Step Functions (SFN) round-trip integration tests.
// The full SFN test suite is in stepfunctions_test.go. This file adds the
// specific tests referenced in G-PENDING-9 for completeness.
package integration

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSFN_CreateStateMachine_Basic(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	out, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String("basic-sm"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.StateMachineArn), "arn:aws:states:")
	assert.Contains(t, aws.ToString(out.StateMachineArn), "basic-sm")
}

func TestSFN_StartAndDescribeExecution_Basic(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"key":"value"}`),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(startOut.ExecutionArn), "execution:")

	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	// In memory mode the execution completes immediately; status must be terminal or RUNNING.
	validStatuses := []sfntypes.ExecutionStatus{
		sfntypes.ExecutionStatusRunning,
		sfntypes.ExecutionStatusSucceeded,
	}
	assert.Contains(t, validStatuses, descOut.Status)
}

func TestSFN_ListStateMachines_AtLeastTwo(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		_, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
			Name:       aws.String(sfnName(t)),
			Definition: aws.String(testDefinition),
			RoleArn:    aws.String(testRoleARN),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.StateMachines), 2)
}

func TestSFN_GetExecutionHistory_AtLeastOneEvent(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	histOut, err := client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(histOut.Events), 1)
}
