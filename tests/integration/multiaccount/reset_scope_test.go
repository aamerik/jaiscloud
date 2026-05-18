package multiaccount

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResetScope_AccountAndRegion verifies that a scoped reset wipes only the
// specified (account, region) and leaves other scopes intact.
func TestResetScope_AccountAndRegion(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	// Seed A and B with queues.
	_, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-scope-test")})
	require.NoError(t, err)
	_, err = sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-scope-test")})
	require.NoError(t, err)

	// Wipe only A's (account, region).
	resetScope(t, AcctA, "us-east-1")

	// A's queue should be gone.
	listA, err := sqsA.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, listA.QueueUrls, "A's queues should be wiped after ResetScope")

	// B's queue must still exist.
	listB, err := sqsB.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	var found bool
	for _, u := range listB.QueueUrls {
		if u != "" {
			found = true
		}
	}
	assert.True(t, found, "B's queue should survive a ResetScope targeting A")
}

// TestResetScope_AccountOnly verifies that resetting by account wipes all
// resources for that account across regions, leaving other accounts intact.
func TestResetScope_AccountOnly(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	_, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-acct-only")})
	require.NoError(t, err)
	_, err = sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-acct-only")})
	require.NoError(t, err)

	resetAccount(t, AcctA)

	listA, err := sqsA.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, listA.QueueUrls, "A's queues should be wiped after ResetAccount")

	listB, err := sqsB.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	var found bool
	for _, u := range listB.QueueUrls {
		if u != "" {
			found = true
		}
	}
	assert.True(t, found, "B's queues should survive ResetAccount targeting A")
}

// TestResetScope_GlobalReset verifies that the unscoped reset wipes everything.
func TestResetScope_GlobalReset(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	_, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-global-reset")})
	require.NoError(t, err)
	_, err = sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q-global-reset")})
	require.NoError(t, err)

	resetState(t) // global reset

	listA, err := sqsA.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, listA.QueueUrls, "global reset should wipe A's queues")

	listB, err := sqsB.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, listB.QueueUrls, "global reset should wipe B's queues")
}
