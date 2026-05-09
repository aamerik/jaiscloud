package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P5.7: SQS Batch Partial Failure Validation ───────────────────────────────

func TestSQS_SendMessageBatch_TooManyEntries(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-too-many")})
	require.NoError(t, err)
	qURL := "http://" + host() + ":4566/000000000000/batch-too-many"

	entries := make([]sqstypes.SendMessageBatchRequestEntry, 11)
	for i := range entries {
		id := aws.String(string(rune('a' + i)))
		entries[i] = sqstypes.SendMessageBatchRequestEntry{
			Id:          id,
			MessageBody: aws.String("body"),
		}
	}

	_, err = c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(qURL),
		Entries:  entries,
	})
	require.Error(t, err, "11 entries in a batch must return TooManyEntriesInBatchRequest")
}

func TestSQS_SendMessageBatch_DuplicateIdsFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-dup-ids")})
	require.NoError(t, err)
	qURL := "http://" + host() + ":4566/000000000000/batch-dup-ids"

	_, err = c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(qURL),
		Entries: []sqstypes.SendMessageBatchRequestEntry{
			{Id: aws.String("same-id"), MessageBody: aws.String("first")},
			{Id: aws.String("same-id"), MessageBody: aws.String("second")},
		},
	})
	require.Error(t, err, "duplicate entry IDs must return BatchEntryIdsNotDistinct")
}

func TestSQS_DeleteMessageBatch_InvalidIdReturnsPerEntryFailure(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-del-id")})
	require.NoError(t, err)
	qURL := "http://" + host() + ":4566/000000000000/batch-del-id"

	out, err := c.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(qURL),
		Entries: []sqstypes.DeleteMessageBatchRequestEntry{
			{Id: aws.String("valid-id"), ReceiptHandle: aws.String("rh1")},
			{Id: aws.String("bad id!"), ReceiptHandle: aws.String("rh2")}, // invalid format
		},
	})
	// The whole request should succeed (not a 400)
	require.NoError(t, err, "invalid entry ID should be a per-entry failure, not whole-batch error")
	// The invalid-ID entry should appear in Failed
	assert.NotEmpty(t, out.Failed, "invalid entry ID should appear in Failed list")

	// Check that the bad entry is in Failed
	var foundBad bool
	for _, f := range out.Failed {
		if aws.ToString(f.Id) == "bad id!" {
			foundBad = true
		}
	}
	assert.True(t, foundBad, "entry 'bad id!' must appear in Failed list")
}

func TestSQS_ChangeMessageVisibilityBatch_TooManyEntries(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-vis-many")})
	require.NoError(t, err)
	qURL := "http://" + host() + ":4566/000000000000/batch-vis-many"

	entries := make([]sqstypes.ChangeMessageVisibilityBatchRequestEntry, 11)
	for i := range entries {
		entries[i] = sqstypes.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(string(rune('a' + i))),
			ReceiptHandle:     aws.String("rh"),
			VisibilityTimeout: 30,
		}
	}

	_, err = c.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(qURL),
		Entries:  entries,
	})
	require.Error(t, err, "11 entries must return TooManyEntriesInBatchRequest")
}
