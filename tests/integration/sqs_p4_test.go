package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P4.10: SQS MessageMoveTask ───────────────────────────────────────────────

func TestSQS_StartMessageMoveTask_MovesMessages(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	// Create source and destination queues
	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("mmt-src")})
	require.NoError(t, err)
	_, err = c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("mmt-dst")})
	require.NoError(t, err)

	srcURL := "http://" + host() + ":4566/000000000000/mmt-src"
	dstURL := "http://" + host() + ":4566/000000000000/mmt-dst"

	// Send messages to source
	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(srcURL),
		MessageBody: aws.String("move-me-1"),
	})
	require.NoError(t, err)
	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(srcURL),
		MessageBody: aws.String("move-me-2"),
	})
	require.NoError(t, err)

	srcArn := "arn:aws:sqs:us-east-1:000000000000:mmt-src"
	dstArn := "arn:aws:sqs:us-east-1:000000000000:mmt-dst"

	startOut, err := c.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(srcArn),
		DestinationArn: aws.String(dstArn),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(startOut.TaskHandle))

	// Allow time for the move goroutine to process both messages
	time.Sleep(500 * time.Millisecond)

	// Destination should have at least 1 message
	recvOut, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(dstURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, recvOut.Messages, "destination queue should have messages after move task")
}

func TestSQS_ListMessageMoveTasks_ReturnsRunningTask(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("list-mmt-src")})
	require.NoError(t, err)

	srcArn := "arn:aws:sqs:us-east-1:000000000000:list-mmt-src"
	_, err = c.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(srcArn),
	})
	require.NoError(t, err)

	listOut, err := c.ListMessageMoveTasks(ctx, &sqs.ListMessageMoveTasksInput{
		SourceArn: aws.String(srcArn),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Results, 1)
	assert.Equal(t, srcArn, aws.ToString(listOut.Results[0].SourceArn))
}

func TestSQS_CancelMessageMoveTask_SucceedsForRunningTask(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("cancel-mmt-src")})
	require.NoError(t, err)

	srcArn := "arn:aws:sqs:us-east-1:000000000000:cancel-mmt-src"
	startOut, err := c.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(srcArn),
	})
	require.NoError(t, err)
	handle := aws.ToString(startOut.TaskHandle)

	cancelOut, err := c.CancelMessageMoveTask(ctx, &sqs.CancelMessageMoveTaskInput{
		TaskHandle: aws.String(handle),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, cancelOut.ApproximateNumberOfMessagesMoved, int64(0))
}

func TestSQS_StartMessageMoveTask_DuplicateSourceFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("dup-mmt-src")})
	require.NoError(t, err)

	srcArn := "arn:aws:sqs:us-east-1:000000000000:dup-mmt-src"

	_, err = c.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(srcArn),
	})
	require.NoError(t, err)

	// Starting a second task for the same source while one is running must fail
	_, err = c.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(srcArn),
	})
	require.Error(t, err, "starting a second move task for the same running source must fail")
}
