package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P3.1: SQS Long Polling ───────────────────────────────────────────────────

func TestSQS_LongPoll_ReceivesMessageSentAfterPollStarts(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("lp-queue")})
	require.NoError(t, err)
	qURL := host() + "/000000000000/lp-queue"

	var recvOut *sqs.ReceiveMessageOutput
	var recvErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvOut, recvErr = c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			WaitTimeSeconds:     5,
			MaxNumberOfMessages: 1,
		})
	}()

	// Send a message 200ms after the poll starts so the waiter is active.
	time.Sleep(200 * time.Millisecond)
	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String("hello-longpoll"),
	})
	require.NoError(t, err)

	wg.Wait()
	require.NoError(t, recvErr)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "hello-longpoll", aws.ToString(recvOut.Messages[0].Body))
}

func TestSQS_LongPoll_BatchSendWakesReceiver(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("lp-batch-queue")})
	require.NoError(t, err)
	qURL := host() + "/000000000000/lp-batch-queue"

	var recvOut *sqs.ReceiveMessageOutput
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvOut, _ = c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			WaitTimeSeconds:     5,
			MaxNumberOfMessages: 10,
		})
	}()

	time.Sleep(200 * time.Millisecond)
	_, err = c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(qURL),
		Entries: []sqstypes.SendMessageBatchRequestEntry{
			{Id: aws.String("m1"), MessageBody: aws.String("batch-msg-1")},
			{Id: aws.String("m2"), MessageBody: aws.String("batch-msg-2")},
		},
	})
	require.NoError(t, err)

	wg.Wait()
	require.NotEmpty(t, recvOut.Messages, "long-poll receiver should be woken by SendMessageBatch")
}

func TestSQS_LongPoll_InvalidWaitTimeReturnsError(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("lp-err-queue")})
	require.NoError(t, err)
	qURL := host() + "/000000000000/lp-err-queue"

	_, err = c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:        aws.String(qURL),
		WaitTimeSeconds: 21,
	})
	require.Error(t, err)
}

func TestSQS_LongPoll_ZeroWaitTimeReturnsImmediately(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("lp-zero-queue")})
	require.NoError(t, err)
	qURL := host() + "/000000000000/lp-zero-queue"

	start := time.Now()
	out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:        aws.String(qURL),
		WaitTimeSeconds: 0,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Empty(t, out.Messages)
	assert.Less(t, elapsed, 2*time.Second, "WaitTimeSeconds=0 must return immediately")
}
