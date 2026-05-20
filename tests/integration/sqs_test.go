package integration_test

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain starts the JaisCloud server before all integration tests.
func TestMain(m *testing.M) {
	// Server is expected to be running on :4566.
	// CI/local: run `go run ./cmd/jaiscloud/ start` in background before running tests,
	// or use the helper below to auto-start if not already up.
	os.Exit(m.Run())
}

// ─── 1. CreateQueue ───────────────────────────────────────────────────────────

func TestSQS_CreateQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("intg-sqs-create"),
	})
	require.NoError(t, err)
	assert.Contains(t, *out.QueueUrl, "intg-sqs-create")
}

// ─── 2. DeleteQueue ───────────────────────────────────────────────────────────

func TestSQS_DeleteQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-delete")})
	require.NoError(t, err)

	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
	require.NoError(t, err)

	// GetQueueUrl should fail after deletion
	_, err = client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("intg-sqs-delete")})
	require.Error(t, err)
}

// ─── 3. ListQueues ────────────────────────────────────────────────────────────

func TestSQS_ListQueues(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-list-alpha")})
	client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-list-beta")})

	resp, err := client.ListQueues(ctx, &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String("intg-sqs-list-"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(resp.QueueUrls), 2)

	var names []string
	for _, u := range resp.QueueUrls {
		names = append(names, u)
	}
	assert.True(t, containsSubstr(names, "intg-sqs-list-alpha"))
	assert.True(t, containsSubstr(names, "intg-sqs-list-beta"))
}

// ─── 4. GetQueueUrl ───────────────────────────────────────────────────────────

func TestSQS_GetQueueUrl(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-geturl")})
	resp, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("intg-sqs-geturl")})
	require.NoError(t, err)
	assert.Contains(t, *resp.QueueUrl, "intg-sqs-geturl")
}

// ─── 5. QueueUrl contains expected host ───────────────────────────────────────

func TestSQS_QueueUrlFormat(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-urlhost")})
	require.NoError(t, err)
	assert.Contains(t, *out.QueueUrl, host())
	assert.Contains(t, *out.QueueUrl, "intg-sqs-urlhost")
}

// ─── 6. Send / Receive / Delete lifecycle ─────────────────────────────────────

func TestSQS_SendReceiveDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-srd")})
	require.NoError(t, err)
	queueURL := out.QueueUrl

	sendOut, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("test-body"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *sendOut.MessageId)

	recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "test-body", *recvOut.Messages[0].Body)

	_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: recvOut.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, empty.Messages)
}

// ─── 7. Message attributes ────────────────────────────────────────────────────

func TestSQS_MessageAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-attrs")})

	client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("with-attrs"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
			"count": {DataType: aws.String("Number"), StringValue: aws.String("42")},
		},
	})

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              out.QueueUrl,
		MaxNumberOfMessages:   1,
		MessageAttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)
	attrs := recv.Messages[0].MessageAttributes
	assert.Equal(t, "blue", *attrs["color"].StringValue)
	assert.Equal(t, "42", *attrs["count"].StringValue)
}

// ─── 8. Batch send ────────────────────────────────────────────────────────────

func TestSQS_BatchSend(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-batchsend")})

	resp, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("m1"), MessageBody: aws.String("batch-1")},
			{Id: aws.String("m2"), MessageBody: aws.String("batch-2")},
			{Id: aws.String("m3"), MessageBody: aws.String("batch-3")},
		},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Successful, 3)
	assert.Empty(t, resp.Failed)
}

// ─── 9. Batch delete ──────────────────────────────────────────────────────────

func TestSQS_BatchDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-batchdel")})
	for i := 0; i < 3; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    out.QueueUrl,
			MessageBody: aws.String(fmt.Sprintf("del-%d", i)),
		})
	}

	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 10,
	})
	entries := make([]types.DeleteMessageBatchRequestEntry, len(recv.Messages))
	for i, m := range recv.Messages {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("%d", i)),
			ReceiptHandle: m.ReceiptHandle,
		}
	}

	delResp, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries:  entries,
	})
	require.NoError(t, err)
	assert.Len(t, delResp.Successful, len(entries))
}

// ─── 10. Purge queue ──────────────────────────────────────────────────────────

func TestSQS_PurgeQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-purge")})
	for i := 0; i < 5; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String(fmt.Sprintf("purge-%d", i))})
	}

	_, err := client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: out.QueueUrl})
	require.NoError(t, err)

	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 10,
	})
	assert.Empty(t, recv.Messages)
}

// ─── 11. Visibility timeout ───────────────────────────────────────────────────

func TestSQS_VisibilityTimeout(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-vis")})
	client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String("vis-test")})

	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.Len(t, recv.Messages, 1)
	rh := recv.Messages[0].ReceiptHandle

	// Set visibility to 0 → immediately visible
	_, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          out.QueueUrl,
		ReceiptHandle:     rh,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)

	recv2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv2.Messages, 1)
	assert.Equal(t, "vis-test", *recv2.Messages[0].Body)
}

// ─── 12. Change visibility batch ──────────────────────────────────────────────

func TestSQS_ChangeVisibilityBatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-visbatch")})
	for i := 0; i < 2; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String(fmt.Sprintf("vb-%d", i))})
	}

	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 10,
	})
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(recv.Messages))
	for i, m := range recv.Messages {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(fmt.Sprintf("%d", i)),
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: 0,
		}
	}

	batchResp, err := client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: out.QueueUrl,
		Entries:  entries,
	})
	require.NoError(t, err)
	assert.Len(t, batchResp.Successful, len(entries))

	recv2, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 10,
	})
	assert.Len(t, recv2.Messages, 2)
}

// ─── 13. Queue attributes get/set ─────────────────────────────────────────────

func TestSQS_QueueAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-qattr")})

	_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   out.QueueUrl,
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.NoError(t, err)

	resp, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []types.QueueAttributeName{"VisibilityTimeout"},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", resp.Attributes["VisibilityTimeout"])
}

// ─── 14. Queue tags ───────────────────────────────────────────────────────────

func TestSQS_QueueTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-tags")})

	client.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: out.QueueUrl,
		Tags:     map[string]string{"env": "test", "team": "backend"},
	})

	tagResp, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: out.QueueUrl})
	require.NoError(t, err)
	assert.Equal(t, "test", tagResp.Tags["env"])
	assert.Equal(t, "backend", tagResp.Tags["team"])

	client.UntagQueue(ctx, &sqs.UntagQueueInput{
		QueueUrl: out.QueueUrl,
		TagKeys:  []string{"team"},
	})

	tagResp2, _ := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: out.QueueUrl})
	assert.NotContains(t, tagResp2.Tags, "team")
	assert.Equal(t, "test", tagResp2.Tags["env"])
}

// ─── 15. FIFO queue ordering ──────────────────────────────────────────────────

func TestSQS_FIFOQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("intg-sqs-fifo.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:       out.QueueUrl,
			MessageBody:    aws.String(fmt.Sprintf("fifo-msg-%d", i)),
			MessageGroupId: aws.String("group-1"),
		})
	}

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(recv.Messages), 1)
	assert.Equal(t, "fifo-msg-0", *recv.Messages[0].Body)
}

// ─── 16. FIFO deduplication ───────────────────────────────────────────────────

func TestSQS_FIFODeduplication(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("intg-sqs-dedup.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})

	r1, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               out.QueueUrl,
		MessageBody:            aws.String("dedup-body"),
		MessageGroupId:         aws.String("g1"),
		MessageDeduplicationId: aws.String("dedup-001"),
	})
	require.NoError(t, err)

	r2, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               out.QueueUrl,
		MessageBody:            aws.String("dedup-body"),
		MessageGroupId:         aws.String("g1"),
		MessageDeduplicationId: aws.String("dedup-001"),
	})
	require.NoError(t, err)

	assert.Equal(t, *r1.MessageId, *r2.MessageId)
}

// ─── 17. Dead letter queue ────────────────────────────────────────────────────

func TestSQS_DeadLetterQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	dlqOut, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-dlq-target")})
	dlqAttr, _ := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []types.QueueAttributeName{"QueueArn"},
	})
	dlqArn := dlqAttr.Attributes["QueueArn"]

	redrivePolicy, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqArn,
		"maxReceiveCount":     "2",
	})
	srcOut, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("intg-sqs-dlq-source"),
		Attributes: map[string]string{"RedrivePolicy": string(redrivePolicy)},
	})

	client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: srcOut.QueueUrl, MessageBody: aws.String("dlq-test"),
	})

	// Receive twice to exceed maxReceiveCount=2; set vis=0 to re-expose each time
	for i := 0; i < 2; i++ {
		recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: srcOut.QueueUrl, MaxNumberOfMessages: 1,
		})
		require.NoError(t, err)
		if len(recv.Messages) == 0 {
			continue
		}
		rh := recv.Messages[0].ReceiptHandle
		client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          srcOut.QueueUrl,
			ReceiptHandle:     rh,
			VisibilityTimeout: 0,
		})
	}

	// Source queue should be empty
	empty, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: srcOut.QueueUrl, MaxNumberOfMessages: 1,
	})
	assert.Empty(t, empty.Messages)

	// DLQ should have the message
	dlqRecv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: dlqOut.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, dlqRecv.Messages, 1)
	assert.Equal(t, "dlq-test", *dlqRecv.Messages[0].Body)
}

// ─── 18. Delay seconds ────────────────────────────────────────────────────────

func TestSQS_DelaySeconds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-delay")})
	client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     out.QueueUrl,
		MessageBody:  aws.String("delayed"),
		DelaySeconds: 2,
	})

	// Not visible yet
	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	assert.Empty(t, recv.Messages)

	// Visible after delay
	time.Sleep(2500 * time.Millisecond)
	recv2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv2.Messages, 1)
	assert.Equal(t, "delayed", *recv2.Messages[0].Body)
}

// ─── 19. System attributes ────────────────────────────────────────────────────

func TestSQS_SystemAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-sysattr")})
	client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String("sysattr-test")})

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
		AttributeNames:      []types.QueueAttributeName{"ApproximateReceiveCount"},
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)
	assert.Equal(t, "1", recv.Messages[0].Attributes["ApproximateReceiveCount"])

	rh := recv.Messages[0].ReceiptHandle
	client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          out.QueueUrl,
		ReceiptHandle:     rh,
		VisibilityTimeout: 0,
	})

	recv2, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
		AttributeNames:      []types.QueueAttributeName{"ApproximateReceiveCount"},
	})
	require.Len(t, recv2.Messages, 1)
	assert.Equal(t, "2", recv2.Messages[0].Attributes["ApproximateReceiveCount"])
}

// ─── 20. Non-existent queue ───────────────────────────────────────────────────

func TestSQS_NonexistentQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	_, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("intg-sqs-does-not-exist")})
	require.Error(t, err)
}

// ─── 21. Receive empty queue ──────────────────────────────────────────────────

func TestSQS_ReceiveEmpty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-empty")})
	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages)
}

// ─── 22. BatchDelete with invalid receipt handle ──────────────────────────────

func TestSQS_BatchDeleteInvalidReceipt(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-sqs-batchdel-invalid")})
	client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String("msg")})

	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	validRH := recv.Messages[0].ReceiptHandle

	resp, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("good"), ReceiptHandle: validRH},
			{Id: aws.String("bad"), ReceiptHandle: aws.String("INVALID-HANDLE-XYZ")},
		},
	})
	require.NoError(t, err)

	successIDs := deleteIDs(resp.Successful)
	failedIDs := failIDs(resp.Failed)
	assert.Contains(t, successIDs, "good")
	assert.Contains(t, failedIDs, "bad")
	assert.Equal(t, "ReceiptHandleIsInvalid", *resp.Failed[0].Code)
}

// ─── 23. Receive max 10 ───────────────────────────────────────────────────────

func TestSQS_ReceiveMax10(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("qa-sqs-max10")})
	for i := 0; i < 15; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: out.QueueUrl, MessageBody: aws.String(fmt.Sprintf("msg%d", i)),
		})
	}

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 15,
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(recv.Messages), 10)
}

// ─── 24. Visibility timeout = 0 ───────────────────────────────────────────────

func TestSQS_VisibilityTimeoutZero(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("qa-sqs-vis0")})
	client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String("vis-test")})

	recv, _ := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
	})
	require.Len(t, recv.Messages, 1)
	rh := recv.Messages[0].ReceiptHandle

	client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: out.QueueUrl, ReceiptHandle: rh, VisibilityTimeout: 0,
	})

	recv2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Len(t, recv2.Messages, 1)
}

// ─── 25. BatchDelete partial failure ──────────────────────────────────────────

func TestSQS_BatchDeletePartialFailure(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("qa-sqs-batchdel-fail")})

	resp, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("bad1"), ReceiptHandle: aws.String("totally-invalid-handle")},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Failed, 1)
	assert.Equal(t, "bad1", *resp.Failed[0].Id)
	assert.Empty(t, resp.Successful)
}

// ─── 26. FIFO group ordering ──────────────────────────────────────────────────

func TestSQS_FIFOGroupOrdering(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("qa-sqs-fifo-order.fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"},
	})

	for i := 0; i < 3; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:       out.QueueUrl,
			MessageBody:    aws.String(fmt.Sprintf("msg%d", i)),
			MessageGroupId: aws.String("g1"),
		})
	}

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)
	assert.Equal(t, "msg0", *recv.Messages[0].Body)
}

// ─── 27. Approximate message count ───────────────────────────────────────────

func TestSQS_ApproximateMessageCount(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("qa-sqs-count")})
	for i := 0; i < 5; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: out.QueueUrl, MessageBody: aws.String(fmt.Sprintf("m%d", i)),
		})
	}

	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages"},
	})
	require.NoError(t, err)
	assert.Equal(t, "5", attrs.Attributes["ApproximateNumberOfMessages"])
}

// ─── 28. Purge empties queue ──────────────────────────────────────────────────

func TestSQS_PurgeEmptiesQueue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("qa-sqs-purge2")})
	for i := 0; i < 5; i++ {
		client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: out.QueueUrl, MessageBody: aws.String(fmt.Sprintf("m%d", i)),
		})
	}

	client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: out.QueueUrl})

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: out.QueueUrl, MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages)
}

// ─── 29. Batch send limit (>10 entries) ──────────────────────────────────────

func TestSQS_BatchSendLimit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-limit-regression")})

	entries := make([]types.SendMessageBatchRequestEntry, 11)
	for i := range entries {
		entries[i] = types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%d", i)),
			MessageBody: aws.String(fmt.Sprintf("msg %d", i)),
		}
	}

	_, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries:  entries,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TooManyEntriesInBatchRequest")

	client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
}

// ─── 30. Typed exception for queue not found ──────────────────────────────────

func TestSQS_TypedExceptionNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	_, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("queue-that-does-not-exist-typed-exc"),
	})
	require.Error(t, err)
	// The SDK should surface a typed QueueDoesNotExist (NonExistentQueue) error.
	assert.Contains(t, err.Error(), "NonExistentQueue")
}

// ─── 31. Query-compat: non-existent queue error code ──────────────────────────

func TestSQS_QueryProtocolNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	_, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("queue-compat-header-test-xyz"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistentQueue")
}

// ─── 32. Query-compat: batch limit error code ─────────────────────────────────

func TestSQS_QueryProtocolBatchLimit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, _ := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("compat-batch-limit-q")})

	entries := make([]types.SendMessageBatchRequestEntry, 11)
	for i := range entries {
		entries[i] = types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%d", i)),
			MessageBody: aws.String(fmt.Sprintf("m%d", i)),
		}
	}

	_, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries:  entries,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TooManyEntriesInBatchRequest")

	client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
}

// ─── Test 34: SendMessage MD5OfMessageAttributes ──────────────────────────────

func TestSQS_SendMessageWithAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-send-attrs")})
	require.NoError(t, err)

	// Scenario 1: SendMessage with one String attribute — response has MD5OfMessageAttributes
	sendOut, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("body-1"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("red")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, sendOut.MD5OfMessageAttributes)
	assert.Equal(t, computeExpectedMD5(map[string]struct {
		DataType    string
		StringValue string
	}{
		"color": {"String", "red"},
	}), *sendOut.MD5OfMessageAttributes)

	// Scenario 2: Multiple attributes with different DataTypes — MD5 computed correctly
	sendOut2, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("body-2"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"color":  {DataType: aws.String("String"), StringValue: aws.String("blue")},
			"count":  {DataType: aws.String("Number"), StringValue: aws.String("99")},
			"region": {DataType: aws.String("String"), StringValue: aws.String("us-east-1")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, sendOut2.MD5OfMessageAttributes)
	assert.Equal(t, computeExpectedMD5(map[string]struct {
		DataType    string
		StringValue string
	}{
		"color":  {"String", "blue"},
		"count":  {"Number", "99"},
		"region": {"String", "us-east-1"},
	}), *sendOut2.MD5OfMessageAttributes)

	// Scenario 3: SendMessage with no attributes — no MD5OfMessageAttributes in response
	sendOut3, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("body-3"),
	})
	require.NoError(t, err)
	assert.Nil(t, sendOut3.MD5OfMessageAttributes)
}

// ─── Test 35: SendMessageBatch MD5OfMessageAttributes ─────────────────────────

func TestSQS_SendMessageBatchWithAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("intg-batch-attrs")})
	require.NoError(t, err)

	resp, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: out.QueueUrl,
		Entries: []types.SendMessageBatchRequestEntry{
			// Scenario 4: entry with attributes → has MD5OfMessageAttributes
			{
				Id:          aws.String("m1"),
				MessageBody: aws.String("batch-with-attrs"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"env": {DataType: aws.String("String"), StringValue: aws.String("prod")},
				},
			},
			// Scenario 5: entry without attributes → no MD5OfMessageAttributes
			{
				Id:          aws.String("m2"),
				MessageBody: aws.String("batch-no-attrs"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Successful, 2)

	byID := map[string]types.SendMessageBatchResultEntry{}
	for _, e := range resp.Successful {
		byID[*e.Id] = e
	}

	require.Contains(t, byID, "m1")
	require.NotNil(t, byID["m1"].MD5OfMessageAttributes)
	assert.Equal(t, computeExpectedMD5(map[string]struct {
		DataType    string
		StringValue string
	}{
		"env": {"String", "prod"},
	}), *byID["m1"].MD5OfMessageAttributes)

	require.Contains(t, byID, "m2")
	assert.Nil(t, byID["m2"].MD5OfMessageAttributes)

	// Scenario 6: attributes stored and returned on ReceiveMessage
	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              out.QueueUrl,
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{"All"},
	})
	require.NoError(t, err)

	attrMsgs := 0
	for _, m := range recv.Messages {
		if _, ok := m.MessageAttributes["env"]; ok {
			attrMsgs++
			assert.Equal(t, "prod", aws.ToString(m.MessageAttributes["env"].StringValue))
			require.NotNil(t, m.MD5OfMessageAttributes)
		}
	}
	assert.Equal(t, 1, attrMsgs, "exactly one message should have 'env' attribute")
}

// ─── Test 33: event_source_mapping_to_lambda — deferred to Phase 1 ───────────

// ─── P1.8: FIFO system attributes in ReceiveMessage ──────────────────────────

func TestSQS_FIFO_SystemAttributesIncludeGroupAndSeq(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sys-attrs-fifo.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       out.QueueUrl,
		MessageBody:    aws.String("hello"),
		MessageGroupId: aws.String("grp-1"),
	})
	require.NoError(t, err)

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		AttributeNames:      []types.QueueAttributeName{"All"},
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)
	assert.Equal(t, "grp-1", recv.Messages[0].Attributes["MessageGroupId"])
}

// ─── P1.9: FIFO GroupId validation ───────────────────────────────────────────

func TestSQS_FIFO_RequiresMessageGroupId(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("groupid-required.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("no group"),
		// MessageGroupId intentionally omitted
	})
	require.Error(t, err)
}

func TestSQS_FIFO_RequiresDedupIdWhenNoContentBasedDedup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("dedup-required.fifo"),
		Attributes: map[string]string{
			"FifoQueue": "true",
			// ContentBasedDeduplication deliberately not set
		},
	})
	require.NoError(t, err)

	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       out.QueueUrl,
		MessageBody:    aws.String("needs dedup"),
		MessageGroupId: aws.String("grp-1"),
		// MessageDeduplicationId intentionally omitted
	})
	require.Error(t, err)
}

func TestSQS_FIFO_AcceptsDedupId(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("with-dedup.fifo"),
		Attributes: map[string]string{"FifoQueue": "true"},
	})
	require.NoError(t, err)

	sendOut, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               out.QueueUrl,
		MessageBody:            aws.String("dedup msg"),
		MessageGroupId:         aws.String("grp-1"),
		MessageDeduplicationId: aws.String("dedup-123"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(sendOut.MessageId))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func containsSubstr(slice []string, sub string) bool {
	for _, s := range slice {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func ids(entries []types.SendMessageBatchResultEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = *e.Id
	}
	return out
}

func deleteIDs(entries []types.DeleteMessageBatchResultEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = *e.Id
	}
	return out
}

func failIDs(entries []types.BatchResultErrorEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = *e.Id
	}
	return out
}

func computeExpectedMD5(attrs map[string]struct {
	DataType    string
	StringValue string
}) string {
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names)
	h := md5.New()
	buf4 := make([]byte, 4)
	writeField := func(b []byte) {
		binary.BigEndian.PutUint32(buf4, uint32(len(b)))
		h.Write(buf4)
		h.Write(b)
	}
	for _, name := range names {
		attr := attrs[name]
		writeField([]byte(name))
		writeField([]byte(attr.DataType))
		h.Write([]byte{1})
		writeField([]byte(attr.StringValue))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// TestSQSFifoSequenceNumberSurface verifies that FIFO SendMessage returns a SequenceNumber (fix 1.1.9).
func TestSQSFifoSequenceNumberSurface(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	// Create FIFO queue.
	cq, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("seq-test.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)
	qURL := aws.ToString(cq.QueueUrl)

	out, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       aws.String(qURL),
		MessageBody:    aws.String("hello fifo"),
		MessageGroupId: aws.String("g1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.SequenceNumber, "SequenceNumber must be returned for FIFO queues")
	assert.Regexp(t, `^\d+$`, aws.ToString(out.SequenceNumber), "SequenceNumber must be numeric")
}

// TestSQSBinaryAttributeRoundtrip verifies that Binary message attributes round-trip (fix 1.1.9).
func TestSQSBinaryAttributeRoundtrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	cq, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("binary-attr-q"),
	})
	require.NoError(t, err)
	qURL := aws.ToString(cq.QueueUrl)

	payload := []byte{0x00, 0xDE, 0xAD, 0xFF}
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String("binary-test"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"data": {
				DataType:    aws.String("Binary"),
				BinaryValue: payload,
			},
		},
	})
	require.NoError(t, err)

	recv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(qURL),
		MaxNumberOfMessages:   1,
		MessageAttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)
	attr, ok := recv.Messages[0].MessageAttributes["data"]
	require.True(t, ok, "data attribute must be present")
	assert.Equal(t, payload, attr.BinaryValue, "binary payload must round-trip unchanged")
}
