package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Phase 0.10: SQS Retention ────────────────────────────────────────────────

// TestRetention_DefaultIs4Days verifies that a newly created queue reports the
// default VisibilityTimeout (30s) and MessageRetentionPeriod (4 days = 345600s).
func TestRetention_DefaultIs4Days(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("retention-default"),
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{
			"VisibilityTimeout",
			"MessageRetentionPeriod",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "30", attrs.Attributes["VisibilityTimeout"])
	assert.Equal(t, "345600", attrs.Attributes["MessageRetentionPeriod"])
}

// TestRetention_BelowMinimum60_Error verifies that creating a queue with
// MessageRetentionPeriod="59" (below the minimum of 60 seconds) returns an error.
func TestRetention_BelowMinimum60_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("retention-below-min"),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "59",
		},
	})
	require.Error(t, err)
	assertAWSError(t, err, "InvalidAttributeValue")
}

// TestRetention_AboveMax14Days_Error verifies that creating a queue with
// MessageRetentionPeriod="1209601" (above the maximum of 14 days = 1209600s)
// returns an error.
func TestRetention_AboveMax14Days_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("retention-above-max"),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "1209601",
		},
	})
	require.Error(t, err)
	assertAWSError(t, err, "InvalidAttributeValue")
}

// TestRetention_LazyExpire_OnReceive_NotReturned verifies that a message sent
// to a queue with the AWS-minimum 60s retention period is not returned after
// the retention window has elapsed.
//
// AWS rejects MessageRetentionPeriod < 60 seconds, so the smallest period we
// can exercise via the public API is 60s. The retention worker runs every 10s,
// so we wait 75s to be safe (60s retention + worst-case 10s tick + buffer).
//
// Skipped under `go test -short` to keep the fast suite under a minute.
func TestRetention_LazyExpire_OnReceive_NotReturned(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 75s retention-expiry test under -short")
	}
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("retention-expire"),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "60", // AWS minimum
		},
	})
	require.NoError(t, err)

	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("will-expire"),
	})
	require.NoError(t, err)

	// Wait past the 60s retention window plus a worker tick.
	time.Sleep(75 * time.Second)

	recv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages, "expired message must not be returned")
}

// TestRetention_MultipleQueues_Independent verifies that two queues created with
// different MessageRetentionPeriod values each report their own independent value.
func TestRetention_MultipleQueues_Independent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	outA, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("retention-indep-a"),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "60",
		},
	})
	require.NoError(t, err)

	outB, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("retention-indep-b"),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "1209600",
		},
	})
	require.NoError(t, err)

	attrsA, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       outA.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"MessageRetentionPeriod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", attrsA.Attributes["MessageRetentionPeriod"])

	attrsB, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       outB.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"MessageRetentionPeriod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "1209600", attrsB.Attributes["MessageRetentionPeriod"])
}

// ─── Phase 1.8-1.13: SQS Gaps ─────────────────────────────────────────────────

// TestCreateQueue_SameAttrs_ReturnsExisting verifies that creating a queue with
// the same name and identical attributes is idempotent and returns the same URL.
func TestCreateQueue_SameAttrs_ReturnsExisting(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	attrs := map[string]string{"VisibilityTimeout": "45"}

	out1, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("idempotent-queue"),
		Attributes: attrs,
	})
	require.NoError(t, err)

	out2, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("idempotent-queue"),
		Attributes: attrs,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(out1.QueueUrl), aws.ToString(out2.QueueUrl))
}

// TestCreateQueue_DifferentAttrs_QueueNameExists verifies that creating a queue
// with the same name but a conflicting attribute value returns QueueAlreadyExists.
func TestCreateQueue_DifferentAttrs_QueueNameExists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("conflict-queue"),
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	_, err = c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("conflict-queue"),
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.Error(t, err)
	assertAWSError(t, err, "QueueAlreadyExists")
}

// TestSendMessage_DelaySeconds_Above900_Error verifies that SendMessage with
// DelaySeconds=901 is rejected with an error (AWS maximum is 900s).
func TestSendMessage_DelaySeconds_Above900_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("delay-limit-queue"),
	})
	require.NoError(t, err)

	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     out.QueueUrl,
		MessageBody:  aws.String("too-long-delay"),
		DelaySeconds: 901,
	})
	require.Error(t, err, "DelaySeconds=901 must be rejected")
}

// TestSendMessage_OverMaxSize_Rejected verifies that a message body exceeding
// the 256 KB maximum message size is rejected.
func TestSendMessage_OverMaxSize_Rejected(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("oversize-queue"),
	})
	require.NoError(t, err)

	// 256 KB + 1 byte exceeds the default MaximumMessageSize of 262144 bytes.
	bigBody := strings.Repeat("x", 262145)

	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String(bigBody),
	})
	require.Error(t, err, "message body over 256 KB must be rejected")
	assertAWSError(t, err, "InvalidParameterValue")
}

// TestCreateQueue_NameTooLong_Error verifies that a queue name longer than 80
// characters is rejected.
func TestCreateQueue_NameTooLong_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	// 81-character name.
	longName := strings.Repeat("a", 81)
	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(longName),
	})
	require.Error(t, err, "queue name longer than 80 chars must be rejected")
	assertAWSError(t, err, "InvalidParameterValue")
}

// TestCreateQueue_NameInvalidChars_Error verifies that a queue name containing
// invalid characters (e.g. "!") is rejected.
func TestCreateQueue_NameInvalidChars_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("invalid!queue"),
	})
	require.Error(t, err, "queue name with invalid characters must be rejected")
	assertAWSError(t, err, "InvalidParameterValue")
}

// TestCreateQueue_FifoNameWithoutSuffix_Error verifies that setting
// FifoQueue=true on a queue whose name does not end in ".fifo" is rejected.
func TestCreateQueue_FifoNameWithoutSuffix_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("fifo-no-suffix"),
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	})
	require.Error(t, err, "FifoQueue=true with non-.fifo name must be rejected")
}

// TestListQueues_PrefixFilter verifies that ListQueues with a QueueNamePrefix
// returns only the queues whose names start with that prefix.
func TestListQueues_PrefixFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	// Create three queues: two with prefix "alpha-" and one with prefix "beta-".
	_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("alpha-one")})
	require.NoError(t, err)
	_, err = c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("alpha-two")})
	require.NoError(t, err)
	_, err = c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("beta-one")})
	require.NoError(t, err)

	listOut, err := c.ListQueues(ctx, &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String("alpha-"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.QueueUrls, 2)

	for _, u := range listOut.QueueUrls {
		assert.True(t, strings.Contains(u, "alpha-"),
			"returned queue URL %q must contain 'alpha-'", u)
		assert.False(t, strings.Contains(u, "beta-"),
			"returned queue URL %q must not contain 'beta-'", u)
	}
}

// TestListQueues_MaxResults_LessThanTotal verifies that listing queues with
// MaxResults=2 when 5 queues exist returns exactly 2 URLs and a NextToken.
func TestListQueues_MaxResults_LessThanTotal(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	for i := 0; i < 5; i++ {
		_, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(strings.Repeat("x", 1) + strings.Repeat("0", i) + "maxres"),
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListQueues(ctx, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.QueueUrls, 2)
	assert.NotNil(t, listOut.NextToken, "NextToken must be set when total exceeds MaxResults")
	assert.NotEmpty(t, aws.ToString(listOut.NextToken))
}

// TestSendMessage_WithAttributes_CorrectSize verifies that sending a message
// with MessageAttributes within the size limit succeeds.
func TestSendMessage_WithAttributes_CorrectSize(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("send-with-attrs"),
	})
	require.NoError(t, err)

	sendOut, err := c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("body-with-attributes"),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"env": {
				DataType:    aws.String("String"),
				StringValue: aws.String("staging"),
			},
			"version": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("42"),
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(sendOut.MessageId))
	assert.NotNil(t, sendOut.MD5OfMessageAttributes)
}

// TestReceiveMessage_AttributeNames_All verifies that receiving a message with
// AttributeNames=["All"] returns system attributes on the message.
func TestReceiveMessage_AttributeNames_All(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("recv-attrnames-all"),
	})
	require.NoError(t, err)

	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("test-sys-attrs"),
	})
	require.NoError(t, err)

	recv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
		AttributeNames:      []sqstypes.QueueAttributeName{"All"},
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)

	msg := recv.Messages[0]
	assert.NotEmpty(t, msg.Attributes, "system attributes must be present when AttributeNames=[All]")
	assert.Contains(t, msg.Attributes, "ApproximateReceiveCount")
	assert.Contains(t, msg.Attributes, "SentTimestamp")
}

// TestGetQueueAttributes_All verifies that GetQueueAttributes with
// AttributeNames=["All"] returns all expected queue attributes.
func TestGetQueueAttributes_All(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("getattr-all"),
		Attributes: map[string]string{
			"VisibilityTimeout":      "45",
			"MessageRetentionPeriod": "86400",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"All"},
	})
	require.NoError(t, err)

	expectedKeys := []string{
		"QueueArn",
		"VisibilityTimeout",
		"MessageRetentionPeriod",
		"MaximumMessageSize",
		"DelaySeconds",
		"ReceiveMessageWaitTimeSeconds",
		"ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible",
		"CreatedTimestamp",
		"LastModifiedTimestamp",
	}
	for _, k := range expectedKeys {
		assert.Contains(t, attrs.Attributes, k, "attribute %q must be present in All response", k)
	}
	assert.Equal(t, "45", attrs.Attributes["VisibilityTimeout"])
	assert.Equal(t, "86400", attrs.Attributes["MessageRetentionPeriod"])
}

// TestPurgeQueue_RemovesMessages verifies that PurgeQueue removes all messages
// from the queue so a subsequent ReceiveMessage returns empty.
func TestPurgeQueue_RemovesMessages(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("purge-coverage"),
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    out.QueueUrl,
			MessageBody: aws.String("msg"),
		})
		require.NoError(t, err)
	}

	_, err = c.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: out.QueueUrl})
	require.NoError(t, err)

	recv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages, "queue must be empty after purge")
}

// TestChangeMessageVisibility_ExtendTimeout verifies that changing visibility
// timeout to 0 on an in-flight message makes it immediately available again.
func TestChangeMessageVisibility_ExtendTimeout(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("changeviz-coverage"),
	})
	require.NoError(t, err)

	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("changeviz-body"),
	})
	require.NoError(t, err)

	// Receive with a 30-second visibility timeout → message goes in-flight.
	recv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)
	rh := recv.Messages[0].ReceiptHandle

	// Immediately reduce visibility to 0 → message becomes visible right away.
	_, err = c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          out.QueueUrl,
		ReceiptHandle:     rh,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)

	// The message must now be receivable again.
	recv2, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv2.Messages, 1, "message must be visible again after ChangeMessageVisibility(0)")
	assert.Equal(t, "changeviz-body", aws.ToString(recv2.Messages[0].Body))
}

// TestDeleteMessage_MakesAvailable verifies that deleting a message removes it
// from the queue permanently — a subsequent receive returns no messages.
func TestDeleteMessage_MakesAvailable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("delete-coverage"),
	})
	require.NoError(t, err)

	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    out.QueueUrl,
		MessageBody: aws.String("to-be-deleted"),
	})
	require.NoError(t, err)

	recv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)

	_, err = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      out.QueueUrl,
		ReceiptHandle: recv.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	// Make the in-flight slot visible by setting visibility to 0, then confirm
	// the message is truly gone (not just in-flight).
	_, _ = c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          out.QueueUrl,
		ReceiptHandle:     recv.Messages[0].ReceiptHandle,
		VisibilityTimeout: 0,
	})

	recv2, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            out.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv2.Messages, "deleted message must not be returned on subsequent receive")
}
