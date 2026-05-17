package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── G-PENDING-6: SQS Queue Attribute Matrix ─────────────────────────────────

// TestSQS_QueueAttr_VisibilityTimeout verifies that creating a queue with
// VisibilityTimeout=45 returns 45 from GetQueueAttributes.
func TestSQS_QueueAttr_VisibilityTimeout(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-viztimeout"),
		Attributes: map[string]string{
			"VisibilityTimeout": "45",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"VisibilityTimeout"},
	})
	require.NoError(t, err)
	assert.Equal(t, "45", attrs.Attributes["VisibilityTimeout"])
}

// TestSQS_QueueAttr_MessageRetentionPeriod verifies that MessageRetentionPeriod
// is stored and returned correctly.
func TestSQS_QueueAttr_MessageRetentionPeriod(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-retention"),
		Attributes: map[string]string{
			"MessageRetentionPeriod": "86400",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"MessageRetentionPeriod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "86400", attrs.Attributes["MessageRetentionPeriod"])
}

// TestSQS_QueueAttr_DelaySeconds verifies that DelaySeconds is stored and
// returned correctly.
func TestSQS_QueueAttr_DelaySeconds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-delay"),
		Attributes: map[string]string{
			"DelaySeconds": "10",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"DelaySeconds"},
	})
	require.NoError(t, err)
	assert.Equal(t, "10", attrs.Attributes["DelaySeconds"])
}

// TestSQS_QueueAttr_MaximumMessageSize verifies that MaximumMessageSize is stored
// and returned correctly.
func TestSQS_QueueAttr_MaximumMessageSize(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-maxmsgsize"),
		Attributes: map[string]string{
			"MaximumMessageSize": "65536",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"MaximumMessageSize"},
	})
	require.NoError(t, err)
	assert.Equal(t, "65536", attrs.Attributes["MaximumMessageSize"])
}

// TestSQS_QueueAttr_ReceiveMessageWaitTimeSeconds verifies that
// ReceiveMessageWaitTimeSeconds is stored and returned for long-polling queues.
func TestSQS_QueueAttr_ReceiveMessageWaitTimeSeconds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-longpoll"),
		Attributes: map[string]string{
			"ReceiveMessageWaitTimeSeconds": "5",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"ReceiveMessageWaitTimeSeconds"},
	})
	require.NoError(t, err)
	assert.Equal(t, "5", attrs.Attributes["ReceiveMessageWaitTimeSeconds"])
}

// TestSQS_QueueAttr_SetAttributes verifies that SetQueueAttributes updates
// VisibilityTimeout and the new value is reflected in GetQueueAttributes.
func TestSQS_QueueAttr_SetAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-setattr"),
		Attributes: map[string]string{
			"VisibilityTimeout": "30",
		},
	})
	require.NoError(t, err)

	// Change VisibilityTimeout to 60.
	_, err = c.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: out.QueueUrl,
		Attributes: map[string]string{
			"VisibilityTimeout": "60",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"VisibilityTimeout"},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", attrs.Attributes["VisibilityTimeout"], "VisibilityTimeout should reflect the new value")
}

// TestSQS_QueueAttr_RedrivePolicy verifies that a queue's RedrivePolicy attribute
// contains the DLQ ARN after creation.
func TestSQS_QueueAttr_RedrivePolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	// Create dead-letter queue first.
	dlqOut, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-dlq"),
	})
	require.NoError(t, err)

	// Retrieve the DLQ ARN.
	dlqAttrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	dlqArn := dlqAttrs.Attributes["QueueArn"]
	require.NotEmpty(t, dlqArn, "DLQ ARN must not be empty")

	// Build the RedrivePolicy JSON.
	redrivePolicy, err := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqArn,
		"maxReceiveCount":     3,
	})
	require.NoError(t, err)

	// Create source queue with RedrivePolicy.
	srcOut, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-source"),
		Attributes: map[string]string{
			"RedrivePolicy": string(redrivePolicy),
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       srcOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"RedrivePolicy"},
	})
	require.NoError(t, err)
	policy := attrs.Attributes["RedrivePolicy"]
	assert.NotEmpty(t, policy, "RedrivePolicy must be returned")
	assert.Contains(t, policy, dlqArn, "RedrivePolicy must contain the DLQ ARN")
}

// TestSQS_QueueAttr_ApproximateNumberOfMessages verifies that sending 3 messages
// causes ApproximateNumberOfMessages to be >= 3.
func TestSQS_QueueAttr_ApproximateNumberOfMessages(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-approxcount"),
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = c.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl:    out.QueueUrl,
			MessageBody: aws.String("test-message"),
		})
		require.NoError(t, err)
	}

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessages"},
	})
	require.NoError(t, err)
	count := attrs.Attributes["ApproximateNumberOfMessages"]
	assert.Equal(t, "3", count, "expected ApproximateNumberOfMessages to be at least 3")
}

// TestSQS_FIFO_ContentBasedDeduplication verifies that a FIFO queue created with
// ContentBasedDeduplication=true reports that attribute correctly.
func TestSQS_FIFO_ContentBasedDeduplication(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-fifo-cbd.fifo"),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{
			"FifoQueue",
			"ContentBasedDeduplication",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "true", attrs.Attributes["FifoQueue"])
	assert.Equal(t, "true", attrs.Attributes["ContentBasedDeduplication"])
}

// TestSQS_QueueAttr_QueueArn verifies that GetQueueAttributes with QueueArn
// returns an ARN in the expected "arn:aws:sqs:..." format.
func TestSQS_QueueAttr_QueueArn(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-arn-check"),
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	arn := attrs.Attributes["QueueArn"]
	require.NotEmpty(t, arn, "QueueArn must not be empty")
	assert.True(t, strings.HasPrefix(arn, "arn:aws:sqs:"),
		"QueueArn must start with 'arn:aws:sqs:', got %s", arn)
	assert.Contains(t, arn, "attr-arn-check", "QueueArn must contain the queue name")
}

// TestSQS_QueueAttr_MultipleAttributes verifies that creating a queue with several
// attributes simultaneously returns all of them correctly.
func TestSQS_QueueAttr_MultipleAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-multi"),
		Attributes: map[string]string{
			"VisibilityTimeout":             "20",
			"MessageRetentionPeriod":        "3600",
			"DelaySeconds":                  "5",
			"MaximumMessageSize":            "131072",
			"ReceiveMessageWaitTimeSeconds": "10",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"All"},
	})
	require.NoError(t, err)
	assert.Equal(t, "20", attrs.Attributes["VisibilityTimeout"])
	assert.Equal(t, "3600", attrs.Attributes["MessageRetentionPeriod"])
	assert.Equal(t, "5", attrs.Attributes["DelaySeconds"])
	assert.Equal(t, "131072", attrs.Attributes["MaximumMessageSize"])
	assert.Equal(t, "10", attrs.Attributes["ReceiveMessageWaitTimeSeconds"])
}

// TestSQS_QueueAttr_SetAttributes_MultipleFields verifies that SetQueueAttributes
// can update multiple attributes at once.
func TestSQS_QueueAttr_SetAttributes_MultipleFields(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSQSClient(t)

	out, err := c.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attr-setmulti"),
	})
	require.NoError(t, err)

	_, err = c.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: out.QueueUrl,
		Attributes: map[string]string{
			"VisibilityTimeout":      "90",
			"MessageRetentionPeriod": "604800",
		},
	})
	require.NoError(t, err)

	attrs, err := c.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"VisibilityTimeout", "MessageRetentionPeriod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "90", attrs.Attributes["VisibilityTimeout"])
	assert.Equal(t, "604800", attrs.Attributes["MessageRetentionPeriod"])
}
