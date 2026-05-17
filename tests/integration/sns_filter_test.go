package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSNSFilterPolicyNumericOperator verifies that the SNS filter policy numeric
// operator (">") is evaluated by the real pattern engine.
// price=50 must be blocked; price=200 must be delivered.
func TestSNSFilterPolicyNumericOperator(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-num-op-topic", "fp-num-op-q",
		`{"price":[{"numeric":[">",100]}]}`)

	// price=50 — below threshold, must NOT arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("cheap item"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"price": {DataType: aws.String("Number"), StringValue: aws.String("50")},
		},
	})
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "price=50 must NOT pass filter {price: [{numeric: ['>', 100]}]}")

	// price=200 — above threshold, must arrive.
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("expensive item"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"price": {DataType: aws.String("Number"), StringValue: aws.String("200")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "price=200 must pass filter {price: [{numeric: ['>', 100]}]}")
}

// TestSNSFilterPolicyStringExact verifies that the SNS filter policy string-exact
// operator is evaluated by the real pattern engine.
// color=blue must be blocked; color=red must be delivered.
func TestSNSFilterPolicyStringExact(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-str-exact-topic", "fp-str-exact-q",
		`{"color":["red"]}`)

	// color=blue — must NOT arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("blue widget"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
		},
	})
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "color=blue must NOT pass filter {color: ['red']}")

	// color=red — must arrive.
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("red widget"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("red")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "color=red must pass filter {color: ['red']}")
}

// TestSNSToSQSEnvelopeKeys verifies that every SNS→SQS notification envelope
// carries all canonical fields required by the AWS SNS specification.
func TestSNSToSQSEnvelopeKeys(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("env-keys-q")})
	require.NoError(t, err)
	qURL := aws.ToString(qOut.QueueUrl)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("env-keys-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(tOut.TopicArn)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qURL),
	})
	require.NoError(t, err)

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("envelope-check"),
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "expected one SQS message")

	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(msgs[0].Body)), &envelope))

	// All canonical SNS notification fields must be present.
	for _, field := range []string{
		"Type", "MessageId", "TopicArn", "Message",
		"Timestamp", "SignatureVersion", "Signature",
		"SigningCertURL", "UnsubscribeURL",
	} {
		_, ok := envelope[field]
		assert.True(t, ok, "SNS envelope missing required field: %s", field)
	}

	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, topicARN, envelope["TopicArn"])
	assert.Equal(t, "envelope-check", envelope["Message"])
	assert.NotEmpty(t, envelope["UnsubscribeURL"], "UnsubscribeURL must be non-empty")
	assert.NotEmpty(t, envelope["SigningCertURL"], "SigningCertURL must be non-empty")
}
