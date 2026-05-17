package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queueURLtoARN derives a best-effort SQS ARN from a queue URL.
// Queue URLs from JaisCloud are of the form http://host:port/account/queue-name.
func queueURLtoARN(queueURL string) string {
	// Strip trailing slash.
	u := strings.TrimRight(queueURL, "/")
	parts := strings.Split(u, "/")
	if len(parts) < 2 {
		return ""
	}
	queueName := parts[len(parts)-1]
	accountID := parts[len(parts)-2]
	return fmt.Sprintf("arn:aws:sqs:us-east-1:%s:%s", accountID, queueName)
}

// TestSNSRedriveToSQSDLQ_LambdaFailure verifies that when an SNS→Lambda delivery
// fails (function does not exist), the original message body is forwarded to the
// dead-letter queue declared in the subscription's RedrivePolicy.
func TestSNSRedriveToSQSDLQ_LambdaFailure(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	// Create the DLQ.
	dlqOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("test-dlq"),
	})
	require.NoError(t, err)
	dlqURL := aws.ToString(dlqOut.QueueUrl)
	dlqArn := queueURLtoARN(dlqURL)
	require.NotEmpty(t, dlqArn)

	// Create the SNS topic.
	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("test-topic"),
	})
	require.NoError(t, err)
	topicArn := aws.ToString(tOut.TopicArn)

	// Subscribe a non-existent Lambda with a RedrivePolicy pointing at the DLQ.
	// The subscription is placed at creation time via the Attributes map.
	rdpJSON := fmt.Sprintf(`{"deadLetterTargetArn":"%s"}`, dlqArn)
	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("lambda"),
		Endpoint: aws.String("arn:aws:lambda:us-east-1:000000000000:function:nonexistent"),
		Attributes: map[string]string{
			"RedrivePolicy": rdpJSON,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(sOut.SubscriptionArn))

	// Publish to the topic — Lambda delivery will fail (function not found).
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("test message"),
	})
	require.NoError(t, err)

	// Poll the DLQ — the failed delivery must appear.
	msgs := pollSQS(ctx, sqsClient, dlqURL)
	require.NotEmpty(t, msgs, "DLQ must receive the failed SNS→Lambda delivery")

	// The DLQ message body should be parseable JSON.
	var dlqBody map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(msgs[0].Body)), &dlqBody),
		"DLQ message body must be valid JSON")
}

// TestSNSRedriveToSQSDLQ_SetAttributeAfterSubscribe verifies that a RedrivePolicy
// set via SetSubscriptionAttributes (after subscription creation) is honoured on
// subsequent delivery failures.
func TestSNSRedriveToSQSDLQ_SetAttributeAfterSubscribe(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	// Create the DLQ.
	dlqOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("post-sub-dlq"),
	})
	require.NoError(t, err)
	dlqURL := aws.ToString(dlqOut.QueueUrl)
	dlqArn := queueURLtoARN(dlqURL)
	require.NotEmpty(t, dlqArn)

	// Create the SNS topic.
	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("post-sub-topic"),
	})
	require.NoError(t, err)
	topicArn := aws.ToString(tOut.TopicArn)

	// Subscribe with no RedrivePolicy initially.
	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("lambda"),
		Endpoint: aws.String("arn:aws:lambda:us-east-1:000000000000:function:also-nonexistent"),
	})
	require.NoError(t, err)
	subArn := aws.ToString(sOut.SubscriptionArn)

	// Set the RedrivePolicy after the subscription was created.
	rdpJSON := fmt.Sprintf(`{"deadLetterTargetArn":"%s"}`, dlqArn)
	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
		AttributeName:   aws.String("RedrivePolicy"),
		AttributeValue:  aws.String(rdpJSON),
	})
	require.NoError(t, err)

	// Verify the attribute was persisted.
	getOut, err := snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
	})
	require.NoError(t, err)
	assert.Equal(t, rdpJSON, getOut.Attributes["RedrivePolicy"],
		"RedrivePolicy must be readable back via GetSubscriptionAttributes")

	// Publish — delivery will fail, DLQ should receive the message.
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("should land in dlq"),
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, dlqURL)
	require.NotEmpty(t, msgs, "DLQ must receive the failed delivery after SetSubscriptionAttributes")
}

// TestSNSRedriveToSQSDLQ_SQSFailure verifies that when an SNS→SQS delivery
// fails (non-existent queue URL), the message is forwarded to the DLQ configured
// in the subscription's RedrivePolicy.
func TestSNSRedriveToSQSDLQ_SQSFailure(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	// Create the DLQ.
	dlqOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sqs-fail-dlq"),
	})
	require.NoError(t, err)
	dlqURL := aws.ToString(dlqOut.QueueUrl)
	dlqArn := queueURLtoARN(dlqURL)
	require.NotEmpty(t, dlqArn)

	// Create the SNS topic.
	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("sqs-fail-topic"),
	})
	require.NoError(t, err)
	topicArn := aws.ToString(tOut.TopicArn)

	// Subscribe an SQS endpoint that does NOT exist, with a RedrivePolicy.
	rdpJSON := fmt.Sprintf(`{"deadLetterTargetArn":"%s"}`, dlqArn)
	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("http://localhost:4566/000000000000/nonexistent-queue"),
		Attributes: map[string]string{
			"RedrivePolicy": rdpJSON,
		},
	})
	require.NoError(t, err)

	// Publish — SQS delivery will fail, DLQ should receive the message.
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("sqs delivery fail"),
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, dlqURL)
	require.NotEmpty(t, msgs, "DLQ must receive the message when SQS delivery fails")
}
