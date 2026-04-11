package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSNS_CreateListDeleteTopic(t *testing.T) {
	resetState(t)
	client := newSNSClient(t)
	ctx := context.Background()

	out, err := client.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("my-topic"),
	})
	require.NoError(t, err)
	require.Contains(t, *out.TopicArn, "my-topic")

	listOut, err := client.ListTopics(ctx, &awssns.ListTopicsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Topics, 1)
	require.Equal(t, *out.TopicArn, *listOut.Topics[0].TopicArn)

	_, err = client.DeleteTopic(ctx, &awssns.DeleteTopicInput{TopicArn: out.TopicArn})
	require.NoError(t, err)

	listOut2, err := client.ListTopics(ctx, &awssns.ListTopicsInput{})
	require.NoError(t, err)
	require.Len(t, listOut2.Topics, 0)
}

func TestSNS_GetSetTopicAttributes(t *testing.T) {
	resetState(t)
	client := newSNSClient(t)
	ctx := context.Background()

	out, err := client.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("attr-topic")})
	require.NoError(t, err)

	attrOut, err := client.GetTopicAttributes(ctx, &awssns.GetTopicAttributesInput{TopicArn: out.TopicArn})
	require.NoError(t, err)
	require.Equal(t, *out.TopicArn, attrOut.Attributes["TopicArn"])

	_, err = client.SetTopicAttributes(ctx, &awssns.SetTopicAttributesInput{
		TopicArn:       out.TopicArn,
		AttributeName:  aws.String("DisplayName"),
		AttributeValue: aws.String("My Topic"),
	})
	require.NoError(t, err)
}

func TestSNS_SubscribeUnsubscribe(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	// Create SQS queue first.
	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sns-target"),
	})
	require.NoError(t, err)

	// Create topic.
	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("sub-topic")})
	require.NoError(t, err)

	// Subscribe queue to topic.
	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)
	require.NotEmpty(t, *sOut.SubscriptionArn)

	// List subscriptions.
	listOut, err := snsClient.ListSubscriptionsByTopic(ctx, &awssns.ListSubscriptionsByTopicInput{
		TopicArn: tOut.TopicArn,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Subscriptions, 1)
	require.Equal(t, "sqs", aws.ToString(listOut.Subscriptions[0].Protocol))

	// Unsubscribe.
	_, err = snsClient.Unsubscribe(ctx, &awssns.UnsubscribeInput{
		SubscriptionArn: sOut.SubscriptionArn,
	})
	require.NoError(t, err)

	listOut2, err := snsClient.ListSubscriptionsByTopic(ctx, &awssns.ListSubscriptionsByTopicInput{
		TopicArn: tOut.TopicArn,
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Subscriptions, 0)
}

func TestSNS_PublishFanoutToSQS(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	// Create queue.
	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("fanout-q")})
	require.NoError(t, err)

	// Create topic and subscribe queue.
	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("fanout-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	// Publish message.
	pOut, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: tOut.TopicArn,
		Message:  aws.String("hello from sns"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *pOut.MessageId)

	// Receive from SQS — message should have arrived.
	rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            qOut.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, rOut.Messages, 1)

	// Body is JSON envelope.
	var envelope map[string]any
	err = json.Unmarshal([]byte(*rOut.Messages[0].Body), &envelope)
	require.NoError(t, err)
	require.Equal(t, "hello from sns", envelope["Message"])
	require.Equal(t, "Notification", envelope["Type"])
}

func TestSNS_PublishMultipleSubscribers(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("multi-topic")})
	require.NoError(t, err)

	// Two queues subscribed to the same topic.
	for _, qName := range []string{"sub-q-1", "sub-q-2"} {
		qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(qName)})
		require.NoError(t, err)
		_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
			TopicArn: tOut.TopicArn,
			Protocol: aws.String("sqs"),
			Endpoint: qOut.QueueUrl,
		})
		require.NoError(t, err)
	}

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: tOut.TopicArn,
		Message:  aws.String("broadcast"),
	})
	require.NoError(t, err)

	// Both queues should receive the message.
	for _, qName := range []string{"sub-q-1", "sub-q-2"} {
		urlOut, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(qName)})
		require.NoError(t, err)
		rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            urlOut.QueueUrl,
			MaxNumberOfMessages: 1,
		})
		require.NoError(t, err)
		require.Len(t, rOut.Messages, 1, "queue %s should have received the message", qName)
	}
}

func TestSNS_DeleteTopicRemovesSubscriptions(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	qOut, _ := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("del-sub-q")})
	tOut, _ := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("del-topic")})
	_, _ = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})

	_, err := snsClient.DeleteTopic(ctx, &awssns.DeleteTopicInput{TopicArn: tOut.TopicArn})
	require.NoError(t, err)

	listOut, err := snsClient.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Subscriptions, 0)
}

func TestSNS_PublishBatch(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("batch-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	_, err = snsClient.PublishBatch(ctx, &awssns.PublishBatchInput{
		TopicArn: tOut.TopicArn,
		PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{
			{Id: aws.String("1"), Message: aws.String("msg-one")},
			{Id: aws.String("2"), Message: aws.String("msg-two")},
			{Id: aws.String("3"), Message: aws.String("msg-three")},
		},
	})
	require.NoError(t, err)

	// All 3 messages should have been delivered to the SQS queue.
	rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            qOut.QueueUrl,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	require.Len(t, rOut.Messages, 3)
}

func TestSNS_SubscriptionAttributes(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	qOut, _ := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("attr-sub-q")})
	tOut, _ := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("attr-sub-topic")})
	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)
	subArn := sOut.SubscriptionArn

	// GetSubscriptionAttributes
	getOut, err := snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: subArn,
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(subArn), getOut.Attributes["SubscriptionArn"])

	// SetSubscriptionAttributes
	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: subArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	getOut2, err := snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: subArn,
	})
	require.NoError(t, err)
	require.Equal(t, "true", getOut2.Attributes["RawMessageDelivery"])
}

func TestSNS_ListSubscriptions(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	// Create two topics, each with one subscriber.
	for i, topicName := range []string{"topic-a", "topic-b"} {
		qOut, _ := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String("ls-q-" + topicName),
		})
		tOut, _ := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String(topicName)})
		_, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
			TopicArn: tOut.TopicArn,
			Protocol: aws.String("sqs"),
			Endpoint: qOut.QueueUrl,
		})
		require.NoError(t, err, "topic %d", i)
	}

	// Global ListSubscriptions should return 2.
	listOut, err := snsClient.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Subscriptions, 2)
}

func TestSNS_TopicTags(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	ctx := context.Background()

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("tags-topic")})
	require.NoError(t, err)
	topicArn := tOut.TopicArn

	// Tag the topic.
	_, err = snsClient.TagResource(ctx, &awssns.TagResourceInput{
		ResourceArn: topicArn,
		Tags: []snstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("infra")},
		},
	})
	require.NoError(t, err)

	listOut, err := snsClient.ListTagsForResource(ctx, &awssns.ListTagsForResourceInput{
		ResourceArn: topicArn,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	// Untag one.
	_, err = snsClient.UntagResource(ctx, &awssns.UntagResourceInput{
		ResourceArn: topicArn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := snsClient.ListTagsForResource(ctx, &awssns.ListTagsForResourceInput{
		ResourceArn: topicArn,
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	require.Equal(t, "team", aws.ToString(listOut2.Tags[0].Key))
}

func TestSNS_MessageAttributes(t *testing.T) {
	resetState(t)
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)
	ctx := context.Background()

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("ma-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("ma-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: tOut.TopicArn,
		Message:  aws.String("hello with attrs"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event-type": {DataType: aws.String("String"), StringValue: aws.String("order-placed")},
			"priority":   {DataType: aws.String("String"), StringValue: aws.String("high")},
		},
	})
	require.NoError(t, err)

	rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            qOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, rOut.Messages, 1)

	// The SNS envelope should contain MessageAttributes.
	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(rOut.Messages[0].Body)), &envelope))
	require.Equal(t, "hello with attrs", envelope["Message"])
	attrs, ok := envelope["MessageAttributes"].(map[string]any)
	require.True(t, ok, "expected MessageAttributes in SNS envelope")
	eventType, ok := attrs["event-type"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "order-placed", eventType["StringValue"])
}

func TestSNS_RawMessageDelivery(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	// Create topic and queue
	topicOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("raw-topic")})
	require.NoError(t, err)
	topicArn := aws.ToString(topicOut.TopicArn)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("raw-queue")})
	require.NoError(t, err)
	qURL := aws.ToString(qOut.QueueUrl)

	// Subscribe with RawMessageDelivery=true
	subOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qURL),
	})
	require.NoError(t, err)

	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: subOut.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	// Publish
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("raw-payload"),
	})
	require.NoError(t, err)

	// Receive — body should be the raw string, not a JSON envelope
	rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, rOut.Messages, 1)
	assert.Equal(t, "raw-payload", aws.ToString(rOut.Messages[0].Body))
}
