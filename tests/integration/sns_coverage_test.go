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
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pollSQS polls the given queue URL for up to 3 seconds and returns messages.
func pollSQS(ctx context.Context, sqsClient *sqs.Client, qURL string) []sqstypes.Message {
	for i := 0; i < 30; i++ {
		out, _ := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		if len(out.Messages) > 0 {
			return out.Messages
		}
	}
	return nil
}

// pollSQSN polls the given queue URL for up to 3 seconds, returning up to n messages.
func pollSQSN(ctx context.Context, sqsClient *sqs.Client, qURL string, n int32) []sqstypes.Message {
	for i := 0; i < 30; i++ {
		out, _ := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: n,
			WaitTimeSeconds:     1,
		})
		if len(out.Messages) > 0 {
			return out.Messages
		}
	}
	return nil
}

// ─── SNS CRUD ────────────────────────────────────────────────────────────────

func TestSNS_CreateTopic_Attributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)

	out, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("arn-check-topic"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TopicArn)
	arn := aws.ToString(out.TopicArn)
	assert.Contains(t, arn, "arn:aws:sns:")
	assert.Contains(t, arn, "arn-check-topic")

	attrOut, err := snsClient.GetTopicAttributes(ctx, &awssns.GetTopicAttributesInput{
		TopicArn: out.TopicArn,
	})
	require.NoError(t, err)
	assert.Equal(t, arn, attrOut.Attributes["TopicArn"])
}

func TestSNS_ListTopics_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)

	names := []string{"list-topic-a", "list-topic-b", "list-topic-c"}
	arns := make(map[string]bool)
	for _, name := range names {
		out, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String(name)})
		require.NoError(t, err)
		arns[aws.ToString(out.TopicArn)] = true
	}

	listOut, err := snsClient.ListTopics(ctx, &awssns.ListTopicsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Topics, 3)
	for _, topic := range listOut.Topics {
		assert.True(t, arns[aws.ToString(topic.TopicArn)], "unexpected ARN: %s", aws.ToString(topic.TopicArn))
	}
}

func TestSNS_SetTopicAttributes_DisplayName(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("dn-topic")})
	require.NoError(t, err)

	_, err = snsClient.SetTopicAttributes(ctx, &awssns.SetTopicAttributesInput{
		TopicArn:       tOut.TopicArn,
		AttributeName:  aws.String("DisplayName"),
		AttributeValue: aws.String("Friendly Name"),
	})
	require.NoError(t, err)

	attrOut, err := snsClient.GetTopicAttributes(ctx, &awssns.GetTopicAttributesInput{
		TopicArn: tOut.TopicArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "Friendly Name", attrOut.Attributes["DisplayName"])
}

func TestSNS_TagTopic_AndList(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("tag-list-topic")})
	require.NoError(t, err)

	_, err = snsClient.TagResource(ctx, &awssns.TagResourceInput{
		ResourceArn: tOut.TopicArn,
		Tags: []snstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("staging")},
			{Key: aws.String("owner"), Value: aws.String("team-a")},
		},
	})
	require.NoError(t, err)

	listOut, err := snsClient.ListTagsForResource(ctx, &awssns.ListTagsForResourceInput{
		ResourceArn: tOut.TopicArn,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range listOut.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "staging", tagMap["env"])
	assert.Equal(t, "team-a", tagMap["owner"])
}

func TestSNS_DeleteTopic_RemovesSubscriptions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("del-subs-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("del-subs-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	// Verify subscription exists before delete.
	listBefore, err := snsClient.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, listBefore.Subscriptions, 1)

	_, err = snsClient.DeleteTopic(ctx, &awssns.DeleteTopicInput{TopicArn: tOut.TopicArn})
	require.NoError(t, err)

	listAfter, err := snsClient.ListSubscriptions(ctx, &awssns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, listAfter.Subscriptions, 0)
}

// ─── SNS Filter Policies ─────────────────────────────────────────────────────

// setupFilterTest creates a queue, topic, and subscription with the given JSON filter policy.
// Returns (topicARN, queueURL, subscriptionARN).
func setupFilterTest(
	t *testing.T,
	ctx context.Context,
	snsClient *awssns.Client,
	sqsClient *sqs.Client,
	topicName, queueName string,
	filterPolicyJSON string,
) (string, string, string) {
	t.Helper()

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	qURL := aws.ToString(qOut.QueueUrl)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicARN := aws.ToString(tOut.TopicArn)

	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qURL),
	})
	require.NoError(t, err)
	subARN := aws.ToString(sOut.SubscriptionArn)

	if filterPolicyJSON != "" {
		_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
			SubscriptionArn: aws.String(subARN),
			AttributeName:   aws.String("FilterPolicy"),
			AttributeValue:  aws.String(filterPolicyJSON),
		})
		require.NoError(t, err)
	}

	return topicARN, qURL, subARN
}

func TestFilterPolicy_ExactString_Match(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-exact-match-topic", "fp-exact-match-q",
		`{"color":["red"]}`)

	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("red widget"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("red")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "expected message to be delivered when color=red matches filter")
}

func TestFilterPolicy_ExactString_NoMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-exact-nomatch-topic", "fp-exact-nomatch-q",
		`{"color":["red"]}`)

	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("blue widget"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
		},
	})
	require.NoError(t, err)

	// Give a brief window then verify nothing arrived.
	time.Sleep(300 * time.Millisecond)
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "message with color=blue should NOT pass a filter for red")
}

func TestFilterPolicy_MultipleValues_OR(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-or-topic", "fp-or-q",
		`{"color":["red","blue"]}`)

	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("blue widget"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "color=blue should match filter [red, blue]")
}

func TestFilterPolicy_NumericEquals(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-numeq-topic", "fp-numeq-q",
		`{"price":[{"numeric":["=",100]}]}`)

	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("exact price"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"price": {DataType: aws.String("Number"), StringValue: aws.String("100")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "price=100 should match numeric equals 100")
}

func TestFilterPolicy_NumericGreaterThan(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-numgt-topic", "fp-numgt-q",
		`{"price":[{"numeric":[">",50]}]}`)

	// Publish price=100 — should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("high price"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"price": {DataType: aws.String("Number"), StringValue: aws.String("100")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "price=100 should match numeric > 50")

	// Drain queue; publish price=30 — should NOT arrive.
	_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msgs[0].ReceiptHandle,
	})

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("low price"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"price": {DataType: aws.String("Number"), StringValue: aws.String("30")},
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
	assert.Empty(t, out.Messages, "price=30 should NOT match numeric > 50")
}

func TestFilterPolicy_NumericRange(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-numrange-topic", "fp-numrange-q",
		`{"score":[{"numeric":[">=",10,"<=",100]}]}`)

	// price=50 — inside range, should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("in-range"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"score": {DataType: aws.String("Number"), StringValue: aws.String("50")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "score=50 should match [>=10, <=100]")

	// Drain; publish score=200 — outside range, should NOT arrive.
	_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msgs[0].ReceiptHandle,
	})

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("out-of-range"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"score": {DataType: aws.String("Number"), StringValue: aws.String("200")},
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
	assert.Empty(t, out.Messages, "score=200 should NOT match [>=10, <=100]")
}

func TestFilterPolicy_AnythingBut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-anybut-topic", "fp-anybut-q",
		`{"status":[{"anything-but":["cancelled"]}]}`)

	// Publish status=active — should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("active order"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"status": {DataType: aws.String("String"), StringValue: aws.String("active")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "status=active should pass anything-but cancelled")

	// Drain; publish status=cancelled — should NOT arrive.
	_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msgs[0].ReceiptHandle,
	})

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("cancelled order"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"status": {DataType: aws.String("String"), StringValue: aws.String("cancelled")},
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
	assert.Empty(t, out.Messages, "status=cancelled should be blocked by anything-but filter")
}

func TestFilterPolicy_Exists_True(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-exists-true-topic", "fp-exists-true-q",
		`{"traceId":[{"exists":true}]}`)

	// Publish with traceId — should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("traced"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"traceId": {DataType: aws.String("String"), StringValue: aws.String("abc-123")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "message with traceId should match exists:true filter")

	// Drain; publish without traceId — should NOT arrive.
	_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msgs[0].ReceiptHandle,
	})

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("untraced"),
	})
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "message without traceId should NOT match exists:true filter")
}

func TestFilterPolicy_Exists_False(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-exists-false-topic", "fp-exists-false-q",
		`{"debugFlag":[{"exists":false}]}`)

	// Publish WITHOUT debugFlag — should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("no debug flag"),
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "message without debugFlag should match exists:false filter")
}

func TestFilterPolicy_StringPrefix(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-prefix-topic", "fp-prefix-q",
		`{"event":[{"prefix":"order_"}]}`)

	// Publish event=order_placed — should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("order placed"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("order_placed")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "event=order_placed should match prefix order_")

	// Drain; publish event=cancel — should NOT arrive.
	_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msgs[0].ReceiptHandle,
	})

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("cancel event"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("cancel")},
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
	assert.Empty(t, out.Messages, "event=cancel should NOT match prefix order_")
}

func TestFilterPolicy_AND_AcrossKeys(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-and-topic", "fp-and-q",
		`{"color":["red"],"size":["large"]}`)

	// Both match — should arrive.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("both match"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("red")},
			"size":  {DataType: aws.String("String"), StringValue: aws.String("large")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1, "both color=red and size=large should satisfy AND filter")

	// Drain; only color matches — should NOT arrive.
	_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msgs[0].ReceiptHandle,
	})

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("partial match"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("red")},
			"size":  {DataType: aws.String("String"), StringValue: aws.String("small")},
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
	assert.Empty(t, out.Messages, "only color matches — AND filter should block the message")
}

func TestFilterPolicy_MissingAttribute_NoMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-missing-attr-topic", "fp-missing-attr-q",
		`{"requiredKey":["someValue"]}`)

	// Publish without the required attribute.
	_, err := snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("no required key"),
	})
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages, "message without requiredKey should be filtered out")
}

func TestFilterPolicy_InvalidJSON_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("invalid-fp-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("invalid-fp-topic")})
	require.NoError(t, err)

	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: sOut.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(`{not valid json`),
	})
	// The emulator should reject an invalid JSON filter policy.
	require.Error(t, err, "setting an invalid JSON filter policy should return an error")
}

func TestFilterPolicy_MultipleSubscriptions_IndependentFilters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("multi-filter-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(tOut.TopicArn)

	// Queue A — filter for event=order_placed.
	qAOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("filter-q-a")})
	require.NoError(t, err)
	qAURL := aws.ToString(qAOut.QueueUrl)

	sAOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qAURL),
	})
	require.NoError(t, err)
	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: sAOut.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(`{"event":["order_placed"]}`),
	})
	require.NoError(t, err)

	// Queue B — filter for event=shipment_sent.
	qBOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("filter-q-b")})
	require.NoError(t, err)
	qBURL := aws.ToString(qBOut.QueueUrl)

	sBOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qBURL),
	})
	require.NoError(t, err)
	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: sBOut.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(`{"event":["shipment_sent"]}`),
	})
	require.NoError(t, err)

	// Publish event=order_placed — only queue A should receive it.
	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("new order"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("order_placed")},
		},
	})
	require.NoError(t, err)

	msgsA := pollSQS(ctx, sqsClient, qAURL)
	require.Len(t, msgsA, 1, "queue A (order_placed filter) should receive the message")

	time.Sleep(300 * time.Millisecond)
	outB, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qBURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, outB.Messages, "queue B (shipment_sent filter) should NOT receive order_placed")
}

func TestFilterPolicy_NoFilter_ReceivesAll(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	// No filter policy set — subscribe without calling SetSubscriptionAttributes.
	topicARN, qURL, _ := setupFilterTest(t, ctx, snsClient, sqsClient,
		"fp-nofilter-topic", "fp-nofilter-q",
		"" /* no filter */)

	// Publish three messages with varying attributes.
	for _, color := range []string{"red", "green", "yellow"} {
		_, err := snsClient.Publish(ctx, &awssns.PublishInput{
			TopicArn: aws.String(topicARN),
			Message:  aws.String("color: " + color),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"color": {DataType: aws.String("String"), StringValue: aws.String(color)},
			},
		})
		require.NoError(t, err)
	}

	// Collect all three.
	var collected []sqstypes.Message
	for i := 0; i < 30 && len(collected) < 3; i++ {
		out, _ := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		collected = append(collected, out.Messages...)
	}
	assert.Len(t, collected, 3, "subscription without filter should receive all 3 messages")
}

// ─── SNS Message Attributes ───────────────────────────────────────────────────

func TestSNS_Publish_WithMessageAttributes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("msg-attr-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("msg-attr-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: tOut.TopicArn,
		Message:  aws.String("attributed message"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"stringAttr": {DataType: aws.String("String"), StringValue: aws.String("hello")},
			"numberAttr": {DataType: aws.String("Number"), StringValue: aws.String("42")},
		},
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, aws.ToString(qOut.QueueUrl))
	require.Len(t, msgs, 1)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(msgs[0].Body)), &envelope))
	assert.Equal(t, "attributed message", envelope["Message"])

	attrs, ok := envelope["MessageAttributes"].(map[string]any)
	require.True(t, ok, "SNS envelope must contain MessageAttributes")

	strAttr, ok := attrs["stringAttr"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello", strAttr["StringValue"])

	numAttr, ok := attrs["numberAttr"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "42", numAttr["StringValue"])
}

func TestSNS_Publish_Subject(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("subject-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("subject-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: tOut.TopicArn,
		Message:  aws.String("body of message"),
		Subject:  aws.String("Test Subject Line"),
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, aws.ToString(qOut.QueueUrl))
	require.Len(t, msgs, 1)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(msgs[0].Body)), &envelope))
	assert.Equal(t, "body of message", envelope["Message"])
	assert.Equal(t, "Test Subject Line", envelope["Subject"])
}

func TestSNS_PublishBatch_MultipleMessages(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("batch-cov-q")})
	require.NoError(t, err)
	qURL := aws.ToString(qOut.QueueUrl)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("batch-cov-topic")})
	require.NoError(t, err)

	_, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	batchOut, err := snsClient.PublishBatch(ctx, &awssns.PublishBatchInput{
		TopicArn: tOut.TopicArn,
		PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{
			{Id: aws.String("batch-1"), Message: aws.String("first")},
			{Id: aws.String("batch-2"), Message: aws.String("second")},
			{Id: aws.String("batch-3"), Message: aws.String("third")},
		},
	})
	require.NoError(t, err)
	assert.Len(t, batchOut.Successful, 3)
	assert.Empty(t, batchOut.Failed)

	// Collect all 3 messages.
	var collected []sqstypes.Message
	for i := 0; i < 30 && len(collected) < 3; i++ {
		out, _ := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		collected = append(collected, out.Messages...)
	}
	assert.Len(t, collected, 3, "all 3 batch messages should be delivered")
}

func TestSNS_Subscription_RawMessageDelivery(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("raw-cov-q")})
	require.NoError(t, err)
	qURL := aws.ToString(qOut.QueueUrl)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("raw-cov-topic")})
	require.NoError(t, err)

	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)

	_, err = snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: sOut.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	// Verify the attribute was stored.
	getOut, err := snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: sOut.SubscriptionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "true", getOut.Attributes["RawMessageDelivery"])

	_, err = snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: tOut.TopicArn,
		Message:  aws.String("raw-body"),
	})
	require.NoError(t, err)

	msgs := pollSQS(ctx, sqsClient, qURL)
	require.Len(t, msgs, 1)
	// With RawMessageDelivery the body is the plain message, not a JSON envelope.
	assert.Equal(t, "raw-body", aws.ToString(msgs[0].Body))
}

func TestSNS_ConfirmSubscription_TokenFlow(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("confirm-q")})
	require.NoError(t, err)

	tOut, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("confirm-topic")})
	require.NoError(t, err)

	sOut, err := snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: tOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: qOut.QueueUrl,
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(sOut.SubscriptionArn))

	// GetSubscriptionAttributes should return valid metadata.
	getOut, err := snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: sOut.SubscriptionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(sOut.SubscriptionArn), getOut.Attributes["SubscriptionArn"])
	assert.Equal(t, aws.ToString(tOut.TopicArn), getOut.Attributes["TopicArn"])
	assert.Equal(t, "sqs", getOut.Attributes["Protocol"])
	assert.Equal(t, aws.ToString(qOut.QueueUrl), getOut.Attributes["Endpoint"])
}
