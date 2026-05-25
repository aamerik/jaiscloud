package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscw "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// ─── §2.1 EventBridge ─────────────────────────────────────────────────────────

func TestPhaseB_EventBridge_PutEvents_DeliverToSQS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	sqsC := newSQSClient(t)
	ebC := newEventBridgeClient(t)

	qURL := pbCreateQueue(t, sqsC, "eb-target-queue")
	qARN := pbQueueARN("eb-target-queue")

	_, err := ebC.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("pb-test-rule"),
		EventPattern: aws.String(`{"source":["test.source"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = ebC.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:    aws.String("pb-test-rule"),
		Targets: []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(qARN)}},
	})
	require.NoError(t, err)

	_, err = ebC.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:       aws.String("test.source"),
				DetailType:   aws.String("TestEvent"),
				Detail:       aws.String(`{"key":"value"}`),
				EventBusName: aws.String("default"),
			},
		},
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out, err := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		return err == nil && len(out.Messages) > 0
	})
}

func TestPhaseB_EventBridge_PatternMatch_SourceFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	sqsC := newSQSClient(t)
	ebC := newEventBridgeClient(t)

	qURL := pbCreateQueue(t, sqsC, "pb-match-queue")
	qARN := pbQueueARN("pb-match-queue")
	qURLNoMatch := pbCreateQueue(t, sqsC, "pb-nomatch-queue")
	qARNNoMatch := pbQueueARN("pb-nomatch-queue")

	_, err := ebC.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("pb-rule-match"),
		EventPattern: aws.String(`{"source":["my.app"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	_, err = ebC.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:    aws.String("pb-rule-match"),
		Targets: []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(qARN)}},
	})
	require.NoError(t, err)

	_, err = ebC.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("pb-rule-nomatch"),
		EventPattern: aws.String(`{"source":["other.app"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	_, err = ebC.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:    aws.String("pb-rule-nomatch"),
		Targets: []ebtypes.Target{{Id: aws.String("t2"), Arn: aws.String(qARNNoMatch)}},
	})
	require.NoError(t, err)

	_, err = ebC.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("my.app"), DetailType: aws.String("T"), Detail: aws.String(`{}`), EventBusName: aws.String("default")},
		},
	})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	got, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(qURL), MaxNumberOfMessages: 5})
	assert.NotEmpty(t, got.Messages, "matching queue should have received message")

	notGot, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(qURLNoMatch), MaxNumberOfMessages: 5})
	assert.Empty(t, notGot.Messages, "non-matching queue should be empty")
}

func TestPhaseB_EventBridge_PutRule_ListTargets_RemoveTargets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ebC := newEventBridgeClient(t)
	sqsC := newSQSClient(t)

	pbCreateQueue(t, sqsC, "pb-tgt-queue")
	qARN := pbQueueARN("pb-tgt-queue")

	_, err := ebC.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("pb-rule1"),
		EventPattern: aws.String(`{"source":["x"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = ebC.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:    aws.String("pb-rule1"),
		Targets: []ebtypes.Target{{Id: aws.String("tgt1"), Arn: aws.String(qARN)}},
	})
	require.NoError(t, err)

	listOut, err := ebC.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{Rule: aws.String("pb-rule1")})
	require.NoError(t, err)
	require.Len(t, listOut.Targets, 1)
	assert.Equal(t, "tgt1", aws.ToString(listOut.Targets[0].Id))

	_, err = ebC.RemoveTargets(ctx, &awseb.RemoveTargetsInput{
		Rule: aws.String("pb-rule1"),
		Ids:  []string{"tgt1"},
	})
	require.NoError(t, err)

	listOut2, err := ebC.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{Rule: aws.String("pb-rule1")})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Targets)
}

func TestPhaseB_EventBridge_CreateArchive_DescribeArchive(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	ebC := newEventBridgeClient(t)

	sourceARN := "arn:aws:events:us-east-1:000000000000:event-bus/default"

	createOut, err := ebC.CreateArchive(ctx, &awseb.CreateArchiveInput{
		ArchiveName:    aws.String("pb-test-archive"),
		EventSourceArn: aws.String(sourceARN),
		RetentionDays:  aws.Int32(7),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createOut.ArchiveArn))

	descOut, err := ebC.DescribeArchive(ctx, &awseb.DescribeArchiveInput{
		ArchiveName: aws.String("pb-test-archive"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(7), aws.ToInt32(descOut.RetentionDays))
}

// ─── §2.2 SNS fan-out ─────────────────────────────────────────────────────────

func TestPhaseB_SNS_FanOut_ToSQS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsC := newSNSClient(t)
	sqsC := newSQSClient(t)

	topicOut, err := snsC.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("pb-fanout-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	qURL := pbCreateQueue(t, sqsC, "pb-fanout-sub-queue")
	qARN := pbQueueARN("pb-fanout-sub-queue")

	subOut, err := snsC.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qARN),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(subOut.SubscriptionArn))

	_, err = snsC.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("hello from sns"),
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out, err := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		return err == nil && len(out.Messages) > 0
	})
}

func TestPhaseB_SNS_FilterPolicy_BlocksNonMatching(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsC := newSNSClient(t)
	sqsC := newSQSClient(t)

	topicOut, err := snsC.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("pb-filter-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	qURL := pbCreateQueue(t, sqsC, "pb-filter-queue")
	qARN := pbQueueARN("pb-filter-queue")

	subOut, err := snsC.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qARN),
		Attributes: map[string]string{
			"FilterPolicy":       `{"color":["red"]}`,
			"RawMessageDelivery": "true",
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(subOut.SubscriptionArn))

	// Publish with non-matching attribute — should NOT arrive.
	_, err = snsC.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("blue message"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("blue")},
		},
	})
	require.NoError(t, err)

	time.Sleep(400 * time.Millisecond)
	out, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(qURL), MaxNumberOfMessages: 5})
	assert.Empty(t, out.Messages, "blue message should be filtered out")

	// Publish with matching attribute — should arrive.
	_, err = snsC.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String("red message"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"color": {DataType: aws.String("String"), StringValue: aws.String("red")},
		},
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out2, err := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		return err == nil && len(out2.Messages) > 0
	})
}

func TestPhaseB_SNS_ConfirmSubscription_SQSAutoConfirms(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	snsC := newSNSClient(t)
	sqsC := newSQSClient(t)

	topicOut, err := snsC.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("pb-confirm-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	pbCreateQueue(t, sqsC, "pb-confirm-queue")
	qARN := pbQueueARN("pb-confirm-queue")

	subOut, err := snsC.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(qARN),
	})
	require.NoError(t, err)
	subARN := aws.ToString(subOut.SubscriptionArn)
	// SQS subscriptions are auto-confirmed — ARN should not be "PendingConfirmation".
	assert.NotEqual(t, "PendingConfirmation", subARN)
	assert.NotEmpty(t, subARN)
}

// ─── §2.3 S3 fan-out notifications ───────────────────────────────────────────

func TestPhaseB_S3_Notification_ToSQS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	s3C := newS3Client(t)
	sqsC := newSQSClient(t)

	bucket := "pb-notif-bucket"
	_, err := s3C.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	qURL := pbCreateQueue(t, sqsC, "pb-s3-notif-queue")
	qARN := pbQueueARN("pb-s3-notif-queue")

	_, err = s3C.PutBucketNotificationConfiguration(ctx, &awss3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket),
		NotificationConfiguration: &awss3types.NotificationConfiguration{
			QueueConfigurations: []awss3types.QueueConfiguration{
				{
					Id:       aws.String("cfg1"),
					QueueArn: aws.String(qARN),
					Events:   []awss3types.Event{"s3:ObjectCreated:*"},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = s3C.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("test-key.txt"),
		Body:   strings.NewReader("hello"),
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out, err := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		if err != nil || len(out.Messages) == 0 {
			return false
		}
		body := aws.ToString(out.Messages[0].Body)
		return strings.Contains(body, "ObjectCreated")
	})
}

func TestPhaseB_S3_Notification_KeyFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	s3C := newS3Client(t)
	sqsC := newSQSClient(t)

	bucket := "pb-filter-notif-bucket"
	_, err := s3C.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	qURL := pbCreateQueue(t, sqsC, "pb-filter-notif-queue")
	qARN := pbQueueARN("pb-filter-notif-queue")

	_, err = s3C.PutBucketNotificationConfiguration(ctx, &awss3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket),
		NotificationConfiguration: &awss3types.NotificationConfiguration{
			QueueConfigurations: []awss3types.QueueConfiguration{
				{
					Id:       aws.String("img-only"),
					QueueArn: aws.String(qARN),
					Events:   []awss3types.Event{"s3:ObjectCreated:*"},
					Filter: &awss3types.NotificationConfigurationFilter{
						Key: &awss3types.S3KeyFilter{
							FilterRules: []awss3types.FilterRule{
								{Name: awss3types.FilterRuleName("prefix"), Value: aws.String("images/")},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Upload non-matching key — should NOT trigger.
	_, err = s3C.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("docs/readme.txt"),
		Body:   strings.NewReader("doc"),
	})
	require.NoError(t, err)

	time.Sleep(400 * time.Millisecond)
	nonMatch, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(qURL), MaxNumberOfMessages: 5})
	assert.Empty(t, nonMatch.Messages, "non-matching key should not trigger notification")

	// Upload matching key — should trigger.
	_, err = s3C.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("images/photo.jpg"),
		Body:   strings.NewReader("img"),
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		return len(out.Messages) > 0
	})
}

func TestPhaseB_S3_Notification_ToSNS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	s3C := newS3Client(t)
	snsC := newSNSClient(t)
	sqsC := newSQSClient(t)

	bucket := "pb-sns-notif-bucket"
	_, err := s3C.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	topicOut, err := snsC.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("pb-s3-sns-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	qURL := pbCreateQueue(t, sqsC, "pb-s3-sns-sub-queue")
	qARN := pbQueueARN("pb-s3-sns-sub-queue")
	_, err = snsC.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn:   aws.String(topicARN),
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(qARN),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	require.NoError(t, err)

	_, err = s3C.PutBucketNotificationConfiguration(ctx, &awss3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket),
		NotificationConfiguration: &awss3types.NotificationConfiguration{
			TopicConfigurations: []awss3types.TopicConfiguration{
				{
					Id:       aws.String("sns-cfg"),
					TopicArn: aws.String(topicARN),
					Events:   []awss3types.Event{"s3:ObjectCreated:*"},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = s3C.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("upload.txt"),
		Body:   strings.NewReader("data"),
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		return len(out.Messages) > 0
	})
}

// ─── §2.4 CloudWatch alarms ───────────────────────────────────────────────────

func TestPhaseB_CloudWatch_Alarm_StateChange(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cwC := newCWClient(t)

	_, err := cwC.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String("pb-test-alarm"),
		MetricName:         aws.String("CPUUtilization"),
		Namespace:          aws.String("AWS/EC2"),
		Statistic:          cwtypes.StatisticAverage,
		Period:             aws.Int32(60),
		EvaluationPeriods:  aws.Int32(1),
		Threshold:          aws.Float64(80),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		ActionsEnabled:     aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = cwC.SetAlarmState(ctx, &awscw.SetAlarmStateInput{
		AlarmName:   aws.String("pb-test-alarm"),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("test transition"),
	})
	require.NoError(t, err)

	out, err := cwC.DescribeAlarms(ctx, &awscw.DescribeAlarmsInput{
		AlarmNames: []string{"pb-test-alarm"},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1)
	assert.Equal(t, cwtypes.StateValueAlarm, out.MetricAlarms[0].StateValue)
}

func TestPhaseB_CloudWatch_Alarm_SNSAction_Fires(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cwC := newCWClient(t)
	snsC := newSNSClient(t)
	sqsC := newSQSClient(t)

	topicOut, err := snsC.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("pb-alarm-topic")})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)

	qURL := pbCreateQueue(t, sqsC, "pb-alarm-sub-queue")
	qARN := pbQueueARN("pb-alarm-sub-queue")
	_, err = snsC.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn:   aws.String(topicARN),
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(qARN),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	})
	require.NoError(t, err)

	_, err = cwC.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String("pb-sns-alarm"),
		MetricName:         aws.String("Errors"),
		Namespace:          aws.String("MyApp"),
		Statistic:          cwtypes.StatisticSum,
		Period:             aws.Int32(60),
		EvaluationPeriods:  aws.Int32(1),
		Threshold:          aws.Float64(1),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanOrEqualToThreshold,
		ActionsEnabled:     aws.Bool(true),
		AlarmActions:       []string{topicARN},
	})
	require.NoError(t, err)

	_, err = cwC.SetAlarmState(ctx, &awscw.SetAlarmStateInput{
		AlarmName:   aws.String("pb-sns-alarm"),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("threshold breached"),
	})
	require.NoError(t, err)

	waitFor(t, 3*time.Second, func() bool {
		out, _ := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		return len(out.Messages) > 0
	})
}

// ─── §2.4 CloudWatch Logs ─────────────────────────────────────────────────────

func TestPhaseB_CWLogs_SubscriptionFilter_Configured(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cwlC := newCWLClient(t)

	_, err := cwlC.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/pb/app/logs"),
	})
	require.NoError(t, err)

	_, err = cwlC.PutSubscriptionFilter(ctx, &awscwl.PutSubscriptionFilterInput{
		LogGroupName:   aws.String("/pb/app/logs"),
		FilterName:     aws.String("error-filter"),
		FilterPattern:  aws.String("ERROR"),
		DestinationArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:log-processor"),
	})
	require.NoError(t, err)

	out, err := cwlC.DescribeSubscriptionFilters(ctx, &awscwl.DescribeSubscriptionFiltersInput{
		LogGroupName: aws.String("/pb/app/logs"),
	})
	require.NoError(t, err)
	require.Len(t, out.SubscriptionFilters, 1)
	assert.Equal(t, "error-filter", aws.ToString(out.SubscriptionFilters[0].FilterName))
}

func TestPhaseB_CWLogs_MetricFilter_PutDescribeDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cwlC := newCWLClient(t)

	_, err := cwlC.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String("/pb/metrics")})
	require.NoError(t, err)

	_, err = cwlC.PutMetricFilter(ctx, &awscwl.PutMetricFilterInput{
		LogGroupName:  aws.String("/pb/metrics"),
		FilterName:    aws.String("pb-error-count"),
		FilterPattern: aws.String("ERROR"),
		MetricTransformations: []cwltypes.MetricTransformation{
			{
				MetricNamespace: aws.String("MyApp"),
				MetricName:      aws.String("ErrorCount"),
				MetricValue:     aws.String("1"),
				Unit:            cwltypes.StandardUnitCount,
			},
		},
	})
	require.NoError(t, err)

	descOut, err := cwlC.DescribeMetricFilters(ctx, &awscwl.DescribeMetricFiltersInput{
		LogGroupName: aws.String("/pb/metrics"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.MetricFilters, 1)
	assert.Equal(t, "pb-error-count", aws.ToString(descOut.MetricFilters[0].FilterName))

	_, err = cwlC.DeleteMetricFilter(ctx, &awscwl.DeleteMetricFilterInput{
		LogGroupName: aws.String("/pb/metrics"),
		FilterName:   aws.String("pb-error-count"),
	})
	require.NoError(t, err)

	descOut2, err := cwlC.DescribeMetricFilters(ctx, &awscwl.DescribeMetricFiltersInput{
		LogGroupName: aws.String("/pb/metrics"),
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.MetricFilters)
}

func TestPhaseB_CWLogs_PutLogEvents_Ingested(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	cwlC := newCWLClient(t)

	_, err := cwlC.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String("/pb/stream")})
	require.NoError(t, err)
	_, err = cwlC.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String("/pb/stream"),
		LogStreamName: aws.String("stream-1"),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	_, err = cwlC.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String("/pb/stream"),
		LogStreamName: aws.String("stream-1"),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("INFO application started")},
			{Timestamp: aws.Int64(now + 1), Message: aws.String("ERROR something failed")},
		},
	})
	require.NoError(t, err)

	getOut, err := cwlC.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
		LogGroupName:  aws.String("/pb/stream"),
		LogStreamName: aws.String("stream-1"),
		StartFromHead: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, getOut.Events, 2)
}

// ─── §2.6 Lambda ESM ─────────────────────────────────────────────────────────

func TestPhaseB_Lambda_ESM_CreateGetDelete(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsC := newSQSClient(t)
	pbCreateQueue(t, sqsC, "pb-esm-source-queue")
	qARN := pbQueueARN("pb-esm-source-queue")

	lambdaC := newLambdaClient(t)
	pbCreateFunction(t, ctx, lambdaC, "pb-esm-function")
	funcARN := "arn:aws:lambda:us-east-1:000000000000:function:pb-esm-function"

	createOut, err := lambdaC.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String(funcARN),
		EventSourceArn: aws.String(qARN),
		BatchSize:      aws.Int32(5),
	})
	require.NoError(t, err)
	esmUUID := aws.ToString(createOut.UUID)
	assert.NotEmpty(t, esmUUID)

	getOut, err := lambdaC.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(5), aws.ToInt32(getOut.BatchSize))

	_, err = lambdaC.DeleteEventSourceMapping(ctx, &awslambda.DeleteEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)
}

func TestPhaseB_Lambda_ESM_UpdateBatchSize(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	sqsC := newSQSClient(t)
	pbCreateQueue(t, sqsC, "pb-esm-upd-queue")
	qARN := pbQueueARN("pb-esm-upd-queue")

	lambdaC := newLambdaClient(t)
	pbCreateFunction(t, ctx, lambdaC, "pb-esm-upd-func")
	funcARN := "arn:aws:lambda:us-east-1:000000000000:function:pb-esm-upd-func"

	createOut, err := lambdaC.CreateEventSourceMapping(ctx, &awslambda.CreateEventSourceMappingInput{
		FunctionName:   aws.String(funcARN),
		EventSourceArn: aws.String(qARN),
		BatchSize:      aws.Int32(10),
	})
	require.NoError(t, err)
	esmUUID := aws.ToString(createOut.UUID)

	_, err = lambdaC.UpdateEventSourceMapping(ctx, &awslambda.UpdateEventSourceMappingInput{
		UUID:      aws.String(esmUUID),
		BatchSize: aws.Int32(20),
	})
	require.NoError(t, err)

	getOut, err := lambdaC.GetEventSourceMapping(ctx, &awslambda.GetEventSourceMappingInput{UUID: aws.String(esmUUID)})
	require.NoError(t, err)
	assert.Equal(t, int32(20), aws.ToInt32(getOut.BatchSize))
}

// ─── §2.8 SecretsManager rotation ────────────────────────────────────────────

func TestPhaseB_SecretsManager_RotateSecret_Persists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	smC := newSMClient(t)

	_, err := smC.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("pb-rotate-secret"),
		SecretString: aws.String(`{"password":"old"}`),
	})
	require.NoError(t, err)

	rotOut, err := smC.RotateSecret(ctx, &awssm.RotateSecretInput{
		SecretId:          aws.String("pb-rotate-secret"),
		RotateImmediately: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(rotOut.VersionId))
}

func TestPhaseB_SecretsManager_RotateSecret_WithLambdaARN(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	smC := newSMClient(t)

	_, err := smC.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("pb-lambda-rotate-secret"),
		SecretString: aws.String(`{"password":"init"}`),
	})
	require.NoError(t, err)

	// Rotation with a non-existent Lambda ARN succeeds at the API level;
	// the background goroutine logs a warning but does not fail the request.
	rotOut, err := smC.RotateSecret(ctx, &awssm.RotateSecretInput{
		SecretId:          aws.String("pb-lambda-rotate-secret"),
		RotationLambdaARN: aws.String("arn:aws:lambda:us-east-1:000000000000:function:rotator"),
		RotateImmediately: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(rotOut.VersionId))
}

// ─── Phase B local helpers ────────────────────────────────────────────────────

// pbCreateQueue creates an SQS queue and returns its URL.
func pbCreateQueue(t *testing.T, c *sqs.Client, name string) string {
	t.Helper()
	out, err := c.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
	require.NoError(t, err)
	return aws.ToString(out.QueueUrl)
}

// pbQueueARN constructs the SQS queue ARN used by JaisCloud (region us-east-1, account 000000000000).
func pbQueueARN(name string) string {
	return fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:%s", name)
}

// pbCreateFunction creates a minimal Lambda function (echo mock) for tests.
func pbCreateFunction(t *testing.T, ctx context.Context, c *awslambda.Client, name string) {
	t.Helper()
	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String(name),
		Runtime:      lambdatypes.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: []byte("fake-zip")},
	})
	require.NoError(t, err, "create function %q", name)
}
