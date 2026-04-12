package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEventBridgeClient(t *testing.T) *awseb.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return awseb.NewFromConfig(cfg, func(o *awseb.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint)
	})
}

// ─── Rule CRUD ────────────────────────────────────────────────────────────────

func TestEventBridge_PutRule_DescribeRule(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	putOut, err := eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("my-rule"),
		EventPattern: aws.String(`{"source":["aws.emr"]}`),
		State:        ebtypes.RuleStateEnabled,
		Description:  aws.String("test rule"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(putOut.RuleArn), "my-rule")

	desc, err := eb.DescribeRule(ctx, &awseb.DescribeRuleInput{Name: aws.String("my-rule")})
	require.NoError(t, err)
	assert.Equal(t, "my-rule", aws.ToString(desc.Name))
	assert.Equal(t, ebtypes.RuleStateEnabled, desc.State)
	assert.Equal(t, `{"source":["aws.emr"]}`, aws.ToString(desc.EventPattern))
	assert.Equal(t, "test rule", aws.ToString(desc.Description))
}

func TestEventBridge_ListRules(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	for _, name := range []string{"rule-alpha", "rule-beta", "other-rule"} {
		_, err := eb.PutRule(ctx, &awseb.PutRuleInput{Name: aws.String(name)})
		require.NoError(t, err)
	}

	listOut, err := eb.ListRules(ctx, &awseb.ListRulesInput{NamePrefix: aws.String("rule-")})
	require.NoError(t, err)
	names := make([]string, 0, len(listOut.Rules))
	for _, r := range listOut.Rules {
		names = append(names, aws.ToString(r.Name))
	}
	assert.Contains(t, names, "rule-alpha")
	assert.Contains(t, names, "rule-beta")
	assert.NotContains(t, names, "other-rule")
}

func TestEventBridge_DeleteRule_CascadesTargets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	// Create queue and rule.
	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("del-cascade-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:del-cascade-q"

	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{Name: aws.String("del-rule")})
	require.NoError(t, err)

	_, err = eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule: aws.String("del-rule"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)

	// Delete the rule.
	_, err = eb.DeleteRule(ctx, &awseb.DeleteRuleInput{Name: aws.String("del-rule")})
	require.NoError(t, err)

	// Rule is gone.
	_, err = eb.DescribeRule(ctx, &awseb.DescribeRuleInput{Name: aws.String("del-rule")})
	assert.Error(t, err)

	// Targets are also gone — ListTargetsByRule on deleted rule returns empty.
	listOut, err := eb.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{Rule: aws.String("del-rule")})
	require.NoError(t, err)
	assert.Empty(t, listOut.Targets)

	_ = qOut // queue created but not needed beyond this point
}

// ─── EnableRule / DisableRule ─────────────────────────────────────────────────

func TestEventBridge_EnableDisableRule(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:  aws.String("toggle-rule"),
		State: ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = eb.DisableRule(ctx, &awseb.DisableRuleInput{Name: aws.String("toggle-rule")})
	require.NoError(t, err)

	desc, err := eb.DescribeRule(ctx, &awseb.DescribeRuleInput{Name: aws.String("toggle-rule")})
	require.NoError(t, err)
	assert.Equal(t, ebtypes.RuleStateDisabled, desc.State)

	_, err = eb.EnableRule(ctx, &awseb.EnableRuleInput{Name: aws.String("toggle-rule")})
	require.NoError(t, err)

	desc2, err := eb.DescribeRule(ctx, &awseb.DescribeRuleInput{Name: aws.String("toggle-rule")})
	require.NoError(t, err)
	assert.Equal(t, ebtypes.RuleStateEnabled, desc2.State)
}

// ─── Targets ──────────────────────────────────────────────────────────────────

func TestEventBridge_PutTargets_ListTargets_RemoveTargets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("target-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:target-q"

	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{Name: aws.String("target-rule")})
	require.NoError(t, err)

	ptOut, err := eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule: aws.String("target-rule"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String(queueARN)},
			{Id: aws.String("t2"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), ptOut.FailedEntryCount)

	listOut, err := eb.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{Rule: aws.String("target-rule")})
	require.NoError(t, err)
	assert.Len(t, listOut.Targets, 2)

	_, err = eb.RemoveTargets(ctx, &awseb.RemoveTargetsInput{
		Rule: aws.String("target-rule"),
		Ids:  []string{"t1"},
	})
	require.NoError(t, err)

	listOut2, err := eb.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{Rule: aws.String("target-rule")})
	require.NoError(t, err)
	assert.Len(t, listOut2.Targets, 1)
	assert.Equal(t, "t2", aws.ToString(listOut2.Targets[0].Id))
}

// ─── PutEvents → SQS delivery ─────────────────────────────────────────────────

func TestEventBridge_PutEvents_DeliverToSQS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("events-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:events-q"

	// Rule matching source="custom.app"
	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("custom-rule"),
		EventPattern: aws.String(`{"source":["custom.app"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule: aws.String("custom-rule"),
		Targets: []ebtypes.Target{
			{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)

	evOut, err := eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:     aws.String("custom.app"),
				DetailType: aws.String("MyEvent"),
				Detail:     aws.String(`{"key":"value"}`),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), evOut.FailedEntryCount)
	require.Len(t, evOut.Entries, 1)
	assert.NotEmpty(t, aws.ToString(evOut.Entries[0].EventId))

	// Message should land in SQS.
	rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            qOut.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, rOut.Messages, 1)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(rOut.Messages[0].Body)), &envelope))
	assert.Equal(t, "custom.app", envelope["source"])
	assert.Equal(t, "MyEvent", envelope["detail-type"])
}

func TestEventBridge_DisabledRule_DoesNotDeliver(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("disabled-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:disabled-q"

	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:  aws.String("disabled-rule"),
		State: ebtypes.RuleStateDisabled,
	})
	require.NoError(t, err)

	_, err = eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule: aws.String("disabled-rule"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)

	_, err = eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("any.source"), DetailType: aws.String("Evt"), Detail: aws.String(`{}`)},
		},
	})
	require.NoError(t, err)

	// Queue should be empty — disabled rule must not deliver.
	rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            qOut.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, rOut.Messages, "disabled rule must not deliver events")
}

// ─── Pattern matching ─────────────────────────────────────────────────────────

func TestEventBridge_PatternMatching_SourceFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("pattern-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:pattern-q"

	// Only match source="aws.emr"
	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("emr-only-rule"),
		EventPattern: aws.String(`{"source":["aws.emr"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	_, err = eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:    aws.String("emr-only-rule"),
		Targets: []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(queueARN)}},
	})
	require.NoError(t, err)

	// Send a non-matching event.
	_, err = eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("aws.s3"), DetailType: aws.String("S3Event"), Detail: aws.String(`{}`)},
		},
	})
	require.NoError(t, err)

	noMatch, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: qOut.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, noMatch.Messages, "non-matching source must not deliver")

	// Send a matching event.
	_, err = eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("aws.emr"), DetailType: aws.String("EMR Step Status Change"), Detail: aws.String(`{}`)},
		},
	})
	require.NoError(t, err)

	match, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: qOut.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
	})
	require.NoError(t, err)
	require.Len(t, match.Messages, 1, "matching source must deliver")

	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(match.Messages[0].Body)), &envelope))
	assert.Equal(t, "aws.emr", envelope["source"])
}

func TestEventBridge_PatternMatching_DetailFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("detail-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:detail-q"

	// Match only events where detail.state == "COMPLETED"
	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("completed-rule"),
		EventPattern: aws.String(`{"detail":{"state":["COMPLETED"]}}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	_, err = eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:    aws.String("completed-rule"),
		Targets: []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(queueARN)}},
	})
	require.NoError(t, err)

	// FAILED event should not match.
	_, err = eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("aws.emr"), DetailType: aws.String("State"), Detail: aws.String(`{"state":"FAILED"}`)},
		},
	})
	require.NoError(t, err)
	noMatch, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: qOut.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, noMatch.Messages)

	// COMPLETED event should match.
	_, err = eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("aws.emr"), DetailType: aws.String("State"), Detail: aws.String(`{"state":"COMPLETED"}`)},
		},
	})
	require.NoError(t, err)
	match, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: qOut.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
	})
	require.NoError(t, err)
	require.Len(t, match.Messages, 1)
}

func TestEventBridge_MultipleTargets_AllReceive(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	var queueURLs []string
	var queueARNs []string
	for _, name := range []string{"multi-q-1", "multi-q-2"} {
		qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)})
		require.NoError(t, err)
		queueURLs = append(queueURLs, aws.ToString(qOut.QueueUrl))
		queueARNs = append(queueARNs, "arn:aws:sqs:us-east-1:000000000000:"+name)
	}

	_, err := eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:  aws.String("multi-target-rule"),
		State: ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule: aws.String("multi-target-rule"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String(queueARNs[0])},
			{Id: aws.String("t2"), Arn: aws.String(queueARNs[1])},
		},
	})
	require.NoError(t, err)

	_, err = eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("test.source"), DetailType: aws.String("TestEvent"), Detail: aws.String(`{}`)},
		},
	})
	require.NoError(t, err)

	for i, qURL := range queueURLs {
		rOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(qURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
		})
		require.NoError(t, err)
		assert.Len(t, rOut.Messages, 1, "queue %d (%s) should receive the event", i+1, qURL)
	}
}
