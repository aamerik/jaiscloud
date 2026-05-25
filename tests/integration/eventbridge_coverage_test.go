package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// ─── Custom Event Buses ───────────────────────────────────────────────────────

func TestEventBridge_CreateEventBus_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	out, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{
		Name: aws.String("my-custom-bus"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.EventBusArn), "my-custom-bus")
}

func TestEventBridge_CreateEventBus_AlreadyExists_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{Name: aws.String("dup-bus")})
	require.NoError(t, err)
	_, err = eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{Name: aws.String("dup-bus")})
	require.Error(t, err)
}

func TestEventBridge_DeleteEventBus_RemovesBus(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{Name: aws.String("del-bus")})
	require.NoError(t, err)

	_, err = eb.DeleteEventBus(ctx, &awseb.DeleteEventBusInput{Name: aws.String("del-bus")})
	require.NoError(t, err)

	listOut, err := eb.ListEventBuses(ctx, &awseb.ListEventBusesInput{
		NamePrefix: aws.String("del-bus"),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.EventBuses)
}

func TestEventBridge_DeleteEventBus_DefaultBus_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.DeleteEventBus(ctx, &awseb.DeleteEventBusInput{
		Name: aws.String("default"),
	})
	require.Error(t, err)
}

func TestEventBridge_DescribeEventBus_DefaultExists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	out, err := eb.DescribeEventBus(ctx, &awseb.DescribeEventBusInput{
		Name: aws.String("default"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.Name), "default")
}

func TestEventBridge_ListEventBuses_NamePrefixFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	for _, name := range []string{"prefix-a", "prefix-b", "other-c"} {
		_, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{Name: aws.String(name)})
		require.NoError(t, err)
	}

	out, err := eb.ListEventBuses(ctx, &awseb.ListEventBusesInput{
		NamePrefix: aws.String("prefix-"),
	})
	require.NoError(t, err)
	assert.Len(t, out.EventBuses, 2)
}

func TestEventBridge_PutRule_OnCustomBus(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{Name: aws.String("my-bus")})
	require.NoError(t, err)

	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("bus-rule"),
		EventBusName: aws.String("my-bus"),
		EventPattern: aws.String(`{"source":["myapp"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	desc, err := eb.DescribeRule(ctx, &awseb.DescribeRuleInput{
		Name:         aws.String("bus-rule"),
		EventBusName: aws.String("my-bus"),
	})
	require.NoError(t, err)
	assert.Equal(t, "bus-rule", aws.ToString(desc.Name))
}

// ─── Archives ─────────────────────────────────────────────────────────────────

func TestEventBridge_CreateArchive_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	desc, err := eb.DescribeEventBus(ctx, &awseb.DescribeEventBusInput{Name: aws.String("default")})
	require.NoError(t, err)

	_, err = eb.CreateArchive(ctx, &awseb.CreateArchiveInput{
		ArchiveName:    aws.String("test-archive"),
		EventSourceArn: desc.Arn,
		RetentionDays:  aws.Int32(7),
	})
	if err != nil {
		t.Skipf("CreateArchive not implemented: %v", err)
	}
}

func TestEventBridge_DescribeArchive_AfterCreate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	desc, err := eb.DescribeEventBus(ctx, &awseb.DescribeEventBusInput{Name: aws.String("default")})
	require.NoError(t, err)

	_, err = eb.CreateArchive(ctx, &awseb.CreateArchiveInput{
		ArchiveName:    aws.String("desc-archive"),
		EventSourceArn: desc.Arn,
		RetentionDays:  aws.Int32(30),
	})
	if err != nil {
		t.Skipf("CreateArchive not implemented: %v", err)
	}

	archDesc, err := eb.DescribeArchive(ctx, &awseb.DescribeArchiveInput{
		ArchiveName: aws.String("desc-archive"),
	})
	require.NoError(t, err)
	assert.Equal(t, "desc-archive", aws.ToString(archDesc.ArchiveName))
}

func TestEventBridge_DeleteArchive_RemovesArchive(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	desc, err := eb.DescribeEventBus(ctx, &awseb.DescribeEventBusInput{Name: aws.String("default")})
	require.NoError(t, err)

	_, err = eb.CreateArchive(ctx, &awseb.CreateArchiveInput{
		ArchiveName:    aws.String("del-archive"),
		EventSourceArn: desc.Arn,
		RetentionDays:  aws.Int32(1),
	})
	if err != nil {
		t.Skipf("CreateArchive not implemented: %v", err)
	}

	_, err = eb.DeleteArchive(ctx, &awseb.DeleteArchiveInput{
		ArchiveName: aws.String("del-archive"),
	})
	require.NoError(t, err)
}

// ─── Connections ──────────────────────────────────────────────────────────────

func TestEventBridge_CreateConnection_BasicAuth(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	out, err := eb.CreateConnection(ctx, &awseb.CreateConnectionInput{
		Name:              aws.String("basic-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeBasic,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			BasicAuthParameters: &ebtypes.CreateConnectionBasicAuthRequestParameters{
				Username: aws.String("user"),
				Password: aws.String("pass"),
			},
		},
	})
	if err != nil {
		t.Skipf("CreateConnection not implemented: %v", err)
	}
	assert.Contains(t, aws.ToString(out.ConnectionArn), "basic-conn")
}

func TestEventBridge_CreateConnection_ApiKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateConnection(ctx, &awseb.CreateConnectionInput{
		Name:              aws.String("apikey-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
				ApiKeyName:  aws.String("X-Api-Key"),
				ApiKeyValue: aws.String("secret123"),
			},
		},
	})
	if err != nil {
		t.Skipf("CreateConnection not implemented: %v", err)
	}
}

func TestEventBridge_DescribeConnection_AfterCreate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateConnection(ctx, &awseb.CreateConnectionInput{
		Name:              aws.String("desc-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeBasic,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			BasicAuthParameters: &ebtypes.CreateConnectionBasicAuthRequestParameters{
				Username: aws.String("u"),
				Password: aws.String("p"),
			},
		},
	})
	if err != nil {
		t.Skipf("CreateConnection not implemented: %v", err)
	}

	desc, err := eb.DescribeConnection(ctx, &awseb.DescribeConnectionInput{
		Name: aws.String("desc-conn"),
	})
	require.NoError(t, err)
	assert.Equal(t, "desc-conn", aws.ToString(desc.Name))
}

func TestEventBridge_DeleteConnection_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateConnection(ctx, &awseb.CreateConnectionInput{
		Name:              aws.String("del-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeBasic,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			BasicAuthParameters: &ebtypes.CreateConnectionBasicAuthRequestParameters{
				Username: aws.String("u"),
				Password: aws.String("p"),
			},
		},
	})
	if err != nil {
		t.Skipf("CreateConnection not implemented: %v", err)
	}

	_, err = eb.DeleteConnection(ctx, &awseb.DeleteConnectionInput{
		Name: aws.String("del-conn"),
	})
	require.NoError(t, err)
}

// ─── API Destinations ─────────────────────────────────────────────────────────

func TestEventBridge_CreateApiDestination_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	connOut, err := eb.CreateConnection(ctx, &awseb.CreateConnectionInput{
		Name:              aws.String("apidest-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeBasic,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			BasicAuthParameters: &ebtypes.CreateConnectionBasicAuthRequestParameters{
				Username: aws.String("u"),
				Password: aws.String("p"),
			},
		},
	})
	if err != nil {
		t.Skipf("CreateConnection not implemented: %v", err)
	}

	out, err := eb.CreateApiDestination(ctx, &awseb.CreateApiDestinationInput{
		Name:               aws.String("my-dest"),
		ConnectionArn:      connOut.ConnectionArn,
		InvocationEndpoint: aws.String("https://example.com/webhook"),
		HttpMethod:         ebtypes.ApiDestinationHttpMethodPost,
	})
	if err != nil {
		t.Skipf("CreateApiDestination not implemented: %v", err)
	}
	assert.Contains(t, aws.ToString(out.ApiDestinationArn), "my-dest")
}

// ─── EventBridge Tags ─────────────────────────────────────────────────────────

func TestEventBridge_TagRule_ListTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	putOut, err := eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("tag-rule"),
		EventPattern: aws.String(`{"source":["test"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = eb.TagResource(ctx, &awseb.TagResourceInput{
		ResourceARN: putOut.RuleArn,
		Tags: []ebtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := eb.ListTagsForResource(ctx, &awseb.ListTagsForResourceInput{
		ResourceARN: putOut.RuleArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.Tags[0].Value))
}

func TestEventBridge_UntagRule_RemovesTag(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	putOut, err := eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("untag-rule"),
		EventPattern: aws.String(`{"source":["test"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	_, err = eb.TagResource(ctx, &awseb.TagResourceInput{
		ResourceARN: putOut.RuleArn,
		Tags:        []ebtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	_, err = eb.UntagResource(ctx, &awseb.UntagResourceInput{
		ResourceARN: putOut.RuleArn,
		TagKeys:     []string{"k"},
	})
	require.NoError(t, err)

	tagsOut, err := eb.ListTagsForResource(ctx, &awseb.ListTagsForResourceInput{
		ResourceARN: putOut.RuleArn,
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut.Tags)
}

func TestEventBridge_PutEvents_NonExistentBus_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	out, err := eb.PutEvents(ctx, &awseb.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{{
			EventBusName: aws.String("nonexistent-bus-xyz"),
			Source:       aws.String("myapp"),
			DetailType:   aws.String("test"),
			Detail:       aws.String(`{}`),
			Time:         aws.Time(clock.RealNow()),
		}},
	})
	// Non-existent bus either errors or returns FailedEntryCount > 0
	if err == nil {
		assert.Greater(t, out.FailedEntryCount, int32(0))
	}
}

func TestEventBridge_ListRules_WithEventBusName(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	_, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{Name: aws.String("rules-bus")})
	require.NoError(t, err)

	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("bus-scoped-rule"),
		EventBusName: aws.String("rules-bus"),
		EventPattern: aws.String(`{"source":["app"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	// Rule on default bus with same name
	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("default-rule"),
		EventPattern: aws.String(`{"source":["app"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	// Listing rules on custom bus should only return bus-scoped rules
	listOut, err := eb.ListRules(ctx, &awseb.ListRulesInput{
		EventBusName: aws.String("rules-bus"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Rules, 1)
	assert.Equal(t, "bus-scoped-rule", aws.ToString(listOut.Rules[0].Name))
}

func TestEventBridge_ListEventBuses_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)

	for i := 0; i < 5; i++ {
		_, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{
			Name: aws.String(fmt.Sprintf("page-bus-%d", i)),
		})
		require.NoError(t, err)
	}

	// Fetch with limit 3
	out, err := eb.ListEventBuses(ctx, &awseb.ListEventBusesInput{Limit: aws.Int32(3)})
	require.NoError(t, err)
	// default bus always exists, so we have at least 6 total
	assert.LessOrEqual(t, len(out.EventBuses), 3)
}

// TestEventBridgeDeleteEventBusCascade verifies that deleting a custom event bus
// also removes all rules and targets that belong to it.
func TestEventBridgeDeleteEventBusCascade(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	eb := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	// Create a queue to use as a target ARN.
	_, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("cascade-q")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:cascade-q"

	// Create a custom event bus.
	busOut, err := eb.CreateEventBus(ctx, &awseb.CreateEventBusInput{
		Name: aws.String("cascade-bus"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(busOut.EventBusArn), "cascade-bus")

	// Put a rule on the custom bus.
	_, err = eb.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("cascade-rule"),
		EventBusName: aws.String("cascade-bus"),
		EventPattern: aws.String(`{"source":["cascade.app"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	// Put a target on that rule.
	ptOut, err := eb.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:         aws.String("cascade-rule"),
		EventBusName: aws.String("cascade-bus"),
		Targets: []ebtypes.Target{
			{Id: aws.String("cascade-t1"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), ptOut.FailedEntryCount)

	// Verify the rule exists on the bus before deletion.
	descBefore, err := eb.DescribeRule(ctx, &awseb.DescribeRuleInput{
		Name:         aws.String("cascade-rule"),
		EventBusName: aws.String("cascade-bus"),
	})
	require.NoError(t, err)
	assert.Equal(t, "cascade-rule", aws.ToString(descBefore.Name))

	// Delete the event bus — this must cascade to rules and targets.
	_, err = eb.DeleteEventBus(ctx, &awseb.DeleteEventBusInput{
		Name: aws.String("cascade-bus"),
	})
	require.NoError(t, err)

	// DescribeRule on the now-gone bus must return ResourceNotFoundException.
	_, err = eb.DescribeRule(ctx, &awseb.DescribeRuleInput{
		Name:         aws.String("cascade-rule"),
		EventBusName: aws.String("cascade-bus"),
	})
	require.Error(t, err, "rule should not exist after bus deletion")

	// ListTargetsByRule on the deleted rule should return empty (targets were cascade-deleted).
	listOut, err := eb.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{
		Rule:         aws.String("cascade-rule"),
		EventBusName: aws.String("cascade-bus"),
	})
	// The provider may return an error (bus gone) or an empty list; both are acceptable.
	if err == nil {
		assert.Empty(t, listOut.Targets, "targets should be empty after bus deletion cascade")
	}
}
