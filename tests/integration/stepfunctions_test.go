package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDefinition = `{"StartAt":"S1","States":{"S1":{"Type":"Pass","End":true}}}`
const testRoleARN = "arn:aws:iam::000000000000:role/sfn-test"

func sfnClient(t *testing.T) *awssfn.Client {
	t.Helper()
	cfg := newAWSConfig(t)
	return awssfn.NewFromConfig(cfg, func(o *awssfn.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint())
	})
}

func sfnName(t *testing.T) string {
	return fmt.Sprintf("sm-%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano()%100000)
}

// ─── State Machine CRUD ───────────────────────────────────────────────────────

func TestSFN_CreateStateMachine(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	out, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String("test-sm"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	assert.Contains(t, *out.StateMachineArn, "arn:aws:states:")
	assert.Contains(t, *out.StateMachineArn, "test-sm")
}

func TestSFN_CreateStateMachine_AlreadyExists(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	_, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	_, err = client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineAlreadyExists")
}

func TestSFN_DescribeStateMachine(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Equal(t, name, *descOut.Name)
	assert.Equal(t, testDefinition, *descOut.Definition)
	assert.Equal(t, sfntypes.StateMachineStatusActive, descOut.Status)
	assert.Equal(t, sfntypes.StateMachineTypeStandard, descOut.Type)
}

func TestSFN_UpdateStateMachine_GeneratesNewRevisionID(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	desc1, err := client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	oldRevision := *desc1.RevisionId

	newDef := `{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`
	_, err = client.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
		Definition:      aws.String(newDef),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.NotEqual(t, oldRevision, *desc2.RevisionId)
	assert.Equal(t, newDef, *desc2.Definition)
}

func TestSFN_DeleteStateMachine(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	_, err = client.DeleteStateMachine(ctx, &awssfn.DeleteStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	_, err = client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineDoesNotExist")
}

func TestSFN_ListStateMachines_Pagination(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
			Name:       aws.String(fmt.Sprintf("sm-page-%02d", i)),
			Definition: aws.String(testDefinition),
			RoleArn:    aws.String(testRoleARN),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{
		MaxResults: aws.Int32(3),
	})
	require.NoError(t, err)
	require.Len(t, page1.StateMachines, 3)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{
		MaxResults: aws.Int32(3),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.StateMachines, 2)
	assert.Nil(t, page2.NextToken)
}

// ─── Execution Lifecycle ──────────────────────────────────────────────────────

func TestSFN_ExecutionLifecycle_LiteMode_InstantSuccess(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	input := `{"key":"value"}`
	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)
	assert.Contains(t, *startOut.ExecutionArn, "execution:")

	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	assert.Equal(t, input, *descOut.Input)
	assert.Equal(t, input, *descOut.Output) // passthrough in lite mode
	assert.NotNil(t, descOut.StopDate)
}

func TestSFN_StartExecution_OutputEqualsInput(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	input := `{"x":42,"nested":{"y":true}}`
	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, input, *descOut.Output)
}

func TestSFN_StartExecution_DuplicateName_SameInput_Idempotent(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	execName := "my-exec-idem"
	input := `{}`

	out1, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	out2, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(input),
	})
	require.NoError(t, err)
	assert.Equal(t, *out1.ExecutionArn, *out2.ExecutionArn)
}

func TestSFN_StopExecution_AlreadyTerminal_NoOp(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	// Stop a SUCCEEDED execution — should be no-op (not error)
	_, err = client.StopExecution(ctx, &awssfn.StopExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)

	// Status should still be SUCCEEDED
	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
}

func TestSFN_GetExecutionHistory_LiteMode_TwoEvents(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	histOut, err := client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.Len(t, histOut.Events, 2)
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionStarted, histOut.Events[0].Type)
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionSucceeded, histOut.Events[1].Type)
}

func TestSFN_ListExecutions(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(fmt.Sprintf(`{"i":%d}`, i)),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Executions, 3)
}

// ─── Versions & Aliases ───────────────────────────────────────────────────────

func TestSFN_VersionsAndAliases(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := *createOut.StateMachineArn

	// Publish version 1
	v1Out, err := client.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: aws.String(smARN),
		Description:     aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Contains(t, *v1Out.StateMachineVersionArn, ":1")

	// Update and publish version 2
	_, err = client.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: aws.String(smARN),
		Definition:      aws.String(`{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`),
	})
	require.NoError(t, err)

	v2Out, err := client.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: aws.String(smARN),
		Description:     aws.String("v2"),
	})
	require.NoError(t, err)
	assert.Contains(t, *v2Out.StateMachineVersionArn, ":2")

	// Create alias with 50/50 routing
	aliasOut, err := client.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("prod"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{
			{StateMachineVersionArn: v1Out.StateMachineVersionArn, Weight: 50},
			{StateMachineVersionArn: v2Out.StateMachineVersionArn, Weight: 50},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, *aliasOut.StateMachineAliasArn, ":prod")

	// List versions
	versionsOut, err := client.ListStateMachineVersions(ctx, &awssfn.ListStateMachineVersionsInput{
		StateMachineArn: aws.String(smARN),
	})
	require.NoError(t, err)
	assert.Len(t, versionsOut.StateMachineVersions, 2)
}

func TestSFN_DeleteVersion_ReferencedByAlias_Error(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	v1Out, err := client.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	_, err = client.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("alias-v1"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{
			{StateMachineVersionArn: v1Out.StateMachineVersionArn, Weight: 100},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteStateMachineVersion(ctx, &awssfn.DeleteStateMachineVersionInput{
		StateMachineVersionArn: v1Out.StateMachineVersionArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConflictException")
}

// ─── Activities ───────────────────────────────────────────────────────────────

func TestSFN_CreateActivity(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	out, err := client.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String("my-activity"),
	})
	require.NoError(t, err)
	assert.Contains(t, *out.ActivityArn, "activity:my-activity")
}

func TestSFN_GetActivityTask_Empty(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	createOut, err := client.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String("empty-act"),
	})
	require.NoError(t, err)

	taskOut, err := client.GetActivityTask(ctx, &awssfn.GetActivityTaskInput{
		ActivityArn: createOut.ActivityArn,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(taskOut.TaskToken))
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestSFN_Tags_AddRemoveList(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := *createOut.StateMachineArn

	_, err = client.TagResource(ctx, &awssfn.TagResourceInput{
		ResourceArn: aws.String(smARN),
		Tags: []sfntypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &awssfn.ListTagsForResourceInput{
		ResourceArn: aws.String(smARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	_, err = client.UntagResource(ctx, &awssfn.UntagResourceInput{
		ResourceArn: aws.String(smARN),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &awssfn.ListTagsForResourceInput{
		ResourceArn: aws.String(smARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
}

// ─── Express Execution ────────────────────────────────────────────────────────

func TestSFN_StartSyncExecution_ExpressOnly(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	// STANDARD machine → error
	stdOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name + "-std"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	_, err = client.StartSyncExecution(ctx, &awssfn.StartSyncExecutionInput{
		StateMachineArn: stdOut.StateMachineArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineTypeNotSupported")

	// EXPRESS machine → success
	expOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name + "-exp"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)

	syncOut, err := client.StartSyncExecution(ctx, &awssfn.StartSyncExecutionInput{
		StateMachineArn: expOut.StateMachineArn,
		Input:           aws.String(`{"hello":"world"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.SyncExecutionStatusSucceeded, syncOut.Status)
}
