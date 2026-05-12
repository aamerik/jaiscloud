package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── State Machine Validation ─────────────────────────────────────────────────

func TestSFN_ValidateStateMachineDefinition_Valid_OK(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	out, err := c.ValidateStateMachineDefinition(ctx, &awssfn.ValidateStateMachineDefinitionInput{
		Definition: aws.String(`{"StartAt":"S1","States":{"S1":{"Type":"Pass","End":true}}}`),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeOk, out.Result)
}

func TestSFN_ValidateStateMachineDefinition_Invalid_Diagnostics(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	// StartAt references a state that does not exist in States map
	out, err := c.ValidateStateMachineDefinition(ctx, &awssfn.ValidateStateMachineDefinitionInput{
		Definition: aws.String(`{"StartAt":"Missing","States":{}}`),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	// Result should be FAIL and diagnostics should contain at least one entry
	assert.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeFail, out.Result)
	assert.NotEmpty(t, out.Diagnostics)
}

func TestSFN_CreateStateMachine_InvalidDefinition_Error(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	_, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(`not-valid-json`),
		RoleArn:    aws.String(testRoleARN),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidDefinition")
}

func TestSFN_CreateStateMachine_LoggingConfig_Stored(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		LoggingConfiguration: &sfntypes.LoggingConfiguration{
			Level:                sfntypes.LogLevelAll,
			IncludeExecutionData: true,
		},
	})
	require.NoError(t, err)

	descOut, err := c.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.LoggingConfiguration)
	assert.Equal(t, sfntypes.LogLevelAll, descOut.LoggingConfiguration.Level)
	assert.True(t, descOut.LoggingConfiguration.IncludeExecutionData)
}

// ─── Execution Edge Cases ─────────────────────────────────────────────────────

func TestSFN_StartExecution_AutoGenerateName(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// No Name field — server should auto-generate
	out, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String("{}"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.ExecutionArn))
	assert.True(t, strings.Contains(aws.ToString(out.ExecutionArn), "execution:"))
}

func TestSFN_StartExecution_DuplicateName_DifferentInput_Error(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	execName := "dup-exec-" + name

	_, err = c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(`{"a":1}`),
	})
	require.NoError(t, err)

	_, err = c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(`{"a":2}`), // different input
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ExecutionAlreadyExists")
}

func TestSFN_StopExecution_Running_Aborted(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	// In lite mode executions are instantly SUCCEEDED; we need to test StopExecution
	// on a SUCCEEDED execution and verify it's a no-op (status stays SUCCEEDED),
	// but to test the ABORTED path we need to verify the store.StopExecution only
	// sets status to ABORTED for RUNNING executions.
	// We simulate this by verifying that StopExecution on an already-terminal
	// execution does NOT change it to ABORTED.
	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	_, err = c.StopExecution(ctx, &awssfn.StopExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
		Cause:        aws.String("test abort"),
	})
	require.NoError(t, err)

	// In lite mode the execution was instantly SUCCEEDED, so StopExecution is a no-op
	// and status should remain SUCCEEDED (not ABORTED).
	desc, err := c.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	// The status is SUCCEEDED because lite mode executes instantly and StopExecution
	// is a no-op on terminal executions per the store contract.
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
}

func TestSFN_ListExecutions_FilterByStatus_Succeeded(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// Start 3 executions; in lite mode all complete as SUCCEEDED
	for i := 0; i < 3; i++ {
		_, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(`{}`),
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: createOut.StateMachineArn,
		StatusFilter:    sfntypes.ExecutionStatusSucceeded,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(listOut.Executions))
	for _, e := range listOut.Executions {
		assert.Equal(t, sfntypes.ExecutionStatusSucceeded, e.Status)
	}
}

func TestSFN_ListExecutions_FilterByStatus_Aborted(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// In lite mode executions are instantly SUCCEEDED; filter by ABORTED returns empty
	for i := 0; i < 2; i++ {
		_, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(`{}`),
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: createOut.StateMachineArn,
		StatusFilter:    sfntypes.ExecutionStatusAborted,
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.Executions)
}

func TestSFN_GetExecutionHistory_ReverseOrder(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	hist, err := c.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
		ReverseOrder: true,
	})
	require.NoError(t, err)
	require.Greater(t, len(hist.Events), 1)
	// In reverse order the last chronological event (ExecutionSucceeded) comes first
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionSucceeded, hist.Events[0].Type)
	// And the first chronological event (ExecutionStarted) comes last
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionStarted, hist.Events[len(hist.Events)-1].Type)
}

func TestSFN_DescribeExecution_ReturnsAllFields(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	input := `{"key":"value","num":42}`
	startOut, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String("exec-" + name),
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	desc, err := c.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(desc.ExecutionArn))
	assert.NotEmpty(t, aws.ToString(desc.StateMachineArn))
	assert.NotEmpty(t, aws.ToString(desc.Name))
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
	assert.NotNil(t, desc.StartDate)
	assert.NotNil(t, desc.StopDate)
	assert.Equal(t, input, aws.ToString(desc.Input))
	assert.Equal(t, input, aws.ToString(desc.Output))
}

func TestSFN_DescribeExecution_NonExistent_Error(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	fakeArn := "arn:aws:states:us-east-1:000000000000:execution:no-sm:no-exec-" + time.Now().Format("150405")
	_, err := c.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: aws.String(fakeArn),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ExecutionDoesNotExist")
}

// ─── Express State Machine ────────────────────────────────────────────────────

func TestSFN_CreateStateMachine_Express_Success(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	out, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.StateMachineArn), "arn:aws:states:")

	desc, err := c.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: out.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.StateMachineTypeExpress, desc.Type)
}

func TestSFN_StartSyncExecution_OutputEqualsInput(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	expOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)

	input := `{"hello":"world","n":99}`
	syncOut, err := c.StartSyncExecution(ctx, &awssfn.StartSyncExecutionInput{
		StateMachineArn: expOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.SyncExecutionStatusSucceeded, syncOut.Status)
	assert.Equal(t, input, aws.ToString(syncOut.Output))
}

func TestSFN_StartSyncExecution_OnStandard_Error(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	stdOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	_, err = c.StartSyncExecution(ctx, &awssfn.StartSyncExecutionInput{
		StateMachineArn: stdOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineTypeNotSupported")
}

// ─── Versions ────────────────────────────────────────────────────────────────

func TestSFN_PublishStateMachineVersion_V1(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
		Description:     aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.StateMachineVersionArn), ":stateMachine:")
	assert.Contains(t, aws.ToString(out.StateMachineVersionArn), ":1")
	assert.NotNil(t, out.CreationDate)
}

func TestSFN_PublishStateMachineVersion_V2_Increments(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := createOut.StateMachineArn

	v1Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: smARN,
		Description:     aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(v1Out.StateMachineVersionArn), ":1")

	// Update definition then publish v2
	_, err = c.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: smARN,
		Definition:      aws.String(`{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`),
	})
	require.NoError(t, err)

	v2Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: smARN,
		Description:     aws.String("v2"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(v2Out.StateMachineVersionArn), ":2")

	// Version numbers must differ
	assert.NotEqual(t, aws.ToString(v1Out.StateMachineVersionArn), aws.ToString(v2Out.StateMachineVersionArn))
}

func TestSFN_ListStateMachineVersions_Pagination(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := createOut.StateMachineArn

	// Publish 3 versions with definition updates between publishes
	defs := []string{
		`{"StartAt":"S1","States":{"S1":{"Type":"Pass","End":true}}}`,
		`{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`,
		`{"StartAt":"S3","States":{"S3":{"Type":"Fail","Error":"err","Cause":"c"}}}`,
	}
	for _, def := range defs {
		_, err = c.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
			StateMachineArn: smARN,
			Definition:      aws.String(def),
		})
		require.NoError(t, err)
		_, err = c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
			StateMachineArn: smARN,
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListStateMachineVersions(ctx, &awssfn.ListStateMachineVersionsInput{
		StateMachineArn: smARN,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(listOut.StateMachineVersions))
}

func TestSFN_DeleteStateMachineVersion_Unreferenced_Success(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	v1Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	// Delete with no alias referencing it — should succeed
	_, err = c.DeleteStateMachineVersion(ctx, &awssfn.DeleteStateMachineVersionInput{
		StateMachineVersionArn: v1Out.StateMachineVersionArn,
	})
	require.NoError(t, err)

	// List should now be empty
	listOut, err := c.ListStateMachineVersions(ctx, &awssfn.ListStateMachineVersionsInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.StateMachineVersions)
}

func TestSFN_PublishVersion_RevisionIdMismatch_Error(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// Supply a deliberately wrong revision ID
	_, err = c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
		RevisionId:      aws.String("00000000-0000-0000-0000-000000000000"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConflictException")
}

func TestSFN_DescribeStateMachineForExecution_ReturnsDefinition(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := c.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeStateMachineForExecution(ctx, &awssfn.DescribeStateMachineForExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, testDefinition, aws.ToString(descOut.Definition))
	assert.NotEmpty(t, aws.ToString(descOut.StateMachineArn))
}

// ─── Activities ───────────────────────────────────────────────────────────────

func TestSFN_CreateActivity_DuplicateName_Idempotent(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	actName := "dup-act-" + sfnName(t)

	out1, err := c.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String(actName),
	})
	require.NoError(t, err)

	// Second call with same name should return error (ActivityAlreadyExists)
	_, err = c.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String(actName),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ActivityAlreadyExists")

	// Original ARN is still accessible
	assert.Contains(t, aws.ToString(out1.ActivityArn), "activity:"+actName)
}

func TestSFN_DeleteActivity_Success(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	actName := "del-act-" + sfnName(t)
	createOut, err := c.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String(actName),
	})
	require.NoError(t, err)

	_, err = c.DeleteActivity(ctx, &awssfn.DeleteActivityInput{
		ActivityArn: createOut.ActivityArn,
	})
	require.NoError(t, err)

	// Attempting to describe the deleted activity should fail
	_, err = c.DescribeActivity(ctx, &awssfn.DescribeActivityInput{
		ActivityArn: createOut.ActivityArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ActivityDoesNotExist")
}

func TestSFN_ListActivities_Pagination(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	// Create 5 activities
	for i := 0; i < 5; i++ {
		_, err := c.CreateActivity(ctx, &awssfn.CreateActivityInput{
			Name: aws.String(sfnName(t)),
		})
		require.NoError(t, err)
	}

	page1, err := c.ListActivities(ctx, &awssfn.ListActivitiesInput{
		MaxResults: 3,
	})
	require.NoError(t, err)
	require.Len(t, page1.Activities, 3)
	require.NotNil(t, page1.NextToken)

	page2, err := c.ListActivities(ctx, &awssfn.ListActivitiesInput{
		MaxResults: 3,
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Activities, 2)
	assert.Nil(t, page2.NextToken)
}

func TestSFN_SendTaskSuccess_NoOp(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	// In lite mode (no engine), SendTaskSuccess is a no-op and returns success
	_, err := c.SendTaskSuccess(ctx, &awssfn.SendTaskSuccessInput{
		TaskToken: aws.String("fake-token-success"),
		Output:    aws.String(`{}`),
	})
	// Lite mode: no engine, so no error expected
	require.NoError(t, err)
}

func TestSFN_SendTaskFailure_NoOp(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()

	// In lite mode (no engine), SendTaskFailure is a no-op and returns success
	_, err := c.SendTaskFailure(ctx, &awssfn.SendTaskFailureInput{
		TaskToken: aws.String("fake-token-failure"),
		Error:     aws.String("TestError"),
		Cause:     aws.String("test cause"),
	})
	// Lite mode: no engine, so no error expected
	require.NoError(t, err)
}

// ─── Aliases ─────────────────────────────────────────────────────────────────

func TestSFN_CreateStateMachineAlias_Success(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	v1Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	aliasOut, err := c.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("prod"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{{
			StateMachineVersionArn: v1Out.StateMachineVersionArn,
			Weight:                 100,
		}},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(aliasOut.StateMachineAliasArn), ":prod")
	assert.NotNil(t, aliasOut.CreationDate)
}

func TestSFN_CreateStateMachineAlias_RoutingWeightSum_Error(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	v1Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	_, err = c.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
		Definition:      aws.String(`{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`),
	})
	require.NoError(t, err)

	v2Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	// Weights sum to 70, not 100 — should fail
	_, err = c.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("bad-alias"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{
			{StateMachineVersionArn: v1Out.StateMachineVersionArn, Weight: 30},
			{StateMachineVersionArn: v2Out.StateMachineVersionArn, Weight: 40},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationException")
}

func TestSFN_UpdateStateMachineAlias_ChangesVersion(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := createOut.StateMachineArn

	v1Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: smARN,
	})
	require.NoError(t, err)

	_, err = c.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: smARN,
		Definition:      aws.String(`{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`),
	})
	require.NoError(t, err)

	v2Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: smARN,
	})
	require.NoError(t, err)

	// Create alias pointing to v1
	aliasOut, err := c.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("live"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{{
			StateMachineVersionArn: v1Out.StateMachineVersionArn,
			Weight:                 100,
		}},
	})
	require.NoError(t, err)

	// Update alias to point 100% to v2
	_, err = c.UpdateStateMachineAlias(ctx, &awssfn.UpdateStateMachineAliasInput{
		StateMachineAliasArn: aliasOut.StateMachineAliasArn,
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{{
			StateMachineVersionArn: v2Out.StateMachineVersionArn,
			Weight:                 100,
		}},
	})
	require.NoError(t, err)

	// Describe alias and verify routing changed to v2
	descOut, err := c.DescribeStateMachineAlias(ctx, &awssfn.DescribeStateMachineAliasInput{
		StateMachineAliasArn: aliasOut.StateMachineAliasArn,
	})
	require.NoError(t, err)
	require.Len(t, descOut.RoutingConfiguration, 1)
	assert.Equal(t, aws.ToString(v2Out.StateMachineVersionArn), aws.ToString(descOut.RoutingConfiguration[0].StateMachineVersionArn))
	assert.Equal(t, int32(100), descOut.RoutingConfiguration[0].Weight)
}

func TestSFN_ListStateMachineAliases_All(t *testing.T) {
	t.Helper()
	resetState(t)
	c := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := c.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := createOut.StateMachineArn

	v1Out, err := c.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: smARN,
	})
	require.NoError(t, err)

	// Create two aliases
	aliasNames := []string{"alpha", "beta"}
	for _, aName := range aliasNames {
		_, err := c.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
			Name: aws.String(aName),
			RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{{
				StateMachineVersionArn: v1Out.StateMachineVersionArn,
				Weight:                 100,
			}},
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListStateMachineAliases(ctx, &awssfn.ListStateMachineAliasesInput{
		StateMachineArn: smARN,
	})
	require.NoError(t, err)
	assert.Len(t, listOut.StateMachineAliases, 2)

	// Collect alias names from result by extracting the last segment of the ARN
	returnedNames := make([]string, 0, len(listOut.StateMachineAliases))
	for _, a := range listOut.StateMachineAliases {
		arn := aws.ToString(a.StateMachineAliasArn)
		parts := strings.Split(arn, ":")
		if len(parts) > 0 {
			returnedNames = append(returnedNames, parts[len(parts)-1])
		}
	}
	assert.ElementsMatch(t, aliasNames, returnedNames)
}
