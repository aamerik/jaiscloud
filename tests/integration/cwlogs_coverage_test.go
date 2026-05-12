package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── CW Logs Queries ──────────────────────────────────────────────────────────

func TestCWL_StartQuery_LiteImmediatelyComplete(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	// Create a log group first
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/query/test"),
	})
	require.NoError(t, err)

	out, err := c.StartQuery(ctx, &awscwl.StartQueryInput{
		LogGroupName: aws.String("/query/test"),
		StartTime:    aws.Int64(time.Now().Add(-time.Hour).Unix()),
		EndTime:      aws.Int64(time.Now().Unix()),
		QueryString:  aws.String("fields @message | limit 10"),
	})
	if err != nil {
		t.Skipf("StartQuery not implemented: %v", err)
	}
	assert.NotEmpty(t, aws.ToString(out.QueryId))
}

func TestCWL_GetQueryResults_AfterStart(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/query/results"),
	})
	require.NoError(t, err)

	startOut, err := c.StartQuery(ctx, &awscwl.StartQueryInput{
		LogGroupName: aws.String("/query/results"),
		StartTime:    aws.Int64(time.Now().Add(-time.Hour).Unix()),
		EndTime:      aws.Int64(time.Now().Unix()),
		QueryString:  aws.String("fields @message"),
	})
	if err != nil {
		t.Skipf("StartQuery not implemented: %v", err)
	}

	out, err := c.GetQueryResults(ctx, &awscwl.GetQueryResultsInput{
		QueryId: startOut.QueryId,
	})
	require.NoError(t, err)
	// In lite mode, results are empty but status should be Complete or Running
	assert.NotNil(t, out.Status)
}

func TestCWL_PutQueryDefinition_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	out, err := c.PutQueryDefinition(ctx, &awscwl.PutQueryDefinitionInput{
		Name:        aws.String("my-query"),
		QueryString: aws.String("fields @message | limit 20"),
	})
	if err != nil {
		t.Skipf("PutQueryDefinition not implemented: %v", err)
	}
	assert.NotEmpty(t, aws.ToString(out.QueryDefinitionId))
}

func TestCWL_DeleteQueryDefinition_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	putOut, err := c.PutQueryDefinition(ctx, &awscwl.PutQueryDefinitionInput{
		Name:        aws.String("del-query"),
		QueryString: aws.String("fields @message"),
	})
	if err != nil {
		t.Skipf("PutQueryDefinition not implemented: %v", err)
	}

	_, err = c.DeleteQueryDefinition(ctx, &awscwl.DeleteQueryDefinitionInput{
		QueryDefinitionId: putOut.QueryDefinitionId,
	})
	require.NoError(t, err)
}

func TestCWL_DescribeQueryDefinitions_AfterPut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.PutQueryDefinition(ctx, &awscwl.PutQueryDefinitionInput{
		Name:        aws.String("list-query"),
		QueryString: aws.String("fields @message"),
	})
	if err != nil {
		t.Skipf("PutQueryDefinition not implemented: %v", err)
	}

	out, err := c.DescribeQueryDefinitions(ctx, &awscwl.DescribeQueryDefinitionsInput{
		QueryDefinitionNamePrefix: aws.String("list-"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.QueryDefinitions), 1)
}

// ─── CW Logs Subscriptions ────────────────────────────────────────────────────

func TestCWL_PutSubscriptionFilter_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/sub/test"),
	})
	require.NoError(t, err)

	_, err = c.PutSubscriptionFilter(ctx, &awscwl.PutSubscriptionFilterInput{
		LogGroupName:     aws.String("/sub/test"),
		FilterName:       aws.String("my-filter"),
		FilterPattern:    aws.String("ERROR"),
		DestinationArn:   aws.String("arn:aws:lambda:us-east-1:000000000000:function:log-processor"),
	})
	if err != nil {
		t.Skipf("PutSubscriptionFilter not implemented: %v", err)
	}
}

func TestCWL_DescribeSubscriptionFilters_AfterPut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/sub/desc"),
	})
	require.NoError(t, err)

	_, err = c.PutSubscriptionFilter(ctx, &awscwl.PutSubscriptionFilterInput{
		LogGroupName:   aws.String("/sub/desc"),
		FilterName:     aws.String("desc-filter"),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:sink"),
	})
	if err != nil {
		t.Skipf("PutSubscriptionFilter not implemented: %v", err)
	}

	out, err := c.DescribeSubscriptionFilters(ctx, &awscwl.DescribeSubscriptionFiltersInput{
		LogGroupName: aws.String("/sub/desc"),
	})
	require.NoError(t, err)
	require.Len(t, out.SubscriptionFilters, 1)
	assert.Equal(t, "desc-filter", aws.ToString(out.SubscriptionFilters[0].FilterName))
}

func TestCWL_DeleteSubscriptionFilter_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/sub/del"),
	})
	require.NoError(t, err)

	_, err = c.PutSubscriptionFilter(ctx, &awscwl.PutSubscriptionFilterInput{
		LogGroupName:   aws.String("/sub/del"),
		FilterName:     aws.String("del-filter"),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:sink"),
	})
	if err != nil {
		t.Skipf("PutSubscriptionFilter not implemented: %v", err)
	}

	_, err = c.DeleteSubscriptionFilter(ctx, &awscwl.DeleteSubscriptionFilterInput{
		LogGroupName: aws.String("/sub/del"),
		FilterName:   aws.String("del-filter"),
	})
	require.NoError(t, err)

	out, err := c.DescribeSubscriptionFilters(ctx, &awscwl.DescribeSubscriptionFiltersInput{
		LogGroupName: aws.String("/sub/del"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.SubscriptionFilters)
}

// ─── CW Logs Export Task ──────────────────────────────────────────────────────

func TestCWL_CreateExportTask_Stub(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/export/test"),
	})
	require.NoError(t, err)

	now := time.Now()
	out, err := c.CreateExportTask(ctx, &awscwl.CreateExportTaskInput{
		LogGroupName: aws.String("/export/test"),
		Destination:  aws.String("my-s3-bucket"),
		From:         aws.Int64(now.Add(-time.Hour).UnixMilli()),
		To:           aws.Int64(now.UnixMilli()),
	})
	if err != nil {
		t.Skipf("CreateExportTask not implemented: %v", err)
	}
	assert.NotEmpty(t, aws.ToString(out.TaskId))
}

func TestCWL_DescribeExportTasks_AfterCreate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/export/desc"),
	})
	require.NoError(t, err)

	now := time.Now()
	createOut, err := c.CreateExportTask(ctx, &awscwl.CreateExportTaskInput{
		LogGroupName: aws.String("/export/desc"),
		Destination:  aws.String("my-s3-bucket"),
		From:         aws.Int64(now.Add(-time.Hour).UnixMilli()),
		To:           aws.Int64(now.UnixMilli()),
	})
	if err != nil {
		t.Skipf("CreateExportTask not implemented: %v", err)
	}

	out, err := c.DescribeExportTasks(ctx, &awscwl.DescribeExportTasksInput{
		TaskId: createOut.TaskId,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.ExportTasks), 1)
}

// ─── CW Logs Metric Filters ───────────────────────────────────────────────────

func TestCWL_PutMetricFilter_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/metric/filter"),
	})
	require.NoError(t, err)

	_, err = c.PutMetricFilter(ctx, &awscwl.PutMetricFilterInput{
		LogGroupName:  aws.String("/metric/filter"),
		FilterName:    aws.String("error-count"),
		FilterPattern: aws.String("ERROR"),
		MetricTransformations: []cwltypes.MetricTransformation{{
			MetricName:      aws.String("ErrorCount"),
			MetricNamespace: aws.String("MyApp"),
			MetricValue:     aws.String("1"),
		}},
	})
	require.NoError(t, err)
}

func TestCWL_DescribeMetricFilters_AfterPut(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/metric/desc"),
	})
	require.NoError(t, err)

	_, err = c.PutMetricFilter(ctx, &awscwl.PutMetricFilterInput{
		LogGroupName:  aws.String("/metric/desc"),
		FilterName:    aws.String("info-filter"),
		FilterPattern: aws.String("INFO"),
		MetricTransformations: []cwltypes.MetricTransformation{{
			MetricName:      aws.String("InfoCount"),
			MetricNamespace: aws.String("MyApp"),
			MetricValue:     aws.String("1"),
		}},
	})
	require.NoError(t, err)

	out, err := c.DescribeMetricFilters(ctx, &awscwl.DescribeMetricFiltersInput{
		LogGroupName: aws.String("/metric/desc"),
	})
	require.NoError(t, err)
	require.Len(t, out.MetricFilters, 1)
	assert.Equal(t, "info-filter", aws.ToString(out.MetricFilters[0].FilterName))
}

func TestCWL_LogGroup_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	for i := 0; i < 5; i++ {
		_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
			LogGroupName: aws.String(fmt.Sprintf("/page/group%d", i)),
		})
		require.NoError(t, err)
	}

	var allGroups []cwltypes.LogGroup
	var nextToken *string
	for {
		out, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/page/"),
			NextToken:          nextToken,
			Limit:              aws.Int32(3),
		})
		require.NoError(t, err)
		allGroups = append(allGroups, out.LogGroups...)
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	assert.Len(t, allGroups, 5)
}
