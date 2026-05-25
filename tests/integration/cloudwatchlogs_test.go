package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// TestCWL_CreateAndDeleteLogGroup verifies basic create/delete lifecycle including
// a second delete which must return an error.
func TestCWL_CreateAndDeleteLogGroup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/test/group"),
	})
	require.NoError(t, err)

	_, err = c.DeleteLogGroup(ctx, &awscwl.DeleteLogGroupInput{
		LogGroupName: aws.String("/test/group"),
	})
	require.NoError(t, err)

	// Second delete must fail — group is gone.
	_, err = c.DeleteLogGroup(ctx, &awscwl.DeleteLogGroupInput{
		LogGroupName: aws.String("/test/group"),
	})
	require.Error(t, err, "deleting a non-existent log group must return an error")
}

// TestCWL_CreateLogGroup_Duplicate verifies that creating the same log group twice
// returns a ResourceAlreadyExistsException.
func TestCWL_CreateLogGroup_Duplicate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/dup/group"),
	})
	require.NoError(t, err)

	_, err = c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/dup/group"),
	})
	require.Error(t, err, "creating a duplicate log group must return an error")
	var alreadyExists *types.ResourceAlreadyExistsException
	assert.ErrorAs(t, err, &alreadyExists, "expected ResourceAlreadyExistsException")
}

// TestCWL_DescribeLogGroups_PrefixFilter creates groups under two different prefixes
// and verifies that filtering by prefix returns only matching groups.
func TestCWL_DescribeLogGroups_PrefixFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	for _, name := range []string{"/prefix/a", "/prefix/b", "/other/c"} {
		_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
			LogGroupName: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String("/prefix/"),
	})
	require.NoError(t, err)
	assert.Len(t, out.LogGroups, 2, "prefix filter must return exactly 2 groups")

	names := make([]string, 0, len(out.LogGroups))
	for _, g := range out.LogGroups {
		names = append(names, aws.ToString(g.LogGroupName))
	}
	assert.Contains(t, names, "/prefix/a")
	assert.Contains(t, names, "/prefix/b")
}

// TestCWL_DescribeLogGroups_PatternFilter verifies that a substring pattern filter
// returns at least the matching group.
func TestCWL_DescribeLogGroups_PatternFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	for _, name := range []string{"/app/prod/service", "/app/dev/service", "/other/thing"} {
		_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
			LogGroupName: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePattern: aws.String("prod"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.LogGroups, "pattern filter must return at least 1 group")

	names := make([]string, 0, len(out.LogGroups))
	for _, g := range out.LogGroups {
		names = append(names, aws.ToString(g.LogGroupName))
	}
	assert.Contains(t, names, "/app/prod/service")
}

// TestCWL_DescribeLogGroups_MutuallyExclusiveParams verifies that supplying both
// LogGroupNamePrefix and LogGroupNamePattern in the same request returns an error.
func TestCWL_DescribeLogGroups_MutuallyExclusiveParams(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix:  aws.String("/prefix/"),
		LogGroupNamePattern: aws.String("pattern"),
	})
	require.Error(t, err, "both prefix and pattern must be mutually exclusive — expected error")
}

// TestCWL_CreateLogStream_And_PutGetEvents verifies putting 3 log events and
// reading them back in ascending timestamp order.
func TestCWL_CreateLogStream_And_PutGetEvents(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String("/stream/test"),
	})
	require.NoError(t, err)

	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String("/stream/test"),
		LogStreamName: aws.String("my-stream"),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	messages := []string{"first event", "second event", "third event"}
	events := []types.InputLogEvent{
		{Timestamp: aws.Int64(now - 2000), Message: aws.String(messages[0])},
		{Timestamp: aws.Int64(now - 1000), Message: aws.String(messages[1])},
		{Timestamp: aws.Int64(now), Message: aws.String(messages[2])},
	}

	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String("/stream/test"),
		LogStreamName: aws.String("my-stream"),
		LogEvents:     events,
	})
	require.NoError(t, err)

	getOut, err := c.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
		LogGroupName:  aws.String("/stream/test"),
		LogStreamName: aws.String("my-stream"),
		StartFromHead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, getOut.Events, 3, "expected 3 events back")

	// Verify ascending timestamp order and message content.
	for i, ev := range getOut.Events {
		assert.Equal(t, messages[i], aws.ToString(ev.Message),
			"event %d message mismatch", i)
	}
	assert.LessOrEqual(t, aws.ToInt64(getOut.Events[0].Timestamp),
		aws.ToInt64(getOut.Events[1].Timestamp), "events must be sorted ascending")
	assert.LessOrEqual(t, aws.ToInt64(getOut.Events[1].Timestamp),
		aws.ToInt64(getOut.Events[2].Timestamp), "events must be sorted ascending")
}

// TestCWL_FilterLogEvents_SubstringMatch verifies filter pattern matching and
// empty-pattern passthrough.
func TestCWL_FilterLogEvents_SubstringMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/filter/test"
	const stream = "filter-stream"

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)

	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []types.InputLogEvent{
			{Timestamp: aws.Int64(now - 2000), Message: aws.String("INFO starting")},
			{Timestamp: aws.Int64(now - 1000), Message: aws.String("ERROR failed")},
			{Timestamp: aws.Int64(now), Message: aws.String("INFO done")},
		},
	})
	require.NoError(t, err)

	// Filter for ERROR — expect exactly 1 result.
	errOut, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String("ERROR"),
	})
	require.NoError(t, err)
	require.Len(t, errOut.Events, 1, "ERROR filter must return exactly 1 event")
	assert.Contains(t, aws.ToString(errOut.Events[0].Message), "ERROR")

	// Empty pattern — expect all 3 events.
	allOut, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String(""),
	})
	require.NoError(t, err)
	assert.Len(t, allOut.Events, 3, "empty filter must return all 3 events")
}

// TestCWL_PutRetentionPolicy verifies setting, observing, and deleting a retention
// policy on a log group.
func TestCWL_PutRetentionPolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/retention/test"

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)

	_, err = c.PutRetentionPolicy(ctx, &awscwl.PutRetentionPolicyInput{
		LogGroupName:    aws.String(group),
		RetentionInDays: aws.Int32(7),
	})
	require.NoError(t, err)

	// Verify retention is reflected in DescribeLogGroups.
	descOut, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(group),
	})
	require.NoError(t, err)
	require.Len(t, descOut.LogGroups, 1)
	assert.Equal(t, int32(7), aws.ToInt32(descOut.LogGroups[0].RetentionInDays),
		"retention must be 7 days after PutRetentionPolicy")

	_, err = c.DeleteRetentionPolicy(ctx, &awscwl.DeleteRetentionPolicyInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)

	// After deletion, RetentionInDays must be nil/absent.
	descOut2, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(group),
	})
	require.NoError(t, err)
	require.Len(t, descOut2.LogGroups, 1)
	assert.Nil(t, descOut2.LogGroups[0].RetentionInDays,
		"RetentionInDays must be nil after DeleteRetentionPolicy")
}

// TestCWL_TagLogGroup verifies tagging, listing, and untagging a log group.
func TestCWL_TagLogGroup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/tag/test"

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)

	_, err = c.TagLogGroup(ctx, &awscwl.TagLogGroupInput{
		LogGroupName: aws.String(group),
		Tags: map[string]string{
			"env":   "test",
			"owner": "team",
		},
	})
	require.NoError(t, err)

	listOut, err := c.ListTagsLogGroup(ctx, &awscwl.ListTagsLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.Equal(t, "test", listOut.Tags["env"], "tag env must be present")
	assert.Equal(t, "team", listOut.Tags["owner"], "tag owner must be present")

	_, err = c.UntagLogGroup(ctx, &awscwl.UntagLogGroupInput{
		LogGroupName: aws.String(group),
		Tags:         []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := c.ListTagsLogGroup(ctx, &awscwl.ListTagsLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.NotContains(t, listOut2.Tags, "env", "tag env must be removed")
	assert.Equal(t, "team", listOut2.Tags["owner"], "tag owner must still be present")
}

// TestCWL_CreateLogStream_GroupNotFound verifies that creating a stream in a
// non-existent group returns an error.
func TestCWL_CreateLogStream_GroupNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	_, err := c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String("/nonexistent/group"),
		LogStreamName: aws.String("some-stream"),
	})
	require.Error(t, err, "creating a stream in a non-existent group must return an error")
}

// TestCWL_DescribeLogStreams verifies listing streams and filtering by name prefix.
func TestCWL_DescribeLogStreams(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/streams/describe"

	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)

	for _, stream := range []string{"stream-a", "stream-b"} {
		_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(stream),
		})
		require.NoError(t, err)
	}

	// List all streams — expect both.
	allOut, err := c.DescribeLogStreams(ctx, &awscwl.DescribeLogStreamsInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.Len(t, allOut.LogStreams, 2, "expected 2 streams")

	streamNames := make([]string, 0, len(allOut.LogStreams))
	for _, s := range allOut.LogStreams {
		streamNames = append(streamNames, aws.ToString(s.LogStreamName))
	}
	assert.Contains(t, streamNames, "stream-a")
	assert.Contains(t, streamNames, "stream-b")

	// Filter by prefix — expect only stream-a.
	prefixOut, err := c.DescribeLogStreams(ctx, &awscwl.DescribeLogStreamsInput{
		LogGroupName:        aws.String(group),
		LogStreamNamePrefix: aws.String("stream-a"),
	})
	require.NoError(t, err)
	assert.Len(t, prefixOut.LogStreams, 1, "prefix filter must return exactly 1 stream")
	assert.Equal(t, "stream-a", aws.ToString(prefixOut.LogStreams[0].LogStreamName))
}

// ─── I-PENDING-4: CW Logs subscription + filter matrix ───────────────────────

// TestCWLogs_FilterPattern_LiteralMatch puts logs with "ERROR" and asserts
// FilterLogEvents with pattern "ERROR" returns only the matching event.
func TestCWLogs_FilterPattern_LiteralMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/filter-literal"
	const stream = "s1"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []types.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("ERROR something went wrong")},
			{Timestamp: aws.Int64(now + 1000), Message: aws.String("INFO all good")},
		},
	})
	require.NoError(t, err)

	out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String("ERROR"),
	})
	require.NoError(t, err)
	require.Len(t, out.Events, 1, "literal 'ERROR' filter must return exactly 1 event")
	assert.Contains(t, aws.ToString(out.Events[0].Message), "ERROR")
}

// TestCWLogs_FilterPattern_NoMatch filters with "CRITICAL" on logs that only
// have "ERROR" and asserts zero results.
func TestCWLogs_FilterPattern_NoMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/filter-nomatch"
	const stream = "s1"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []types.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("ERROR minor failure")},
			{Timestamp: aws.Int64(now + 1000), Message: aws.String("ERROR another failure")},
		},
	})
	require.NoError(t, err)

	out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String("CRITICAL"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Events, "CRITICAL filter must return zero events when none match")
}

// TestCWLogs_FilterPattern_JSONField puts a JSON log entry and filters on a
// JSON field using a CloudWatch Logs JSON filter pattern.
func TestCWLogs_FilterPattern_JSONField(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/filter-json"
	const stream = "s1"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []types.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String(`{"level":"error","msg":"oops"}`)},
			{Timestamp: aws.Int64(now + 1000), Message: aws.String(`{"level":"info","msg":"ok"}`)},
		},
	})
	require.NoError(t, err)

	// JSON field filter — AWS pattern syntax: { $.level = "error" }
	out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String(`{ $.level = "error" }`),
	})
	require.NoError(t, err)
	// In memory mode the filter may be treated as a substring match or a JSON match;
	// either way the "error" message must be in the results.
	assert.NotEmpty(t, out.Events, "JSON field filter must return at least 1 event")
	for _, ev := range out.Events {
		assert.Contains(t, aws.ToString(ev.Message), "error")
	}
}

// TestCWLogs_FilterPattern_MetricFilter verifies PutMetricFilter, DescribeMetricFilters,
// and DeleteMetricFilter roundtrip.
func TestCWLogs_FilterPattern_MetricFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/metric-filter"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	const filterName = "error-count"
	_, err = c.PutMetricFilter(ctx, &awscwl.PutMetricFilterInput{
		LogGroupName:  aws.String(group),
		FilterName:    aws.String(filterName),
		FilterPattern: aws.String("ERROR"),
		MetricTransformations: []types.MetricTransformation{
			{
				MetricName:      aws.String("ErrorCount"),
				MetricNamespace: aws.String("MyApp"),
				MetricValue:     aws.String("1"),
			},
		},
	})
	require.NoError(t, err)

	descOut, err := c.DescribeMetricFilters(ctx, &awscwl.DescribeMetricFiltersInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	require.Len(t, descOut.MetricFilters, 1, "expected exactly 1 metric filter")
	assert.Equal(t, filterName, aws.ToString(descOut.MetricFilters[0].FilterName))
	assert.Equal(t, "ERROR", aws.ToString(descOut.MetricFilters[0].FilterPattern))

	_, err = c.DeleteMetricFilter(ctx, &awscwl.DeleteMetricFilterInput{
		LogGroupName: aws.String(group),
		FilterName:   aws.String(filterName),
	})
	require.NoError(t, err)

	descOut2, err := c.DescribeMetricFilters(ctx, &awscwl.DescribeMetricFiltersInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.MetricFilters, "metric filter must be gone after delete")
}

// TestCWLogs_SubscriptionFilter_CRUD verifies PutSubscriptionFilter,
// DescribeSubscriptionFilters, and DeleteSubscriptionFilter roundtrip.
func TestCWLogs_SubscriptionFilter_CRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/sub-filter"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	const filterName = "my-sub-filter"
	const destARN = "arn:aws:lambda:us-east-1:000000000000:function:my-processor"

	_, err = c.PutSubscriptionFilter(ctx, &awscwl.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(group),
		FilterName:     aws.String(filterName),
		FilterPattern:  aws.String("ERROR"),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeSubscriptionFilters(ctx, &awscwl.DescribeSubscriptionFiltersInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	require.Len(t, descOut.SubscriptionFilters, 1, "expected 1 subscription filter")
	assert.Equal(t, filterName, aws.ToString(descOut.SubscriptionFilters[0].FilterName))
	assert.Equal(t, destARN, aws.ToString(descOut.SubscriptionFilters[0].DestinationArn))

	_, err = c.DeleteSubscriptionFilter(ctx, &awscwl.DeleteSubscriptionFilterInput{
		LogGroupName: aws.String(group),
		FilterName:   aws.String(filterName),
	})
	require.NoError(t, err)

	descOut2, err := c.DescribeSubscriptionFilters(ctx, &awscwl.DescribeSubscriptionFiltersInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.SubscriptionFilters, "subscription filter must be gone after delete")
}

// TestCWLogs_GetLogEvents_Pagination puts 20 log events then paginates with
// limit=5, following nextForwardToken until all events are consumed.
func TestCWLogs_GetLogEvents_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/get-pagination"
	const stream = "s1"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	const total = 20
	now := clock.RealNow().UnixMilli()
	events := make([]types.InputLogEvent, total)
	for i := 0; i < total; i++ {
		events[i] = types.InputLogEvent{
			Timestamp: aws.Int64(now + int64(i)*1000),
			Message:   aws.String(fmt.Sprintf("msg-%02d", i)),
		}
	}
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents:     events,
	})
	require.NoError(t, err)

	// Paginate with limit=5.
	var collected []string
	var nextToken *string
	for {
		out, err := c.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(stream),
			StartFromHead: aws.Bool(true),
			Limit:         aws.Int32(5),
			NextToken:     nextToken,
		})
		require.NoError(t, err)
		if len(out.Events) == 0 {
			break
		}
		for _, ev := range out.Events {
			collected = append(collected, aws.ToString(ev.Message))
		}
		// GetLogEvents returns the same token when exhausted — detect that.
		if nextToken != nil && aws.ToString(out.NextForwardToken) == aws.ToString(nextToken) {
			break
		}
		nextToken = out.NextForwardToken
		if nextToken == nil {
			break
		}
	}

	assert.Len(t, collected, total, "pagination must collect all %d events", total)
}

// TestCWLogs_FilterLogEvents_Pagination puts 15 events and paginates
// FilterLogEvents with limit=5, following nextToken until all consumed.
func TestCWLogs_FilterLogEvents_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/filter-pagination"
	const stream = "s1"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	const total = 15
	now := clock.RealNow().UnixMilli()
	events := make([]types.InputLogEvent, total)
	for i := 0; i < total; i++ {
		events[i] = types.InputLogEvent{
			Timestamp: aws.Int64(now + int64(i)*1000),
			Message:   aws.String(fmt.Sprintf("event-%02d", i)),
		}
	}
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents:     events,
	})
	require.NoError(t, err)

	var collected []string
	var nextToken *string
	for {
		out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
			LogGroupName: aws.String(group),
			Limit:        aws.Int32(5),
			NextToken:    nextToken,
		})
		require.NoError(t, err)
		for _, ev := range out.Events {
			collected = append(collected, aws.ToString(ev.Message))
		}
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	assert.Len(t, collected, total, "pagination must collect all %d events", total)
}

// TestCWLogs_PutLogEvents_SequenceToken puts two batches to the same stream,
// using the sequence token from the first response for the second call.
func TestCWLogs_PutLogEvents_SequenceToken(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/seq-token"
	const stream = "s1"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()

	// First batch — no sequence token needed.
	put1, err := c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []types.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("first batch")},
		},
	})
	require.NoError(t, err)

	// Second batch — pass sequence token from first response if provided.
	put2Input := &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []types.InputLogEvent{
			{Timestamp: aws.Int64(now + 5000), Message: aws.String("second batch")},
		},
	}
	if put1.NextSequenceToken != nil {
		put2Input.SequenceToken = put1.NextSequenceToken
	}
	_, err = c.PutLogEvents(ctx, put2Input)
	require.NoError(t, err)

	// Verify both batches are readable.
	out, err := c.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		StartFromHead: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, out.Events, 2, "both batches must be readable")
}

// TestCWLogs_CreateExportTask verifies CreateExportTask returns without error
// (stub implementation — no actual S3 export).
func TestCWLogs_CreateExportTask(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/export-task"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	now := clock.RealNow().UnixMilli()
	out, err := c.CreateExportTask(ctx, &awscwl.CreateExportTaskInput{
		LogGroupName: aws.String(group),
		Destination:  aws.String("my-export-bucket"),
		From:         aws.Int64(now - 3600_000),
		To:           aws.Int64(now),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.TaskId), "CreateExportTask must return a non-empty task ID")
}

// TestCWLogs_DescribeLogStreams_OrderBy creates 3 streams and verifies that
// DescribeLogStreams returns them in consistent order with and without Descending.
func TestCWLogs_DescribeLogStreams_OrderBy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/order-by"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	// Create in alphabetical order.
	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(name),
		})
		require.NoError(t, err)
	}

	// Ascending order (default).
	ascOut, err := c.DescribeLogStreams(ctx, &awscwl.DescribeLogStreamsInput{
		LogGroupName: aws.String(group),
		Descending:   aws.Bool(false),
		OrderBy:      types.OrderByLogStreamName,
	})
	require.NoError(t, err)
	require.Len(t, ascOut.LogStreams, 3, "expected 3 streams")

	// Descending order.
	descOut, err := c.DescribeLogStreams(ctx, &awscwl.DescribeLogStreamsInput{
		LogGroupName: aws.String(group),
		Descending:   aws.Bool(true),
		OrderBy:      types.OrderByLogStreamName,
	})
	require.NoError(t, err)
	require.Len(t, descOut.LogStreams, 3, "expected 3 streams")

	// The first stream in ascending must differ from the first in descending.
	ascFirst := aws.ToString(ascOut.LogStreams[0].LogStreamName)
	descFirst := aws.ToString(descOut.LogStreams[0].LogStreamName)
	assert.NotEqual(t, ascFirst, descFirst, "ascending and descending order must differ")
}

// TestCWLogs_TagLogGroup verifies TagLogGroup, ListTagsLogGroup, and UntagLogGroup.
func TestCWLogs_TagLogGroup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/tag-test"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	// Apply tags.
	_, err = c.TagLogGroup(ctx, &awscwl.TagLogGroupInput{
		LogGroupName: aws.String(group),
		Tags: map[string]string{
			"env":   "staging",
			"owner": "platform-team",
		},
	})
	require.NoError(t, err)

	// List tags — both must be present.
	listOut, err := c.ListTagsLogGroup(ctx, &awscwl.ListTagsLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.Equal(t, "staging", listOut.Tags["env"], "tag 'env' must be 'staging'")
	assert.Equal(t, "platform-team", listOut.Tags["owner"], "tag 'owner' must be 'platform-team'")

	// Remove one tag.
	_, err = c.UntagLogGroup(ctx, &awscwl.UntagLogGroupInput{
		LogGroupName: aws.String(group),
		Tags:         []string{"env"},
	})
	require.NoError(t, err)

	// Verify 'env' is gone, 'owner' remains.
	listOut2, err := c.ListTagsLogGroup(ctx, &awscwl.ListTagsLogGroupInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)
	assert.NotContains(t, listOut2.Tags, "env", "tag 'env' must be removed")
	assert.Equal(t, "platform-team", listOut2.Tags["owner"], "tag 'owner' must still be present")
}

// TestCWLogs_RetentionPolicy verifies PutRetentionPolicy, DescribeLogGroups reflects
// the setting, and DeleteRetentionPolicy removes it.
func TestCWLogs_RetentionPolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	const group = "/cwlogs/retention-policy"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	// Set retention to 7 days.
	_, err = c.PutRetentionPolicy(ctx, &awscwl.PutRetentionPolicyInput{
		LogGroupName:    aws.String(group),
		RetentionInDays: aws.Int32(7),
	})
	require.NoError(t, err)

	// DescribeLogGroups must reflect retentionInDays=7.
	descOut, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(group),
	})
	require.NoError(t, err)
	require.Len(t, descOut.LogGroups, 1)
	assert.Equal(t, int32(7), aws.ToInt32(descOut.LogGroups[0].RetentionInDays),
		"retentionInDays must be 7 after PutRetentionPolicy")

	// Delete retention policy.
	_, err = c.DeleteRetentionPolicy(ctx, &awscwl.DeleteRetentionPolicyInput{
		LogGroupName: aws.String(group),
	})
	require.NoError(t, err)

	// After deletion, RetentionInDays must be nil/absent.
	descOut2, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(group),
	})
	require.NoError(t, err)
	require.Len(t, descOut2.LogGroups, 1)
	assert.Nil(t, descOut2.LogGroups[0].RetentionInDays,
		"RetentionInDays must be nil after DeleteRetentionPolicy")
}
