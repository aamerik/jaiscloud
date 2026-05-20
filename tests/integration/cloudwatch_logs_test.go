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

// TestCWLogs_PutAndGetLogEvents verifies basic PutLogEvents + GetLogEvents roundtrip.
func TestCWLogs_PutAndGetLogEvents(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	group := "/cwlogs/put-and-get"
	stream := "test-stream"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	msgs := []string{"alpha", "beta", "gamma"}
	events := make([]cwltypes.InputLogEvent, len(msgs))
	for i, m := range msgs {
		events[i] = cwltypes.InputLogEvent{
			Timestamp: aws.Int64(now + int64(i)*1000),
			Message:   aws.String(m),
		}
	}

	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents:     events,
	})
	require.NoError(t, err)

	out, err := c.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		StartFromHead: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.Events, 3)
	for i, ev := range out.Events {
		assert.Equal(t, msgs[i], aws.ToString(ev.Message))
	}
}

// TestCWLogs_FilterLogEvents_Pattern verifies pattern matching in FilterLogEvents.
// Puts "ERROR foo", "INFO bar", "ERROR baz" then filters for "ERROR" → 2 matches.
func TestCWLogs_FilterLogEvents_Pattern(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	group := "/cwlogs/filter-pattern"
	stream := "filter-stream"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("ERROR foo")},
			{Timestamp: aws.Int64(now + 1000), Message: aws.String("INFO bar")},
			{Timestamp: aws.Int64(now + 2000), Message: aws.String("ERROR baz")},
		},
	})
	require.NoError(t, err)

	out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String("ERROR"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Events, 2, "expected 2 ERROR events")
	for _, ev := range out.Events {
		assert.Contains(t, aws.ToString(ev.Message), "ERROR")
	}
}

// TestCWLogs_FilterLogEvents_TimeRange verifies that FilterLogEvents respects
// StartTime and EndTime boundaries.
func TestCWLogs_FilterLogEvents_TimeRange(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	group := "/cwlogs/filter-timerange"
	stream := "time-stream"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	require.NoError(t, err)

	// Three events: t-60s, t-30s, t (now)
	now := time.Now()
	old := now.Add(-60 * time.Second).UnixMilli()
	mid := now.Add(-30 * time.Second).UnixMilli()
	cur := now.UnixMilli()

	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(old), Message: aws.String("old event")},
			{Timestamp: aws.Int64(mid), Message: aws.String("mid event")},
			{Timestamp: aws.Int64(cur), Message: aws.String("current event")},
		},
	})
	require.NoError(t, err)

	// Query only the last 45s window — should return mid and current.
	startTime := now.Add(-45 * time.Second).UnixMilli()
	endTime := now.Add(1 * time.Second).UnixMilli()
	out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName: aws.String(group),
		StartTime:    aws.Int64(startTime),
		EndTime:      aws.Int64(endTime),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.Events), 1, "expected at least 1 event in time range")
	for _, ev := range out.Events {
		assert.GreaterOrEqual(t, aws.ToInt64(ev.Timestamp), startTime,
			"event timestamp should be within range")
	}
}

// TestCWLogs_DescribeLogGroups_Prefix verifies prefix filtering in DescribeLogGroups.
func TestCWLogs_DescribeLogGroups_Prefix(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	prefix := "/cwlogs/desc-prefix"
	groups := []string{
		prefix + "/app1",
		prefix + "/app2",
		"/other/unrelated",
	}
	for _, g := range groups {
		_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(g)})
		require.NoError(t, err)
	}

	out, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(prefix),
	})
	require.NoError(t, err)
	assert.Len(t, out.LogGroups, 2, "prefix filter should return exactly 2 groups")

	names := make(map[string]bool, len(out.LogGroups))
	for _, lg := range out.LogGroups {
		names[aws.ToString(lg.LogGroupName)] = true
	}
	assert.True(t, names[prefix+"/app1"], "app1 should be in result")
	assert.True(t, names[prefix+"/app2"], "app2 should be in result")
	assert.False(t, names["/other/unrelated"], "unrelated group must not be in result")
}

// TestCWLogs_CreateLogGroup_AlreadyExists verifies the correct error is returned
// when creating a log group that already exists.
func TestCWLogs_CreateLogGroup_AlreadyExists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	group := "/cwlogs/dup-test"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	_, err = c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.Error(t, err, "creating duplicate log group must fail")
	var alreadyExists *cwltypes.ResourceAlreadyExistsException
	assert.ErrorAs(t, err, &alreadyExists,
		"expected ResourceAlreadyExistsException, got: %v", err)
}

// TestCWLogs_MultiStream_FilterByStream verifies that FilterLogEvents with
// a logStreamNames filter returns only events from the specified stream.
func TestCWLogs_MultiStream_FilterByStream(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWLClient(t)

	group := "/cwlogs/multi-stream"
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)

	for _, stream := range []string{"stream-1", "stream-2"} {
		_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(stream),
		})
		require.NoError(t, err)
	}

	now := time.Now().UnixMilli()
	// Put 2 events to stream-1, 1 event to stream-2
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String("stream-1"),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("from-stream-1-a")},
			{Timestamp: aws.Int64(now + 100), Message: aws.String("from-stream-1-b")},
		},
	})
	require.NoError(t, err)

	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String("stream-2"),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(now + 200), Message: aws.String("from-stream-2")},
		},
	})
	require.NoError(t, err)

	// Filter to stream-1 only
	out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
		LogGroupName:   aws.String(group),
		LogStreamNames: []string{"stream-1"},
	})
	require.NoError(t, err)
	assert.Len(t, out.Events, 2, "expected 2 events from stream-1")
	for _, ev := range out.Events {
		assert.Contains(t, aws.ToString(ev.Message), "stream-1",
			fmt.Sprintf("event should be from stream-1, got: %s", aws.ToString(ev.Message)))
	}
}
