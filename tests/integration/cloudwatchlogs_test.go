package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	now := time.Now().UnixMilli()
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

	now := time.Now().UnixMilli()
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
