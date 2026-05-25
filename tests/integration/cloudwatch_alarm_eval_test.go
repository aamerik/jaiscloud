package integration_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscw "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// triggerCWEvaluate calls the test-only admin endpoint that runs one synchronous
// alarm-evaluation pass without waiting for the 30-second ticker.
func triggerCWEvaluate(t *testing.T) {
	t.Helper()
	resp, err := http.Post(jaiscloudEndpoint+"/_jaiscloud/cw-evaluate", "", nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "/_jaiscloud/cw-evaluate must return 200")
}

// TestAlarmEvaluatorTransitionsToAlarm verifies that the alarm evaluator automatically
// moves a metric alarm from OK → ALARM when metric data exceeds the threshold.
func TestAlarmEvaluatorTransitionsToAlarm(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWClient(t)

	// Create an alarm: threshold=10, GreaterThanThreshold, period=60s, 1 eval period.
	_, err := c.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String("eval-test-alarm"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("EvalTestMetric"),
		Namespace:          aws.String("JaisCloud/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		Threshold:          aws.Float64(10),
	})
	require.NoError(t, err)

	// Publish a value of 20 — above the threshold.
	_, err = c.PutMetricData(ctx, &awscw.PutMetricDataInput{
		Namespace: aws.String("JaisCloud/Test"),
		MetricData: []cwtypes.MetricDatum{{
			MetricName: aws.String("EvalTestMetric"),
			Value:      aws.Float64(20),
			Unit:       cwtypes.StandardUnitCount,
			Timestamp:  aws.Time(clock.RealNow()),
		}},
	})
	require.NoError(t, err)

	// Trigger evaluation synchronously via the admin endpoint.
	triggerCWEvaluate(t)

	// The alarm should now be in ALARM state.
	out, err := c.DescribeAlarms(ctx, &awscw.DescribeAlarmsInput{
		AlarmNames: []string{"eval-test-alarm"},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1, "alarm must exist after PutMetricAlarm")
	assert.Equal(t, cwtypes.StateValueAlarm, out.MetricAlarms[0].StateValue,
		"alarm must be in ALARM state after metric exceeds threshold")
}

// TestAlarmEvaluatorTransitionsToOK verifies OK state when metric is below threshold.
func TestAlarmEvaluatorTransitionsToOK(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWClient(t)

	// Create alarm with threshold=100.
	_, err := c.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String("eval-ok-alarm"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("OKTestMetric"),
		Namespace:          aws.String("JaisCloud/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		Threshold:          aws.Float64(100),
	})
	require.NoError(t, err)

	// Set alarm to ALARM first.
	_, err = c.SetAlarmState(ctx, &awscw.SetAlarmStateInput{
		AlarmName:   aws.String("eval-ok-alarm"),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("manual override"),
	})
	require.NoError(t, err)

	// Publish a value of 5 — below the threshold of 100.
	_, err = c.PutMetricData(ctx, &awscw.PutMetricDataInput{
		Namespace: aws.String("JaisCloud/Test"),
		MetricData: []cwtypes.MetricDatum{{
			MetricName: aws.String("OKTestMetric"),
			Value:      aws.Float64(5),
			Unit:       cwtypes.StandardUnitCount,
			Timestamp:  aws.Time(clock.RealNow()),
		}},
	})
	require.NoError(t, err)

	// Trigger evaluation — alarm should recover to OK.
	triggerCWEvaluate(t)

	out, err := c.DescribeAlarms(ctx, &awscw.DescribeAlarmsInput{
		AlarmNames: []string{"eval-ok-alarm"},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1)
	assert.Equal(t, cwtypes.StateValueOk, out.MetricAlarms[0].StateValue,
		"alarm must recover to OK when metric drops below threshold")
}

// TestAlarmEvaluatorInsufficientData verifies INSUFFICIENT_DATA when no metric points exist.
func TestAlarmEvaluatorInsufficientData(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWClient(t)

	// Create alarm with no metric data pushed.
	_, err := c.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String("eval-insufficient-alarm"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("NoDataMetric"),
		Namespace:          aws.String("JaisCloud/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		Threshold:          aws.Float64(10),
	})
	require.NoError(t, err)

	// Force a non-INSUFFICIENT_DATA state first so we can observe the transition.
	_, err = c.SetAlarmState(ctx, &awscw.SetAlarmStateInput{
		AlarmName:   aws.String("eval-insufficient-alarm"),
		StateValue:  cwtypes.StateValueOk,
		StateReason: aws.String("manual"),
	})
	require.NoError(t, err)

	// Trigger evaluation — no metric data in ring → INSUFFICIENT_DATA.
	triggerCWEvaluate(t)

	out, err := c.DescribeAlarms(ctx, &awscw.DescribeAlarmsInput{
		AlarmNames: []string{"eval-insufficient-alarm"},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1)
	assert.Equal(t, cwtypes.StateValueInsufficientData, out.MetricAlarms[0].StateValue,
		"alarm must be INSUFFICIENT_DATA when no metric data exists")
}

// TestAlarmHistoryRecorded verifies that SetAlarmState and evaluator transitions
// produce DescribeAlarmHistory entries.
func TestAlarmHistoryRecorded(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWClient(t)

	alarmName := "history-test-alarm"

	// PutMetricAlarm produces a ConfigurationUpdate history item.
	_, err := c.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("HistoryMetric"),
		Namespace:          aws.String("JaisCloud/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		Threshold:          aws.Float64(10),
	})
	require.NoError(t, err)

	// Three explicit state changes via SetAlarmState.
	for _, state := range []cwtypes.StateValue{
		cwtypes.StateValueAlarm,
		cwtypes.StateValueOk,
		cwtypes.StateValueInsufficientData,
	} {
		_, err = c.SetAlarmState(ctx, &awscw.SetAlarmStateInput{
			AlarmName:   aws.String(alarmName),
			StateValue:  state,
			StateReason: aws.String("history test"),
		})
		require.NoError(t, err)
	}

	out, err := c.DescribeAlarmHistory(ctx, &awscw.DescribeAlarmHistoryInput{
		AlarmName: aws.String(alarmName),
	})
	require.NoError(t, err)
	// 1 ConfigurationUpdate (PutMetricAlarm) + 3 StateUpdate (SetAlarmState×3) = 4 items minimum.
	assert.GreaterOrEqual(t, len(out.AlarmHistoryItems), 4,
		"must have at least 4 history items (1 config + 3 state changes), got %d", len(out.AlarmHistoryItems))
}

// TestAlarmHistoryEvaluatorRecorded verifies that automatic evaluator transitions
// also produce alarm history entries.
func TestAlarmHistoryEvaluatorRecorded(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newCWClient(t)

	alarmName := "eval-history-alarm"

	_, err := c.PutMetricAlarm(ctx, &awscw.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("EvalHistoryMetric"),
		Namespace:          aws.String("JaisCloud/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		Threshold:          aws.Float64(10),
	})
	require.NoError(t, err)

	// Push data above threshold.
	_, err = c.PutMetricData(ctx, &awscw.PutMetricDataInput{
		Namespace: aws.String("JaisCloud/Test"),
		MetricData: []cwtypes.MetricDatum{{
			MetricName: aws.String("EvalHistoryMetric"),
			Value:      aws.Float64(50),
			Unit:       cwtypes.StandardUnitCount,
			Timestamp:  aws.Time(clock.RealNow()),
		}},
	})
	require.NoError(t, err)

	// Trigger evaluator — should produce a StateUpdate history entry.
	triggerCWEvaluate(t)

	out, err := c.DescribeAlarmHistory(ctx, &awscw.DescribeAlarmHistoryInput{
		AlarmName: aws.String(alarmName),
	})
	require.NoError(t, err)
	// At least 2: 1 ConfigurationUpdate (PutMetricAlarm) + 1 StateUpdate (evaluator→ALARM).
	assert.GreaterOrEqual(t, len(out.AlarmHistoryItems), 2,
		"evaluator transition must produce a history entry")

	// Verify at least one StateUpdate item exists.
	var hasStateUpdate bool
	for _, item := range out.AlarmHistoryItems {
		if item.HistoryItemType == cwtypes.HistoryItemTypeStateUpdate {
			hasStateUpdate = true
			break
		}
	}
	assert.True(t, hasStateUpdate, "must have at least one StateUpdate history item from evaluator")
}
