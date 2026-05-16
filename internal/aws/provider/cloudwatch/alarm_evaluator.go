package cloudwatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"jaiscloud/internal/store"
)

// alarmEvaluator is a background worker that periodically evaluates metric alarms.
type alarmEvaluator struct {
	p    *Provider
	tick time.Duration
}

// Evaluator returns a workers.Worker that evaluates metric alarms every 30s.
func (p *Provider) Evaluator() *alarmEvaluator {
	return &alarmEvaluator{p: p, tick: 30 * time.Second}
}

// Run implements workers.Worker — blocks until ctx is cancelled.
func (e *alarmEvaluator) Run(ctx context.Context) {
	t := time.NewTicker(e.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.evaluateAll(ctx)
		}
	}
}

func (e *alarmEvaluator) evaluateAll(ctx context.Context) {
	entries, err := e.p.resources.List(ctx, "cloudwatch_alarm", "")
	if err != nil {
		return
	}
	for _, entry := range entries {
		var alarm map[string]any
		if err := json.Unmarshal(entry.Data, &alarm); err != nil {
			continue
		}
		e.evaluateAlarm(ctx, alarm, entry)
	}
}

func (e *alarmEvaluator) evaluateAlarm(ctx context.Context, alarm map[string]any, entry store.ResourceEntry) {
	name, _ := alarm["AlarmName"].(string)
	ns, _ := alarm["Namespace"].(string)
	metricName, _ := alarm["MetricName"].(string)
	statistic, _ := alarm["Statistic"].(string)
	threshold, _ := alarm["Threshold"].(float64)
	comparison, _ := alarm["ComparisonOperator"].(string)

	periodSecs := 60
	if v, ok := alarm["Period"].(float64); ok && v > 0 {
		periodSecs = int(v)
	}
	evalPeriods := 1
	if v, ok := alarm["EvaluationPeriods"].(float64); ok && v > 0 {
		evalPeriods = int(v)
	}

	if ns == "" || metricName == "" || statistic == "" || comparison == "" {
		return
	}

	window := time.Duration(periodSecs*evalPeriods) * time.Second
	now := time.Now().UTC()
	startTime := now.Add(-window)

	e.p.mu.Lock()
	dims := extractDimensionsFromAlarm(alarm)
	key := ringKey(ns, metricName, dims)
	ring, ok := e.p.metrics[key]
	if !ok {
		e.p.mu.Unlock()
		e.transitionState(ctx, name, alarm, entry, "INSUFFICIENT_DATA")
		return
	}

	// Collect datapoints within the window.
	var vals []float64
	for _, dp := range ring.points {
		if dp.Timestamp.IsZero() {
			continue
		}
		if !dp.Timestamp.Before(startTime) && !dp.Timestamp.After(now) {
			vals = append(vals, dp.Value)
		}
	}
	e.p.mu.Unlock()

	if len(vals) == 0 {
		e.transitionState(ctx, name, alarm, entry, "INSUFFICIENT_DATA")
		return
	}

	// Compute the requested statistic.
	stat := computeStat(statistic, vals)

	// Compare to threshold.
	var newState string
	if compareToThreshold(stat, comparison, threshold) {
		newState = "ALARM"
	} else {
		newState = "OK"
	}

	e.transitionState(ctx, name, alarm, entry, newState)
}

func (e *alarmEvaluator) transitionState(ctx context.Context, name string, alarm map[string]any, entry store.ResourceEntry, newState string) {
	current, _ := alarm["StateValue"].(string)
	if current == newState {
		return
	}

	slog.Debug("cloudwatch: alarm state transition", "alarm", name, "old", current, "new", newState)
	now := time.Now().UTC()
	alarm["StateValue"] = newState
	alarm["StateUpdatedTimestamp"] = now.Format(time.RFC3339)

	var reason string
	switch newState {
	case "ALARM":
		reason = "Threshold crossed: metric exceeded threshold"
	case "OK":
		reason = "Threshold not breached"
	case "INSUFFICIENT_DATA":
		reason = "Insufficient data for alarm evaluation"
	}
	alarm["StateReason"] = reason

	data, err := json.Marshal(alarm)
	if err != nil {
		return
	}
	if err := e.p.resources.Update(ctx, store.ResourceEntry{Type: "cloudwatch_alarm", ID: entry.ID, Data: data}); err != nil {
		slog.Warn("cloudwatch: failed to persist alarm state", "alarm", name, "err", err)
		return
	}

	// Record alarm history entry for this state transition.
	e.p.writeAlarmHistory(ctx, name, "MetricAlarm", "StateUpdate",
		fmt.Sprintf("Alarm updated from %s to %s", current, newState),
		map[string]any{"version": "1.0", "oldState": map[string]any{"stateValue": current}, "newState": map[string]any{"stateValue": newState, "stateReason": reason}},
		now)

	go e.p.fireAlarmActions(context.Background(), alarm, newState)
}

// computeStat computes the requested CloudWatch statistic over a slice of values.
func computeStat(statistic string, vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	switch statistic {
	case "Sum":
		var s float64
		for _, v := range vals {
			s += v
		}
		return s
	case "Average":
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	case "Maximum":
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	case "Minimum":
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	case "SampleCount":
		return float64(len(vals))
	default:
		return vals[0]
	}
}

// compareToThreshold evaluates stat op threshold.
func compareToThreshold(stat float64, op string, threshold float64) bool {
	switch op {
	case "GreaterThanThreshold":
		return stat > threshold
	case "GreaterThanOrEqualToThreshold":
		return stat >= threshold
	case "LessThanThreshold":
		return stat < threshold
	case "LessThanOrEqualToThreshold":
		return stat <= threshold
	default:
		return false
	}
}

// extractDimensionsFromAlarm extracts the Dimensions map stored in an alarm's params.
func extractDimensionsFromAlarm(alarm map[string]any) map[string]string {
	dims := make(map[string]string)
	for i := 1; ; i++ {
		prefix := "Dimensions.member." + itoa(i) + "."
		k, ok := alarm[prefix+"Name"].(string)
		if !ok || k == "" {
			break
		}
		v, _ := alarm[prefix+"Value"].(string)
		dims[k] = v
	}
	return dims
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
