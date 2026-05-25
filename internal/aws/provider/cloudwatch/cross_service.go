package cloudwatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"jaiscloud/internal/clock"
)

// MetricDatum is a single metric observation for internal use.
type MetricDatum struct {
	Name  string
	Value float64
	Unit  string
}

// SNSPublisher is the narrow interface CloudWatch uses to fire SNS alarm actions.
type SNSPublisher interface {
	InternalPublishRaw(ctx context.Context, topicARN string, message string) error
}

// LambdaInvoker is the narrow interface CloudWatch uses to invoke Lambda alarm actions.
type LambdaInvoker interface {
	InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error
}

// SetSNSPublisher wires the SNS publisher (second-pass wiring).
func (p *Provider) SetSNSPublisher(pub SNSPublisher) { p.snsPublisher = pub }

// SetLambdaInvoker wires the Lambda invoker (second-pass wiring).
func (p *Provider) SetLambdaInvoker(inv LambdaInvoker) { p.lambdaInvoker = inv }

// InternalPutMetricData stores one or more metric data points bypassing the wire codec.
func (p *Provider) InternalPutMetricData(_ context.Context, namespace string, data []MetricDatum) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := clock.Now()
	for _, d := range data {
		key := ringKey(namespace, d.Name, nil)
		ring, ok := p.metrics[key]
		if !ok {
			ring = &metricRing{namespace: namespace, name: d.Name, points: make([]datapoint, ringSize)}
			p.metrics[key] = ring
		}
		ring.points[ring.idx] = datapoint{Timestamp: now, Value: d.Value, Unit: d.Unit}
		ring.idx = (ring.idx + 1) % ringSize
	}
	return nil
}

// fireAlarmActions dispatches alarm action ARNs for the given new state.
// Called after SetAlarmState persists the change.
func (p *Provider) fireAlarmActions(_ context.Context, alarmParams map[string]any, newState string) {
	actionsEnabled, _ := alarmParams["ActionsEnabled"].(bool)
	if !actionsEnabled {
		return
	}
	var actionKey string
	switch newState {
	case "ALARM":
		actionKey = "AlarmActions.member."
	case "OK":
		actionKey = "OKActions.member."
	case "INSUFFICIENT_DATA":
		actionKey = "InsufficientDataActions.member."
	default:
		return
	}

	alarmName, _ := alarmParams["AlarmName"].(string)
	alarmARN, _ := alarmParams["AlarmArn"].(string)
	payload, _ := json.Marshal(map[string]any{
		"AlarmName":       alarmName,
		"AlarmArn":        alarmARN,
		"NewStateValue":   newState,
		"NewStateReason":  alarmParams["StateReason"],
		"StateChangeTime": clock.Now().Format(time.RFC3339),
	})

	bgCtx := context.Background()
	for i := 1; i <= 10; i++ {
		arn, _ := alarmParams[actionKey+fmt.Sprintf("%d", i)].(string)
		if arn == "" {
			break
		}
		go p.dispatchAction(bgCtx, arn, payload)
	}
}

func (p *Provider) dispatchAction(ctx context.Context, arn string, payload []byte) {
	switch {
	case isSNSArn(arn):
		if p.snsPublisher != nil {
			if err := p.snsPublisher.InternalPublishRaw(ctx, arn, string(payload)); err != nil {
				slog.Warn("cloudwatch: alarm SNS action failed", "arn", arn, "err", err)
			}
		}
	case isLambdaArn(arn):
		if p.lambdaInvoker != nil {
			if err := p.lambdaInvoker.InternalInvokeRaw(ctx, arn, payload); err != nil {
				slog.Warn("cloudwatch: alarm Lambda action failed", "arn", arn, "err", err)
			}
		}
	default:
		slog.Warn("cloudwatch: unsupported alarm action ARN", "arn", arn)
	}
}

func isSNSArn(arn string) bool {
	parts := splitARN(arn)
	return len(parts) >= 3 && parts[2] == "sns"
}

func isLambdaArn(arn string) bool {
	parts := splitARN(arn)
	return len(parts) >= 3 && parts[2] == "lambda"
}

func splitARN(arn string) []string {
	out := make([]string, 0, 6)
	start := 0
	for i := 0; i < len(arn); i++ {
		if arn[i] == ':' {
			out = append(out, arn[start:i])
			start = i + 1
		}
	}
	if start < len(arn) {
		out = append(out, arn[start:])
	}
	return out
}
