// Package targets implements EventBridge target dispatch.
package targets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/aws/provider/events/transform"
	fnprovider "jaiscloud/internal/aws/provider/lambda"
	notifprovider "jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/aws/provider/queue"
)

// LogsWriter is the narrow interface for writing logs to CW Logs.
type LogsWriter interface {
	InternalPutLogEvents(ctx context.Context, logGroupName, logStreamName string, events []string) error
}

// EventBusSender is the narrow interface for cross-bus event delivery.
type EventBusSender interface {
	InternalPutEvents(ctx context.Context, entries []map[string]any) error
}

// Target mirrors the AWS EventBridge target fields.
type Target struct {
	ID              string
	Arn             string
	RoleArn         string
	Input           string
	InputPath       string
	InputTransformer *transform.InputTransformer
	DeadLetterConfig struct {
		Arn string
	}
	HttpParameters *HttpParameters
}

// HttpParameters for API Gateway / API Destination targets.
type HttpParameters struct {
	HeaderParameters    map[string]string
	QueryStringParameters map[string]string
	PathParameterValues []string
}

// Dispatcher routes events to the correct AWS service.
type Dispatcher struct {
	sqs   queue.InternalSendAPI
	sns   notifprovider.InternalPublisher
	fn    fnprovider.InternalInvoker
	logs  LogsWriter
	eb    EventBusSender
	http  *http.Client
}

// New constructs a Dispatcher wired with the provided providers.
func New(sqs queue.InternalSendAPI, fn fnprovider.InternalInvoker, sns notifprovider.InternalPublisher, logs LogsWriter, eb EventBusSender) *Dispatcher {
	return &Dispatcher{
		sqs:  sqs,
		sns:  sns,
		fn:   fn,
		logs: logs,
		eb:   eb,
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// Send delivers the transformed event payload to the target.
func (d *Dispatcher) Send(ctx context.Context, t Target, envelope map[string]any) error {
	tgt := transform.Target{
		Input:     t.Input,
		InputPath: t.InputPath,
	}
	if t.InputTransformer != nil {
		tgt.InputTransformer = t.InputTransformer
	}
	payload, err := transform.Apply(tgt, envelope)
	if err != nil {
		slog.Warn("eventbridge: transform failed", "target", t.Arn, "err", err)
		payload, _ = json.Marshal(envelope)
	}

	sendErr := d.route(ctx, t, payload)
	if sendErr != nil && t.DeadLetterConfig.Arn != "" {
		dlqErr := d.sqs.InternalSend(ctx, t.DeadLetterConfig.Arn, string(payload), nil, queue.SourceContext{
			SourceArn:        t.Arn,
			ServicePrincipal: "events.amazonaws.com",
		})
		if dlqErr != nil {
			slog.Warn("eventbridge: DLQ delivery failed", "dlq", t.DeadLetterConfig.Arn, "err", dlqErr)
		}
	}
	return sendErr
}

func (d *Dispatcher) route(ctx context.Context, t Target, payload []byte) error {
	arn := t.Arn
	switch {
	case strings.Contains(arn, ":sqs:"):
		return d.deliverSQS(ctx, arn, payload)
	case strings.Contains(arn, ":sns:"):
		return d.deliverSNS(ctx, arn, payload)
	case strings.Contains(arn, ":lambda:"):
		return d.deliverLambda(ctx, arn, payload)
	case strings.Contains(arn, ":logs:"):
		return d.deliverLogs(ctx, arn, payload)
	case strings.Contains(arn, ":events:"):
		return d.deliverEventBus(ctx, arn, payload)
	case strings.Contains(arn, ":execute-api:"):
		return d.deliverAPIGW(ctx, t, payload)
	case strings.Contains(arn, "events.amazonaws.com/v1/connections/"):
		return d.deliverAPIDestination(ctx, arn, payload)
	case strings.Contains(arn, ":ecs:"):
		return nil // ECS RunTask stub
	case strings.Contains(arn, ":kinesis:"):
		return nil // Kinesis stub
	case strings.Contains(arn, ":firehose:"):
		return nil // Firehose stub
	case strings.Contains(arn, ":states:"):
		return nil // Step Functions stub
	case strings.Contains(arn, ":batch:"):
		return nil // Batch stub
	default:
		slog.Warn("eventbridge: unsupported target ARN", "arn", arn)
		return nil
	}
}

func (d *Dispatcher) deliverSQS(ctx context.Context, arn string, payload []byte) error {
	if d.sqs == nil {
		return nil
	}
	return d.sqs.InternalSend(ctx, arn, string(payload), nil, queue.SourceContext{
		SourceArn:        arn,
		ServicePrincipal: "events.amazonaws.com",
	})
}

func (d *Dispatcher) deliverSNS(ctx context.Context, arn string, payload []byte) error {
	if d.sns == nil {
		return nil
	}
	return d.sns.InternalPublish(ctx, arn, string(payload), nil)
}

func (d *Dispatcher) deliverLambda(ctx context.Context, arn string, payload []byte) error {
	if d.fn == nil {
		return nil
	}
	_, err := d.fn.InternalInvoke(ctx, arn, payload, "Event")
	return err
}

func (d *Dispatcher) deliverLogs(ctx context.Context, arn string, payload []byte) error {
	if d.logs == nil {
		return nil
	}
	// ARN format: arn:aws:logs:{region}:{account}:log-group:{name}
	parts := strings.Split(arn, ":")
	logGroupName := ""
	if len(parts) >= 7 {
		logGroupName = strings.Join(parts[6:], ":")
	}
	if logGroupName == "" {
		return fmt.Errorf("targets: cannot parse log group from ARN %q", arn)
	}
	return d.logs.InternalPutLogEvents(ctx, logGroupName, "eventbridge", []string{string(payload)})
}

func (d *Dispatcher) deliverEventBus(ctx context.Context, arn string, payload []byte) error {
	if d.eb == nil {
		return nil
	}
	var detail any
	_ = json.Unmarshal(payload, &detail)
	entry := map[string]any{
		"Source":       "aws.events",
		"DetailType":   "Scheduled Event",
		"Detail":       string(payload),
		"EventBusName": arn,
	}
	return d.eb.InternalPutEvents(ctx, []map[string]any{entry})
}

func (d *Dispatcher) deliverAPIGW(ctx context.Context, t Target, payload []byte) error {
	// Parse ARN: arn:execute-api:{region}:{account}:{apiId}/{stage}/{method}/{path}
	arn := t.Arn
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 {
		return fmt.Errorf("targets: malformed execute-api ARN %q", arn)
	}
	pathParts := strings.SplitN(parts[6], "/", 4)
	if len(pathParts) < 4 {
		return fmt.Errorf("targets: malformed execute-api path in ARN %q", arn)
	}
	method := pathParts[2]
	path := "/" + pathParts[3]

	// Substitute path parameters if present.
	if t.HttpParameters != nil {
		for i, v := range t.HttpParameters.PathParameterValues {
			path = strings.Replace(path, fmt.Sprintf("{%d}", i), v, 1)
		}
	}

	url := fmt.Sprintf("https://%s.execute-api.amazonaws.com/%s%s", pathParts[0], pathParts[1], path)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.HttpParameters != nil {
		for k, v := range t.HttpParameters.HeaderParameters {
			req.Header.Set(k, v)
		}
		q := req.URL.Query()
		for k, v := range t.HttpParameters.QueryStringParameters {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (d *Dispatcher) deliverAPIDestination(ctx context.Context, arn string, payload []byte) error {
	// API Destination delivery — minimal stub that POSTs to the stored endpoint.
	slog.Debug("eventbridge: api-destination delivery (stub)", "arn", arn)
	return nil
}
