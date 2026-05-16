package esm

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"jaiscloud/internal/aws/provider/events/pattern"
)

// applyESMFilterCriteria filters messages using the ESM filter criteria pattern.
// If filterCriteria is empty all messages pass through.
// AWS ESM filter criteria JSON format: the "Pattern" value is a JSON pattern
// that is matched against the message body (parsed as JSON).
func applyESMFilterCriteria(msgs []InternalMessage, filterCriteria string) []InternalMessage {
	if filterCriteria == "" {
		return msgs
	}
	pat, err := pattern.Compile(filterCriteria, pattern.ModeEventBridge)
	if err != nil {
		// Invalid pattern — pass all messages through rather than silently dropping.
		return msgs
	}
	out := msgs[:0:0]
	for _, m := range msgs {
		var envelope map[string]any
		if json.Unmarshal([]byte(m.Body), &envelope) != nil {
			// Non-JSON body — treat as matching (pass through).
			out = append(out, m)
			continue
		}
		if pat.Match(envelope) {
			out = append(out, m)
		}
	}
	return out
}

// dlqRecord is the JSON envelope sent to the DLQ on failure.
type dlqRecord struct {
	RequestContext  dlqRequestContext  `json:"requestContext"`
	ResponseContext dlqResponseContext `json:"responseContext"`
	Version         string             `json:"version"`
	Timestamp       string             `json:"timestamp"`
	SqsBatchInfo    *dlqSQSBatchInfo   `json:"SqsBatchInfo,omitempty"`
}

type dlqRequestContext struct {
	FunctionARN string `json:"functionArn"`
	Condition   string `json:"condition"`
}

type dlqResponseContext struct {
	StatusCode int `json:"statusCode"`
}

type dlqSQSBatchInfo struct {
	Messages []dlqSQSMessage `json:"batchItems"`
}

type dlqSQSMessage struct {
	MessageID     string `json:"messageId"`
	ReceiptHandle string `json:"receiptHandle"`
	Body          string `json:"body"`
}

// buildDLQRecord constructs the DLQ record envelope.
func buildDLQRecord(functionARN string, msgs []InternalMessage, invokeErr error) dlqRecord {
	_ = invokeErr // error message could be added to responseContext in future
	items := make([]dlqSQSMessage, len(msgs))
	for i, m := range msgs {
		items[i] = dlqSQSMessage{
			MessageID:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
		}
	}
	return dlqRecord{
		RequestContext: dlqRequestContext{
			FunctionARN: functionARN,
			Condition:   "RetryAttemptsExhausted",
		},
		ResponseContext: dlqResponseContext{StatusCode: 200},
		Version:         "1.0",
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
		SqsBatchInfo:    &dlqSQSBatchInfo{Messages: items},
	}
}

// sendToFailureDestination delivers failed messages to the configured DLQ/SNS.
func (p *Provider) sendToFailureDestination(ctx context.Context, esm EventSourceMapping, msgs []InternalMessage, invokeErr error) {
	dest := esm.DestinationConfig.OnFailure.Destination
	if dest == "" {
		return
	}
	if p.sqsSender == nil {
		p.logger.Warn("esm: DLQ destination configured but no SQS sender wired", "destination", dest)
		return
	}

	rec := buildDLQRecord(esm.FunctionArn, msgs, invokeErr)
	body, err := json.Marshal(rec)
	if err != nil {
		p.logger.Error("esm: failed to marshal DLQ record", "err", err)
		return
	}

	if isSQSARN(dest) {
		if err := p.sqsSender.InternalSend(ctx, dest, string(body), nil, SQSSourceContext{
			SourceArn:        esm.EventSourceArn,
			ServicePrincipal: "lambda.amazonaws.com",
		}); err != nil {
			p.logger.Warn("esm: failed to send to DLQ", "destination", dest, "err", err)
		}
	} else {
		p.logger.Warn("esm: unsupported DLQ destination type (SNS not yet implemented)", "destination", dest)
	}
}

// isSQSARN returns true if the ARN looks like an SQS ARN.
func isSQSARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:") && strings.Contains(arn, ":sqs:")
}
