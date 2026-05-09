package queue

import (
	"context"
	"fmt"
	"strings"

	lambdaesm "jaiscloud/internal/provider/aws/lambda/esm"
)

// InternalReceive is the ESM-internal SQS receive that bypasses the wire layer.
// It resolves the queue URL from the queue name, then calls the message store directly.
func (p *QueueProvider) InternalReceive(ctx context.Context, queueName string, maxMessages int, waitTimeSec int) ([]lambdaesm.InternalMessage, error) {
	queueURL, err := p.resolveQueueURLByName(ctx, queueName)
	if err != nil {
		return nil, err
	}

	now := p.clock.Now()
	msgs, err := p.messages.Receive(ctx, queueURL, maxMessages, now)
	if err != nil {
		return nil, err
	}

	result := make([]lambdaesm.InternalMessage, 0, len(msgs))
	for _, m := range msgs {
		attrs := map[string]string{}
		for k, v := range m.Attributes {
			attrs[k] = v
		}
		// Build message attributes as map[string]any for ESM payload
		msgAttrs := make(map[string]any, len(m.MessageAttributes))
		for k, v := range m.MessageAttributes {
			msgAttrs[k] = map[string]any{
				"DataType":    v.DataType,
				"StringValue": v.StringValue,
			}
		}
		result = append(result, lambdaesm.InternalMessage{
			MessageID:         m.MessageID,
			ReceiptHandle:     m.ReceiptHandle,
			Body:              m.Body,
			Attributes:        attrs,
			MessageAttributes: msgAttrs,
			MD5OfBody:         m.MD5OfBody,
		})
	}
	return result, nil
}

// InternalDeleteBatch deletes a batch of messages by receipt handle for ESM use.
func (p *QueueProvider) InternalDeleteBatch(ctx context.Context, queueName string, receiptHandles []string) error {
	queueURL, err := p.resolveQueueURLByName(ctx, queueName)
	if err != nil {
		return err
	}
	for _, rh := range receiptHandles {
		// SQS silently succeeds for invalid handles — mirror that behaviour here
		_ = p.messages.Delete(ctx, queueURL, rh)
	}
	return nil
}

// resolveQueueURLByName scans all SQS queues and finds the one whose URL ends
// with the given queue name (last path segment).
func (p *QueueProvider) resolveQueueURLByName(ctx context.Context, queueName string) (string, error) {
	entries, err := p.resources.List(ctx, "sqs_queues", "")
	if err != nil {
		return "", fmt.Errorf("esm: failed to list queues: %w", err)
	}
	for _, e := range entries {
		// The resource ID for SQS queues is the queue URL itself
		url := e.ID
		// Match last path segment
		idx := strings.LastIndex(url, "/")
		if idx >= 0 && url[idx+1:] == queueName {
			return url, nil
		}
	}
	return "", fmt.Errorf("esm: queue not found: %s", queueName)
}

// Ensure QueueProvider satisfies QueueInternalAPI at compile time.
var _ interface {
	InternalReceive(ctx context.Context, queueName string, maxMessages int, waitTimeSec int) ([]lambdaesm.InternalMessage, error)
	InternalDeleteBatch(ctx context.Context, queueName string, receiptHandles []string) error
} = (*QueueProvider)(nil)
