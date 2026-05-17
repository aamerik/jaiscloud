package queue

import (
	"context"
	"fmt"
	"strings"

	sqsstore "jaiscloud/internal/store/aws/sqs"
)

// MessageAttribute is a simplified SQS message attribute for internal cross-service use.
type MessageAttribute struct {
	DataType    string
	StringValue string
}

// SourceContext carries caller identity metadata for internal deliveries.
type SourceContext struct {
	SourceArn        string
	ServicePrincipal string
}

// InternalSendAPI is the interface used by other providers to deliver to SQS without going through the wire codec.
type InternalSendAPI interface {
	InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]MessageAttribute, src SourceContext) error
}

// InternalSend delivers a message to an SQS queue identified by ARN or URL.
// It resolves the target queue and writes directly to the message store.
func (p *QueueProvider) InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]MessageAttribute, src SourceContext) error {
	queueURL, err := p.resolveQueueURLFromARNorURL(ctx, queueARNorURL)
	if err != nil {
		return err
	}
	msgAttrs := make(map[string]sqsstore.MessageAttribute, len(attrs))
	for k, v := range attrs {
		msgAttrs[k] = sqsstore.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue}
	}
	msg := sqsstore.SQSMessage{
		MessageID:         newMessageID(),
		QueueURL:          queueURL,
		Body:              body,
		SentAt:            p.clock.Now(),
		Attributes:        map[string]string{},
		MessageAttributes: msgAttrs,
	}
	_, _, err = p.messages.Send(ctx, msg)
	if err != nil {
		return err
	}
	p.waiters.Notify(queueURL)
	return nil
}

// resolveQueueURLFromARNorURL returns a queue URL given either an ARN or URL.
func (p *QueueProvider) resolveQueueURLFromARNorURL(ctx context.Context, arnOrURL string) (string, error) {
	if strings.HasPrefix(arnOrURL, "http://") || strings.HasPrefix(arnOrURL, "https://") {
		if _, err := p.resources.Get(ctx, "sqs_queues", arnOrURL); err != nil {
			return "", fmt.Errorf("queue: URL not found: %s", arnOrURL)
		}
		return arnOrURL, nil
	}
	// ARN: arn:aws:sqs:{region}:{account}:{name}
	if strings.HasPrefix(arnOrURL, "arn:") {
		parts := strings.Split(arnOrURL, ":")
		if len(parts) >= 6 {
			name := parts[len(parts)-1]
			return p.resolveQueueURLByName(ctx, name)
		}
		return "", fmt.Errorf("queue: invalid ARN: %s", arnOrURL)
	}
	// Treat as name
	return p.resolveQueueURLByName(ctx, arnOrURL)
}
