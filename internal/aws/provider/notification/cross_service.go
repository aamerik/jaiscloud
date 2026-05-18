package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// MessageAttribute is a simplified SNS message attribute for internal cross-service use.
type MessageAttribute struct {
	DataType    string
	StringValue string
}

// InternalPublisher is the interface used by other providers to publish to SNS without going through the wire codec.
type InternalPublisher interface {
	InternalPublish(ctx context.Context, topicARN string, message string, msgAttrs map[string]MessageAttribute) error
}

// InternalPublish delivers a message to an SNS topic, fanning out to all confirmed subscriptions.
func (p *SNSProvider) InternalPublish(ctx context.Context, topicARN string, message string, msgAttrs map[string]MessageAttribute) error {
	wireAttrs := make(map[string]any, len(msgAttrs))
	for k, v := range msgAttrs {
		wireAttrs[k] = map[string]any{
			"DataType":    v.DataType,
			"StringValue": v.StringValue,
		}
	}
	messageID := fmt.Sprintf("%x", rand.Int63())
	entries, _ := p.resources.List(ctx, "", "", "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != topicARN {
			continue
		}
		if !sd.Confirmed {
			continue
		}
		if fp := sd.Attributes["FilterPolicy"]; fp != "" {
			if !matchesFilterPolicy(fp, wireAttrs) {
				continue
			}
		}
		rawDelivery := sd.Attributes["RawMessageDelivery"] == "true"
		switch sd.Protocol {
		case "sqs":
			p.deliverToSQS(ctx, sd.Endpoint, topicARN, messageID, message, "", "", "", sd.SubscriptionArn, wireAttrs, rawDelivery)
		case "lambda":
			go p.deliverToLambda(ctx, sd.SubscriptionArn, topicARN, messageID, message, "", sd.Endpoint, wireAttrs)
		case "http", "https":
			go p.deliverToHTTP(topicARN, messageID, message, "", sd.Endpoint, wireAttrs)
		}
	}
	return nil
}

// InternalPublishRaw delivers a plain string message to an SNS topic with no message attributes.
// Satisfies the object.SNSInternalPublisher interface for S3 fan-out.
func (p *SNSProvider) InternalPublishRaw(ctx context.Context, topicARN string, message string) error {
	return p.InternalPublish(ctx, topicARN, message, nil)
}

// internalMsgID generates a message ID for internal use.
func internalMsgID() string {
	return fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), time.Now().UnixNano())
}
