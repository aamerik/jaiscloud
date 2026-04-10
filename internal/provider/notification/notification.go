// Package notification implements the SNS provider.
// Topics and subscriptions are stored as control-plane entries in ResourceStore.
// Publish delivers to SQS-subscribed queues via SQSMessageStore.
package notification

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	sqsstore "jaiscloud/internal/store/aws/sqs"
)

// SNSProvider handles SNS operations.
type SNSProvider struct {
	resources store.ResourceStore
	messages  sqsstore.SQSMessageStore
	bus       *events.EventBus
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, bus *events.EventBus) *SNSProvider {
	return &SNSProvider{resources: resources, messages: messages, bus: bus}
}

func (p *SNSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Notification.CreateTopic":              p.CreateTopic,
		"Notification.DeleteTopic":              p.DeleteTopic,
		"Notification.GetTopicAttributes":       p.GetTopicAttributes,
		"Notification.SetTopicAttributes":       p.SetTopicAttributes,
		"Notification.ListTopics":               p.ListTopics,
		"Notification.Subscribe":                p.Subscribe,
		"Notification.Unsubscribe":              p.Unsubscribe,
		"Notification.ListSubscriptions":        p.ListSubscriptions,
		"Notification.ListSubscriptionsByTopic": p.ListSubscriptionsByTopic,
		"Notification.GetSubscriptionAttributes": p.GetSubscriptionAttributes,
		"Notification.SetSubscriptionAttributes": p.SetSubscriptionAttributes,
		"Notification.Publish":                  p.Publish,
		"Notification.PublishBatch":             p.PublishBatch,
		"Notification.TagResource":              p.TagResource,
		"Notification.UntagResource":            p.UntagResource,
		"Notification.ListTagsForResource":      p.ListTagsForResource,
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func topicArn(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, name)
}

func subArn(region, accountID, topicName string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s:%x",
		region, accountID, topicName, md5.Sum([]byte(topicName+time.Now().String())))
}

func saveEntry(ctx context.Context, rs store.ResourceStore, resType, id string, data any) error {
	raw, _ := json.Marshal(data)
	entry := store.ResourceEntry{Type: resType, ID: id, Data: json.RawMessage(raw)}
	if err := rs.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			return rs.Update(ctx, entry)
		}
		return err
	}
	return nil
}

func loadEntry(ctx context.Context, rs store.ResourceStore, resType, id string, out any) error {
	e, err := rs.Get(ctx, resType, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(e.Data, out)
}

// ─── Topic data ───────────────────────────────────────────────────────────────

type topicData struct {
	TopicArn   string            `json:"TopicArn"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"Tags"`
	CreatedAt  time.Time         `json:"CreatedAt"`
}

func (p *SNSProvider) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, model.NewProviderError("InvalidParameter", "Name is required", 400)
	}
	arn := topicArn(nr.Region, nr.AccountID, name)

	td := topicData{
		TopicArn: arn,
		Attributes: map[string]string{
			"TopicArn":                 arn,
			"DisplayName":              name,
			"SubscriptionsConfirmed":   "0",
			"SubscriptionsPending":     "0",
			"SubscriptionsDeleted":     "0",
			"EffectiveDeliveryPolicy":  `{"defaultHealthyRetryPolicy":{"numRetries":3}}`,
		},
		Tags:      map[string]string{},
		CreatedAt: time.Now().UTC(),
	}
	// CreateTopic is idempotent — return existing ARN if name matches.
	if err := saveEntry(ctx, p.resources, "sns_topics", arn, td); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"TopicArn": arn}), nil
}

func (p *SNSProvider) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	if err := p.resources.Delete(ctx, "sns_topics", arn); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "Topic not found", 404)
		}
		return nil, err
	}
	// Delete all subscriptions for this topic.
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	for _, e := range entries {
		var s subscriptionData
		if json.Unmarshal(e.Data, &s) == nil && s.TopicArn == arn {
			_ = p.resources.Delete(ctx, "sns_subscriptions", s.SubscriptionArn)
		}
	}
	return provider.OK(nil), nil
}

func (p *SNSProvider) GetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	return provider.OK(map[string]any{"Attributes": td.Attributes}), nil
}

func (p *SNSProvider) SetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	attr := strParam(nr.Params, "AttributeName")
	val := strParam(nr.Params, "AttributeValue")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	td.Attributes[attr] = val
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "sns_topics", "")
	var topics []map[string]any
	for _, e := range entries {
		var td topicData
		if json.Unmarshal(e.Data, &td) == nil {
			topics = append(topics, map[string]any{"TopicArn": td.TopicArn})
		}
	}
	if topics == nil {
		topics = []map[string]any{}
	}
	return provider.OK(map[string]any{"Topics": topics}), nil
}

// ─── Subscriptions ────────────────────────────────────────────────────────────

type subscriptionData struct {
	SubscriptionArn string            `json:"SubscriptionArn"`
	TopicArn        string            `json:"TopicArn"`
	Protocol        string            `json:"Protocol"`
	Endpoint        string            `json:"Endpoint"`
	Owner           string            `json:"Owner"`
	Attributes      map[string]string `json:"Attributes"`
}

func (p *SNSProvider) Subscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	topicArn := strParam(nr.Params, "TopicArn")
	protocol := strParam(nr.Params, "Protocol")
	endpoint := strParam(nr.Params, "Endpoint")

	if topicArn == "" || protocol == "" {
		return nil, model.NewProviderError("InvalidParameter", "TopicArn and Protocol are required", 400)
	}

	// Derive topic name from ARN for subArn generation.
	parts := strings.Split(topicArn, ":")
	topicName := parts[len(parts)-1]
	sArn := subArn(nr.Region, nr.AccountID, topicName)

	sd := subscriptionData{
		SubscriptionArn: sArn,
		TopicArn:        topicArn,
		Protocol:        protocol,
		Endpoint:        endpoint,
		Owner:           nr.AccountID,
		Attributes:      map[string]string{"SubscriptionArn": sArn},
	}
	if err := saveEntry(ctx, p.resources, "sns_subscriptions", sArn, sd); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"SubscriptionArn": sArn}), nil
}

func (p *SNSProvider) Unsubscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	_ = p.resources.Delete(ctx, "sns_subscriptions", sArn)
	return provider.OK(nil), nil
}

func (p *SNSProvider) ListSubscriptions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	return provider.OK(map[string]any{"Subscriptions": subscriptionList(entries)}), nil
}

func (p *SNSProvider) ListSubscriptionsByTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	topicArn := strParam(nr.Params, "TopicArn")
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	var filtered []store.ResourceEntry
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) == nil && sd.TopicArn == topicArn {
			filtered = append(filtered, e)
		}
	}
	return provider.OK(map[string]any{"Subscriptions": subscriptionList(filtered)}), nil
}

func (p *SNSProvider) GetSubscriptionAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	var sd subscriptionData
	if err := loadEntry(ctx, p.resources, "sns_subscriptions", sArn, &sd); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Subscription not found")
	}
	attrs := map[string]string{
		"SubscriptionArn": sd.SubscriptionArn,
		"TopicArn":        sd.TopicArn,
		"Protocol":        sd.Protocol,
		"Endpoint":        sd.Endpoint,
		"Owner":           sd.Owner,
	}
	for k, v := range sd.Attributes {
		attrs[k] = v
	}
	return provider.OK(map[string]any{"Attributes": attrs}), nil
}

func (p *SNSProvider) SetSubscriptionAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	attr := strParam(nr.Params, "AttributeName")
	val := strParam(nr.Params, "AttributeValue")
	var sd subscriptionData
	if err := loadEntry(ctx, p.resources, "sns_subscriptions", sArn, &sd); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Subscription not found")
	}
	sd.Attributes[attr] = val
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_subscriptions", sArn, sd)
}

func subscriptionList(entries []store.ResourceEntry) []map[string]any {
	var subs []map[string]any
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) == nil {
			subs = append(subs, map[string]any{
				"SubscriptionArn": sd.SubscriptionArn,
				"TopicArn":        sd.TopicArn,
				"Protocol":        sd.Protocol,
				"Endpoint":        sd.Endpoint,
				"Owner":           sd.Owner,
			})
		}
	}
	if subs == nil {
		subs = []map[string]any{}
	}
	return subs
}

// ─── Publish ──────────────────────────────────────────────────────────────────

func (p *SNSProvider) Publish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	topicArn := strParam(nr.Params, "TopicArn")
	message := strParam(nr.Params, "Message")
	subject := strParam(nr.Params, "Subject")
	messageID := fmt.Sprintf("%x", md5.Sum([]byte(message+time.Now().String())))

	if topicArn == "" {
		return nil, model.NewProviderError("InvalidParameter", "TopicArn is required", 400)
	}

	var msgAttrs map[string]any
	if ma, ok := nr.Params["MessageAttributes"].(map[string]any); ok {
		msgAttrs = ma
	}

	// Load subscriptions and deliver.
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != topicArn {
			continue
		}
		switch sd.Protocol {
		case "sqs":
			p.deliverToSQS(ctx, sd.Endpoint, topicArn, messageID, message, subject, nr.Region, nr.AccountID, msgAttrs)
		// http/https: log and no-op for Phase 1
		}
	}

	return provider.OK(map[string]any{"MessageId": messageID}), nil
}

func (p *SNSProvider) deliverToSQS(ctx context.Context, queueURL, topicArn, messageID, message, subject, region, accountID string, msgAttrs map[string]any) {
	if p.messages == nil {
		return
	}
	// SNS wraps the message in a JSON envelope when delivering to SQS.
	envelope := map[string]any{
		"Type":      "Notification",
		"MessageId": messageID,
		"TopicArn":  topicArn,
		"Subject":   subject,
		"Message":   message,
		"Timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if len(msgAttrs) > 0 {
		envelope["MessageAttributes"] = msgAttrs
	}
	body, _ := json.Marshal(envelope)
	// Each SQS delivery gets its own unique MessageID — the SNS notification
	// messageID is preserved in the envelope body but must not be reused as the
	// SQS row key, otherwise fan-out to N queues would conflict on the PK.
	sqsMsgID := fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), rand.Int31())
	msg := sqsstore.SQSMessage{
		MessageID: sqsMsgID,
		QueueURL:  queueURL,
		Body:      string(body),
		SentAt:    time.Now(),
	}
	_, _ = p.messages.Send(ctx, msg)
}

func (p *SNSProvider) PublishBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	topicArn := strParam(nr.Params, "TopicArn")
	entries, _ := nr.Params["PublishBatchRequestEntries"].([]any)
	var successful []map[string]any
	for _, e := range entries {
		m, _ := e.(map[string]any)
		id, _ := m["Id"].(string)
		message, _ := m["Message"].(string)
		msgID := fmt.Sprintf("%x", md5.Sum([]byte(message)))
		// Re-use Publish logic via recursive single publish.
		fakeNR := &model.NormalizedRequest{
			Params:    map[string]any{"TopicArn": topicArn, "Message": message},
			Region:    nr.Region,
			AccountID: nr.AccountID,
		}
		if _, err := p.Publish(ctx, fakeNR); err == nil {
			successful = append(successful, map[string]any{"Id": id, "MessageId": msgID})
		}
	}
	return provider.OK(map[string]any{"Successful": successful, "Failed": []map[string]any{}}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *SNSProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	if tags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range tags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				td.Tags[k] = v
			}
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(td.Tags, fmt.Sprintf("%v", k))
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	var tags []map[string]any
	for k, v := range td.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	if tags == nil {
		tags = []map[string]any{}
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}
