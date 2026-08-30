// Package pubsub implements the Google Cloud Pub/Sub provider.
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtTopic        = "gcp_topic"
	rtSubscription = "gcp_subscription"
	rtMessage      = "gcp_message"
)

// Provider handles Pub/Sub topics and subscriptions.
type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"PubSub.TopicCreate":                   p.TopicCreate,
		"PubSub.TopicGet":                      p.TopicGet,
		"PubSub.TopicDelete":                   p.TopicDelete,
		"PubSub.TopicList":                     p.TopicList,
		"PubSub.TopicPublish":                  p.TopicPublish,
		"PubSub.SubscriptionCreate":            p.SubscriptionCreate,
		"PubSub.SubscriptionGet":               p.SubscriptionGet,
		"PubSub.SubscriptionDelete":            p.SubscriptionDelete,
		"PubSub.SubscriptionList":              p.SubscriptionList,
		"PubSub.SubscriptionPull":              p.SubscriptionPull,
		"PubSub.SubscriptionAcknowledge":       p.SubscriptionAcknowledge,
		"PubSub.SubscriptionModifyAckDeadline": p.SubscriptionModifyAckDeadline,
	}
}

func (p *Provider) TopicCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t := strings.TrimPrefix(nr.Params["name"].(string), "topics/")
	if t == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing topic name", 400)
	}
	data, _ := json.Marshal(map[string]any{"name": nr.ResourceID("pubsub-topic", t)})
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: t, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("AlreadyExists", "topic already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"name": nr.ResourceID("pubsub-topic", t)}), nil
}

func (p *Provider) TopicGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t := strings.TrimPrefix(nr.Params["name"].(string), "topics/")
	if _, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtTopic, t); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"name": nr.ResourceID("pubsub-topic", t)}), nil
}

func (p *Provider) TopicDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t := strings.TrimPrefix(nr.Params["name"].(string), "topics/")
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtTopic, t); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) TopicList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtTopic, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(entries))
	for _, e := range entries {
		items = append(items, map[string]any{"name": nr.ResourceID("pubsub-topic", e.ID)})
	}
	return provider.OK(map[string]any{"topics": items}), nil
}

func (p *Provider) TopicPublish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t := strings.TrimPrefix(nr.Params["name"].(string), "topics/")
	if _, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtTopic, t); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	msgs, _ := body["messages"].([]any)
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		id := fmt.Sprintf("%d", clock.Now().UnixNano())
		msg := map[string]any{
			"messageId": id,
			"data":      mm["data"],
		}
		if attrs, ok := mm["attributes"].(map[string]any); ok {
			msg["attributes"] = attrs
		}
		msg["publishTime"] = clock.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
		data, _ := json.Marshal(msg)
		_ = p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtMessage, ID: t + "/" + id, Data: data})
		ids = append(ids, id)
	}
	return provider.OK(map[string]any{"messageIds": ids}), nil
}

func (p *Provider) SubscriptionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	s := strings.TrimPrefix(nr.Params["name"].(string), "subscriptions/")
	if s == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing subscription name", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	topic, _ := body["topic"].(string)
	ackDeadline := 10
	if ad, ok := body["ackDeadlineSeconds"].(float64); ok {
		ackDeadline = int(ad)
	}
	meta := map[string]any{
		"name":               nr.ResourceID("pubsub-subscription", s),
		"topic":              topic,
		"ackDeadlineSeconds": ackDeadline,
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: s, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("AlreadyExists", "subscription already exists", 409)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"name": meta["name"], "topic": topic, "ackDeadlineSeconds": ackDeadline}), nil
}

func (p *Provider) SubscriptionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	s := strings.TrimPrefix(nr.Params["name"].(string), "subscriptions/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	var m map[string]any
	json.Unmarshal(e.Data, &m)
	return provider.OK(m), nil
}

func (p *Provider) SubscriptionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	s := strings.TrimPrefix(nr.Params["name"].(string), "subscriptions/")
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) SubscriptionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, m)
		}
	}
	return provider.OK(map[string]any{"subscriptions": items}), nil
}

func (p *Provider) SubscriptionPull(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	s := strings.TrimPrefix(nr.Params["name"].(string), "subscriptions/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	var sub map[string]any
	json.Unmarshal(e.Data, &sub)
	topic, _ := sub["topic"].(string)
	// topic full name → topic ID (last segment).
	topicID := topic
	if i := strings.LastIndex(topicID, "/"); i >= 0 {
		topicID = topicID[i+1:]
	}

	maxMsgs := 100
	if mm, ok := nr.Params["maxMessages"].(string); ok {
		var n int
		if _, err := fmt.Sscanf(mm, "%d", &n); err == nil && n > 0 {
			maxMsgs = n
		}
	}

	entries, _ := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtMessage, topicID+"/")
	received := make([]any, 0, len(entries))
	for _, me := range entries {
		if len(received) >= maxMsgs {
			break
		}
		var msg map[string]any
		if json.Unmarshal(me.Data, &msg) == nil {
			ackID := me.ID
			received = append(received, map[string]any{"ackId": ackID, "message": msg})
		}
	}
	return provider.OK(map[string]any{"receivedMessages": received}), nil
}

func (p *Provider) SubscriptionAcknowledge(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	ackIDs, _ := body["ackIds"].([]any)
	for _, a := range ackIDs {
		if id, ok := a.(string); ok {
			// ackId = topic + "/" + messageId
			parts := strings.SplitN(id, "/", 2)
			if len(parts) == 2 {
				_ = p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtMessage, id)
			}
		}
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) SubscriptionModifyAckDeadline(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
