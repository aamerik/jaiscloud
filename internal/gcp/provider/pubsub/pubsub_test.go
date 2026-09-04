package pubsub

import (
	"context"
	"encoding/base64"
	"testing"

	"jaiscloud/internal/gcp/resource"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func TestPubSubRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	// Create topic.
	nr := newNR(map[string]any{"name": "topics/my-topic"})
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Publish.
	nr = newNR(map[string]any{"name": "topics/my-topic", "body": map[string]any{"messages": []any{map[string]any{"data": "aGVsbG8="}}}})
	resp, err := p.TopicPublish(ctx, nr)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	ids, _ := resp.Data["messageIds"].([]string)
	if len(ids) != 1 {
		t.Fatalf("expected 1 message id, got %d", len(ids))
	}

	// Create subscription.
	nr = newNR(map[string]any{"name": "subscriptions/my-sub", "body": map[string]any{"topic": "projects/proj/topics/my-topic"}})
	if _, err := p.SubscriptionCreate(ctx, nr); err != nil {
		t.Fatalf("subscription create: %v", err)
	}

	// Pull.
	nr = newNR(map[string]any{"name": "subscriptions/my-sub"})
	resp, err = p.SubscriptionPull(ctx, nr)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 received message, got %d", len(received))
	}
	rm := received[0].(map[string]any)
	ackID, _ := rm["ackId"].(string)
	msg := rm["message"].(map[string]any)
	if msg["data"] != "aGVsbG8=" {
		t.Errorf("unexpected message data: %v", msg["data"])
	}

	// Ack.
	nr = newNR(map[string]any{"name": "subscriptions/my-sub", "body": map[string]any{"ackIds": []any{ackID}}})
	if _, err := p.SubscriptionAcknowledge(ctx, nr); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestPubSubOpaqueAckID verifies the wire ackId is an opaque base64url of
// "topicID/messageID" (not the raw topic/message pair) and that both ack and
// modifyAckDeadline round-trip through the opaque form.
func TestPubSubOpaqueAckID(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/t"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	nr := newNR(map[string]any{"name": "topics/t", "body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}}})
	if _, err := p.TopicPublish(ctx, nr); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{"name": "subscriptions/s", "body": map[string]any{"topic": "projects/proj/topics/t"}})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}

	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/s"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 received message, got %d", len(received))
	}
	rm := received[0].(map[string]any)
	ackID, _ := rm["ackId"].(string)
	msgID, _ := rm["message"].(map[string]any)["messageId"].(string)

	// Opaque: not the raw "topicID/messageID" string.
	if ackID == "t/"+msgID {
		t.Errorf("ackId %q is not opaque (raw topic/messageID leaked)", ackID)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(ackID)
	if err != nil {
		t.Fatalf("ackId is not base64url: %v", err)
	}
	if string(decoded) != "t/"+msgID {
		t.Errorf("decoded ackId = %q, want t/%s", decoded, msgID)
	}

	// modifyAckDeadline round-trips through the opaque ackId.
	if _, err := p.SubscriptionModifyAckDeadline(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"ackIds": []any{ackID}, "ackDeadlineSeconds": float64(60)},
	})); err != nil {
		t.Fatalf("modifyAckDeadline: %v", err)
	}

	// Ack round-trips through the opaque ackId → message removed.
	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"ackIds": []any{ackID}},
	})); err != nil {
		t.Fatalf("ack: %v", err)
	}
	resp, err = p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/s", "body": map[string]any{"returnImmediately": true}}))
	if err != nil {
		t.Fatalf("pull after ack: %v", err)
	}
	if received, _ := resp.Data["receivedMessages"].([]any); len(received) != 0 {
		t.Fatalf("expected no messages after ack, got %d", len(received))
	}
}

// TestTopicGetListFieldFidelity verifies that labels and
// messageRetentionDuration round-trip through TopicGet and TopicList (the
// stored topic map is returned, not just {"name": ...}).
func TestTopicGetListFieldFidelity(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	nr := newNR(map[string]any{
		"name": "topics/with-fields",
		"body": map[string]any{
			"messageRetentionDuration": "600s",
			"labels":                   map[string]any{"env": "test", "team": "platform"},
		},
	})
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Get round-trips both fields.
	nr = newNR(map[string]any{"name": "topics/with-fields"})
	get, err := p.TopicGet(ctx, nr)
	if err != nil {
		t.Fatalf("topic get: %v", err)
	}
	if ret, _ := get.Data["messageRetentionDuration"].(string); ret != "600s" {
		t.Errorf("messageRetentionDuration = %q, want 600s", ret)
	}
	labels, _ := get.Data["labels"].(map[string]any)
	if labels["env"] != "test" || labels["team"] != "platform" {
		t.Errorf("labels = %v, want env=test team=platform", labels)
	}

	// List round-trips both fields.
	nr = newNR(nil)
	list, err := p.TopicList(ctx, nr)
	if err != nil {
		t.Fatalf("topic list: %v", err)
	}
	topics, _ := list.Data["topics"].([]any)
	var found map[string]any
	for _, it := range topics {
		m, _ := it.(map[string]any)
		if m["name"] == "projects/proj/topics/with-fields" {
			found = m
		}
	}
	if found == nil {
		t.Fatal("expected topic in list result")
	}
	if ret, _ := found["messageRetentionDuration"].(string); ret != "600s" {
		t.Errorf("list messageRetentionDuration = %q, want 600s", ret)
	}
	labels, _ = found["labels"].(map[string]any)
	if labels["env"] != "test" || labels["team"] != "platform" {
		t.Errorf("list labels = %v, want env=test team=platform", labels)
	}
}
