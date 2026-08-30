package pubsub

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
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
	p := New(store.NewMemoryResourceStore())

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
