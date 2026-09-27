package pubsub

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/eventing"
)

// TestEventarcSubscriptionLifecycle covers the platform-provisioned backing
// subscription of an Eventarc trigger (FD9): idempotent creation, the
// user-configurable deadLetterPolicy via subscriptions.patch, dead-letter
// republish, and deletion.
func TestEventarcSubscriptionLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")
	mustTopic(t, p, "dlq")

	sub, err := p.EnsureEventarcSubscription(ctx, "proj", "us-central1", "functions-fn", "src")
	if err != nil {
		t.Fatalf("ensure eventarc subscription: %v", err)
	}
	wantID := eventing.EventarcSubscriptionID("us-central1", "functions-fn")
	if sub != wantID {
		t.Fatalf("subscription id = %q, want %q", sub, wantID)
	}

	// Idempotent: a second ensure returns the same id without erroring.
	again, err := p.EnsureEventarcSubscription(ctx, "proj", "us-central1", "functions-fn", "src")
	if err != nil || again != wantID {
		t.Fatalf("re-ensure = %q, %v", again, err)
	}

	// Missing topic is NotFound.
	if _, err := p.EnsureEventarcSubscription(ctx, "proj", "us-central1", "functions-x", "missing"); err == nil {
		t.Fatal("expected NotFound for a missing transport topic")
	}

	// No policy yet.
	if _, _, ok, err := p.SubscriptionDeadLetter(ctx, "proj", sub); err != nil || ok {
		t.Fatalf("dead-letter before configure = ok %v, err %v", ok, err)
	}

	// Configure the dead-letter policy through subscriptions.patch (the REST
	// UpdateSubscriptionRequest shape: fields under "subscription", mask in the
	// body).
	_, err = p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/" + sub,
		"body": map[string]any{
			"subscription": map[string]any{
				"deadLetterPolicy": map[string]any{
					"deadLetterTopic":     "projects/proj/topics/dlq",
					"maxDeliveryAttempts": float64(10),
				},
			},
			"updateMask": "dead_letter_policy.max_delivery_attempts",
		},
	}))
	if err != nil {
		t.Fatalf("subscription update: %v", err)
	}
	topic, attempts, ok, err := p.SubscriptionDeadLetter(ctx, "proj", sub)
	if err != nil || !ok || topic != "dlq" || attempts != 10 {
		t.Fatalf("dead-letter after configure = %q %d %v %v", topic, attempts, ok, err)
	}

	// A subscription on the dead-letter topic receives the forwarded message.
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/dlq-sub", "body": map[string]any{"topic": "projects/proj/topics/dlq"},
	})); err != nil {
		t.Fatalf("create dlq subscription: %v", err)
	}
	if err := p.PublishDeadLetter(ctx, "proj", "dlq", []byte("payload"), map[string]string{
		"CloudPubSubDeadLetterSourceSubscription": sub,
	}); err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}
	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/dlq-sub", "body": map[string]any{"returnImmediately": true}}))
	if err != nil {
		t.Fatalf("pull dlq: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 dead-letter message, got %d", len(received))
	}

	// Delete removes the subscription (tolerating a repeat).
	if err := p.DeleteEventarcSubscription(ctx, "proj", sub); err != nil {
		t.Fatalf("delete eventarc subscription: %v", err)
	}
	if err := p.DeleteEventarcSubscription(ctx, "proj", sub); err != nil {
		t.Fatalf("delete eventarc subscription (repeat): %v", err)
	}
}

// TestSubscriptionUpdateValidation covers the updateMask contract: the filter is
// immutable and an unknown path fails loud.
func TestSubscriptionUpdateValidation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")
	if _, err := p.EnsureEventarcSubscription(ctx, "proj", "us-central1", "functions-fn", "src"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	sub := eventing.EventarcSubscriptionID("us-central1", "functions-fn")

	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/" + sub,
		"body": map[string]any{"subscription": map[string]any{"filter": "x"}, "updateMask": "filter"},
	})); err == nil {
		t.Fatal("expected an immutable-filter error")
	}
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/" + sub,
		"body": map[string]any{"subscription": map[string]any{}, "updateMask": "nope"},
	})); err == nil {
		t.Fatal("expected an unsupported-mask-path error")
	}
	if _, err := p.SubscriptionUpdate(ctx, newNR(map[string]any{
		"name": "subscriptions/missing",
		"body": map[string]any{"subscription": map[string]any{}, "updateMask": "labels"},
	})); err == nil {
		t.Fatal("expected NotFound for a missing subscription")
	}
}

func mustTopic(t *testing.T, p *Provider, id string) {
	t.Helper()
	if _, err := p.TopicCreate(context.Background(), newNR(map[string]any{"name": "topics/" + id})); err != nil {
		t.Fatalf("create topic %s: %v", id, err)
	}
}

// fakeDispatcher records eventing.Event dispatches.
type fakeDispatcher struct{ n int }

func (f *fakeDispatcher) DispatchEvent(context.Context, eventing.Event) { f.n++ }

// TestPublishDeadLetterDoesNotRedispatch verifies a dead-letter republish is not
// handed back to the Cloud Functions delivery engine (which would recurse when a
// function subscribes to its own dead-letter topic), while a normal publish is.
func TestPublishDeadLetterDoesNotRedispatch(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	mustTopic(t, p, "src")
	mustTopic(t, p, "dlq")
	d := &fakeDispatcher{}
	p.SetFunctionDispatcher(d)

	if _, err := p.PublishEvent(ctx, "proj", "src", []byte("x"), nil); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if d.n != 1 {
		t.Fatalf("PublishEvent dispatch = %d, want 1", d.n)
	}
	if err := p.PublishDeadLetter(ctx, "proj", "dlq", []byte("x"), nil); err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}
	if d.n != 1 {
		t.Fatalf("PublishDeadLetter re-dispatched (%d)", d.n)
	}
}
