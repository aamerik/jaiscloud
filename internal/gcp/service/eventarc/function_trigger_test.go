package eventarc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/eventing"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// fakeSubs is a recording eventing.SubscriptionProvisioner.
type fakeSubs struct {
	ensured  []string
	topics   []string
	deleted  []string
	failWith error
}

func (f *fakeSubs) EnsureEventarcSubscription(_ context.Context, _, location, triggerID, topic string) (string, error) {
	if f.failWith != nil {
		return "", f.failWith
	}
	if topic == "missing" {
		return "", model.NewProviderError("NotFound", "topic not found", 404)
	}
	name := eventing.EventarcSubscriptionID(location, triggerID)
	f.ensured = append(f.ensured, name)
	f.topics = append(f.topics, topic)
	return name, nil
}

func (f *fakeSubs) DeleteEventarcSubscription(_ context.Context, _, subscription string) error {
	f.deleted = append(f.deleted, subscription)
	return nil
}

func (f *fakeSubs) SubscriptionDeadLetter(context.Context, string, string) (string, int, bool, error) {
	return "", 0, false, nil
}

func (f *fakeSubs) PublishDeadLetter(context.Context, string, string, []byte, map[string]string) error {
	return nil
}

// TestEnsureFunctionTrigger covers materializing a function's backing Eventarc
// trigger and its transport subscription (FD9), including the rendered
// output-only transport.pubsub.subscription and target subscription.
func TestEnsureFunctionTrigger(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "t")
	st := eventarcstore.NewMemoryStore()
	svc := NewService(st, resources, workflowsstore.NewMemoryStore())
	subs := &fakeSubs{}
	svc.SetSubscriptionProvisioner(subs)

	name, sub, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		Topic: "t",
	})
	if err != nil {
		t.Fatalf("ensure function trigger: %v", err)
	}
	if want := TriggerName("proj", "us-central1", "functions-fn"); name != want {
		t.Fatalf("trigger name = %q, want %q", name, want)
	}
	if want := eventing.EventarcSubscriptionID("us-central1", "functions-fn"); sub != want {
		t.Fatalf("subscription = %q, want %q", sub, want)
	}

	stored, err := svc.GetTrigger(ctx, "proj", "us-central1", "functions-fn")
	if err != nil {
		t.Fatalf("get trigger: %v", err)
	}
	rendered := TriggerJSON("proj", stored)
	transport, _ := rendered["transport"].(map[string]any)
	pubsub, _ := transport["pubsub"].(map[string]any)
	if got, _ := pubsub["subscription"].(string); got != "projects/proj/subscriptions/"+sub {
		t.Fatalf("rendered subscription = %q", got)
	}

	// A matching Pub/Sub event resolves to the function and carries the backing
	// subscription so the delivery engine can resolve its deadLetterPolicy.
	targets := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
	})
	if len(targets) != 1 || targets[0].FunctionID != "fn" || targets[0].Subscription != sub {
		t.Fatalf("targets = %+v", targets)
	}

	// Re-ensure is idempotent.
	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		Topic: "t",
	}); err != nil {
		t.Fatalf("re-ensure: %v", err)
	}

	// Delete removes the trigger and its subscription.
	if err := svc.DeleteFunctionTrigger(ctx, "proj", "us-central1", "fn"); err != nil {
		t.Fatalf("delete function trigger: %v", err)
	}
	if _, err := st.GetTrigger(ctx, "proj", "us-central1", "functions-fn"); !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		t.Fatalf("trigger still present after delete: %v", err)
	}
	if len(subs.deleted) != 1 || subs.deleted[0] != sub {
		t.Fatalf("deleted subscriptions = %v", subs.deleted)
	}
}

// TestEnsureFunctionTriggerMissingTopic verifies a missing transport topic does
// not leave a dangling backing trigger behind (the subscription is provisioned
// first so its NotFound aborts the whole materialization).
func TestEnsureFunctionTriggerMissingTopic(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	st := eventarcstore.NewMemoryStore()
	svc := NewService(st, resources, workflowsstore.NewMemoryStore())
	svc.SetSubscriptionProvisioner(&fakeSubs{})
	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		Topic: "missing",
	}); err == nil {
		t.Fatal("expected an error for a missing topic")
	}
	if _, err := st.GetTrigger(ctx, "proj", "us-central1", "functions-fn"); !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		t.Fatalf("expected no dangling trigger, got %v", err)
	}
}

// TestUpdateTriggerRepointsSubscription verifies a PATCH of a cloudFunction
// trigger's transport topic re-points its backing subscription (FD9).
func TestUpdateTriggerRepointsSubscription(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "a")
	seedTopic(t, resources, "b")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	svc.SetFunctionExister(fakeFunctions{exists: map[string]bool{"us-central1/fn": true}})
	subs := &fakeSubs{}
	svc.SetSubscriptionProvisioner(subs)

	cfg := func(topic string) json.RawMessage {
		return json.RawMessage(`{"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/fn"},` +
			`"transport":{"pubsub":{"topic":"projects/proj/topics/` + topic + `"}},` +
			`"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	}
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", cfg("a"), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(subs.topics) != 1 || subs.topics[0] != "a" {
		t.Fatalf("provisioned topics = %v", subs.topics)
	}
	// PATCH the transport topic; the subscription follows.
	if _, _, err := svc.UpdateTrigger(ctx, "proj", "us-central1", "t1", cfg("b"), "transport", "", false); err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(subs.topics) != 2 || subs.topics[1] != "b" {
		t.Fatalf("re-pointed topics = %v", subs.topics)
	}
}
