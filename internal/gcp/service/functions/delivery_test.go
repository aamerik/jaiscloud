package functions

import (
	"context"
	"errors"
	"testing"
	"time"

	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

// failingExecutor always fails an invocation, for retry/dead-letter tests.
type failingExecutor struct{ calls int }

func (e *failingExecutor) Invoke(context.Context, lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	e.calls++
	return lambdaexec.InvokeResult{}, errors.New("boom")
}
func (e *failingExecutor) DeleteFunction(context.Context, string) {}
func (e *failingExecutor) Reset(context.Context)                  {}
func (e *failingExecutor) Close() error                           { return nil }

// fakeTargets is a static eventing.TargetIndex.
type fakeTargets struct{ targets []eventing.Target }

func (f fakeTargets) TargetsForEvent(context.Context, eventing.Event) []eventing.Target {
	return f.targets
}

// fakeSubs is a recording eventing.SubscriptionProvisioner.
type fakeSubs struct {
	dlqTopic    string
	maxAttempts int
	ok          bool
	published   int
	lastAttrs   map[string]string
}

func (f *fakeSubs) EnsureEventarcSubscription(context.Context, string, string, string, string) (string, error) {
	return "sub", nil
}
func (f *fakeSubs) DeleteEventarcSubscription(context.Context, string, string) error { return nil }
func (f *fakeSubs) SubscriptionDeadLetter(context.Context, string, string) (string, int, bool, error) {
	return f.dlqTopic, f.maxAttempts, f.ok, nil
}
func (f *fakeSubs) PublishDeadLetter(_ context.Context, _, _ string, _ []byte, attrs map[string]string) error {
	f.published++
	f.lastAttrs = attrs
	return nil
}

// setDeliverySubscription records a backing subscription on a stored function so
// the delivery engine can resolve its deadLetterPolicy.
func setDeliverySubscription(t *testing.T, fs *functionsstore.MemoryStore, id, sub string) {
	t.Helper()
	ctx := context.Background()
	f, err := fs.GetFunction(ctx, "proj", "us-central1", id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if f.EventTrigger == nil {
		t.Fatalf("%s has no event trigger", id)
	}
	f.EventTrigger.Subscription = sub
	if err := fs.UpdateFunction(ctx, "proj", "us-central1", id, f); err != nil {
		t.Fatalf("update %s: %v", id, err)
	}
}

func newDeliveryService(t *testing.T, opts ...Option) (*Service, *functionsstore.MemoryStore) {
	t.Helper()
	fs := functionsstore.NewMemoryStore()
	all := append([]Option{}, opts...)
	s := NewService(fs, store.NewMemoryResourceStore(), all...)
	return s, fs
}

func createEventFunction(t *testing.T, s *Service, id string, trigger map[string]any) {
	t.Helper()
	in := FunctionInputFromMap(map[string]any{
		"runtime": "nodejs20", "entryPoint": "handler", "eventTrigger": trigger,
	}, V1)
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", id, in, V1); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}

func deliveries(t *testing.T, s *Service) []functionsstore.Delivery {
	t.Helper()
	got, err := s.ListDeliveries(context.Background(), "proj", "us-central1")
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	return got
}

func TestDispatchEventDeliversToMatchingFunction(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t)
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("hello"),
	})

	got := deliveries(t, s)
	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d: %+v", len(got), got)
	}
	d := got[0]
	if d.Status != functionsstore.DeliveryDelivered || d.Attempts != 1 || d.Result != "hello" {
		t.Fatalf("delivery = %+v", d)
	}
	if d.FunctionID != "fn" || d.Source != eventing.SourcePubSub {
		t.Fatalf("delivery metadata = %+v", d)
	}

	// A different topic does not match.
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/other", Source: eventing.SourcePubSub, Data: []byte("x"),
	})
	if got := deliveries(t, s); len(got) != 1 {
		t.Fatalf("non-matching topic produced a delivery: %+v", got)
	}
}

func TestDispatchEventLegacyStorageMatching(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t)
	// The v1 object.change catch-all subscribes to finalize and delete.
	createEventFunction(t, s, "stor", map[string]any{
		"eventType": "providers/cloud.storage/eventTypes/object.change",
		"resource":  "projects/_/buckets/bkt",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/bkt", Source: eventing.SourceStorage, Data: []byte("{}"),
	})
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageDelete,
		Resource: "projects/_/buckets/bkt", Source: eventing.SourceStorage, Data: []byte("{}"),
	})
	// A different bucket does not match.
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/other", Source: eventing.SourceStorage, Data: []byte("{}"),
	})

	got := deliveries(t, s)
	if len(got) != 2 {
		t.Fatalf("expected 2 deliveries (finalize+delete), got %d: %+v", len(got), got)
	}
	for _, d := range got {
		if d.Status != functionsstore.DeliveryDelivered {
			t.Fatalf("delivery not delivered: %+v", d)
		}
	}
}

func TestDispatchRetryDeadLetter(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	s, _ := newDeliveryService(t, WithExecutor(fail))
	createEventFunction(t, s, "retry", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	createEventFunction(t, s, "noretry", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("x"),
	})

	got := deliveries(t, s)
	if len(got) != 2 {
		t.Fatalf("expected 2 deliveries, got %d: %+v", len(got), got)
	}
	byFn := map[string]functionsstore.Delivery{}
	for _, d := range got {
		byFn[d.FunctionID] = d
	}
	if d := byFn["retry"]; d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != maxDeliveryAttempts {
		t.Fatalf("retry delivery = %+v", d)
	}
	if d := byFn["noretry"]; d.Status != functionsstore.DeliveryFailed || d.Attempts != 1 {
		t.Fatalf("no-retry delivery = %+v", d)
	}
}

func TestDispatchForwardToDeadLetter(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	// maxAttempts 2 exercises the configured cap without the ~3s a real policy's
	// minimum of 5 attempts would cost; the value is honoured verbatim.
	subs := &fakeSubs{dlqTopic: "dlq", maxAttempts: 2, ok: true}
	s, fs := newDeliveryService(t, WithExecutor(fail), WithSubscriptions(subs))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	setDeliverySubscription(t, fs, "fn", "eventarc-us-central1-functions-fn")

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
		EventID: "m1", Data: []byte("payload"),
	})

	got := deliveries(t, s)
	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(got))
	}
	d := got[0]
	if d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != 2 || d.DeadLetterTopic != "dlq" {
		t.Fatalf("delivery = %+v", d)
	}
	if subs.published != 1 {
		t.Fatalf("published = %d, want 1", subs.published)
	}
	if subs.lastAttrs["CloudPubSubDeadLetterSourceSubscription"] != "eventarc-us-central1-functions-fn" {
		t.Fatalf("attrs = %+v", subs.lastAttrs)
	}
	if subs.lastAttrs["CloudPubSubDeadLetterSourceDeliveryCount"] != "2" {
		t.Fatalf("delivery count attr = %q", subs.lastAttrs["CloudPubSubDeadLetterSourceDeliveryCount"])
	}
}

func TestDispatchNoDeadLetterWithoutPolicy(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	subs := &fakeSubs{ok: false}
	s, fs := newDeliveryService(t, WithExecutor(fail), WithSubscriptions(subs))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	setDeliverySubscription(t, fs, "fn", "sub")

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("x"),
	})

	d := deliveries(t, s)[0]
	if d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != maxDeliveryAttempts || d.DeadLetterTopic != "" {
		t.Fatalf("delivery = %+v", d)
	}
	if subs.published != 0 {
		t.Fatalf("published without a policy = %d", subs.published)
	}
}

func TestDispatchEventarcTarget(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t, WithEventTargets(fakeTargets{targets: []eventing.Target{
		{Project: "proj", Location: "us-central1", FunctionID: "target"},
	}}))
	// The target function exists but has no eventTrigger of its own.
	in := FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1)
	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "target", in, V1); err != nil {
		t.Fatalf("create target: %v", err)
	}

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/triggered", Source: eventing.SourcePubSub, Data: []byte("via-eventarc"),
	})

	got := deliveries(t, s)
	if len(got) != 1 || got[0].FunctionID != "target" || got[0].Result != "via-eventarc" {
		t.Fatalf("eventarc delivery = %+v", got)
	}
}

func TestDispatchDedupesOwnAndEventarcTarget(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t, WithEventTargets(fakeTargets{targets: []eventing.Target{
		{Project: "proj", Location: "us-central1", FunctionID: "fn"},
	}}))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("once"),
	})
	if got := deliveries(t, s); len(got) != 1 {
		t.Fatalf("expected a single deduplicated delivery, got %d: %+v", len(got), got)
	}
}

func TestRetryPolicyRoundTrip(t *testing.T) {
	v1 := FunctionInputFromMap(map[string]any{
		"runtime": "nodejs20",
		"eventTrigger": map[string]any{
			"eventType": "google.pubsub.topic.publish", "resource": "t",
			"failurePolicy": map[string]any{"retry": map[string]any{}},
		},
	}, V1)
	if v1.EventTrigger == nil || !v1.EventTrigger.Retry {
		t.Fatalf("v1 retry not parsed: %+v", v1.EventTrigger)
	}
	renderedV1 := functionJSONV1("proj", functionsstore.Function{
		ID: "f", Location: "us-central1", EventTrigger: v1.EventTrigger,
	})
	if fp, _ := renderedV1["eventTrigger"].(map[string]any)["failurePolicy"].(map[string]any); fp == nil || fp["retry"] == nil {
		t.Fatalf("v1 failurePolicy not rendered: %+v", renderedV1["eventTrigger"])
	}

	v2 := FunctionInputFromMap(map[string]any{
		"buildConfig":  map[string]any{"runtime": "nodejs22"},
		"eventTrigger": map[string]any{"eventType": "google.pubsub.topic.publish", "pubsubTopic": "t", "retryPolicy": "RETRY_POLICY_RETRY"},
	}, V2)
	if v2.EventTrigger == nil || !v2.EventTrigger.Retries() {
		t.Fatalf("v2 retryPolicy not parsed: %+v", v2.EventTrigger)
	}
	renderedV2 := functionJSONV2("proj", functionsstore.Function{
		ID: "f", Location: "us-central1", EventTrigger: v2.EventTrigger,
	})
	if rp, _ := renderedV2["eventTrigger"].(map[string]any)["retryPolicy"].(string); rp != "RETRY_POLICY_RETRY" {
		t.Fatalf("v2 retryPolicy not rendered: %+v", renderedV2["eventTrigger"])
	}
}

func TestDispatchWithWorkerPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := newDeliveryService(t)
	s.Start(ctx)
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("async"),
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		got := deliveries(t, s)
		if len(got) == 1 && got[0].Status == functionsstore.DeliveryDelivered {
			if got[0].Result != "async" {
				t.Fatalf("result = %q", got[0].Result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery did not complete: %+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFunctionExists(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t)
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})
	ok, err := s.FunctionExists(ctx, "proj", "us-central1", "fn")
	if err != nil || !ok {
		t.Fatalf("FunctionExists = %v, %v", ok, err)
	}
	ok, err = s.FunctionExists(ctx, "proj", "us-central1", "missing")
	if err != nil || ok {
		t.Fatalf("FunctionExists missing = %v, %v", ok, err)
	}
}
