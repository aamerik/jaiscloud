package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestPubSubNegativesAndPagination(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	// Create three topics for pagination.
	for _, id := range []string{"a", "b", "c"} {
		nr := newNR(map[string]any{"name": "topics/" + id})
		if _, err := p.TopicCreate(ctx, nr); err != nil {
			t.Fatalf("create topic %s: %v", id, err)
		}
	}

	// Page 1.
	nr := newNR(map[string]any{"pageSize": "2"})
	resp, err := p.TopicList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	topics, _ := resp.Data["topics"].([]any)
	if len(topics) != 2 {
		t.Fatalf("page 1 expected 2 topics, got %d", len(topics))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken on page 1")
	}

	// Page 2.
	nr = newNR(map[string]any{"pageSize": "2", "pageToken": token})
	resp, err = p.TopicList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	topics, _ = resp.Data["topics"].([]any)
	if len(topics) != 1 {
		t.Fatalf("page 2 expected 1 topic, got %d", len(topics))
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("expected no nextPageToken on final page")
	}

	// 409 on duplicate create.
	nr = newNR(map[string]any{"name": "topics/a"})
	if _, err := p.TopicCreate(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate topic, got %v", err)
	}

	// 404 on missing get/delete.
	nr = newNR(map[string]any{"name": "topics/missing"})
	if _, err := p.TopicGet(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing topic get, got %v", err)
	}
	if _, err := p.TopicDelete(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing topic delete, got %v", err)
	}

	// 400 on missing name.
	nr = newNR(nil)
	if _, err := p.TopicGet(ctx, nr); err == nil || errStatus(err) != 400 {
		t.Fatalf("expected 400 on missing name, got %v", err)
	}
}

// TestPubSubDLQ verifies deadLetterPolicy: a message is delivered up to
// maxDeliveryAttempts times, then republished to the dead-letter topic.
func TestPubSubDLQ(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	for _, id := range []string{"src", "dlq"} {
		if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/" + id})); err != nil {
			t.Fatalf("create topic %s: %v", id, err)
		}
	}

	// Subscription with DLQ (maxDeliveryAttempts=2).
	nr := newNR(map[string]any{
		"name": "subscriptions/sub",
		"body": map[string]any{
			"topic": "projects/proj/topics/src",
			"deadLetterPolicy": map[string]any{
				"deadLetterTopic":     "projects/proj/topics/dlq",
				"maxDeliveryAttempts": float64(2),
			},
		},
	})
	if _, err := p.SubscriptionCreate(ctx, nr); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	// Publish one message.
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/src",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	pull := func() int {
		resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/sub"}))
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		received, _ := resp.Data["receivedMessages"].([]any)
		return len(received)
	}
	// redeliver makes every message in the source topic immediately visible
	// again, simulating the ack deadline expiring.
	redeliver := func() {
		msgs, _ := p.messages.List(ctx, "src")
		ids := make([]string, 0, len(msgs))
		for _, m := range msgs {
			ids = append(ids, "src/"+m.MessageID)
		}
		_ = p.messages.ModifyAckDeadline(ctx, "src", ids, 0, time.Now())
	}

	if got := pull(); got != 1 {
		t.Fatalf("pull 1 expected 1 message, got %d", got)
	}
	redeliver()
	if got := pull(); got != 1 {
		t.Fatalf("pull 2 expected 1 message, got %d", got)
	}
	redeliver()
	if got := pull(); got != 0 {
		t.Fatalf("pull 3 expected 0 messages (moved to DLQ), got %d", got)
	}

	// The message now lives on the DLQ topic.
	msgs, err := p.messages.List(ctx, "dlq")
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected 1 message in DLQ topic, got %d / %v", len(msgs), err)
	}
	if msgs[0].Data != "aGk=" {
		t.Errorf("unexpected DLQ message data: %q", msgs[0].Data)
	}
}

// TestPubSubPushSubscription verifies pushConfig.pushEndpoint delivery (SNS
// deliverToHTTP analogue).
func TestPubSubPushSubscription(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/src"})); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/push",
		"body": map[string]any{
			"topic":      "projects/proj/topics/src",
			"pushConfig": map[string]any{"pushEndpoint": srv.URL},
		},
	})); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/src",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGVsbG8="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatalf("push payload not JSON: %v (body=%q)", err, gotBody)
	}
	msg, _ := payload["message"].(map[string]any)
	if msg["data"] != "aGVsbG8=" {
		t.Errorf("push message data = %v, want aGVsbG8=", msg["data"])
	}
}

// TestPubSubPushDeliveryTimeout verifies a hung push endpoint cannot block
// publish indefinitely — the delivery client has a bounded timeout.
func TestPubSubPushDeliveryTimeout(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/src"})); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// Endpoint that blocks far longer than the delivery client timeout.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
	}))
	defer srv.Close()

	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/push",
		"body": map[string]any{
			"topic":      "projects/proj/topics/src",
			"pushConfig": map[string]any{"pushEndpoint": srv.URL},
		},
	})); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	old := pushClient
	pushClient = &http.Client{Timeout: 200 * time.Millisecond}
	defer func() { pushClient = old }()

	start := time.Now()
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/src",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("publish blocked for %v despite push timeout", elapsed)
	}
}

// TestPubSubIamPolicy verifies topic getIamPolicy/setIamPolicy/testIamPermissions.
func TestPubSubIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages())

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/t1"})); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Default policy.
	resp, err := p.TopicGetIamPolicy(ctx, newNR(map[string]any{"name": "topics/t1"}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if resp.Data["version"] != 1 {
		t.Errorf("expected version 1, got %v", resp.Data["version"])
	}

	// setIamPolicy (etag OCC).
	bindings := []any{map[string]any{"role": "roles/pubsub.publisher", "members": []any{"allUsers"}}}
	if _, err := p.TopicSetIamPolicy(ctx, newNR(map[string]any{"name": "topics/t1", "body": map[string]any{"bindings": bindings, "etag": "BOGUS="}})); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on stale etag, got %v", err)
	}
	set, err := p.TopicSetIamPolicy(ctx, newNR(map[string]any{"name": "topics/t1", "body": map[string]any{"bindings": bindings}}))
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	if etag, _ := set.Data["etag"].(string); etag == "" {
		t.Error("expected fresh etag")
	}

	// testIamPermissions.
	tr, err := p.TopicTestIamPermissions(ctx, newNR(map[string]any{"name": "topics/t1", "body": map[string]any{"permissions": []any{"pubsub.topics.publish"}}}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	if perms, _ := tr.Data["permissions"].([]string); len(perms) != 1 {
		t.Errorf("expected 1 granted permission, got %v", tr.Data["permissions"])
	}

	// 404 on missing topic.
	if _, err := p.TopicGetIamPolicy(ctx, newNR(map[string]any{"name": "topics/missing"})); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing topic, got %v", err)
	}
}
