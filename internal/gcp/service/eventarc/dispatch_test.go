package eventarc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/eventing"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

// capturedRequest is one HTTP request a test sink received.
type capturedRequest struct {
	method string
	header http.Header
	body   []byte
}

// requestSink is a test HTTP endpoint that records every request.
type requestSink struct {
	*httptest.Server
	mu  sync.Mutex
	got []capturedRequest
}

func newRequestSink(t *testing.T) *requestSink {
	t.Helper()
	s := &requestSink{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.got = append(s.got, capturedRequest{method: r.Method, header: r.Header.Clone(), body: body})
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *requestSink) requests() []capturedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capturedRequest(nil), s.got...)
}

// newDispatchService returns a service whose outbound client posts to sink.
func newDispatchService(t *testing.T, sink *requestSink) *Service {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "t")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	if sink != nil {
		svc.httpClient = sink.Client()
	}
	return svc
}

func createHTTPTrigger(t *testing.T, svc *Service, id, uri, filters string) {
	t.Helper()
	body := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"` + uri + `"}},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":` + filters + `}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", id, body, false); err != nil {
		t.Fatalf("create trigger %s: %v", id, err)
	}
}

func pubsubEvent() eventing.Event {
	return eventing.Event{
		Project:    "proj",
		EventType:  eventing.TypePubSubPublish,
		Resource:   "projects/proj/topics/t",
		EventID:    "msg-123",
		Source:     eventing.SourcePubSub,
		Data:       []byte("hello world"),
		Attributes: map[string]string{"k": "v"},
		OccurredAt: time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC),
	}
}

func TestDispatchEventPostsCloudEventToHTTPEndpoint(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createHTTPTrigger(t, svc, "trig1", sink.URL, `[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()

	got := sink.requests()
	if len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}
	req := got[0]
	if req.method != http.MethodPost {
		t.Fatalf("method = %s, want POST", req.method)
	}
	headers := map[string]string{
		"ce-id":          "msg-123",
		"ce-source":      "//pubsub.googleapis.com/projects/proj/topics/t",
		"ce-specversion": "1.0",
		"ce-type":        "google.cloud.pubsub.topic.v1.messagePublished",
		"ce-time":        "2026-06-25T12:00:00Z",
		"Content-Type":   "application/json",
	}
	for k, want := range headers {
		if gotV := req.header.Get(k); gotV != want {
			t.Errorf("header %s = %q, want %q", k, gotV, want)
		}
	}

	var payload struct {
		Message struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
		} `json:"message"`
		Subscription string `json:"subscription"`
	}
	if err := json.Unmarshal(req.body, &payload); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if payload.Message.Data != base64.StdEncoding.EncodeToString([]byte("hello world")) {
		t.Errorf("message.data = %q", payload.Message.Data)
	}
	if payload.Message.MessageID != "msg-123" {
		t.Errorf("message.messageId = %q", payload.Message.MessageID)
	}
	if payload.Message.Attributes["k"] != "v" {
		t.Errorf("message.attributes = %+v", payload.Message.Attributes)
	}
	if want := "projects/proj/subscriptions/eventarc-us-central1-trig1"; payload.Subscription != want {
		t.Errorf("subscription = %q, want %q", payload.Subscription, want)
	}
}

func TestDispatchEventTypeMismatchDoesNotDeliver(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	// A storage-typed filter on a Pub/Sub transport must not match a Pub/Sub event.
	createHTTPTrigger(t, svc, "trig1", sink.URL, `[{"attribute":"type","value":"google.storage.object.finalize"}]`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0", len(got))
	}
}

func TestDispatchEventTopicAttributeLastSegmentFallback(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	// Filter uses the short topic id while the event carries the full resource.
	createHTTPTrigger(t, svc, "trig1", sink.URL,
		`[{"attribute":"type","value":"google.pubsub.topic.publish"},{"attribute":"topic","value":"t"}]`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}

	// A different topic does not match.
	svc2 := newDispatchService(t, sink)
	createHTTPTrigger(t, svc2, "trig2", sink.URL,
		`[{"attribute":"type","value":"google.pubsub.topic.publish"},{"attribute":"topic","value":"other"}]`)
	svc2.DispatchEvent(context.Background(), pubsubEvent())
	svc2.waitDeliveries()
	if got := sink.requests(); len(got) != 1 {
		t.Fatalf("after non-matching topic, sink requests = %d, want still 1", len(got))
	}
}

func TestDispatchEventSkipsCloudFunctionDestination(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	svc.SetFunctionExister(fakeFunctions{exists: map[string]bool{"us-central1/fn": true}})
	body := json.RawMessage(`{
		"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/fn"},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", "cf", body, false); err != nil {
		t.Fatalf("create cloudFunction trigger: %v", err)
	}

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0 (functions owns cloudFunction)", len(got))
	}
}

func TestDispatchEventDropsCloudRunDestination(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	body := json.RawMessage(`{
		"destination":{"cloudRun":{"service":"projects/proj/locations/us-central1/services/s"}},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", "run", body, false); err != nil {
		t.Fatalf("create cloudRun trigger: %v", err)
	}

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0 (cloudRun is a later phase)", len(got))
	}
}

func TestDispatchEventIgnoresNonPubSubSource(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createHTTPTrigger(t, svc, "trig1", sink.URL, `[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]`)

	ev := pubsubEvent()
	ev.Source = eventing.SourceStorage
	svc.DispatchEvent(context.Background(), ev)
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0 for a storage source", len(got))
	}
}

func TestDispatchEventConnectionFailureIsLoggedNotPanicked(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createHTTPTrigger(t, svc, "trig1", sink.URL, `[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]`)
	// Close the sink first so the POST fails at connect time.
	sink.Close()

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries() // must return without panicking
}

func TestFiltersMatchNoTypeFallback(t *testing.T) {
	attrs := map[string]string{"type": "google.cloud.pubsub.topic.v1.messagePublished", "topic": "projects/p/topics/t"}
	if !filtersMatch([]any{map[string]any{"attribute": "topic", "value": "t"}}, attrs, "") {
		t.Error("topic filter should match by last segment")
	}
	if filtersMatch([]any{map[string]any{"attribute": "type", "value": "google.storage.object.finalize"}}, attrs, "google.pubsub.topic.publish") {
		t.Error("storage type filter must not match a pubsub event")
	}
	if filtersMatch([]any{map[string]any{"attribute": "missing", "value": "x"}}, attrs, "x") {
		t.Error("filter on an absent attribute must not match")
	}
	// match-path-pattern still applies to non-type attributes.
	pathAttrs := map[string]string{"object": "dir/sub/file.txt"}
	if !filtersMatch([]any{map[string]any{"attribute": "object", "value": "dir/", "operator": "match-path-pattern"}}, pathAttrs, "") {
		t.Error("match-path-pattern prefix should match")
	}
}
