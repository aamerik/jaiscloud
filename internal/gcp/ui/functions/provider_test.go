package functionsui

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/blobfs"
	functionscore "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

// newProvider builds a real UI provider over the Functions core with an
// in-memory store + blob store, so the adapter's input construction and inline
// archive packaging are exercised against the core's validation.
func newProvider() (*Provider, functionsstore.Store) {
	mem := functionsstore.NewMemoryStore()
	svc := functionscore.NewService(mem, store.NewMemoryResourceStore(), functionscore.WithBlobs(blobfs.NewMemoryBlobStore()))
	return NewProvider(svc), mem
}

func TestProvider_CreateListGetFunction(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider()

	created, err := p.CreateFunction(ctx, "test-project", "us-central1", CreateInput{
		ID:                "hello",
		Location:          "us-central1",
		Runtime:           "nodejs20",
		EntryPoint:        "handler",
		Description:       "a test function",
		AvailableMemoryMB: 256,
		Timeout:           "60s",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != "hello" || created.Location != "us-central1" {
		t.Fatalf("unexpected created function: %+v", created)
	}

	got, err := p.GetFunction(ctx, "test-project", "us-central1", "hello")
	if err != nil || got.ID != "hello" {
		t.Fatalf("get: %v %+v", err, got)
	}

	list, err := p.ListFunctions(ctx, "test-project")
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %d", err, len(list))
	}

	// The core's canonical v2 rendering must supply the resource name + url.
	row := renderFunction("test-project", got)
	if row.Name != "projects/test-project/locations/us-central1/functions/hello" {
		t.Fatalf("name = %q", row.Name)
	}
	if row.TriggerType != "http" || row.URL == "" {
		t.Fatalf("http trigger not rendered: %+v", row)
	}
}

func TestProvider_CreateEventFunctionRetries(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider()

	created, err := p.CreateFunction(ctx, "test-project", "europe-west1", CreateInput{
		ID:            "on-event",
		Location:      "europe-west1",
		Runtime:       "python312",
		EntryPoint:    "main.handler",
		TriggerType:   "event",
		EventType:     "google.cloud.pubsub.topic.v1.messagePublished",
		EventResource: "projects/test-project/topics/t1",
		Retry:         true,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.EventTrigger == nil || !created.EventTrigger.Retries() {
		t.Fatalf("event trigger/retry not persisted: %+v", created.EventTrigger)
	}
	row := renderFunction("test-project", created)
	if row.TriggerType != "event" || row.EventTrigger == nil || !row.EventTrigger.Retry {
		t.Fatalf("event trigger not rendered: %+v", row)
	}
	if row.EventTrigger.Resource != "projects/test-project/topics/t1" {
		t.Fatalf("event resource = %q", row.EventTrigger.Resource)
	}
}

func TestProvider_InlineArchivePersistsSource(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider()

	created, err := p.CreateFunction(ctx, "test-project", "us-central1", CreateInput{
		ID:             "inline",
		Location:       "us-central1",
		Runtime:        "nodejs20",
		EntryPoint:     "handler",
		SourceInline:   "exports.handler = () => 'ok'",
		SourceFilename: "index.js",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.SourceSHA256 == "" || created.SourceSize == 0 || created.SourceBlobKey == "" {
		t.Fatalf("inline source not persisted: %+v", created)
	}
	if created.Revision != 1 {
		t.Fatalf("revision = %d, want 1", created.Revision)
	}
}

func TestProvider_CreateValidation(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider()
	// An out-of-range v2 memory must be rejected by the core.
	if _, err := p.CreateFunction(ctx, "test-project", "us-central1", CreateInput{
		ID: "bad", Location: "us-central1", Runtime: "nodejs20", AvailableMemoryMB: 100000,
	}); err == nil {
		t.Fatal("expected validation error for availableMemoryMB 100000")
	}
}

func TestProvider_Delete(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider()
	if _, err := p.CreateFunction(ctx, "test-project", "us-central1", CreateInput{
		ID: "gone", Location: "us-central1", Runtime: "nodejs20",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := p.DeleteFunction(ctx, "test-project", "us-central1", "gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if list, err := p.ListFunctions(ctx, "test-project"); err != nil || len(list) != 0 {
		t.Fatalf("function not deleted: %v %+v", err, list)
	}
}

func TestProvider_ListDeliveriesFiltersFunction(t *testing.T) {
	ctx := context.Background()
	p, mem := newProvider()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, d := range []functionsstore.Delivery{
		{ID: "d1", FunctionID: "f1", Location: "us-central1", Status: functionsstore.DeliveryDelivered, CreateTime: t0},
		{ID: "d2", FunctionID: "f2", Location: "us-central1", Status: functionsstore.DeliveryFailed, CreateTime: t0},
		{ID: "d3", FunctionID: "f1", Location: "us-central1", Status: functionsstore.DeliveryDeadLetter, CreateTime: t0.Add(time.Second)},
	} {
		if err := mem.CreateDelivery(ctx, "test-project", "us-central1", d); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
	}
	got, err := p.ListDeliveries(ctx, "test-project", "us-central1", "f1")
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	if len(got) != 2 || got[0].ID != "d1" || got[1].ID != "d3" {
		t.Fatalf("deliveries = %+v, want d1+d3 for f1", got)
	}
}

func TestInlineArchive_EmptySource(t *testing.T) {
	if b, err := inlineArchive("", ""); err != nil || b != nil {
		t.Fatalf("empty inline = %v, %v", b, err)
	}
	b, err := inlineArchive("code", "")
	if err != nil || len(b) == 0 {
		t.Fatalf("inline archive = %d bytes, err %v", len(b), err)
	}
}
