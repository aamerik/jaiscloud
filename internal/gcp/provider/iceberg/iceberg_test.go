package iceberg

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/resource"
	icebergstore "jaiscloud/internal/gcp/store/iceberg"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func newProvider() *Provider {
	return New(icebergstore.NewMemoryStore())
}

func mustCreateNamespace(t *testing.T, p *Provider, ns string) {
	t.Helper()
	if _, err := p.CreateNamespace(context.Background(), newNR(map[string]any{
		"body": map[string]any{"namespace": []any{ns}, "properties": map[string]any{}},
	})); err != nil {
		t.Fatalf("CreateNamespace(%s): %v", ns, err)
	}
}

func mustCreateTable(t *testing.T, p *Provider, ns, name string) map[string]any {
	t.Helper()
	resp, err := p.CreateTable(context.Background(), newNR(map[string]any{
		"namespace": ns,
		"body": map[string]any{
			"name":     name,
			"location": "s3://warehouse/" + ns + "/" + name,
			"schema": map[string]any{
				"type": "struct",
				"fields": []any{
					map[string]any{"id": 1, "name": "id", "type": "long", "required": true},
					map[string]any{"id": 2, "name": "data", "type": "string", "required": false},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return resp.Data
}

func TestNamespaceAndTableLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.GetNamespace(ctx, newNR(map[string]any{"namespace": "db"})); err == nil {
		t.Fatal("expected NoSuchNamespace for missing namespace")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "NoSuchNamespaceException" {
		t.Fatalf("expected NoSuchNamespaceException, got %v", err)
	}

	mustCreateNamespace(t, p, "db")
	create := mustCreateTable(t, p, "db", "t1")
	loc, _ := create["metadata-location"].(string)
	if !strings.HasPrefix(loc, "s3://warehouse/db/t1/metadata/00000-") || !strings.HasSuffix(loc, ".metadata.json") {
		t.Fatalf("metadata-location malformed: %q", loc)
	}
	meta := create["metadata"].(map[string]any)
	if meta["format-version"] != float64(2) {
		t.Errorf("format-version = %v, want 2", meta["format-version"])
	}
	if meta["table-uuid"] == "" {
		t.Error("table-uuid missing")
	}
	if meta["last-column-id"] != float64(2) {
		t.Errorf("last-column-id = %v, want 2", meta["last-column-id"])
	}
	schemas := meta["schemas"].([]any)
	if len(schemas) != 1 {
		t.Fatalf("schemas len = %d, want 1", len(schemas))
	}

	// LoadTable round-trips the same metadata.
	loaded, err := p.LoadTable(ctx, newNR(map[string]any{"namespace": "db", "table": "t1"}))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if loaded.Data["metadata-location"] != create["metadata-location"] {
		t.Errorf("metadata-location changed on load: %v vs %v", loaded.Data["metadata-location"], create["metadata-location"])
	}

	// ListTables includes it.
	listResp, err := p.ListTables(ctx, newNR(map[string]any{"namespace": "db"}))
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	ids := listResp.Data["identifiers"].([]any)
	if len(ids) != 1 {
		t.Fatalf("identifiers len = %d, want 1", len(ids))
	}

	// Namespace drop refused while a table remains.
	if _, err := p.DropNamespace(ctx, newNR(map[string]any{"namespace": "db"})); err == nil {
		t.Fatal("expected NamespaceNotEmpty, got nil")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "NamespaceNotEmptyException" {
		t.Fatalf("expected NamespaceNotEmptyException, got %v", err)
	}
}

func TestCommitRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	// First commit: set-properties + add-schema.
	resp, err := p.CommitTable(ctx, newNR(map[string]any{
		"namespace": "db",
		"table":     "t1",
		"body": map[string]any{
			"requirements": []any{},
			"updates": []any{
				map[string]any{"action": "set-properties", "updates": map[string]any{"owner": "me"}},
				map[string]any{"action": "add-schema", "schema": map[string]any{
					"type": "struct", "schema-id": 1,
					"fields": []any{map[string]any{"id": 3, "name": "extra", "type": "string", "required": false}},
				}, "last-column-id": 3},
				map[string]any{"action": "set-current-schema", "schema-id": 1},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CommitTable: %v", err)
	}
	meta := resp.Data["metadata"].(map[string]any)
	props := meta["properties"].(map[string]any)
	if props["owner"] != "me" {
		t.Errorf("properties = %v, want owner=me", props)
	}
	if len(meta["schemas"].([]any)) != 2 {
		t.Errorf("schemas len = %d, want 2", len(meta["schemas"].([]any)))
	}
	if meta["current-schema-id"] != float64(1) {
		t.Errorf("current-schema-id = %v, want 1", meta["current-schema-id"])
	}
	if meta["last-column-id"] != float64(3) {
		t.Errorf("last-column-id = %v, want 3", meta["last-column-id"])
	}
	loc1 := resp.Data["metadata-location"].(string)

	// Second commit: failing assert-table-uuid → 409, state unchanged.
	if _, err := p.CommitTable(ctx, newNR(map[string]any{
		"namespace": "db",
		"table":     "t1",
		"body": map[string]any{
			"requirements": []any{map[string]any{"type": "assert-table-uuid", "uuid": "wrong"}},
			"updates":      []any{map[string]any{"action": "set-properties", "updates": map[string]any{"owner": "hacker"}}},
		},
	})); err == nil {
		t.Fatal("expected CommitFailedException, got nil")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "CommitFailedException" || pe.HTTPStatus != 409 {
		t.Fatalf("expected CommitFailedException 409, got %v", err)
	}
	loaded, err := p.LoadTable(ctx, newNR(map[string]any{"namespace": "db", "table": "t1"}))
	if err != nil {
		t.Fatalf("LoadTable after rejected commit: %v", err)
	}
	lmeta := loaded.Data["metadata"].(map[string]any)
	if lmeta["properties"].(map[string]any)["owner"] != "me" {
		t.Errorf("state changed after rejected commit: owner = %v", lmeta["properties"].(map[string]any)["owner"])
	}
	if loaded.Data["metadata-location"] != loc1 {
		t.Errorf("metadata-location changed after rejected commit")
	}

	// Third commit with correct assert-table-uuid succeeds.
	resp2, err := p.CommitTable(ctx, newNR(map[string]any{
		"namespace": "db",
		"table":     "t1",
		"body": map[string]any{
			"requirements": []any{map[string]any{"type": "assert-table-uuid", "uuid": meta["table-uuid"]}},
			"updates":      []any{map[string]any{"action": "add-snapshot", "snapshot": map[string]any{"snapshot-id": 100, "timestamp-ms": 123, "manifest-list": "s3://m/ml"}}},
		},
	}))
	if err != nil {
		t.Fatalf("CommitTable (valid uuid): %v", err)
	}
	meta2 := resp2.Data["metadata"].(map[string]any)
	if meta2["current-snapshot-id"] != float64(100) {
		t.Errorf("current-snapshot-id = %v, want 100", meta2["current-snapshot-id"])
	}
	if len(meta2["snapshots"].([]any)) != 1 {
		t.Errorf("snapshots len = %d, want 1", len(meta2["snapshots"].([]any)))
	}
	if resp2.Data["metadata-location"] == loc1 {
		t.Errorf("metadata-location should advance on successful commit")
	}
}

func TestRenameAndDrop(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateNamespace(t, p, "db1")
	mustCreateNamespace(t, p, "db2")
	mustCreateTable(t, p, "db1", "t1")

	resp, err := p.RenameTable(ctx, newNR(map[string]any{
		"body": map[string]any{
			"source":      map[string]any{"namespace": []any{"db1"}, "name": "t1"},
			"destination": map[string]any{"namespace": []any{"db2"}, "name": "t2"},
		},
	}))
	if err != nil {
		t.Fatalf("RenameTable: %v", err)
	}
	if resp.HTTPStatus != 204 {
		t.Errorf("rename status = %d, want 204", resp.HTTPStatus)
	}
	if _, err := p.LoadTable(ctx, newNR(map[string]any{"namespace": "db1", "table": "t1"})); err == nil {
		t.Fatal("source table should be gone after rename")
	}
	if _, err := p.LoadTable(ctx, newNR(map[string]any{"namespace": "db2", "table": "t2"})); err != nil {
		t.Fatalf("destination table missing after rename: %v", err)
	}

	// Drop.
	if _, err := p.DropTable(ctx, newNR(map[string]any{"namespace": "db2", "table": "t2"})); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if _, err := p.LoadTable(ctx, newNR(map[string]any{"namespace": "db2", "table": "t2"})); err == nil {
		t.Fatal("table should be gone after drop")
	}
}

func TestRoutes_AllHandlersRegistered(t *testing.T) {
	p := newProvider()
	routes := p.Routes()
	want := []string{
		"Iceberg.GetConfig", "Iceberg.ListNamespaces", "Iceberg.CreateNamespace",
		"Iceberg.GetNamespace", "Iceberg.NamespaceExists", "Iceberg.DropNamespace",
		"Iceberg.UpdateNamespaceProperties", "Iceberg.ListTables", "Iceberg.CreateTable",
		"Iceberg.LoadTable", "Iceberg.CommitTable", "Iceberg.DropTable",
		"Iceberg.RenameTable", "Iceberg.TableMetrics",
	}
	for _, k := range want {
		if routes[k] == nil {
			t.Errorf("missing route %q", k)
		}
	}
	if len(routes) != len(want) {
		t.Errorf("got %d routes, want %d", len(routes), len(want))
	}
}
