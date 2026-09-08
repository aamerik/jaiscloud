package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// commitUpdates applies the given updates in a commit and returns the rendered
// metadata, failing the test on any error.
func commitUpdates(t *testing.T, p *Provider, ns, table string, updates []any) map[string]any {
	t.Helper()
	resp, err := p.CommitTable(context.Background(), newNR(map[string]any{
		"namespace": ns,
		"table":     table,
		"body":      map[string]any{"requirements": []any{}, "updates": updates},
	}))
	if err != nil {
		t.Fatalf("CommitTable: %v", err)
	}
	return resp.Data["metadata"].(map[string]any)
}

// assertCommitFailed asserts the updates are rejected with CommitFailedException.
func assertCommitFailed(t *testing.T, p *Provider, ns, table string, updates []any) {
	t.Helper()
	_, err := p.CommitTable(context.Background(), newNR(map[string]any{
		"namespace": ns,
		"table":     table,
		"body":      map[string]any{"requirements": []any{}, "updates": updates},
	}))
	if err == nil {
		t.Fatal("expected CommitFailedException, got nil")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "CommitFailedException" {
		t.Fatalf("expected CommitFailedException, got %v", err)
	}
}

func addSnapshot(t *testing.T, p *Provider, ns, table string, id int) map[string]any {
	t.Helper()
	return commitUpdates(t, p, ns, table, []any{
		map[string]any{"action": "add-snapshot", "snapshot": map[string]any{
			"snapshot-id": id, "timestamp-ms": id, "manifest-list": "s3://m/ml",
		}},
	})
}

func TestSetSnapshotRefMainSyncsCurrentSnapshotID(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	addSnapshot(t, p, "db", "t1", 100)
	addSnapshot(t, p, "db", "t1", 200) // current is now 200

	// Setting the main ref must move current-snapshot-id in lockstep.
	meta := commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 100, "type": "branch"},
	})
	if meta["current-snapshot-id"] != float64(100) {
		t.Errorf("current-snapshot-id = %v, want 100", meta["current-snapshot-id"])
	}
	refs := meta["refs"].(map[string]any)
	main := refs["main"].(map[string]any)
	if main["snapshot-id"] != float64(100) {
		t.Errorf("refs.main.snapshot-id = %v, want 100", main["snapshot-id"])
	}
}

func TestRemoveSnapshotRefMainClearsCurrentSnapshotID(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	addSnapshot(t, p, "db", "t1", 100)
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 100, "type": "branch"},
	})

	// Removing main clears current-snapshot-id and the explicit ref.
	meta := commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "remove-snapshot-ref", "ref-name": "main"},
	})
	if meta["current-snapshot-id"] != float64(-1) {
		t.Errorf("current-snapshot-id = %v, want -1", meta["current-snapshot-id"])
	}
	if refs := meta["refs"].(map[string]any); refs["main"] != nil {
		t.Errorf("main ref should be removed, got %v", refs["main"])
	}
}

func TestRemoveSnapshotsClearsCurrentNoPromoteKeepsLog(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	addSnapshot(t, p, "db", "t1", 100)
	addSnapshot(t, p, "db", "t1", 200) // current = 200, snapshot-log has 2 entries

	// Removing the current snapshot must set current-snapshot-id = -1 (no
	// auto-promote to 100) and must NOT filter the append-only snapshot-log.
	meta := commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "remove-snapshots", "snapshot-ids": []any{200}},
	})
	if meta["current-snapshot-id"] != float64(-1) {
		t.Errorf("current-snapshot-id = %v, want -1 (no auto-promote)", meta["current-snapshot-id"])
	}
	snapshots := meta["snapshots"].([]any)
	if len(snapshots) != 1 || snapshots[0].(map[string]any)["snapshot-id"] != float64(100) {
		t.Errorf("snapshots = %v, want only [100]", snapshots)
	}
	if log := meta["snapshot-log"].([]any); len(log) != 2 {
		t.Errorf("snapshot-log len = %d, want 2 (append-only)", len(log))
	}
}

func TestMaxSchemaColumnIDNested(t *testing.T) {
	schema := map[string]any{
		"type": "struct",
		"fields": []any{
			map[string]any{"id": 1, "name": "id", "type": "long", "required": true},
			map[string]any{"name": "nested", "type": map[string]any{
				"type": "struct",
				"fields": []any{
					map[string]any{"name": "x", "type": "string", "required": false},
					map[string]any{"name": "y", "type": "int", "required": false},
				},
			}},
		},
	}
	if got := maxSchemaColumnID(schema); got != 4 {
		t.Fatalf("maxSchemaColumnID = %d, want 4", got)
	}
	nested := schema["fields"].([]any)[1].(map[string]any)
	inner := nested["type"].(map[string]any)
	if inner["fields"].([]any)[0].(map[string]any)["id"] != 3 {
		t.Errorf("nested field id not assigned: %v", inner["fields"])
	}
}

func TestCreateTableNestedLastColumnID(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	resp, err := p.CreateTable(context.Background(), newNR(map[string]any{
		"namespace": "db",
		"body": map[string]any{
			"name":     "t1",
			"location": "s3://w/db/t1",
			"schema": map[string]any{
				"type": "struct",
				"fields": []any{
					map[string]any{"id": 1, "name": "id", "type": "long", "required": true},
					map[string]any{"name": "nested", "type": map[string]any{
						"type": "struct",
						"fields": []any{
							map[string]any{"name": "x", "type": "string", "required": false},
							map[string]any{"name": "y", "type": "int", "required": false},
						},
					}},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if got := resp.Data["metadata"].(map[string]any)["last-column-id"]; got != float64(4) {
		t.Errorf("last-column-id = %v, want 4", got)
	}
}

func TestAddSchemaNestedFallback(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	// add-schema without last-column-id falls back to maxSchemaColumnID, which
	// must recurse into the nested struct.
	meta := commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "add-schema", "schema": map[string]any{
			"type": "struct", "schema-id": 1,
			"fields": []any{
				map[string]any{"name": "nested", "type": map[string]any{
					"type": "struct",
					"fields": []any{
						map[string]any{"id": 10, "name": "x", "type": "string", "required": false},
						map[string]any{"name": "y", "type": "int", "required": false},
					},
				}},
			},
		}},
	})
	if meta["last-column-id"] != float64(11) {
		t.Errorf("last-column-id = %v, want 11", meta["last-column-id"])
	}
}

func TestAssignUUIDRejectsDifferent(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	create := mustCreateTable(t, p, "db", "t1")
	existing := create["metadata"].(map[string]any)["table-uuid"].(string)

	// A different uuid is rejected.
	assertCommitFailed(t, p, "db", "t1", []any{
		map[string]any{"action": "assign-uuid", "uuid": "different-uuid"},
	})

	// The same uuid is accepted (no-op).
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "assign-uuid", "uuid": existing},
	})
}

func TestUpgradeFormatVersionValidation(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1") // format-version 2

	// Downgrade below current → rejected.
	assertCommitFailed(t, p, "db", "t1", []any{
		map[string]any{"action": "upgrade-format-version", "format-version": 1},
	})
	// Out of {1,2} → rejected.
	assertCommitFailed(t, p, "db", "t1", []any{
		map[string]any{"action": "upgrade-format-version", "format-version": 3},
	})
	// == current, in set → accepted.
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "upgrade-format-version", "format-version": 2},
	})
}

func TestSetCurrentSchemaValidation(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	assertCommitFailed(t, p, "db", "t1", []any{
		map[string]any{"action": "set-current-schema", "schema-id": 99},
	})
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "add-schema", "schema": map[string]any{"type": "struct", "schema-id": 1, "fields": []any{}}, "last-column-id": 2},
	})
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "set-current-schema", "schema-id": 1},
	})
}

func TestSetDefaultSpecValidation(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	assertCommitFailed(t, p, "db", "t1", []any{
		map[string]any{"action": "set-default-spec", "spec-id": 99},
	})
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "add-spec", "spec": map[string]any{"spec-id": 1, "fields": []any{}}},
	})
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "set-default-spec", "spec-id": 1},
	})
}

func TestSetDefaultSortOrderValidation(t *testing.T) {
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	assertCommitFailed(t, p, "db", "t1", []any{
		map[string]any{"action": "set-default-sort-order", "sort-order-id": 99},
	})
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "add-sort-order", "sort-order": map[string]any{"order-id": 1, "fields": []any{}}},
	})
	commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "set-default-sort-order", "sort-order-id": 1},
	})
}

func TestMetadataLogAppendedOncePerCommit(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateNamespace(t, p, "db")
	create := mustCreateTable(t, p, "db", "t1")
	loc0 := create["metadata-location"].(string)

	// A set-properties-only commit still appends a metadata-log entry whose
	// metadata-file is the previous metadata-location.
	resp, err := p.CommitTable(ctx, newNR(map[string]any{
		"namespace": "db",
		"table":     "t1",
		"body": map[string]any{
			"requirements": []any{},
			"updates":      []any{map[string]any{"action": "set-properties", "updates": map[string]any{"a": "b"}}},
		},
	}))
	if err != nil {
		t.Fatalf("CommitTable: %v", err)
	}
	log := resp.Data["metadata"].(map[string]any)["metadata-log"].([]any)
	if len(log) != 1 {
		t.Fatalf("metadata-log len = %d, want 1", len(log))
	}
	if got := log[0].(map[string]any)["metadata-file"]; got != loc0 {
		t.Errorf("metadata-file = %v, want %s", got, loc0)
	}

	// A second commit appends a second entry.
	meta := commitUpdates(t, p, "db", "t1", []any{
		map[string]any{"action": "set-properties", "updates": map[string]any{"c": "d"}},
	})
	if got := meta["metadata-log"].([]any); len(got) != 2 {
		t.Errorf("metadata-log len = %d, want 2", len(got))
	}
}

func TestCommitCorruptMetadata500(t *testing.T) {
	ctx := context.Background()
	s := icebergstore.NewMemoryStore()
	p := New(s)
	_ = s.CreateNamespace(ctx, "db", nil)
	_ = s.CreateTable(ctx, "db", "t1", icebergstore.Table{
		Namespace:        "db",
		Name:             "t1",
		Metadata:         json.RawMessage("{corrupt"),
		MetadataLocation: "loc",
		UUID:             "u",
		Version:          0,
	})

	_, err := p.CommitTable(ctx, newNR(map[string]any{
		"namespace": "db",
		"table":     "t1",
		"body":      map[string]any{"requirements": []any{}, "updates": []any{}},
	}))
	if err == nil {
		t.Fatal("expected error for corrupt metadata")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 500 {
		t.Fatalf("expected 500 server error, got %v", err)
	}
}

func TestLargeSnapshotIDRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := icebergstore.NewMemoryStore()
	p := New(s)
	mustCreateNamespace(t, p, "db")
	mustCreateTable(t, p, "db", "t1")

	// > 2^53: would silently corrupt into float64 without UseNumber.
	const snapshotID = 1000000000000000001
	bodyJSON := fmt.Sprintf(`{"requirements":[],"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":%d,"timestamp-ms":123,"manifest-list":"s3://m/ml"}}]}`, snapshotID)
	var body map[string]any
	dec := json.NewDecoder(strings.NewReader(bodyJSON))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if _, err := p.CommitTable(ctx, newNR(map[string]any{"namespace": "db", "table": "t1", "body": body})); err != nil {
		t.Fatalf("CommitTable: %v", err)
	}

	got, err := s.GetTable(ctx, "db", "t1")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	var meta map[string]any
	d2 := json.NewDecoder(bytes.NewReader(got.Metadata))
	d2.UseNumber()
	if err := d2.Decode(&meta); err != nil {
		t.Fatalf("decode stored metadata: %v", err)
	}
	cur, _ := meta["current-snapshot-id"].(json.Number).Int64()
	if cur != snapshotID {
		t.Fatalf("current-snapshot-id = %d, want %d", cur, snapshotID)
	}
}
