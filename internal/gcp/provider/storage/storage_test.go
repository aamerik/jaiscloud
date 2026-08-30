package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newTestProvider() *Provider {
	return New(store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore())
}

func bucketParams() *model.NormalizedRequest {
	return &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{}, ResourceID: resource.ResourceID("proj")}
}

func TestBucketRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "my-bucket", "location": "US"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "my-bucket"
	resp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["name"] != "my-bucket" {
		t.Errorf("expected bucket name my-bucket, got %v", resp.Data["name"])
	}

	list, err := p.BucketsList(ctx, bucketParams())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(items))
	}
}

func TestObjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Create bucket first.
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Insert object with media bytes.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "dir/file.txt"
	nr.Params[wire.MediaKey] = []byte("hello world")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}
	if resp.Data["size"] != "11" {
		t.Errorf("expected size 11, got %v", resp.Data["size"])
	}
	if resp.Data["md5Hash"] == "" {
		t.Error("expected md5Hash to be set")
	}

	// Get media back.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "dir/file.txt"
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	got, _ := media.Data[wire.MediaKey].([]byte)
	if string(got) != "hello world" {
		t.Errorf("expected media bytes, got %q", got)
	}

	// List objects.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	list, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 object, got %d", len(items))
	}
	obj := items[0].(map[string]any)
	if obj["name"] != "dir/file.txt" {
		t.Errorf("expected object name dir/file.txt, got %v", obj["name"])
	}

	// Delete object.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "dir/file.txt"
	if _, err := p.ObjectsDelete(ctx, nr); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "dir/file.txt")); err == nil {
		t.Error("expected NotFound after delete")
	}
}

func bucketParamsWithObj(bucket, object string) *model.NormalizedRequest {
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	return nr
}

func TestObjectJSONRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Insert via JSON metadata body (no media) then round-trip through JSON.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"name": "meta.json", "contentType": "application/json"}
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	resp, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "meta.json"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Ensure the response is JSON-serialisable without internal wire keys.
	b, _ := json.Marshal(resp.Data)
	if string(b) == "" {
		t.Error("expected non-empty JSON")
	}
	if resp.Data["contentType"] != "application/json" {
		t.Errorf("expected contentType application/json, got %v", resp.Data["contentType"])
	}
}

func TestObjectsListPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// Page 1: maxResults=2 → 2 items + nextPageToken.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["maxResults"] = "2"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 items on page 1, got %d", len(items))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}

	// Page 2: pageToken → 1 item.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["pageToken"] = token
	resp, err = p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	items, _ = resp.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 item on page 2, got %d", len(items))
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("did not expect nextPageToken on final page")
	}
}

func TestObjectsListPrefixFilter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"logs/a", "logs/b", "data/c"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "logs/"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 prefixed items, got %d", len(items))
	}
}

func TestResumableMultiChunkUpload(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Start the resumable session.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "big.bin"
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start resumable: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	if loc == "" {
		t.Fatal("expected Location header")
	}
	// Extract upload_id from the Location query string.
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}
	if uploadID == "" {
		t.Fatalf("no upload_id in Location %q", loc)
	}

	// First chunk (unknown total) → 308 + Range.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if resp.HTTPStatus != 308 {
		t.Fatalf("expected 308, got %d", resp.HTTPStatus)
	}
	if rng, _ := resp.Data[wire.RangeKey].(string); rng != "bytes=0-4" {
		t.Fatalf("expected Range bytes=0-4, got %q", rng)
	}

	// Final chunk (known total) → 200, object stored.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 5-10/11"
	nr.Params[wire.MediaKey] = []byte(" world")
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil {
		t.Fatalf("final chunk: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("expected 200, got %d", resp.HTTPStatus)
	}

	// Verify the assembled object.
	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "big.bin"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(media.Data[wire.MediaKey].([]byte)); got != "hello world" {
		t.Fatalf("expected assembled 'hello world', got %q", got)
	}
}

func TestBucketIAMRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Default policy has bindings.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get iam: %v", err)
	}
	bindings, _ := resp.Data["bindings"].([]iamBinding)
	if len(bindings) == 0 {
		t.Fatal("expected default bindings")
	}

	// Set a new policy and read it back.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{
		"bindings": []any{
			map[string]any{"role": "roles/storage.objectViewer", "members": []any{"allUsers"}},
		},
	}
	if _, err := p.BucketsSetIamPolicy(ctx, nr); err != nil {
		t.Fatalf("set iam: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err = p.BucketsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get iam after set: %v", err)
	}
	bindings, _ = resp.Data["bindings"].([]iamBinding)
	if len(bindings) != 1 || bindings[0].Role != "roles/storage.objectViewer" {
		t.Fatalf("unexpected bindings: %+v", bindings)
	}
}

func TestACLRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Default bucket ACL has entries.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketACLList(ctx, nr)
	if err != nil {
		t.Fatalf("acl list: %v", err)
	}
	if items, _ := resp.Data["items"].([]aclEntry); len(items) == 0 {
		t.Fatal("expected default ACL entries")
	}

	// Insert a new ACL entry and confirm it appears.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"entity": "allUsers", "role": "READER"}
	if _, err := p.BucketACLInsert(ctx, nr); err != nil {
		t.Fatalf("acl insert: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err = p.BucketACLList(ctx, nr)
	if err != nil {
		t.Fatalf("acl list after insert: %v", err)
	}
	found := false
	for _, it := range resp.Data["items"].([]aclEntry) {
		if it.Entity == "allUsers" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected allUsers ACL entry after insert")
	}
}

func TestObjectsListDelimiter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"folder/a.txt", "folder/b.txt", "root.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["delimiter"] = "/"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	prefixes, _ := resp.Data["prefixes"].([]string)
	if len(prefixes) != 1 || prefixes[0] != "folder/" {
		t.Errorf("expected prefixes [folder/], got %v", prefixes)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 1 {
		t.Errorf("expected 1 top-level item, got %d", len(items))
	}
}

func TestBucketDeleteNonEmpty(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "obj"
	nr.Params[wire.MediaKey] = []byte("x")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	if _, err := p.BucketsDelete(ctx, nr); err == nil {
		t.Fatal("expected 409 on non-empty bucket delete")
	}
}

func TestResumableOffsetRepairAndStatusQuery(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Start with a content type.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "big.bin"
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}

	// Chunk 1.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk1: %v / %v", resp, err)
	}

	// Duplicate chunk retry — must NOT duplicate bytes.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("dup chunk: %v / %v", resp, err)
	}
	if rng, _ := resp.Data[wire.RangeKey].(string); rng != "bytes=0-4" {
		t.Fatalf("dup chunk range = %q, want bytes=0-4", rng)
	}

	// Status query (bytes */11) — must not finalize.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */11"
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("status query: %v / %v", resp, err)
	}

	// Final chunk.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 5-10/11"
	nr.Params[wire.MediaKey] = []byte(" world")
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("final: %v / %v", resp, err)
	}
	if resp.Data["size"] != "11" {
		t.Fatalf("expected size 11, got %v", resp.Data["size"])
	}

	// Content type preserved from start.
	if resp.Data["contentType"] != "application/octet-stream" {
		t.Errorf("expected contentType application/octet-stream, got %v", resp.Data["contentType"])
	}
}

func TestObjectChecksumAndIdentityFields(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "hello.txt"
	nr.Params[wire.MediaKey] = []byte("hello")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}
	// crc32c (CRC32C-Castagnoli) must be present and base64.
	if c, _ := resp.Data["crc32c"].(string); c == "" {
		t.Error("expected crc32c field")
	}
	// id must be bucket/object/generation.
	gen, _ := resp.Data["generation"].(string)
	expectedID := "bkt/hello.txt/" + gen
	if id, _ := resp.Data["id"].(string); id != expectedID {
		t.Errorf("expected id %q, got %q", expectedID, id)
	}
	if e, _ := resp.Data["etag"].(string); e == "" {
		t.Error("expected etag field")
	}

	// Bucket must include id/metageneration/projectNumber.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	bresp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if id, _ := bresp.Data["id"].(string); id != "bkt" {
		t.Errorf("expected bucket id bkt, got %q", id)
	}
	if mg, _ := bresp.Data["metageneration"].(string); mg == "" {
		t.Error("expected bucket metageneration")
	}
	if _, ok := bresp.Data["projectNumber"]; !ok {
		t.Error("expected bucket projectNumber")
	}
}
