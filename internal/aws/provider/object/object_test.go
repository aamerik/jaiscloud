package object

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	objectstore "jaiscloud/internal/aws/store/object"
	s3impl "jaiscloud/internal/aws/store/s3"
	"jaiscloud/internal/clock"
		"jaiscloud/internal/blobfs"
	"jaiscloud/internal/model"
)

// ─── stubs ────────────────────────────────────────────────────────────────────

// stubMeta wraps MemoryS3ObjectMetaStore and lets tests inject controlled errors.
type stubMeta struct {
	*s3impl.MemoryS3ObjectMetaStore
	deleteMetaErr    error
	deleteMetaCalled bool
	getMetaCallCount int
	// getMetaErrAfter: first N calls succeed; N+1 onward return an error.
	// Set to -1 to never fail.
	getMetaErrAfter int
}

func newStubMeta() *stubMeta {
	return &stubMeta{
		MemoryS3ObjectMetaStore: s3impl.NewMemoryS3ObjectMetaStore(),
		getMetaErrAfter:         -1,
	}
}

func (s *stubMeta) DeleteObjectMeta(ctx context.Context, bucket, key string) error {
	s.deleteMetaCalled = true
	if s.deleteMetaErr != nil {
		return s.deleteMetaErr
	}
	return s.MemoryS3ObjectMetaStore.DeleteObjectMeta(ctx, bucket, key)
}

func (s *stubMeta) GetObjectMeta(ctx context.Context, bucket, key string) (objectstore.ObjectMeta, error) {
	s.getMetaCallCount++
	if s.getMetaErrAfter >= 0 && s.getMetaCallCount > s.getMetaErrAfter {
		return objectstore.ObjectMeta{}, errors.New("not found")
	}
	return s.MemoryS3ObjectMetaStore.GetObjectMeta(ctx, bucket, key)
}

// stubBlob wraps MemoryBlobStore and lets tests inject failures on specific ops.
type stubBlob struct {
	*blobfs.MemoryBlobStore
	deleteCalled bool
	deleteErr    error
	getStreamErr error
}

func newStubBlob() *stubBlob {
	return &stubBlob{MemoryBlobStore: blobfs.NewMemoryBlobStore()}
}

func (s *stubBlob) Delete(ctx context.Context, bucket, key string) error {
	s.deleteCalled = true
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.MemoryBlobStore.Delete(ctx, bucket, key)
}

func (s *stubBlob) GetStream(ctx context.Context, bucket, key string, offset, length int64) (io.ReadCloser, error) {
	if s.getStreamErr != nil {
		return nil, s.getStreamErr
	}
	return s.MemoryBlobStore.GetStream(ctx, bucket, key, offset, length)
}

func makeNR(bucket, key string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Params: map[string]any{"_bucket": bucket, "_key": key},
	}
}

// ─── DeleteObject ─────────────────────────────────────────────────────────────

func TestDeleteObject_MetadataFailsReturns204BlobUntouched(t *testing.T) {
	// When metadata delete fails, blob.Delete must not be called (would create
	// reverse torn state: metadata present + blob absent).
	meta := newStubMeta()
	meta.deleteMetaErr = errors.New("store unavailable")
	blob := newStubBlob()
	p := New(meta, blob)

	resp, err := p.DeleteObject(context.Background(), makeNR("b", "k"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.HTTPStatus != 204 {
		t.Fatalf("status=%d want 204", resp.HTTPStatus)
	}
	if blob.deleteCalled {
		t.Fatal("blob.Delete must not be called when metadata delete fails")
	}
}

func TestDeleteObject_BlobFailIsGracefulReturns204(t *testing.T) {
	// Blob delete failure after metadata delete must not surface as an error.
	ctx := context.Background()
	meta := newStubMeta()
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k"})
	blob := newStubBlob()
	blob.deleteErr = errors.New("blob store down")
	p := New(meta, blob)

	resp, err := p.DeleteObject(ctx, makeNR("b", "k"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.HTTPStatus != 204 {
		t.Fatalf("status=%d want 204", resp.HTTPStatus)
	}
}

func TestDeleteObject_HappyPath(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)

	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k", Size: 4})
	_ = blob.Put(ctx, "b", "k", []byte("data"))

	resp, err := p.DeleteObject(ctx, makeNR("b", "k"))
	if err != nil || resp.HTTPStatus != 204 {
		t.Fatalf("status=%d err=%v", resp.HTTPStatus, err)
	}
	if _, err := meta.GetObjectMeta(ctx, "b", "k"); err == nil {
		t.Fatal("metadata should be deleted after DeleteObject")
	}
}

// ─── GetObject ────────────────────────────────────────────────────────────────

func TestGetObject_BlobMissAfterConcurrentDelete_Returns404(t *testing.T) {
	// Simulates: GetObjectMeta succeeds (first call), GetStream fails,
	// GetObjectMeta (recheck) fails → the delete already completed → 404.
	ctx := context.Background()
	meta := newStubMeta()
	meta.getMetaErrAfter = 1 // first call OK, second call returns error
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k", Size: 4})

	blob := newStubBlob()
	blob.getStreamErr = errors.New("object not found in blob store")
	p := New(meta, blob)

	_, err := p.GetObject(ctx, makeNR("b", "k"))
	if err == nil {
		t.Fatal("expected error for concurrent delete")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 404 {
		t.Fatalf("status=%d want 404 (concurrent delete)", pe.HTTPStatus)
	}
}

func TestGetObject_BlobMissingMetadataPresent_Returns500(t *testing.T) {
	// Simulates: GetObjectMeta succeeds both times but blob is absent → storage
	// corruption → 500.
	ctx := context.Background()
	meta := newStubMeta()
	// getMetaErrAfter = -1 means both calls succeed.
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k", Size: 4})

	blob := newStubBlob()
	blob.getStreamErr = errors.New("blob file missing")
	p := New(meta, blob)

	_, err := p.GetObject(ctx, makeNR("b", "k"))
	if err == nil {
		t.Fatal("expected error for blob corruption")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 500 {
		t.Fatalf("status=%d want 500 (storage corruption)", pe.HTTPStatus)
	}
}

func TestGetObject_MetadataMissReturns404(t *testing.T) {
	// Object does not exist at all → first GetObjectMeta fails → 404.
	ctx := context.Background()
	p := New(s3impl.NewMemoryS3ObjectMetaStore(), blobfs.NewMemoryBlobStore())

	_, err := p.GetObject(ctx, makeNR("b", "missing"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("want 404, got %v", err)
	}
}

// ─── DeleteObjects ────────────────────────────────────────────────────────────

func TestDeleteObjects_ReturnsAllDeletedKeys(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)

	_ = meta.CreateBucket(ctx, "b", nil)
	for _, k := range []string{"k1", "k2", "k3"} {
		_ = meta.PutObjectMeta(ctx, "b", k, objectstore.ObjectMeta{Key: k})
		_ = blob.Put(ctx, "b", k, []byte("data"))
	}

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b",
			"Delete": map[string]any{
				"Object": []any{
					map[string]any{"Key": "k1"},
					map[string]any{"Key": "k2"},
				},
			},
		},
	}
	resp, err := p.DeleteObjects(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	deleted, _ := resp.Data["Deleted"].([]map[string]any)
	if len(deleted) != 2 {
		t.Fatalf("deleted count=%d want 2", len(deleted))
	}
	// k3 untouched
	if _, err := meta.GetObjectMeta(ctx, "b", "k3"); err != nil {
		t.Fatal("k3 should still exist")
	}
}

func TestDeleteObjects_BlobFailDoesNotAbortMetadataDelete(t *testing.T) {
	// Blob delete failure must not prevent metadata from being deleted.
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := newStubBlob()
	blob.deleteErr = errors.New("blob store down")
	p := New(meta, blob)

	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k"})

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b",
			"Delete": map[string]any{
				"Object": []any{map[string]any{"Key": "k"}},
			},
		},
	}
	resp, err := p.DeleteObjects(ctx, nr)
	if err != nil {
		t.Fatalf("blob failure should not propagate: %v", err)
	}
	deleted, _ := resp.Data["Deleted"].([]map[string]any)
	if len(deleted) != 1 {
		t.Fatalf("deleted count=%d want 1", len(deleted))
	}
	// Metadata must be gone — it was deleted before the blob failure.
	if _, err := meta.GetObjectMeta(ctx, "b", "k"); err == nil {
		t.Fatal("metadata must be deleted before blob delete is attempted")
	}
}

func TestDeleteObjects_EmptyBodyReturnsEmptyDeleted(t *testing.T) {
	p := New(s3impl.NewMemoryS3ObjectMetaStore(), blobfs.NewMemoryBlobStore())
	nr := &model.NormalizedRequest{Params: map[string]any{"_bucket": "b"}}
	resp, err := p.DeleteObjects(context.Background(), nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	deleted, _ := resp.Data["Deleted"].([]any)
	if len(deleted) != 0 {
		t.Fatalf("want empty Deleted, got %v", deleted)
	}
}

// ─── GetBucketLocation ────────────────────────────────────────────────────────

func TestGetBucketLocation_ReturnsRegionAsIs(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	// Provider returns the region unchanged; the S3 codec translates us-east-1 → ""
	// in s3BuildXML so the wire response is spec-compliant.
	nr := &model.NormalizedRequest{
		Region: "us-east-1",
		Params: map[string]any{"_bucket": "b"},
	}
	resp, err := p.GetBucketLocation(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lc, _ := resp.Data["LocationConstraint"].(string)
	if lc != "us-east-1" {
		t.Fatalf("provider must return region as-is, got %q", lc)
	}
}

func TestGetBucketLocation_OtherRegionReturnsRegion(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Region: "eu-west-1",
		Params: map[string]any{"_bucket": "b"},
	}
	resp, err := p.GetBucketLocation(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lc, _ := resp.Data["LocationConstraint"].(string)
	if lc != "eu-west-1" {
		t.Fatalf("non-us-east-1 must return region name, got %q", lc)
	}
}

// ─── P2-7: Tagging ────────────────────────────────────────────────────────────

func TestTagging_PutObjectWithTaggingHeader(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket":  "b",
			"_key":     "k",
			"_tagging": "env=prod&team=backend",
		},
	}
	_, err := p.PutObject(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, _ := meta.GetObjectMeta(ctx, "b", "k")
	if m.Tags["env"] != "prod" || m.Tags["team"] != "backend" {
		t.Fatalf("tags not stored: %v", m.Tags)
	}
}

func TestTagging_PutGetDeleteObjectTags(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k"})

	body := []byte(`<Tagging><TagSet><Tag><Key>owner</Key><Value>alice</Value></Tag></TagSet></Tagging>`)
	putNR := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "_body": body},
	}
	if _, err := p.PutObjectTagging(ctx, putNR); err != nil {
		t.Fatalf("put tags: %v", err)
	}
	resp, err := p.GetObjectTagging(ctx, makeNR("b", "k"))
	if err != nil {
		t.Fatalf("get tags: %v", err)
	}
	tags, _ := resp.Data["Tags"].(map[string]string)
	if tags["owner"] != "alice" {
		t.Fatalf("tag not found: %v", tags)
	}
	if _, err = p.DeleteObjectTagging(ctx, makeNR("b", "k")); err != nil {
		t.Fatalf("delete tags: %v", err)
	}
	resp, _ = p.GetObjectTagging(ctx, makeNR("b", "k"))
	tags, _ = resp.Data["Tags"].(map[string]string)
	if len(tags) != 0 {
		t.Fatalf("tags should be empty after delete, got %v", tags)
	}
}

func TestTagging_BucketTags_MaxValidation(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k"})

	var sb strings.Builder
	sb.WriteString(`<Tagging><TagSet>`)
	for i := 0; i < 11; i++ {
		sb.WriteString(`<Tag><Key>k`)
		sb.WriteByte(byte('0' + i))
		sb.WriteString(`</Key><Value>v</Value></Tag>`)
	}
	sb.WriteString(`</TagSet></Tagging>`)

	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "_body": []byte(sb.String())},
	}
	_, err := p.PutObjectTagging(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 {
		t.Fatalf("want 400 InvalidTag for >10 tags, got %v", err)
	}
}

func TestTagging_CopyObject_CopyDirective(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "src", nil)
	_ = meta.CreateBucket(ctx, "dst", nil)
	_ = meta.PutObjectMeta(ctx, "src", "k", objectstore.ObjectMeta{Key: "k", Tags: map[string]string{"x": "1"}})
	_ = blob.Put(ctx, "src", "k", []byte("data"))

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket":            "dst",
			"_key":               "k2",
			"_copy_source":       "src/k",
			"_tagging_directive": "COPY",
		},
	}
	if _, err := p.CopyObject(ctx, nr); err != nil {
		t.Fatalf("copy: %v", err)
	}
	m, _ := meta.GetObjectMeta(ctx, "dst", "k2")
	if m.Tags["x"] != "1" {
		t.Fatalf("COPY directive should preserve tags: %v", m.Tags)
	}
}

func TestTagging_CopyObject_ReplaceDirective(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "src", nil)
	_ = meta.CreateBucket(ctx, "dst", nil)
	_ = meta.PutObjectMeta(ctx, "src", "k", objectstore.ObjectMeta{Key: "k", Tags: map[string]string{"x": "1"}})
	_ = blob.Put(ctx, "src", "k", []byte("data"))

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket":            "dst",
			"_key":               "k2",
			"_copy_source":       "src/k",
			"_tagging_directive": "REPLACE",
			"_tagging":           "y=2",
		},
	}
	if _, err := p.CopyObject(ctx, nr); err != nil {
		t.Fatalf("copy: %v", err)
	}
	m, _ := meta.GetObjectMeta(ctx, "dst", "k2")
	if _, hasX := m.Tags["x"]; hasX {
		t.Fatalf("REPLACE directive should drop source tags: %v", m.Tags)
	}
	if m.Tags["y"] != "2" {
		t.Fatalf("REPLACE directive should set new tags: %v", m.Tags)
	}
}

func TestTagging_Count_Header(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{
		Key: "k", Tags: map[string]string{"a": "1", "b": "2"},
	})
	_ = blob.Put(ctx, "b", "k", []byte("data"))

	resp, err := p.GetObject(ctx, makeNR("b", "k"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count, _ := resp.Data["_tagging_count"].(int)
	if count != 2 {
		t.Fatalf("_tagging_count want 2, got %d", count)
	}
}

// ─── P2-1: SSE ────────────────────────────────────────────────────────────────

func TestPutObject_SSE_S3_DefaultEncryption(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket":                 "b",
			"_key":                    "k",
			"_server_side_encryption": "AES256",
		},
	}
	resp, err := p.PutObject(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["_sse"] != "AES256" {
		t.Fatalf("want _sse=AES256, got %v", resp.Data["_sse"])
	}
}

func TestPutObject_SSE_KMS_KeyId(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket":                                "b",
			"_key":                                   "k",
			"_server_side_encryption":                "aws:kms",
			"_server_side_encryption_aws_kms_key_id": "arn:aws:kms:us-east-1:000:key/my-key",
		},
	}
	resp, err := p.PutObject(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Data["_sse"] != "aws:kms" {
		t.Fatalf("want _sse=aws:kms, got %v", resp.Data["_sse"])
	}
	if resp.Data["_sse_kms_key_id"] != "arn:aws:kms:us-east-1:000:key/my-key" {
		t.Fatalf("want kms key id in response, got %v", resp.Data["_sse_kms_key_id"])
	}
}

func TestPutObject_SSE_C_Validation(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b",
			"_key":    "k",
			"_server_side_encryption_customer_algorithm": "DES3",
		},
	}
	_, err := p.PutObject(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "InvalidEncryptionAlgorithmError" {
		t.Fatalf("want InvalidEncryptionAlgorithmError, got %v", err)
	}
}

func TestPutObject_SSE_C_KeyMD5Mismatch(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	validKeyB64 := base64.StdEncoding.EncodeToString(make([]byte, 32))
	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b",
			"_key":    "k",
			"_server_side_encryption_customer_algorithm": "AES256",
			"_server_side_encryption_customer_key":       validKeyB64,
			"_server_side_encryption_customer_key_md5":   "WRONG==",
		},
	}
	_, err := p.PutObject(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 {
		t.Fatalf("want 400 for MD5 mismatch, got %v", err)
	}
}

func TestGetBucketEncryption_Default(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	resp, err := p.GetBucketEncryption(ctx, makeNR("b", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rule, _ := resp.Data["EncryptionRule"].(map[string]any)
	if rule["Algorithm"] != "AES256" {
		t.Fatalf("want default AES256, got %v", rule)
	}
}

func TestPutBucketEncryption_KMS(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	body := []byte(`<ServerSideEncryptionConfiguration><Rule>` +
		`<ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>aws:kms</SSEAlgorithm>` +
		`<KMSMasterKeyID>my-key</KMSMasterKeyID>` +
		`</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_body": body},
	}
	if _, err := p.PutBucketEncryption(ctx, nr); err != nil {
		t.Fatalf("put encryption: %v", err)
	}
	resp, err := p.GetBucketEncryption(ctx, makeNR("b", ""))
	if err != nil {
		t.Fatalf("get encryption: %v", err)
	}
	rule, _ := resp.Data["EncryptionRule"].(map[string]any)
	if rule["Algorithm"] != "aws:kms" {
		t.Fatalf("want aws:kms, got %v", rule["Algorithm"])
	}
	if rule["KMSKeyID"] != "my-key" {
		t.Fatalf("want KMSKeyID=my-key, got %v", rule["KMSKeyID"])
	}
}

// ─── P2-2: Versioning ─────────────────────────────────────────────────────────

func enableVersioning(t *testing.T, ctx context.Context, meta *s3impl.MemoryS3ObjectMetaStore, bucket string) {
	t.Helper()
	if err := meta.SetBucketVersioning(ctx, bucket, "Enabled"); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
}

func TestVersioning_Enable_PutCreatesVersion(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	enableVersioning(t, ctx, meta, "b")

	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "_body": []byte("v1")},
	}
	resp, err := p.PutObject(ctx, nr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	vID, _ := resp.Data["_version_id"].(string)
	if vID == "" {
		t.Fatal("want version_id in response when versioning enabled")
	}
	// Version must be retrievable.
	vm, err := meta.GetObjectVersion(ctx, "b", "k", vID)
	if err != nil {
		t.Fatalf("GetObjectVersion: %v", err)
	}
	if vm.IsLatest != true {
		t.Fatal("new version must be latest")
	}
}

func TestVersioning_DeleteInsertsMarker(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	enableVersioning(t, ctx, meta, "b")

	putNR := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "_body": []byte("data")},
	}
	if _, err := p.PutObject(ctx, putNR); err != nil {
		t.Fatalf("put: %v", err)
	}

	resp, err := p.DeleteObject(ctx, makeNR("b", "k"))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp.Data["_delete_marker"] != true {
		t.Fatal("want _delete_marker=true in DeleteObject response")
	}
	markerID, _ := resp.Data["_version_id"].(string)
	if markerID == "" {
		t.Fatal("want _version_id for delete marker")
	}
	// Marker version must be IsDeleteMarker.
	vm, err := meta.GetObjectVersion(ctx, "b", "k", markerID)
	if err != nil {
		t.Fatalf("GetObjectVersion marker: %v", err)
	}
	if !vm.IsDeleteMarker {
		t.Fatal("marker version must have IsDeleteMarker=true")
	}
}

func TestVersioning_GetLatestSkipsMarker(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	enableVersioning(t, ctx, meta, "b")

	putNR := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "_body": []byte("data")},
	}
	if _, err := p.PutObject(ctx, putNR); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := p.DeleteObject(ctx, makeNR("b", "k")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// GetObject without versionId — latest is a delete marker → 404.
	_, err := p.GetObject(ctx, makeNR("b", "k"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("want 404 when latest is delete marker, got %v", err)
	}
}

func TestVersioning_GetSpecificVersion(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	enableVersioning(t, ctx, meta, "b")

	put := func(body []byte) string {
		t.Helper()
		nr := &model.NormalizedRequest{
			Params: map[string]any{"_bucket": "b", "_key": "k", "_body": body},
		}
		resp, err := p.PutObject(ctx, nr)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		return resp.Data["_version_id"].(string)
	}

	v1 := put([]byte("version-one"))
	_ = put([]byte("version-two"))

	// GetObject with versionId=v1 must return version-one content.
	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "versionId": v1},
	}
	resp, err := p.GetObject(ctx, nr)
	if err != nil {
		t.Fatalf("get specific version: %v", err)
	}
	rc, _ := resp.Data["_stream"].(io.ReadCloser)
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "version-one" {
		t.Fatalf("want version-one, got %q", string(data))
	}
}

func TestVersioning_ListObjectVersions(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	enableVersioning(t, ctx, meta, "b")

	for i := 0; i < 2; i++ {
		nr := &model.NormalizedRequest{
			Params: map[string]any{"_bucket": "b", "_key": "k", "_body": []byte("v")},
		}
		if _, err := p.PutObject(ctx, nr); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// Add a delete marker.
	if _, err := p.DeleteObject(ctx, makeNR("b", "k")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b"},
	}
	resp, err := p.ListObjectVersions(ctx, nr)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	all, _ := resp.Data["Versions"].([]map[string]any)
	var versions, deleteMarkers []map[string]any
	for _, v := range all {
		if dm, _ := v["IsDeleteMarker"].(bool); dm {
			deleteMarkers = append(deleteMarkers, v)
		} else {
			versions = append(versions, v)
		}
	}
	if len(versions) != 2 {
		t.Fatalf("want 2 object versions, got %d", len(versions))
	}
	if len(deleteMarkers) != 1 {
		t.Fatalf("want 1 delete marker, got %d", len(deleteMarkers))
	}
}

func TestVersioning_SuspendedNullVersion(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.SetBucketVersioning(ctx, "b", "Enabled")
	_ = meta.SetBucketVersioning(ctx, "b", "Suspended")

	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "k", "_body": []byte("data")},
	}
	resp, err := p.PutObject(ctx, nr)
	if err != nil {
		t.Fatalf("put suspended: %v", err)
	}
	vID, _ := resp.Data["_version_id"].(string)
	if vID != "null" {
		t.Fatalf("want versionId=null when suspended, got %q", vID)
	}
}

// ─── P2-3: Object Lock ────────────────────────────────────────────────────────

func TestObjectLock_DeleteBlockedByCompliance(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)

	future := clock.RealNow().Add(24 * time.Hour)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{
		Key: "k", LockMode: "COMPLIANCE", LockRetainUntil: &future,
	})

	_, err := p.DeleteObject(ctx, makeNR("b", "k"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 403 {
		t.Fatalf("want 403 for COMPLIANCE lock, got %v", err)
	}
}

func TestObjectLock_GovernanceBypassAllowed(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)

	future := clock.RealNow().Add(24 * time.Hour)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{
		Key: "k", LockMode: "GOVERNANCE", LockRetainUntil: &future,
	})
	_ = blob.Put(ctx, "b", "k", []byte("data"))

	nr := &model.NormalizedRequest{
		Params: map[string]any{
			"_bucket": "b", "_key": "k",
			"_bypass_governance_retention": "true",
		},
	}
	resp, err := p.DeleteObject(ctx, nr)
	if err != nil || resp.HTTPStatus != 204 {
		t.Fatalf("want 204 with bypass, got status=%d err=%v", resp.HTTPStatus, err)
	}
}

func TestObjectLock_LegalHoldBlocksDelete(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k", LegalHoldStatus: "ON"})

	_, err := p.DeleteObject(ctx, makeNR("b", "k"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 403 {
		t.Fatalf("want 403 for legal hold, got %v", err)
	}
}

func TestObjectLock_RequiresVersioning(t *testing.T) {
	// PutObjectLockConfiguration must succeed; object lock config is stored in bucket meta.
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	body := []byte(`<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>` +
		`<Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule>` +
		`</ObjectLockConfiguration>`)
	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_body": body},
	}
	if _, err := p.PutObjectLockConfiguration(ctx, nr); err != nil {
		t.Fatalf("put lock config: %v", err)
	}
	resp, err := p.GetObjectLockConfiguration(ctx, makeNR("b", ""))
	if err != nil {
		t.Fatalf("get lock config: %v", err)
	}
	cfg, _ := resp.Data["ObjectLockConfig"].(map[string]any)
	if cfg["ObjectLockEnabled"] != "Enabled" {
		t.Fatalf("want ObjectLockEnabled=Enabled, got %v", cfg)
	}
}

// ─── P2-4: ACLs ──────────────────────────────────────────────────────────────

func TestACL_CannedPublicRead(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)

	nr := &model.NormalizedRequest{
		AccountID: "000000000000",
		Params: map[string]any{
			"_bucket": "b", "_key": "k",
			"_acl":  "public-read",
			"_body": []byte("data"),
		},
	}
	if _, err := p.PutObject(ctx, nr); err != nil {
		t.Fatalf("put: %v", err)
	}
	m, _ := meta.GetObjectMeta(ctx, "b", "k")
	if !strings.Contains(m.ACL, "AllUsers") {
		t.Fatalf("public-read ACL should include AllUsers group, got %q", m.ACL)
	}
}

func TestACL_GetBucketAcl_Default(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	resp, err := p.GetBucketAcl(ctx, &model.NormalizedRequest{
		AccountID: "owner-id",
		Params:    map[string]any{"_bucket": "b"},
	})
	if err != nil {
		t.Fatalf("get bucket acl: %v", err)
	}
	owner, _ := resp.Data["Owner"].(map[string]any)
	if owner["ID"] != "owner-id" {
		t.Fatalf("want Owner.ID=owner-id, got %v", owner)
	}
}

func TestACL_PutObjectAcl_XML(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)
	_ = meta.PutObjectMeta(ctx, "b", "k", objectstore.ObjectMeta{Key: "k"})

	nr := &model.NormalizedRequest{
		AccountID: "acct",
		Params: map[string]any{
			"_bucket": "b", "_key": "k",
			"_acl": "public-read",
		},
	}
	if _, err := p.PutObjectAcl(ctx, nr); err != nil {
		t.Fatalf("put object acl: %v", err)
	}
	resp, err := p.GetObjectAcl(ctx, &model.NormalizedRequest{
		AccountID: "acct",
		Params:    map[string]any{"_bucket": "b", "_key": "k"},
	})
	if err != nil {
		t.Fatalf("get object acl: %v", err)
	}
	grants, _ := resp.Data["Grants"].([]map[string]any)
	var hasPublicRead bool
	for _, g := range grants {
		if g["Permission"] == "READ" && strings.Contains(g["GranteeURI"].(string), "AllUsers") {
			hasPublicRead = true
		}
	}
	if !hasPublicRead {
		t.Fatalf("want public-read grant in ACL, got %v", grants)
	}
}

// ─── P2-5: Lifecycle ──────────────────────────────────────────────────────────

func TestLifecycle_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	body := []byte(`<LifecycleConfiguration>` +
		`<Rule><ID>rule1</ID><Status>Enabled</Status>` +
		`<Filter><Prefix>logs/</Prefix></Filter>` +
		`<Expiration><Days>30</Days></Expiration></Rule>` +
		`</LifecycleConfiguration>`)
	putNR := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_body": body},
	}
	if _, err := p.PutBucketLifecycleConfiguration(ctx, putNR); err != nil {
		t.Fatalf("put lifecycle: %v", err)
	}
	resp, err := p.GetBucketLifecycleConfiguration(ctx, makeNR("b", ""))
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	rules, _ := resp.Data["LifecycleRules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("want 1 rule, got %d", len(rules))
	}
	// Delete and verify gone.
	if _, err := p.DeleteBucketLifecycle(ctx, makeNR("b", "")); err != nil {
		t.Fatalf("delete lifecycle: %v", err)
	}
	_, err = p.GetBucketLifecycleConfiguration(ctx, makeNR("b", ""))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "NoSuchLifecycleConfiguration" {
		t.Fatalf("want NoSuchLifecycleConfiguration after delete, got %v", err)
	}
}

func TestLifecycle_ExpirationHeader(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	blob := blobfs.NewMemoryBlobStore()
	p := New(meta, blob)
	_ = meta.CreateBucket(ctx, "b", nil)

	// Put a lifecycle rule: objects under "logs/" expire in 1 day.
	body := []byte(`<LifecycleConfiguration>` +
		`<Rule><ID>expire-logs</ID><Status>Enabled</Status>` +
		`<Filter><Prefix>logs/</Prefix></Filter>` +
		`<Expiration><Days>1</Days></Expiration></Rule>` +
		`</LifecycleConfiguration>`)
	_, _ = p.PutBucketLifecycleConfiguration(ctx, &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_body": body},
	})

	_ = meta.PutObjectMeta(ctx, "b", "logs/access.log", objectstore.ObjectMeta{
		Key:          "logs/access.log",
		LastModified: clock.RealNow().Add(-25 * time.Hour), // 25 hours old
	})
	_ = blob.Put(ctx, "b", "logs/access.log", []byte("log data"))

	nr := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_key": "logs/access.log"},
	}
	resp, err := p.GetObject(ctx, nr)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	exp, _ := resp.Data["_expiration"].(string)
	if !strings.Contains(exp, "expire-logs") {
		t.Fatalf("want expiration header with rule-id, got %q", exp)
	}
}

// ─── P2-6: CORS ───────────────────────────────────────────────────────────────

func TestCORS_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	body := []byte(`<CORSConfiguration>` +
		`<CORSRule>` +
		`<AllowedOrigin>https://example.com</AllowedOrigin>` +
		`<AllowedMethod>GET</AllowedMethod>` +
		`<AllowedMethod>PUT</AllowedMethod>` +
		`<MaxAgeSeconds>3600</MaxAgeSeconds>` +
		`</CORSRule></CORSConfiguration>`)
	putNR := &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_body": body},
	}
	if _, err := p.PutBucketCors(ctx, putNR); err != nil {
		t.Fatalf("put cors: %v", err)
	}
	resp, err := p.GetBucketCors(ctx, makeNR("b", ""))
	if err != nil {
		t.Fatalf("get cors: %v", err)
	}
	rules, _ := resp.Data["CORSRules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("want 1 CORS rule, got %d", len(rules))
	}
	// Delete and verify gone.
	if _, err := p.DeleteBucketCors(ctx, makeNR("b", "")); err != nil {
		t.Fatalf("delete cors: %v", err)
	}
	_, err = p.GetBucketCors(ctx, makeNR("b", ""))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "NoSuchCORSConfiguration" {
		t.Fatalf("want NoSuchCORSConfiguration after delete, got %v", err)
	}
}

func TestCORS_GetBucketCORSRules(t *testing.T) {
	ctx := context.Background()
	meta := s3impl.NewMemoryS3ObjectMetaStore()
	p := New(meta, blobfs.NewMemoryBlobStore())
	_ = meta.CreateBucket(ctx, "b", nil)

	body := []byte(`<CORSConfiguration>` +
		`<CORSRule>` +
		`<AllowedOrigin>*</AllowedOrigin>` +
		`<AllowedMethod>GET</AllowedMethod>` +
		`</CORSRule></CORSConfiguration>`)
	_, _ = p.PutBucketCors(ctx, &model.NormalizedRequest{
		Params: map[string]any{"_bucket": "b", "_body": body},
	})

	rules := p.GetBucketCORSRules("b")
	if len(rules) != 1 {
		t.Fatalf("want 1 rule from GetBucketCORSRules, got %d", len(rules))
	}
	origins, _ := rules[0]["AllowedOrigins"].([]string)
	if len(origins) == 0 || origins[0] != "*" {
		t.Fatalf("want AllowedOrigins=[*], got %v", origins)
	}
}
