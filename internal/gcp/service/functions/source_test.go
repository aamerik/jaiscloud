package functions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"jaiscloud/internal/blobfs"
	lambdaexec "jaiscloud/internal/executor/lambda"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/store"
)

// fakeFetcher is a SourceFetcher over an in-memory {bucket/object: bytes} map.
type fakeFetcher struct {
	objects map[string][]byte
	err     error
}

func (f *fakeFetcher) FetchObjectBytes(_ context.Context, bucket, object string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	if b, ok := f.objects[bucket+"/"+object]; ok {
		return b, nil
	}
	return nil, gcs.ErrNoSuchObject
}

// recordingExecutor captures the last InvokeRequest and echoes the payload.
type recordingExecutor struct {
	req     lambdaexec.InvokeRequest
	invoked int
}

func (e *recordingExecutor) Invoke(_ context.Context, req lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	e.req = req
	e.invoked++
	return lambdaexec.InvokeResult{Payload: req.Payload}, nil
}
func (e *recordingExecutor) DeleteFunction(context.Context, string) {}
func (e *recordingExecutor) Reset(context.Context)                  {}
func (e *recordingExecutor) Close() error                           { return nil }

func newSourceTestService(t *testing.T, blobs blobfs.BlobStore, f SourceFetcher, ex lambdaexec.LambdaExecutor) *Service {
	t.Helper()
	opts := []Option{WithBlobs(blobs), WithSourceFetcher(f)}
	if ex != nil {
		opts = append(opts, WithExecutor(ex))
	}
	return NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(), opts...)
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestCreateFunctionPersistsSourceAndInvokesWithCode(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	zip := []byte("PK\x03\x04 fake function archive")
	fetcher := &fakeFetcher{objects: map[string][]byte{"src-bucket/code.zip": zip}}
	exec := &recordingExecutor{}
	s := newSourceTestService(t, blobs, fetcher, exec)

	f, _, err := s.CreateFunction(ctx, "proj", "us-central1", "hello",
		FunctionInput{Runtime: "python312", EntryPoint: "main.handler", SourceBucket: "src-bucket", SourceObject: "code.zip"}, V1)
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	wantSHA := sha256hex(zip)
	if f.SourceSHA256 != wantSHA || f.SourceSize != int64(len(zip)) || f.SourceBlobKey == "" {
		t.Fatalf("source metadata = sha=%q size=%d key=%q", f.SourceSHA256, f.SourceSize, f.SourceBlobKey)
	}
	if got, err := blobs.Get(ctx, functionsSourceBucket, f.SourceBlobKey); err != nil || string(got) != string(zip) {
		t.Fatalf("stored source = %q, err=%v", got, err)
	}

	// The persisted archive round-trips through the CodeLoader using the
	// executor's composite key.
	loaded, err := s.LoadCode(ctx, "proj", CodeKey("us-central1", "hello"), "$LATEST")
	if err != nil || string(loaded) != string(zip) {
		t.Fatalf("LoadCode = %q, err=%v", loaded, err)
	}

	// CallFunction hands the executor the composite CodeKey and the runtime
	// image mapped from the GCP runtime.
	if _, _, _, err := s.CallFunction(ctx, "proj", "us-central1", "hello", `{"x":1}`); err != nil {
		t.Fatalf("CallFunction: %v", err)
	}
	if exec.req.CodeKey != "us-central1.hello" {
		t.Fatalf("CodeKey = %q, want us-central1.hello", exec.req.CodeKey)
	}
	if exec.req.Image != "public.ecr.aws/lambda/python:3.12" {
		t.Fatalf("Image = %q", exec.req.Image)
	}
	if exec.req.FunctionName != "hello" {
		t.Fatalf("FunctionName = %q, want hello", exec.req.FunctionName)
	}
}

func TestCreateFunctionMissingSourceIsNotFound(t *testing.T) {
	ctx := context.Background()
	s := newSourceTestService(t, blobfs.NewMemoryBlobStore(), &fakeFetcher{objects: map[string][]byte{}}, nil)
	_, _, err := s.CreateFunction(ctx, "proj", "us-central1", "gone",
		FunctionInput{Runtime: "python312", SourceBucket: "b", SourceObject: "missing.zip"}, V1)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if _, gerr := s.GetFunction(ctx, "proj", "us-central1", "gone"); gerr == nil {
		t.Fatal("function should not exist after a failed source fetch")
	}
}

func TestCreateFunctionGCsSourceOnAlreadyExists(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	in := FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}
	if _, _, err := s.CreateFunction(ctx, "p", "us", "dup", in, V1); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, _, err := s.CreateFunction(ctx, "p", "us", "dup", in, V1); err == nil {
		t.Fatal("expected AlreadyExists")
	}
	// The second (failed) create must not leave a second blob behind.
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 1 {
		t.Fatalf("source blobs = %v, want exactly 1", keys)
	}
}

func TestUpdateFunctionReplacesSourceAndGCsPrior(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{
		"b/one.zip": []byte("zip-one"),
		"b/two.zip": []byte("zip-two"),
	}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "one.zip"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, _, err := s.UpdateFunction(ctx, "p", "us", "f",
		FunctionInput{SourceBucket: "b", SourceObject: "two.zip"}, []string{"build_config.source"}, V1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if f.SourceSHA256 != sha256hex([]byte("zip-two")) {
		t.Fatalf("revision not updated: %q", f.SourceSHA256)
	}
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 1 {
		t.Fatalf("source blobs = %v, want the prior revision GC'd", keys)
	}
}

func TestDeleteFunctionRemovesSource(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.DeleteFunction(ctx, "p", "us", "f"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 0 {
		t.Fatalf("source blobs after delete = %v, want none", keys)
	}
}

func TestResetClearsSources(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	s.Reset(ctx)
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 0 {
		t.Fatalf("source blobs after reset = %v, want none", keys)
	}
}

func TestSourceRefAndCodeKey(t *testing.T) {
	if b, o := sourceRef(FunctionInput{SourceArchiveURL: "gs://bkt/dir/o.zip"}); b != "bkt" || o != "dir/o.zip" {
		t.Fatalf("v1 sourceRef = %q/%q", b, o)
	}
	if b, o := sourceRef(FunctionInput{SourceBucket: "b2", SourceObject: "o2"}); b != "b2" || o != "o2" {
		t.Fatalf("v2 sourceRef = %q/%q", b, o)
	}
	if b, o := sourceRef(FunctionInput{SourceArchiveURL: "https://example/x.zip"}); b != "" || o != "" {
		t.Fatalf("non-gs sourceRef = %q/%q", b, o)
	}
	for _, loc := range []string{"us-central1", "europe-west1"} {
		key := CodeKey(loc, "fn-1")
		gotLoc, gotID, ok := splitCodeKey(key)
		if !ok || gotLoc != loc || gotID != "fn-1" {
			t.Fatalf("splitCodeKey(%q) = %q/%q/%v", key, gotLoc, gotID, ok)
		}
	}
}

func TestUpdateUnrelatedFieldKeepsSource(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	created, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}, V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	f, _, err := s.UpdateFunction(ctx, "p", "us", "f",
		FunctionInput{Description: "d"}, []string{"description"}, V1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if f.SourceBlobKey != created.SourceBlobKey {
		t.Fatalf("source blob key changed on an unrelated update: %q -> %q", created.SourceBlobKey, f.SourceBlobKey)
	}
	if keys, _ := blobs.List(ctx, functionsSourceBucket, ""); len(keys) != 1 {
		t.Fatalf("source blobs = %v, want the existing archive retained", keys)
	}
}

func TestLoadCodeWithoutSourceIsNil(t *testing.T) {
	ctx := context.Background()
	s := newSourceTestService(t, blobfs.NewMemoryBlobStore(), &fakeFetcher{objects: map[string][]byte{}}, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f", FunctionInput{Runtime: "nodejs20"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	code, err := s.LoadCode(ctx, "p", "us.f", "$LATEST")
	if err != nil || code != nil {
		t.Fatalf("LoadCode = %q, %v; want nil,nil", code, err)
	}
}
