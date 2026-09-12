package storage

import (
	"context"
	"io"
	"net"
	"testing"

	"jaiscloud/internal/blobfs"
	gcpcrypto "jaiscloud/internal/gcp/crypto"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/store/gcs"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func storageTestService(t *testing.T) (storagepb.StorageClient, func()) {
	t.Helper()
	objects := gcs.NewMemoryObjectStore()
	blobs := blobfs.NewMemoryBlobStore()
	keys := kmsstore.NewMemoryStore()
	resources := store.NewMemoryResourceStore()
	provider := storageprovider.New(objects, resources, blobs, gcpcrypto.NewEnvelopeEncryptor(keys))
	svc := NewService(objects, provider, "test-project")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	storagepb.RegisterStorageServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return storagepb.NewStorageClient(conn), cleanup
}

const testBucket = "projects/_/buckets/bucket-a"

func createBucket(t *testing.T, client storagepb.StorageClient, name, location string) {
	t.Helper()
	if _, err := client.CreateBucket(context.Background(), &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: name,
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: location},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func TestBucketCRUD(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			Labels:   map[string]string{"transport": "grpc"},
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if created.GetName() != testBucket {
		t.Fatalf("CreateBucket name = %q, want %q", created.GetName(), testBucket)
	}
	if created.GetLocation() != "US" {
		t.Fatalf("CreateBucket location = %q, want US", created.GetLocation())
	}

	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetLabels()["transport"] != "grpc" {
		t.Fatalf("GetBucket labels = %v, want transport=grpc", got.GetLabels())
	}

	list, err := client.ListBuckets(ctx, &storagepb.ListBucketsRequest{Parent: "projects/test-project"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(list.GetBuckets()) != 1 || list.GetBuckets()[0].GetName() != testBucket {
		t.Fatalf("ListBuckets = %v, want exactly [%s]", list.GetBuckets(), testBucket)
	}

	if _, err := client.DeleteBucket(ctx, &storagepb.DeleteBucketRequest{Name: testBucket}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetBucket after delete err = %v, want NotFound", err)
	}
}

// writeSingleShot writes an object via the client-streaming WriteObject RPC.
func writeSingleShot(t *testing.T, client storagepb.StorageClient, bucket, object, contentType string, data []byte) *storagepb.Object {
	t.Helper()
	stream, err := client.WriteObject(context.Background())
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource: &storagepb.Object{Name: object, Bucket: bucket, ContentType: contentType},
			},
		},
		WriteOffset: 0,
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: data}},
		FinishWrite: true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("WriteObject CloseAndRecv: %v", err)
	}
	return resp.GetResource()
}

// readObject reads an object's full content via the server-streaming ReadObject.
func readObject(t *testing.T, client storagepb.StorageClient, bucket, object string) []byte {
	t.Helper()
	stream, err := client.ReadObject(context.Background(), &storagepb.ReadObjectRequest{Bucket: bucket, Object: object})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	var out []byte
	first := true
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadObject Recv: %v", err)
		}
		if first {
			if msg.GetMetadata() == nil {
				t.Fatal("ReadObject first message missing metadata")
			}
			first = false
		}
		if cd := msg.GetChecksummedData(); cd != nil {
			out = append(out, cd.GetContent()...)
		}
	}
	return out
}

func TestWriteReadRoundTrip(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("hello grpc storage")
	obj := writeSingleShot(t, client, testBucket, "obj-a", "text/plain", payload)
	if obj.GetGeneration() <= 0 {
		t.Fatalf("WriteObject generation = %d, want > 0", obj.GetGeneration())
	}
	if obj.GetSize() != int64(len(payload)) {
		t.Fatalf("WriteObject size = %d, want %d", obj.GetSize(), len(payload))
	}

	got := readObject(t, client, testBucket, "obj-a")
	if string(got) != string(payload) {
		t.Fatalf("ReadObject content = %q, want %q", got, payload)
	}

	// GetObject reflects the stored metadata.
	meta, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "obj-a"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if meta.GetContentType() != "text/plain" {
		t.Fatalf("GetObject contentType = %q, want text/plain", meta.GetContentType())
	}
}

func TestResumableWrite(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "resumable-a", Bucket: testBucket, ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()
	if uploadID == "" {
		t.Fatal("StartResumableWrite upload_id is empty")
	}

	payload := []byte("resumable payload bytes")

	stream, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_UploadId{UploadId: uploadID},
		WriteOffset:  0,
		Data:         &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payload}},
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("BidiWriteObject Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("BidiWriteObject CloseSend: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("BidiWriteObject Recv: %v", err)
	}
	if resp.GetResource() == nil {
		t.Fatalf("BidiWriteObject response = %v, want Resource", resp.GetWriteStatus())
	}

	got := readObject(t, client, testBucket, "resumable-a")
	if string(got) != string(payload) {
		t.Fatalf("ReadObject resumable content = %q, want %q", got, payload)
	}
}

func TestComposeAndUpdateAndList(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	a := writeSingleShot(t, client, testBucket, "comp-a", "text/plain", []byte("part-a-"))
	b := writeSingleShot(t, client, testBucket, "comp-b", "text/plain", []byte("part-b"))

	composed, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination: &storagepb.Object{Name: "composed", Bucket: testBucket},
		SourceObjects: []*storagepb.ComposeObjectRequest_SourceObject{
			{Name: "comp-a", Generation: a.GetGeneration()},
			{Name: "comp-b", Generation: b.GetGeneration()},
		},
	})
	if err != nil {
		t.Fatalf("ComposeObject: %v", err)
	}
	if composed.GetComponentCount() != 2 {
		t.Fatalf("ComposeObject componentCount = %d, want 2", composed.GetComponentCount())
	}
	if got := readObject(t, client, testBucket, "composed"); string(got) != "part-a-part-b" {
		t.Fatalf("composed content = %q, want %q", got, "part-a-part-b")
	}

	// Update metadata.
	updated, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object:     &storagepb.Object{Name: "comp-a", Bucket: testBucket, Metadata: map[string]string{"updated": "true"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.updated"}},
	})
	if err != nil {
		t.Fatalf("UpdateObject: %v", err)
	}
	if updated.GetMetadata()["updated"] != "true" {
		t.Fatalf("UpdateObject metadata = %v, want updated=true", updated.GetMetadata())
	}

	// ListObjects sees all three.
	list, err := client.ListObjects(ctx, &storagepb.ListObjectsRequest{Parent: testBucket})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	names := map[string]bool{}
	for _, o := range list.GetObjects() {
		names[o.GetName()] = true
	}
	for _, want := range []string{"comp-a", "comp-b", "composed"} {
		if !names[want] {
			t.Fatalf("ListObjects missing %q (got %v)", want, names)
		}
	}
}

func TestWriteObjectResumableRemovesSession(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "resumable-wo", Bucket: testBucket, ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()

	payload := []byte("client-streaming resumable bytes")

	stream, err := client.WriteObject(ctx)
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_UploadId{UploadId: uploadID},
		WriteOffset:  0,
		Data:         &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payload}},
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("WriteObject CloseAndRecv: %v", err)
	}
	if resp.GetResource() == nil {
		t.Fatalf("WriteObject response = %v, want Resource", resp.GetWriteStatus())
	}

	// The finished session is removed from the in-memory upload map.
	if _, err := client.QueryWriteStatus(ctx, &storagepb.QueryWriteStatusRequest{UploadId: uploadID}); status.Code(err) != codes.NotFound {
		t.Fatalf("QueryWriteStatus after finish err = %v, want NotFound", err)
	}

	got := readObject(t, client, testBucket, "resumable-wo")
	if string(got) != string(payload) {
		t.Fatalf("ReadObject content = %q, want %q", got, payload)
	}
}

func TestWriteObjectOffsetGapOutOfRange(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	stream, err := client.WriteObject(ctx)
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource: &storagepb.Object{Name: "gap-obj", Bucket: testBucket, ContentType: "text/plain"},
			},
		},
		WriteOffset: 5, // gap: the first write must begin at offset 0
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: []byte("hello")}},
		FinishWrite: true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.OutOfRange {
		t.Fatalf("WriteObject offset gap err = %v, want OutOfRange", err)
	}
}

func TestGetObjectHonorsGeneration(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	// Versioned bucket so overwrites retain prior generations.
	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket: &storagepb.Bucket{
			Project:    "projects/test-project",
			Location:   "US",
			Versioning: &storagepb.Bucket_Versioning{Enabled: true},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	v1 := writeSingleShot(t, client, testBucket, "gen-obj", "text/plain", []byte("version-one"))
	v2 := writeSingleShot(t, client, testBucket, "gen-obj", "application/json", []byte("version-two"))
	if v1.GetGeneration() == v2.GetGeneration() {
		t.Fatal("expected distinct generations")
	}

	got, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "gen-obj", Generation: v1.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("GetObject(stale generation): %v", err)
	}
	if got.GetGeneration() != v1.GetGeneration() {
		t.Fatalf("GetObject generation = %d, want %d", got.GetGeneration(), v1.GetGeneration())
	}
	if got.GetContentType() != "text/plain" {
		t.Fatalf("GetObject contentType = %q, want text/plain", got.GetContentType())
	}
}

func TestDeleteAndUpdatePreconditions(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	writeSingleShot(t, client, testBucket, "pre-obj", "text/plain", []byte("data"))

	stale := int64(999999)

	// DeleteObject if_generation_match mismatch.
	if _, err := client.DeleteObject(ctx, &storagepb.DeleteObjectRequest{
		Bucket: testBucket, Object: "pre-obj", IfGenerationMatch: &stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteObject precondition err = %v, want FailedPrecondition", err)
	}

	// DeleteObject targeting a non-live generation.
	if _, err := client.DeleteObject(ctx, &storagepb.DeleteObjectRequest{
		Bucket: testBucket, Object: "pre-obj", Generation: stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteObject generation err = %v, want FailedPrecondition", err)
	}

	// UpdateObject if_metageneration_match mismatch.
	if _, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object:                &storagepb.Object{Name: "pre-obj", Bucket: testBucket, Metadata: map[string]string{"x": "y"}},
		UpdateMask:            &fieldmaskpb.FieldMask{Paths: []string{"metadata.x"}},
		IfMetagenerationMatch: &stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateObject precondition err = %v, want FailedPrecondition", err)
	}

	// The object is still present (preconditions failed without mutating).
	if got := readObject(t, client, testBucket, "pre-obj"); string(got) != "data" {
		t.Fatalf("ReadObject content = %q, want %q", got, "data")
	}
}

// The compose/write paths below thread their request preconditions into the
// store's atomic *Checked methods — the follow-up #48 deliberately deferred
// (its DeleteObject/ComposeObject/finalize calls passed precondition=nil). A
// stale precondition must be rejected atomically, without mutating the object.

func TestComposeObjectDestinationPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	a := writeSingleShot(t, client, testBucket, "cs-a", "text/plain", []byte("A"))
	b := writeSingleShot(t, client, testBucket, "cs-b", "text/plain", []byte("B"))
	writeSingleShot(t, client, testBucket, "cs-dest", "text/plain", []byte("OLD"))

	sources := []*storagepb.ComposeObjectRequest_SourceObject{
		{Name: "cs-a", Generation: a.GetGeneration()},
		{Name: "cs-b", Generation: b.GetGeneration()},
	}

	// if_generation_match=0 ("create only if absent") against an existing
	// destination is rejected atomically; the existing object is untouched.
	zero := int64(0)
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:       &storagepb.Object{Name: "cs-dest", Bucket: testBucket},
		SourceObjects:     sources,
		IfGenerationMatch: &zero,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("compose if_generation_match=0 on existing dest: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := readObject(t, client, testBucket, "cs-dest"); string(got) != "OLD" {
		t.Fatalf("destination unchanged after rejected compose, got %q, want OLD", got)
	}

	// The same create-only precondition succeeds against a new destination.
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:       &storagepb.Object{Name: "cs-new", Bucket: testBucket},
		SourceObjects:     sources,
		IfGenerationMatch: &zero,
	}); err != nil {
		t.Fatalf("compose if_generation_match=0 on new dest: %v", err)
	}
	if got := readObject(t, client, testBucket, "cs-new"); string(got) != "AB" {
		t.Fatalf("composed content = %q, want AB", got)
	}
}

// writeSingleShotPre writes via the client-streaming WriteObject RPC with an
// optional if_generation_match precondition, returning the RPC error.
func writeSingleShotPre(t *testing.T, client storagepb.StorageClient, bucket, object string, data []byte, ifGenMatch *int64) error {
	t.Helper()
	stream, err := client.WriteObject(context.Background())
	if err != nil {
		return err
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource:          &storagepb.Object{Name: object, Bucket: bucket, ContentType: "text/plain"},
				IfGenerationMatch: ifGenMatch,
			},
		},
		WriteOffset: 0,
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: data}},
		FinishWrite: true,
	}); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return err
}

func TestWriteObjectPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	obj := writeSingleShot(t, client, testBucket, "wp", "text/plain", []byte("first"))
	gen := obj.GetGeneration()
	zero := int64(0)

	// Create-only (if_generation_match=0) against an existing object is
	// rejected; the stored bytes are untouched.
	if err := writeSingleShotPre(t, client, testBucket, "wp", []byte("second"), &zero); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("write if_generation_match=0 on existing: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := readObject(t, client, testBucket, "wp"); string(got) != "first" {
		t.Fatalf("object unchanged after rejected write, got %q, want first", got)
	}

	// A matching generation overwrites.
	if err := writeSingleShotPre(t, client, testBucket, "wp", []byte("second"), &gen); err != nil {
		t.Fatalf("write with matching if_generation_match: %v", err)
	}
	if got := readObject(t, client, testBucket, "wp"); string(got) != "second" {
		t.Fatalf("object content = %q, want second", got)
	}

	// Create-only against a new object succeeds.
	if err := writeSingleShotPre(t, client, testBucket, "wp-new", []byte("fresh"), &zero); err != nil {
		t.Fatalf("write if_generation_match=0 on new object: %v", err)
	}

	// A resumable write carries the write spec's precondition through to
	// finalize (StartResumableWrite captures it on the session).
	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource:          &storagepb.Object{Name: "wp", Bucket: testBucket, ContentType: "text/plain"},
			IfGenerationMatch: &zero,
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	stream, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_UploadId{UploadId: srw.GetUploadId()},
		Data:         &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: []byte("resumed")}},
	}); err != nil {
		t.Fatalf("BidiWriteObject send data: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{FinishWrite: true}); err != nil {
		t.Fatalf("BidiWriteObject send finish: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resumable finalize with stale precondition: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}
