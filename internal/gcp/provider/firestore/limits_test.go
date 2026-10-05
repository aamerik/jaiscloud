package firestore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// paddedID returns s padded with 'a' to exactly n bytes (a longer s is returned
// unchanged).
func paddedID(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat("a", n-len(s))
}

// deepRelPath builds a relative document path with n collection levels, each
// collection/document id padded to idLen bytes.
func deepRelPath(n, idLen int) string {
	parts := make([]string, 0, 2*n)
	for i := 0; i < n; i++ {
		parts = append(parts, paddedID(fmt.Sprintf("c%d", i), idLen))
		parts = append(parts, paddedID(fmt.Sprintf("d%d", i), idLen))
	}
	return strings.Join(parts, "/")
}

// deepFullName builds the full resource name for deepRelPath.
func deepFullName(project, database string, n, idLen int) string {
	return "projects/" + project + "/databases/" + database + "/documents/" + deepRelPath(n, idLen)
}

// TestValidateDocumentName covers the shared limit checker directly: malformed
// names, the 100-collection depth cap, and the 6 KiB document-name cap.
func TestValidateDocumentName(t *testing.T) {
	if err := validateDocumentName("projects/proj/databases/(default)/documents/cities/SF"); err != nil {
		t.Fatalf("valid name rejected: %v", err)
	}

	// Malformed / non-document names are rejected.
	for _, bad := range []string{
		"",
		"projects/proj/databases/(default)/documents",
		"projects/proj/databases/(default)/documents/cities",
		"not-a-document-name",
		// Empty path elements (trailing / doubled slash) are not documents.
		"projects/proj/databases/(default)/documents/cities/",
		"projects/proj/databases/(default)/documents//cities",
	} {
		if err := validateDocumentName(bad); err == nil {
			t.Errorf("validateDocumentName(%q) = nil, want error", bad)
		} else {
			assertInvalidArgumentErr(t, err)
		}
	}

	// Depth boundary: 100 collection levels is accepted, 101 is rejected. This
	// matches the official Firestore emulator, which fails 101 levels with
	// "Key path is too long. Cannot exceed 100 elements." (INVALID_ARGUMENT).
	if err := validateDocumentName(deepFullName("proj", "(default)", maxSubcollectionDepth, 3)); err != nil {
		t.Errorf("depth %d rejected: %v", maxSubcollectionDepth, err)
	}
	if err := validateDocumentName(deepFullName("proj", "(default)", maxSubcollectionDepth+1, 3)); err == nil {
		t.Error("depth 101 accepted, want error")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// Name-size boundary (each id stays well under the 1500-byte id limit):
	// 8 levels with 380-byte ids is under 6 KiB, 381-byte ids is over.
	under := deepFullName("proj", "(default)", 8, 380)
	if len(under) > maxDocumentNameBytes {
		t.Fatalf("test path length %d unexpectedly over the cap", len(under))
	}
	if err := validateDocumentName(under); err != nil {
		t.Errorf("under-cap name rejected: %v", err)
	}
	over := deepFullName("proj", "(default)", 8, 381)
	if len(over) <= maxDocumentNameBytes {
		t.Fatalf("test path length %d unexpectedly under the cap", len(over))
	}
	if err := validateDocumentName(over); err == nil {
		t.Error("over-long document name accepted, want error")
	} else {
		assertInvalidArgumentErr(t, err)
	}
}

// TestWriteLimitsRejected checks every enforcement point: the unary
// create/patch/delete service methods and the Commit/BatchWrite write builder
// all reject a violating target with 400 INVALID_ARGUMENT.
func TestWriteLimitsRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	deep := deepRelPath(maxSubcollectionDepth+1, 3)
	deepName := deepFullName("proj", "(default)", maxSubcollectionDepth+1, 3)
	overLong := deepFullName("proj", "(default)", 8, 381)

	// Unary patch/delete at too-deep a path.
	if _, err := p.Service.PatchDocument(ctx, "proj", "(default)", deep, nil, nil, nil); err == nil {
		t.Error("PatchDocument accepted an over-deep document")
	} else {
		assertInvalidArgumentErr(t, err)
	}
	if err := p.Service.DeleteDocument(ctx, "proj", "(default)", deep, nil); err == nil {
		t.Error("DeleteDocument accepted an over-deep document")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// Unary create: the parent path plus the doc id forms a 101-level document.
	parent := deepRelPath(maxSubcollectionDepth, 3) + "/cKeep"
	if _, err := p.Service.CreateDocument(ctx, "proj", "(default)", parent, "doc", nil); err == nil {
		t.Error("CreateDocument accepted an over-deep document")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// REST plumbing: the same rejection through the documents.patch handler.
	nr := patchNR()
	nr.Params["name"] = "databases/(default)/documents/" + deep
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Error("DocumentsPatch accepted an over-deep document")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// Commit: update, delete and transform writes are all validated.
	up := &writeWire{Update: &documentWire{Name: deepName, Fields: map[string]*firestorestore.Value{"a": intField(1)}}}
	if _, _, err := p.Service.Commit(ctx, nil, []*writeWire{up}); err == nil {
		t.Error("Commit update accepted an over-deep document")
	} else {
		assertInvalidArgumentErr(t, err)
	}
	if _, _, err := p.Service.Commit(ctx, nil, []*writeWire{{Delete: deepName}}); err == nil {
		t.Error("Commit delete accepted an over-deep document")
	} else {
		assertInvalidArgumentErr(t, err)
	}
	tx := &writeWire{Transform: &documentTransformWire{
		Document:        overLong,
		FieldTransforms: []fieldTransformWire{{FieldPath: "n", Increment: firestorestore.IntVal(1)}},
	}}
	if _, _, err := p.Service.Commit(ctx, nil, []*writeWire{tx}); err == nil {
		t.Error("Commit transform accepted an over-long document name")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// BatchWrite is non-atomic: the violation becomes a per-write
	// INVALID_ARGUMENT (google.rpc code 3) status, not a top-level error.
	statuses, _, err := p.Service.BatchWrite(ctx, []*writeWire{up})
	if err != nil {
		t.Fatalf("BatchWrite returned a top-level error: %v", err)
	}
	st, _ := statuses[0].(map[string]any)
	if code, _ := st["code"].(int64); code != 3 {
		t.Fatalf("expected INVALID_ARGUMENT (3) per-write status, got %+v", st)
	}
}

// TestWriteLimitsAtBoundaryAccepted confirms a document exactly at the depth
// cap is still writable (the check is >, not >=).
func TestWriteLimitsAtBoundaryAccepted(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	path := deepRelPath(maxSubcollectionDepth, 3)
	if _, err := p.Service.PatchDocument(ctx, "proj", "(default)", path, nil, nil, nil); err != nil {
		t.Fatalf("PatchDocument at depth %d: %v", maxSubcollectionDepth, err)
	}
	if _, err := p.Service.GetDocument(ctx, deepFullName("proj", "(default)", maxSubcollectionDepth, 3), nil, nil); err != nil {
		t.Fatalf("GetDocument at depth %d: %v", maxSubcollectionDepth, err)
	}
}
