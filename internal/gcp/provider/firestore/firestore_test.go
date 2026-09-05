package firestore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
)

func newTestProvider() *Provider {
	return New(firestorestore.NewMemoryStore(), nil)
}

func testNR() *model.NormalizedRequest {
	return &model.NormalizedRequest{
		AccountID: "proj",
		Params:    map[string]any{},
		ResourceID: func(rt, n string) string {
			return "projects/proj/" + n
		},
	}
}

func patchNR() *model.NormalizedRequest {
	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{
			"name": map[string]any{"stringValue": "SF"},
		},
	}
	return nr
}

func assertPreconditionErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected FAILED_PRECONDITION error, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 400 || pe.Status != "FAILED_PRECONDITION" {
		t.Fatalf("expected FAILED_PRECONDITION@400, got HTTP=%d status=%q", pe.HTTPStatus, pe.Status)
	}
}

func assertInvalidArgumentErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected INVALID_ARGUMENT error, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument@400, got HTTP=%d code=%q", pe.HTTPStatus, pe.Code)
	}
}

func TestDocumentsPatchPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	// Seed a document.
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name, Fields: map[string]*firestorestore.Value{"a": intField(1)}, CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// exists=false on a present doc → FAILED_PRECONDITION.
	nr := patchNR()
	nr.Params["currentDocument.exists"] = "false"
	_, err := p.DocumentsPatch(ctx, nr)
	assertPreconditionErr(t, err)

	// stale updateTime → FAILED_PRECONDITION.
	nr = patchNR()
	nr.Params["currentDocument.updateTime"] = now.Add(-time.Hour).Format(time.RFC3339Nano)
	_, err = p.DocumentsPatch(ctx, nr)
	assertPreconditionErr(t, err)

	// exists=true on a missing doc → FAILED_PRECONDITION.
	nr = patchNR()
	nr.Params["name"] = "databases/(default)/documents/cities/MISSING"
	nr.Params["currentDocument.exists"] = "true"
	_, err = p.DocumentsPatch(ctx, nr)
	assertPreconditionErr(t, err)

	// matching updateTime → succeeds.
	nr = patchNR()
	nr.Params["currentDocument.updateTime"] = now.Format(time.RFC3339Nano)
	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch with matching updateTime: %v", err)
	}
	if resp == nil {
		t.Fatal("expected a response")
	}
}

func TestDocumentsPatchMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name,
		Fields: map[string]*firestorestore.Value{
			"a": intField(1),
			"b": intField(2),
		},
		CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{
			"a": map[string]any{"integerValue": "99"},
		},
	}
	nr.Params["mask.fieldPaths"] = "a"

	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch with mask: %v", err)
	}
	m, _ := resp.Data["fields"].(map[string]any)
	if _, ok := m["a"]; !ok {
		t.Errorf("expected field a to be present, got %+v", m)
	}
	if _, ok := m["b"]; !ok {
		t.Errorf("field b should be untouched by mask, got %+v", m)
	}
}

func TestDocumentsDeletePrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name, Fields: map[string]*firestorestore.Value{"a": intField(1)}, CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// exists=false on a present doc → FAILED_PRECONDITION.
	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["currentDocument.exists"] = "false"
	_, err := p.DocumentsDelete(ctx, nr)
	assertPreconditionErr(t, err)

	// stale updateTime → FAILED_PRECONDITION.
	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["currentDocument.updateTime"] = now.Add(-time.Hour).Format(time.RFC3339Nano)
	_, err = p.DocumentsDelete(ctx, nr)
	assertPreconditionErr(t, err)

	// exists=true on a missing doc → FAILED_PRECONDITION.
	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/MISSING"
	nr.Params["currentDocument.exists"] = "true"
	_, err = p.DocumentsDelete(ctx, nr)
	assertPreconditionErr(t, err)

	// matching updateTime → succeeds.
	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["currentDocument.updateTime"] = now.Format(time.RFC3339Nano)
	if _, err := p.DocumentsDelete(ctx, nr); err != nil {
		t.Fatalf("delete with matching updateTime: %v", err)
	}
}

func TestDocumentsGetMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name,
		Fields: map[string]*firestorestore.Value{
			"a": intField(1),
			"b": intField(2),
		},
		CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["mask.fieldPaths"] = "a"

	resp, err := p.DocumentsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get with mask: %v", err)
	}
	m, _ := resp.Data["fields"].(map[string]any)
	if _, ok := m["a"]; !ok {
		t.Errorf("expected field a to be present, got %+v", m)
	}
	if _, ok := m["b"]; ok {
		t.Errorf("field b should be masked out, got %+v", m)
	}
}

func TestStringValueSizeLimit(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Over the per-field string limit → 400 INVALID_ARGUMENT.
	tooBig := strings.Repeat("x", maxStringBytes+1)
	nr := patchNR()
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{"s": map[string]any{"stringValue": tooBig}},
	}
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for oversized stringValue")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// Exactly at the limit → accepted by the field-limit check.
	atLimit := strings.Repeat("x", maxStringBytes)
	if err := checkFieldLimits(map[string]*firestorestore.Value{"s": firestorestore.StringVal(atLimit)}); err != nil {
		t.Fatalf("expected %d-byte stringValue to be accepted, got %v", maxStringBytes, err)
	}
}

func TestFieldNameLimits(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Empty field name → 400 INVALID_ARGUMENT.
	nr := patchNR()
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{"": map[string]any{"integerValue": "1"}},
	}
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for empty field name")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// 1,501-byte field name → 400 INVALID_ARGUMENT.
	longName := strings.Repeat("f", maxFieldNameBytes+1)
	nr = patchNR()
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{longName: map[string]any{"integerValue": "1"}},
	}
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for oversized field name")
	} else {
		assertInvalidArgumentErr(t, err)
	}
}
