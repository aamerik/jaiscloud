package firestoreui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// mockProvider records the last NormalizedRequest and returns a canned response.
type mockProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockProvider) reply(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.resp != nil {
		return m.resp, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockProvider) ListCollectionIds(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListDocuments(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DocumentsGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CreateDocument(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DocumentsPatch(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DocumentsDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      8080,
		UIPort:    4567,
		Region:    "global",
		AccountID: "test-project",
		ProjectID: "test-project",
		Clock:     clock.RealClock{},
	}
}

func do(t *testing.T, mock *mockProvider, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestListCollections_MapsIDs(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"collectionIds": []any{"users", "orders"},
			"nextPageToken": "abc",
		},
	}}

	w := do(t, mock, http.MethodGet, "/collections?pageSize=5", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListCollectionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Collections) != 2 {
		t.Fatalf("got %+v, want 2 collections", resp)
	}
	if resp.Collections[0].ID != "users" || resp.Collections[1].ID != "orders" {
		t.Fatalf("unexpected collections: %+v", resp.Collections)
	}
	if resp.NextPageToken != "abc" {
		t.Fatalf("nextPageToken = %q", resp.NextPageToken)
	}
	if mock.lastNR.Cloud != model.CloudGCP || mock.lastNR.AccountID != "test-project" {
		t.Fatalf("NR not GCP-scoped: %+v", mock.lastNR)
	}
	if mock.lastNR.Params["name"] != "databases/(default)/documents" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["pageSize"] != "5" {
		t.Fatalf("pageSize = %v", mock.lastNR.Params["pageSize"])
	}
}

func TestListDocuments_PassesFieldsThrough(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"documents": []any{
				map[string]any{
					"name":       "projects/test-project/databases/(default)/documents/users/u1",
					"createTime": "2026-01-01T00:00:00Z",
					"updateTime": "2026-01-02T00:00:00Z",
					"fields": map[string]any{
						"name": map[string]any{"stringValue": "Ada"},
						"age":  map[string]any{"integerValue": "42"},
						"tags": map[string]any{"arrayValue": map[string]any{
							"values": []any{map[string]any{"stringValue": "a"}},
						}},
					},
				},
			},
		},
	}}

	w := do(t, mock, http.MethodGet, "/collections/users/documents", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListDocumentsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 {
		t.Fatalf("total = %d, want 1", resp.Total)
	}
	doc := resp.Documents[0]
	if doc.ID != "u1" || doc.Collection != "users" || doc.UpdateTime != "2026-01-02T00:00:00Z" {
		t.Fatalf("unexpected document: %+v", doc)
	}
	// The raw Firestore encoding is passed through verbatim: an integerValue
	// stays the decimal string "42" (no int/double collapse).
	age, _ := doc.Fields["age"].(map[string]any)
	if age["integerValue"] != "42" {
		t.Fatalf("age field = %#v, want integerValue \"42\"", doc.Fields["age"])
	}
	name, _ := doc.Fields["name"].(map[string]any)
	if name["stringValue"] != "Ada" {
		t.Fatalf("name field = %#v", doc.Fields["name"])
	}
	if mock.lastNR.Params["name"] != "databases/(default)/documents/users" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestCreateDocument_PassesFieldsThrough(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"name": "projects/test-project/databases/(default)/documents/users/u1",
		},
	}}
	w := do(t, mock, http.MethodPost, "/collections/users/documents",
		`{"documentId":"u1","fields":{"name":{"stringValue":"Ada"},"age":{"integerValue":"42"},"score":{"doubleValue":1.5}}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if mock.lastNR.Params["name"] != "databases/(default)/documents/users" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["documentId"] != "u1" {
		t.Fatalf("documentId = %v", mock.lastNR.Params["documentId"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	fields, _ := body["fields"].(map[string]any)
	if age, _ := fields["age"].(map[string]any); age["integerValue"] != "42" {
		t.Fatalf("age = %#v, want integerValue \"42\"", fields["age"])
	}
	// UseNumber keeps the literal text, so doubleValue is json.Number("1.5").
	if score, _ := fields["score"].(map[string]any); score["doubleValue"] != json.Number("1.5") {
		t.Fatalf("score = %#v, want doubleValue 1.5", fields["score"])
	}
}

func TestCreateDocument_RejectsInvalidBody(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/collections/users/documents", `{`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateDocument_RejectsTrailingData(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/collections/users/documents", `{"fields":{}}garbage`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateDocument_RejectsDocumentIDWithSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/collections/users/documents",
		`{"documentId":"a/b","fields":{}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestGetDocument_PassesNameAndDecodesURLParam(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"name":   "projects/test-project/databases/(default)/documents/users/a+b",
			"fields": map[string]any{"name": map[string]any{"stringValue": "Ada"}},
		},
	}}
	// encodeURIComponent escapes '+' as %2B; chi alone would leave "%2B".
	w := do(t, mock, http.MethodGet, "/collections/users/documents/a%2Bb", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "databases/(default)/documents/users/a+b" {
		t.Fatalf("name = %v, want decoded '+'", mock.lastNR.Params["name"])
	}
	var doc Document
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if doc.ID != "a+b" || doc.Fields["name"].(map[string]any)["stringValue"] != "Ada" {
		t.Fatalf("unexpected document: %+v", doc)
	}
}

func TestGetDocument_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/collections/users/documents/a%2Fb", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateDocument_RequiresFields(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPatch, "/collections/users/documents/u1", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateDocument_PassesFieldsAndPrecondition(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/databases/(default)/documents/users/u1"},
	}}
	w := do(t, mock, http.MethodPatch, "/collections/users/documents/u1",
		`{"fields":{"name":{"stringValue":"Ada"}},"updateTime":"2026-01-02T00:00:00Z"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["name"] != "databases/(default)/documents/users/u1" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
	if mock.lastNR.Params["currentDocument.updateTime"] != "2026-01-02T00:00:00Z" {
		t.Fatalf("precondition = %v", mock.lastNR.Params["currentDocument.updateTime"])
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if _, hasMask := body["updateMask"]; hasMask {
		t.Fatalf("unexpected updateMask: %#v", body["updateMask"])
	}
}

func TestDeleteDocument_PassesName(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/collections/users/documents/u1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if mock.lastNR.Params["name"] != "databases/(default)/documents/users/u1" {
		t.Fatalf("name = %v", mock.lastNR.Params["name"])
	}
}

func TestListDocuments_RequestsShowMissing(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/collections/users/documents", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["showMissing"]; got != "true" {
		t.Fatalf("showMissing = %v, want \"true\"", got)
	}
}

func TestListDocuments_FlagsMissingDocuments(t *testing.T) {
	const base = "projects/test-project/databases/(default)/documents/users/"
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"documents": []any{
			// No create/update time → a showMissing placeholder.
			map[string]any{"name": base + "alice"},
			map[string]any{
				"name":       base + "bob",
				"createTime": "2026-01-01T00:00:00Z",
				"updateTime": "2026-01-02T00:00:00Z",
			},
		},
	}}}
	w := do(t, mock, http.MethodGet, "/collections/users/documents", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListDocumentsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Documents) != 2 {
		t.Fatalf("got %d documents, want 2", len(resp.Documents))
	}
	if !resp.Documents[0].Missing {
		t.Fatalf("alice should be flagged missing: %+v", resp.Documents[0])
	}
	if resp.Documents[1].Missing {
		t.Fatalf("bob should not be flagged missing: %+v", resp.Documents[1])
	}
}

func TestListDocuments_NestedCollectionPath(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	// encodeURIComponent escapes the nested collection path's '/' as %2F, which
	// chi routes as a single {collection} segment.
	w := do(t, mock, http.MethodGet, "/collections/users%2Falice%2Forders/documents", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice/orders" {
		t.Fatalf("name = %v, want nested collection path", got)
	}
}

func TestGetDocument_NestedCollectionPath(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/databases/(default)/documents/users/alice/orders/o1"},
	}}
	w := do(t, mock, http.MethodGet, "/collections/users%2Falice%2Forders/documents/o1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice/orders/o1" {
		t.Fatalf("name = %v", got)
	}
}

func TestListSubcollections_MapsIDs(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"collectionIds": []any{"orders", "invoices"},
			"nextPageToken": "tok",
		},
	}}
	w := do(t, mock, http.MethodGet, "/collections/users/documents/alice/collections?pageSize=5", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListCollectionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Collections) != 2 {
		t.Fatalf("got %+v, want 2 subcollections", resp)
	}
	if resp.Collections[0].ID != "orders" || resp.Collections[1].ID != "invoices" {
		t.Fatalf("unexpected subcollections: %+v", resp.Collections)
	}
	if resp.NextPageToken != "tok" {
		t.Fatalf("nextPageToken = %q", resp.NextPageToken)
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice" {
		t.Fatalf("name = %v, want parent document path", got)
	}
	if mock.lastNR.Params["pageSize"] != "5" {
		t.Fatalf("pageSize = %v", mock.lastNR.Params["pageSize"])
	}
}

func TestListSubcollections_NestedParentDocument(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/collections/users%2Falice%2Forders/documents/o1/collections", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice/orders/o1" {
		t.Fatalf("name = %v", got)
	}
}

// TestBuildRouter_MountedUnderParentPreservesEncodedSlash exercises the
// production chi.Mount hop (registrar mounts the package router under the outer
// mux); that hop is what makes %2F survive as a single {collection} segment.
func TestBuildRouter_MountedUnderParentPreservesEncodedSlash(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	parent := chi.NewRouter()
	parent.Mount("/api/ui/v1/gcp/firestore", BuildRouter(mock, testCfg()))

	r := httptest.NewRequest(http.MethodGet,
		"/api/ui/v1/gcp/firestore/collections/users%2Falice%2Forders/documents", nil)
	w := httptest.NewRecorder()
	parent.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice/orders" {
		t.Fatalf("name = %v", got)
	}
}

func TestCreateDocument_NestedCollectionPath(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "projects/test-project/databases/(default)/documents/users/alice/orders/o1"},
	}}
	w := do(t, mock, http.MethodPost, "/collections/users%2Falice%2Forders/documents",
		`{"documentId":"o1","fields":{}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice/orders" {
		t.Fatalf("name = %v", got)
	}
}

func TestDeleteDocument_NestedCollectionPath(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/collections/users%2Falice%2Forders/documents/o1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "databases/(default)/documents/users/alice/orders/o1" {
		t.Fatalf("name = %v", got)
	}
}

func TestDocumentParam_RejectsDotSegments(t *testing.T) {
	for _, path := range []string{
		"/collections/users/documents/%2E%2E",
		"/collections/users/documents/%2E",
	} {
		w := do(t, &mockProvider{}, http.MethodGet, path, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", path, w.Code)
		}
	}
}

func TestCollectionParam_RejectsMalformedPaths(t *testing.T) {
	// Even segment count (a document path, not a collection), dot segments and
	// empty segments are all rejected before reaching the provider.
	paths := []string{
		"/collections/users%2Falice/documents",     // document path
		"/collections/%2E%2E/documents",            // ".."
		"/collections/users%2F%2E/documents",       // "users/."
		"/collections/users%2F%2Forders/documents", // empty segment
	}
	for _, path := range paths {
		w := do(t, &mockProvider{}, http.MethodGet, path, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", path, w.Code)
		}
	}
}

func TestWriteError_MapsProviderError(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "document not found", 404)}
	w := do(t, mock, http.MethodGet, "/collections/users/documents/missing", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	var e map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if e["code"] != "NotFound" {
		t.Fatalf("code = %q, want NotFound", e["code"])
	}
}
