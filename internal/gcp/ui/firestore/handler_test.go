package firestoreui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
