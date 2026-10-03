package storageui

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

type mockProvider struct {
	listBucketsResp  *model.ProviderResponse
	listBucketsErr   error
	createBucketResp *model.ProviderResponse
	createBucketErr  error
	deleteBucketErr  error
	listObjectsResp  *model.ProviderResponse
	listObjectsErr   error

	lastNR *model.NormalizedRequest
}

func (m *mockProvider) BucketsList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listBucketsResp != nil {
		return m.listBucketsResp, m.listBucketsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.listBucketsErr
}

func (m *mockProvider) BucketsInsert(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.createBucketResp != nil {
		return m.createBucketResp, m.createBucketErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.createBucketErr
}

func (m *mockProvider) BucketsDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, m.deleteBucketErr
}

func (m *mockProvider) ObjectsList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listObjectsResp != nil {
		return m.listObjectsResp, m.listObjectsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.listObjectsErr
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

func TestListBuckets_MapsItems(t *testing.T) {
	mock := &mockProvider{listBucketsResp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{"items": []any{
			map[string]any{
				"name":         "media",
				"location":     "US",
				"storageClass": "STANDARD",
				"timeCreated":  "2026-01-01T00:00:00Z",
				"versioning":   map[string]any{"enabled": true},
			},
		}},
	}}

	w := do(t, mock, http.MethodGet, "/buckets", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListBucketsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || len(resp.Items) != 1 {
		t.Fatalf("got %+v, want 1 item", resp)
	}
	if resp.Items[0].Name != "media" || !resp.Items[0].Versioning {
		t.Fatalf("unexpected bucket: %+v", resp.Items[0])
	}
	if mock.lastNR.Cloud != model.CloudGCP || mock.lastNR.AccountID != "test-project" {
		t.Fatalf("NR not GCP-scoped: %+v", mock.lastNR)
	}
}

func TestCreateBucket_RequiresName(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/buckets", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateBucket_PassesBody(t *testing.T) {
	mock := &mockProvider{createBucketResp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "logs", "location": "EU"},
	}}
	w := do(t, mock, http.MethodPost, "/buckets", `{"name":"logs","location":"EU"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["name"] != "logs" || body["location"] != "EU" {
		t.Fatalf("body = %+v", body)
	}
}

func TestDeleteBucket_PassesName(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/buckets/media", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if mock.lastNR.Params["bucket"] != "media" {
		t.Fatalf("bucket param = %v", mock.lastNR.Params["bucket"])
	}
}

func TestListObjects_ReturnsItemsAndPrefixes(t *testing.T) {
	mock := &mockProvider{listObjectsResp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"items":    []any{map[string]any{"name": "a.txt", "size": "3", "contentType": "text/plain"}},
			"prefixes": []string{"dir/"},
		},
	}}
	w := do(t, mock, http.MethodGet, "/buckets/media/objects?prefix=&delimiter=/", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListObjectsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Name != "a.txt" {
		t.Fatalf("items = %+v", resp.Items)
	}
	if len(resp.Prefixes) != 1 || resp.Prefixes[0] != "dir/" {
		t.Fatalf("prefixes = %+v", resp.Prefixes)
	}
}
