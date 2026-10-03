package kmsui

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

func (m *mockProvider) KeyRingCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) KeyRingList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) KeyRingGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionDestroy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionUpdate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyUpdatePrimaryVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TestIamPermissions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func TestListKeyRings_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"keyRings": []any{map[string]any{
				"name":       "projects/p/locations/global/keyRings/kr1",
				"createTime": "2024-01-01T00:00:00Z",
			}},
			"nextPageToken": "tok",
		},
	}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListKeyRingsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.KeyRings[0].KeyRingID != "kr1" || resp.KeyRings[0].Location != "global" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if mock.lastNR.Params["location"] != "global" {
		t.Fatalf("location = %v", mock.lastNR.Params["location"])
	}
}

func TestCreateKeyRing_SetsLocationAndID(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/locations/global/keyRings/kr1",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings", `{"keyRingId":"kr1"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["keyRingId"] != "kr1" || mock.lastNR.Params["location"] != "global" {
		t.Fatalf("params = %+v", mock.lastNR.Params)
	}
}

func TestCreateCryptoKey_BuildsBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name":    "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1",
		"purpose": "ENCRYPT_DECRYPT",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys",
		`{"cryptoKeyId":"ck1","purpose":"ENCRYPT_DECRYPT","algorithm":"GOOGLE_SYMMETRIC_ENCRYPTION","rotationPeriod":"86400s"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1" {
		t.Fatalf("name = %v", got)
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["rotationPeriod"] != "86400s" {
		t.Fatalf("body = %+v", body)
	}
	if vt, ok := body["versionTemplate"].(map[string]any); !ok || vt["algorithm"] != "GOOGLE_SYMMETRIC_ENCRYPTION" {
		t.Fatalf("versionTemplate = %+v", body["versionTemplate"])
	}
}

func TestSetVersionState_Disable(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/2", "state": "DISABLED",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/2/disable", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/2" {
		t.Fatalf("name = %v", got)
	}
	if got := mock.lastNR.Params["updateMask"]; got != "state" {
		t.Fatalf("updateMask = %v", got)
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["state"]; got != "DISABLED" {
		t.Fatalf("state = %v", got)
	}
}

func TestSetPrimaryVersion(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/setPrimary", `{"cryptoKeyVersionId":"3"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["cryptoKeyVersionId"]; got != "3" {
		t.Fatalf("cryptoKeyVersionId = %v", got)
	}
}

func TestGetKeyRingIam_UsesName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "e"}}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings/kr1/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1" {
		t.Fatalf("name = %v", got)
	}
}

func TestGetCryptoKeyIam_UsesName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "e"}}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings/kr1/cryptoKeys/ck1/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1" {
		t.Fatalf("name = %v", got)
	}
}
