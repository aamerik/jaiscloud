package secretmanagerui

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

func (m *mockProvider) Create(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) List(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Get(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Update(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Delete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) AddVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Access(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ListVersions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DestroyVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) DisableVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) EnableVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func TestListSecrets_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"secrets": []any{
				map[string]any{
					"name":       "projects/p/secrets/db-pass",
					"createTime": "2024-01-01T00:00:00Z",
					"etag":       "e",
					"labels":     map[string]string{"env": "prod"},
					"rotation":   map[string]any{"rotationPeriod": "86400s", "nextRotationTime": "2024-01-02T00:00:00Z"},
				},
			},
			"nextPageToken": "tok",
		},
	}}
	w := do(t, mock, http.MethodGet, "/secrets", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListSecretsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.Secrets[0].SecretID != "db-pass" || resp.NextPageToken != "tok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.Secrets[0].Labels["env"] != "prod" || resp.Secrets[0].RotationPeriod != "86400s" {
		t.Fatalf("secret = %+v", resp.Secrets[0])
	}
}

func TestCreateSecret_SetsIDAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "projects/p/secrets/db-pass"}}}
	w := do(t, mock, http.MethodPost, "/secrets", `{"secretId":"db-pass","labels":{"env":"prod"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["secretId"] != "db-pass" {
		t.Fatalf("secretId = %v", mock.lastNR.Params["secretId"])
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if labels, ok := body["labels"].(map[string]any); !ok || labels["env"] != "prod" {
		t.Fatalf("body = %+v", body)
	}
}

func TestAddVersion_Base64Payload(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/secrets/db-pass/versions/1", "state": "ENABLED",
	}}}
	w := do(t, mock, http.MethodPost, "/secrets/db-pass/versions", `{"payload":"MDEyMzQ1"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "secrets/db-pass" {
		t.Fatalf("name = %v", got)
	}
	payload := mock.lastNR.Params["body"].(map[string]any)["payload"].(map[string]any)
	if payload["data"] != "MDEyMzQ1" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestAccessVersion_ExtractsPayload(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name":    "projects/p/secrets/db-pass/versions/1",
		"payload": map[string]any{"data": "MDEyMzQ1", "dataCrc32c": "x"},
	}}}
	w := do(t, mock, http.MethodGet, "/secrets/db-pass/versions/1/access", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp AccessSecretVersionResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Data != "MDEyMzQ1" {
		t.Fatalf("data = %q", resp.Data)
	}
	if got := mock.lastNR.Params["name"]; got != "secrets/db-pass/versions/1" {
		t.Fatalf("name = %v", got)
	}
}

func TestDisableVersion_UsesVersionName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"state": "DISABLED"}}}
	w := do(t, mock, http.MethodPost, "/secrets/db-pass/versions/2/disable", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "secrets/db-pass/versions/2" {
		t.Fatalf("name = %v", got)
	}
}

func TestGetIamPolicy_UsesSecretName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "e"}}}
	w := do(t, mock, http.MethodGet, "/secrets/db-pass/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "secrets/db-pass" {
		t.Fatalf("name = %v", got)
	}
}

func TestUpdateSecret_PreservesAbsentFields(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "projects/p/secrets/db-pass"}}}
	w := do(t, mock, http.MethodPatch, "/secrets/db-pass", `{"labels":{"env":"dev"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if _, ok := body["rotation"]; ok {
		t.Fatalf("absent rotation should not be sent: %+v", body)
	}
}
