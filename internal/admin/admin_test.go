package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// mockSnapshotter is a test double implementing admin.Snapshotter.
type mockSnapshotter struct {
	data    map[string]json.RawMessage
	isEmpty bool
	snapErr error
	restErr error
}

func (m *mockSnapshotter) Snapshot(_ context.Context, w io.Writer) error {
	if m.snapErr != nil {
		return m.snapErr
	}
	return json.NewEncoder(w).Encode(m.data)
}

func (m *mockSnapshotter) Restore(_ context.Context, r io.Reader) error {
	if m.restErr != nil {
		return m.restErr
	}
	return json.NewDecoder(r).Decode(&m.data)
}

func (m *mockSnapshotter) IsEmpty(_ context.Context) (bool, error) {
	return m.isEmpty, nil
}

func (m *mockSnapshotter) Reset() { m.data = nil; m.isEmpty = true }

// mockScopedResetter tracks which Reset variant was called.
type mockScopedResetter struct {
	resetCalled       bool
	resetAccountCalls []string
	resetScopeCalls   [][2]string
}

func (m *mockScopedResetter) Reset()                           { m.resetCalled = true }
func (m *mockScopedResetter) ResetAccount(account string)      { m.resetAccountCalls = append(m.resetAccountCalls, account) }
func (m *mockScopedResetter) ResetScope(account, region string) {
	m.resetScopeCalls = append(m.resetScopeCalls, [2]string{account, region})
}

// --- snapshotHasKMSMaterial ---

func TestSnapshotHasKMSMaterial_Null(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage("null"),
	}
	if snapshotHasKMSMaterial(stores) {
		t.Fatal("expected false for null JSON value")
	}
}

func TestSnapshotHasKMSMaterial_Empty(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage("{}"),
	}
	if snapshotHasKMSMaterial(stores) {
		t.Fatal("expected false for empty JSON object")
	}
}

func TestSnapshotHasKMSMaterial_Unparseable(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage("not-json"),
	}
	if !snapshotHasKMSMaterial(stores) {
		t.Fatal("expected true (fail-safe) for unparseable bytes")
	}
}

func TestSnapshotHasKMSMaterial_HasKeys(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage(`{"key-id-1":{}}`),
	}
	if !snapshotHasKMSMaterial(stores) {
		t.Fatal("expected true when keys map is non-empty")
	}
}

func TestSnapshotHasKMSMaterial_WrongKey(t *testing.T) {
	// Old "kms-keys" key must NOT trigger the gate — only "keys" should.
	stores := map[string]json.RawMessage{
		"kms-keys": json.RawMessage(`{"key-id-1":{}}`),
	}
	if snapshotHasKMSMaterial(stores) {
		t.Fatal("expected false: gate must use key 'keys', not 'kms-keys'")
	}
}

// --- Handler.Export ---

func TestHandler_Export_IncludesAllRegisteredStores(t *testing.T) {
	h := NewHandler()
	for _, name := range []string{"resources", "keys", "secrets", "parameters"} {
		h.RegisterSnapshotter(name, &mockSnapshotter{isEmpty: true})
	}
	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	var env SnapshotEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&env); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, name := range []string{"resources", "keys", "secrets", "parameters"} {
		if _, ok := env.Stores[name]; !ok {
			t.Errorf("expected store key %q in export", name)
		}
	}
}

func TestHandler_Export_SchemaVersion3(t *testing.T) {
	h := NewHandler()
	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	var env SnapshotEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.SchemaVersion != 3 {
		t.Errorf("expected schema_version 3, got %d", env.SchemaVersion)
	}
}

func TestHandler_Export_CloudField(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	var env SnapshotEnvelope
	json.NewDecoder(rec.Body).Decode(&env)
	if env.Cloud != "aws" {
		t.Errorf("expected cloud 'aws', got %q", env.Cloud)
	}
}

func TestHandler_Export_SnapshotterError(t *testing.T) {
	h := NewHandler()
	snap := &mockSnapshotter{}
	snap.snapErr = errTest("boom")
	h.RegisterSnapshotter("resources", snap)

	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// --- Handler.Import ---

func TestHandler_Import_CloudMismatch(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "gcp", Stores: map[string]json.RawMessage{}}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var resp CloudMismatchError
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Code != "cloud_mismatch" {
		t.Errorf("expected error 'cloud_mismatch', got %q", resp.Code)
	}
	if resp.EnvelopeCloud != "gcp" {
		t.Errorf("expected envelope_cloud 'gcp', got %q", resp.EnvelopeCloud)
	}
	if resp.InstanceCloud != "aws" {
		t.Errorf("expected instance_cloud 'aws', got %q", resp.InstanceCloud)
	}
	if resp.Message == "" {
		t.Error("expected non-empty message")
	}
}

func TestHandler_Import_NonEmptyState_409Body(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	// store-a is non-empty, store-b is empty
	h.RegisterSnapshotter("store-a", &mockSnapshotter{isEmpty: false})
	h.RegisterSnapshotter("store-b", &mockSnapshotter{isEmpty: true})

	stores := map[string]json.RawMessage{"store-a": json.RawMessage("{}"), "store-b": json.RawMessage("{}")}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var resp NonEmptyStateError
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Code != "non_empty_state" {
		t.Errorf("expected error 'non_empty_state', got %q", resp.Code)
	}
	if len(resp.NonEmptyStores) == 0 {
		t.Error("expected non_empty_stores to be non-empty")
	}
	found := false
	for _, s := range resp.NonEmptyStores {
		if s == "store-a" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'store-a' in non_empty_stores, got %v", resp.NonEmptyStores)
	}
	// message must contain at least one escape path
	for _, hint := range []string{"POST /_jaiscloud/reset", "--fresh-start", "--data-dir"} {
		if bytes.Contains([]byte(resp.Message), []byte(hint)) {
			goto foundHint
		}
	}
	t.Error("expected message to contain at least one escape path (reset / --fresh-start / --data-dir)")
foundHint:
}

func TestHandler_Import_V1BareMapRejected(t *testing.T) {
	h := NewHandler()
	// bare map (v1) must be rejected
	body := []byte(`{"resources":"{}"}`)
	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bare-map body, got %d", rec.Code)
	}
}

func TestHandler_Import_UnknownStoreKey(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	h.RegisterSnapshotter("resources", &mockSnapshotter{isEmpty: true})

	stores := map[string]json.RawMessage{
		"unknown-store": json.RawMessage("{}"),
	}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 (unknown key silently skipped), got %d", rec.Code)
	}
}

func TestHandler_Import_RestoreError(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	snap := &mockSnapshotter{isEmpty: true}
	snap.restErr = errTest("restore failed")
	h.RegisterSnapshotter("resources", snap)

	stores := map[string]json.RawMessage{"resources": json.RawMessage("{}")}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// --- Handler.Reset ---

func TestHandler_Reset_NoParams(t *testing.T) {
	h := NewHandler()
	r := &mockScopedResetter{}
	h.RegisterResetter(r)

	rec := httptest.NewRecorder()
	h.Reset(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/reset", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if !r.resetCalled {
		t.Error("expected Reset() to be called")
	}
}

func TestHandler_Reset_AccountScope(t *testing.T) {
	h := NewHandler()
	r := &mockScopedResetter{}
	h.RegisterResetter(r)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/reset?account=111111111111", nil)
	h.Reset(rec, req)

	if len(r.resetAccountCalls) != 1 || r.resetAccountCalls[0] != "111111111111" {
		t.Errorf("expected ResetAccount('111111111111'), got %v", r.resetAccountCalls)
	}
	if r.resetCalled {
		t.Error("expected Reset() NOT to be called when account param is set")
	}
}

func TestHandler_Reset_AccountRegionScope(t *testing.T) {
	h := NewHandler()
	r := &mockScopedResetter{}
	h.RegisterResetter(r)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/reset?account=111111111111&region=us-east-1", nil)
	h.Reset(rec, req)

	if len(r.resetScopeCalls) != 1 {
		t.Fatalf("expected one ResetScope call, got %d", len(r.resetScopeCalls))
	}
	got := r.resetScopeCalls[0]
	if got[0] != "111111111111" || got[1] != "us-east-1" {
		t.Errorf("expected ResetScope('111111111111','us-east-1'), got %v", got)
	}
	if r.resetCalled {
		t.Error("expected Reset() NOT to be called when account+region params are set")
	}
}

// errTest is a simple error value for tests.
type errTest string

func (e errTest) Error() string { return string(e) }
