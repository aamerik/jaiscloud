package aws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func newIMDSRouter(cfg IMDSConfig) chi.Router {
	r := chi.NewRouter()
	RegisterIMDSRoutes(r, cfg)
	return r
}

func TestIMDS_TokenPUT(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "us-west-2"})
	req := httptest.NewRequest(http.MethodPut, "/latest/api/token", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	if got := rec.Body.String(); got != imdsStaticToken {
		t.Fatalf("token body=%q want %q", got, imdsStaticToken)
	}
	if rec.Header().Get("X-Aws-Ec2-Metadata-Token-Ttl-Seconds") == "" {
		t.Fatal("missing TTL header")
	}
}

func TestIMDS_Region(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "eu-west-1"})
	req := httptest.NewRequest(http.MethodGet, "/latest/meta-data/placement/region", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.String() != "eu-west-1" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestIMDS_AvailabilityZone(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "ap-south-1"})
	req := httptest.NewRequest(http.MethodGet, "/latest/meta-data/placement/availability-zone", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if got := rec.Body.String(); got != "ap-south-1a" {
		t.Fatalf("az=%q want ap-south-1a", got)
	}
}

func TestIMDS_IdentityDocument_IsValidJSON(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "us-east-1", AccountID: "000000000000"})
	req := httptest.NewRequest(http.MethodGet, "/latest/dynamic/instance-identity/document", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status=%d", rec.Code)
	}
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("response is not JSON: %v\nbody=%s", err, rec.Body.String())
	}
	if got := doc["region"]; got != "us-east-1" {
		t.Fatalf("region=%v want us-east-1", got)
	}
	if got := doc["accountId"]; got != "000000000000" {
		t.Fatalf("accountId=%v", got)
	}
}

func TestIMDS_CredentialJSON_SafeAgainstInjection(t *testing.T) {
	malicious := `evil","AccessKeyId":"injected`
	r := newIMDSRouter(IMDSConfig{
		Region:          "us-east-1",
		RoleName:        "jaiscloud-emulator-role",
		AccessKeyID:     malicious,
		SecretAccessKey: "secret",
	})
	req := httptest.NewRequest(http.MethodGet,
		"/latest/meta-data/iam/security-credentials/jaiscloud-emulator-role", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	var creds map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &creds); err != nil {
		t.Fatalf("not valid JSON: %v\nbody=%s", err, rec.Body.String())
	}
	if got := creds["AccessKeyId"]; got != malicious {
		t.Fatalf("AccessKeyId=%v want %q (injection broke the envelope)", got, malicious)
	}
}

func TestIMDS_SecurityCredentialsListing(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "us-east-1", RoleName: "my-role"})
	req := httptest.NewRequest(http.MethodGet, "/latest/meta-data/iam/security-credentials/", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != "my-role" {
		t.Fatalf("listing=%q", rec.Body.String())
	}
}

func TestIMDS_MetadataRootListing(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "us-east-1"})
	req := httptest.NewRequest(http.MethodGet, "/latest/meta-data/", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	body := rec.Body.String()
	for _, want := range []string{"iam/", "placement/", "instance-id", "region"} {
		if !strings.Contains(body, want) {
			t.Errorf("listing missing %q: %q", want, body)
		}
	}
}

func TestIMDS_UnknownPathReturns404(t *testing.T) {
	r := newIMDSRouter(IMDSConfig{Region: "us-east-1"})
	req := httptest.NewRequest(http.MethodGet, "/latest/meta-data/does/not/exist", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404", rec.Code)
	}
}
