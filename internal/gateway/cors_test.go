package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// ─── corsExtractBucket ────────────────────────────────────────────────────────

func TestCorsExtractBucket_VirtualHosted(t *testing.T) {
	req, _ := http.NewRequest(http.MethodOptions, "/", nil)
	req.Host = "mybucket.s3.us-east-1.amazonaws.com"
	got := corsExtractBucket(req)
	if got != "mybucket" {
		t.Fatalf("want mybucket, got %q", got)
	}
}

func TestCorsExtractBucket_PathStyle(t *testing.T) {
	req, _ := http.NewRequest(http.MethodOptions, "/mybucket/key.txt", nil)
	got := corsExtractBucket(req)
	if got != "mybucket" {
		t.Fatalf("want mybucket, got %q", got)
	}
}

// ─── corsMatchRule ────────────────────────────────────────────────────────────

func TestCORS_Preflight_Match(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"https://example.com"},
			"AllowedMethods": []string{"GET", "PUT"},
			"MaxAgeSeconds":  300,
		},
	}
	rule, ok := corsMatchRule(rules, "https://example.com", "GET")
	if !ok {
		t.Fatal("want match for allowed origin+method")
	}
	if rule == nil {
		t.Fatal("matched rule must not be nil")
	}
}

func TestCORS_Preflight_NoMatch_Origin(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"https://example.com"},
			"AllowedMethods": []string{"GET"},
		},
	}
	_, ok := corsMatchRule(rules, "https://evil.com", "GET")
	if ok {
		t.Fatal("want no match for unlisted origin")
	}
}

func TestCORS_Preflight_NoMatch_Method(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"*"},
			"AllowedMethods": []string{"GET"},
		},
	}
	_, ok := corsMatchRule(rules, "https://any.com", "DELETE")
	if ok {
		t.Fatal("want no match for disallowed method")
	}
}

func TestCORS_Wildcard_Origin(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"*"},
			"AllowedMethods": []string{"GET"},
		},
	}
	_, ok := corsMatchRule(rules, "https://anything.com", "GET")
	if !ok {
		t.Fatal("wildcard origin must match any origin")
	}
}

// ─── corsWritePreflightHeaders ────────────────────────────────────────────────

func TestCORS_RegularRequest_HeadersAdded(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"https://example.com"},
			"AllowedMethods": []string{"GET"},
			"ExposeHeaders":  []string{"x-custom-header"},
		},
	}
	h := http.Header{}
	CORSAddResponseHeaders(h, rules, "https://example.com")
	if h.Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Fatalf("want ACAO header, got %q", h.Get("Access-Control-Allow-Origin"))
	}
	if h.Get("Access-Control-Expose-Headers") != "x-custom-header" {
		t.Fatalf("want expose header, got %q", h.Get("Access-Control-Expose-Headers"))
	}
}

func TestCORS_PreflightHeaders_MaxAge(t *testing.T) {
	rule := map[string]any{
		"AllowedOrigins": []string{"*"},
		"AllowedMethods": []string{"GET", "POST"},
		"MaxAgeSeconds":  600,
	}
	w := httptest.NewRecorder()
	corsWritePreflightHeaders(w, rule, "https://example.com", "")
	if w.Header().Get("Access-Control-Max-Age") != "600" {
		t.Fatalf("want Max-Age=600, got %q", w.Header().Get("Access-Control-Max-Age"))
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Fatalf("want ACAO header set")
	}
}
