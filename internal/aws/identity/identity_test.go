package identity_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/aws/identity"
)

func makeReq(auth string) *http.Request {
	r := httptest.NewRequest("POST", "/", nil)
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	return r
}

func makeReqQuery(query string) *http.Request {
	r := httptest.NewRequest("GET", "/?"+query, nil)
	return r
}

// TestAccountFromAccessKey covers §5.1 resolution rules.
func TestAccountFromAccessKey(t *testing.T) {
	cases := []struct {
		key  string
		want string
	}{
		// 12-digit literal
		{"000000000000", "000000000000"},
		{"420420420420", "420420420420"},
		{"999999999999", "999999999999"},
		// LSIA-encoded (round-trip via EncodeLSIA)
		// AKIA/ASIA → default (no parity mode)
		{"AKIAIOSFODNN7EXAMPLE", identity.DefaultAccountID},
		{"ASIAJJJJJJJJJJJJJJJJ", identity.DefaultAccountID},
		// short / garbage → default
		{"test", identity.DefaultAccountID},
		{"", identity.DefaultAccountID},
		{"short", identity.DefaultAccountID},
		// 13-digit → default (strict regex)
		{"1234567890123", identity.DefaultAccountID},
	}
	for _, c := range cases {
		got := identity.AccountFromAccessKey(c.key)
		if got != c.want {
			t.Errorf("AccountFromAccessKey(%q) = %q, want %q", c.key, got, c.want)
		}
	}
}

// TestAccountFromAccessKey_LSIARoundTrip verifies that LSIA-encoded keys decode correctly.
func TestAccountFromAccessKey_LSIARoundTrip(t *testing.T) {
	accounts := []string{"000000000000", "420420420420", "133713371337", "999999999999"}
	for _, acct := range accounts {
		key, err := identity.EncodeLSIA(acct)
		if err != nil {
			t.Fatalf("EncodeLSIA(%q): %v", acct, err)
		}
		got := identity.AccountFromAccessKey(key)
		if got != acct {
			t.Errorf("AccountFromAccessKey(EncodeLSIA(%q)) = %q, want %q", acct, got, acct)
		}
	}
}

// TestFromRequest_SigV4Header covers the Authorization header path.
func TestFromRequest_SigV4Header(t *testing.T) {
	// 12-digit access key → account is the key
	r := makeReq("AWS4-HMAC-SHA256 Credential=420420420420/20260516/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=xxx")
	p := identity.FromRequest(r)
	if p.AccountID != "420420420420" {
		t.Errorf("AccountID = %q, want 420420420420", p.AccountID)
	}
	if p.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", p.Region)
	}
	if p.Source != identity.SourceSigV4Header {
		t.Errorf("Source = %v, want SourceSigV4Header", p.Source)
	}
}

// TestFromRequest_SigV4Query covers the X-Amz-Credential presigned path.
func TestFromRequest_SigV4Query(t *testing.T) {
	r := makeReqQuery("X-Amz-Credential=111111111111%2F20260516%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Algorithm=AWS4-HMAC-SHA256")
	p := identity.FromRequest(r)
	if p.AccountID != "111111111111" {
		t.Errorf("AccountID = %q, want 111111111111", p.AccountID)
	}
	if p.Region != "us-west-2" {
		t.Errorf("Region = %q, want us-west-2", p.Region)
	}
	if p.Source != identity.SourceSigV4Query {
		t.Errorf("Source = %v, want SourceSigV4Query", p.Source)
	}
}

// TestFromRequest_SigV2Header covers "Authorization: AWS <key>:<sig>" path.
func TestFromRequest_SigV2Header(t *testing.T) {
	r := makeReq("AWS 222222222222:sigXXX")
	p := identity.FromRequest(r)
	if p.AccountID != "222222222222" {
		t.Errorf("AccountID = %q, want 222222222222", p.AccountID)
	}
}

// TestFromRequest_SigV2Query covers AWSAccessKeyId= query param.
func TestFromRequest_SigV2Query(t *testing.T) {
	r := makeReqQuery("AWSAccessKeyId=333333333333&Signature=sig")
	p := identity.FromRequest(r)
	if p.AccountID != "333333333333" {
		t.Errorf("AccountID = %q, want 333333333333", p.AccountID)
	}
	if p.Source != identity.SourceSigV2Query {
		t.Errorf("Source = %v, want SourceSigV2Query", p.Source)
	}
}

// TestFromRequest_Anonymous covers requests with no credential at all.
func TestFromRequest_Anonymous(t *testing.T) {
	r := makeReq("")
	p := identity.FromRequest(r)
	if p.AccountID != identity.DefaultAccountID {
		t.Errorf("AccountID = %q, want %q", p.AccountID, identity.DefaultAccountID)
	}
	if p.Source != identity.SourceDefault {
		t.Errorf("Source = %v, want SourceDefault", p.Source)
	}
}

// TestAccountFromAccessKey_MalformedAuth matches §5.4.2 table.
func TestAccountFromAccessKey_MalformedAuth(t *testing.T) {
	cases := []struct {
		name string
		auth string
	}{
		{"absent", ""},
		{"empty", "   "},
		{"no_credential", "AWS4-HMAC-SHA256 "},
		{"empty_credential", "AWS4-HMAC-SHA256 Credential="},
		{"key_only", "AWS4-HMAC-SHA256 Credential=AKIA"},
		{"one_sep", "AWS4-HMAC-SHA256 Credential=AKIA/20260516"},
		{"four_fields", "AWS4-HMAC-SHA256 Credential=AKIA/20260516/us-east-1/iam"},
		{"bearer", "Bearer eyJhbGciOiJIUzI1NiIs"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", nil)
			if c.auth != "" {
				r.Header.Set("Authorization", c.auth)
			}
			p := identity.FromRequest(r)
			// All malformed cases fall back to DefaultAccountID; no panic.
			if p.AccountID != identity.DefaultAccountID {
				t.Errorf("AccountID = %q, want %q", p.AccountID, identity.DefaultAccountID)
			}
		})
	}
}
