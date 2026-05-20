// Package identity extracts per-request AWS identity (account ID, region,
// access key) from SigV4/SigV2 credentials. It has no dependencies on other
// JaisCloud packages and is safe to import from the gateway layer.
package identity

import (
	"net/http"
	"regexp"
	"strings"
)

const (
	// DefaultAccountID is the fallback for requests without recognisable credentials.
	DefaultAccountID = "000000000000"
	// DefaultRegion is the fallback when no region is present in the credential.
	DefaultRegion = "us-east-1"
)

// TwelveDigit is the strict-anchored 12-digit account-ID validator.
// Exported so the bundle package can reuse it without a shared package import.
// We deviate from LocalStack's unanchored \d{12} intentionally — see §5.4.
var TwelveDigit = regexp.MustCompile(`^\d{12}$`)

// Source describes how the identity was derived.
type Source uint8

const (
	SourceDefault     Source = iota // no credential found; identity is DefaultAccountID
	SourceSigV4Header               // Authorization: AWS4-HMAC-SHA256 Credential=...
	SourceSigV4Query                // X-Amz-Credential=... (presigned URL)
	SourceSigV2Header               // Authorization: AWS <key>:<sig>
	SourceSigV2Query                // AWSAccessKeyId=... in query string (legacy S3)
	SourceFallback                  // SigV4 shape valid, account not decodable; region extracted
)

// Parsed is the per-request identity derived from the HTTP request.
type Parsed struct {
	AccountID string // always non-empty; falls back to DefaultAccountID
	Region    string // empty if not in credential; caller applies cfg.Region fallback
	AccessKey string // raw access-key-id string; empty for anonymous requests
	Source    Source
}

// sigV4Cred holds the parsed fields of a SigV4 Credential= string.
type sigV4Cred struct {
	AccessKey string
	Date      string
	Region    string
	Service   string
}

// parseSigV4Credential extracts the SigV4 credential fields from an
// Authorization header value or "Credential=..." query-param value.
// Returns (zero, false) for any malformed input — no panics.
func parseSigV4Credential(value string) (sigV4Cred, bool) {
	idx := strings.Index(value, "Credential=")
	if idx < 0 {
		return sigV4Cred{}, false
	}
	cred := value[idx+len("Credential="):]
	if end := strings.IndexByte(cred, ','); end >= 0 {
		cred = cred[:end]
	}
	cred = strings.TrimSpace(cred)
	parts := strings.Split(cred, "/")
	if len(parts) < 5 {
		return sigV4Cred{}, false
	}
	return sigV4Cred{
		AccessKey: parts[0],
		Date:      parts[1],
		Region:    parts[2],
		Service:   parts[3],
	}, true
}

// FromRequest parses the access-key + region from the HTTP request and returns
// the resolved identity. It never errors — any unparseable or missing credential
// falls back to (DefaultAccountID, Source=SourceDefault).
//
// Resolution order (mirrors LocalStack auth.py:39-55):
//  1. Authorization: AWS4-HMAC-SHA256 Credential=<key>/…  → SigV4Header
//  2. ?X-Amz-Credential=<key>%2F…                         → SigV4Query
//  3. Authorization: AWS <accessKey>:<sig>                 → SigV2Header
//  4. ?AWSAccessKeyId=<key>                                → SigV2Query
//  5. nothing                                              → Default
func FromRequest(r *http.Request) Parsed {
	// 1. SigV4 Authorization header
	if auth := r.Header.Get("Authorization"); auth != "" {
		if cred, ok := parseSigV4Credential(auth); ok {
			acct := AccountFromAccessKey(cred.AccessKey)
			src := SourceSigV4Header
			if acct == DefaultAccountID && cred.AccessKey != DefaultAccountID {
				// Key couldn't decode to an account, but region is still valid.
				src = SourceFallback
			}
			return Parsed{
				AccountID: acct,
				Region:    cred.Region,
				AccessKey: cred.AccessKey,
				Source:    src,
			}
		}
		// SigV2 Authorization header: "AWS <key>:<sig>"
		if strings.HasPrefix(auth, "AWS ") {
			s := strings.TrimPrefix(auth, "AWS ")
			if i := strings.IndexByte(s, ':'); i > 0 {
				key := s[:i]
				return Parsed{
					AccountID: AccountFromAccessKey(key),
					AccessKey: key,
					Source:    SourceSigV2Header,
				}
			}
		}
	}

	// 2. X-Amz-Credential query param (presigned URL)
	if v := r.URL.Query().Get("X-Amz-Credential"); v != "" {
		if cred, ok := parseSigV4Credential("Credential=" + v); ok {
			return Parsed{
				AccountID: AccountFromAccessKey(cred.AccessKey),
				Region:    cred.Region,
				AccessKey: cred.AccessKey,
				Source:    SourceSigV4Query,
			}
		}
	}

	// 3. AWSAccessKeyId query param (SigV2 presigned, always S3)
	if v := r.URL.Query().Get("AWSAccessKeyId"); v != "" {
		return Parsed{
			AccountID: AccountFromAccessKey(v),
			AccessKey: v,
			Source:    SourceSigV2Query,
		}
	}

	return Parsed{AccountID: DefaultAccountID, Source: SourceDefault}
}

// AccountFromAccessKey resolves an access-key string to a 12-digit account ID.
// Reference: localstack-core/localstack/aws/accounts.py:60-81.
//
//  1. ^\d{12}$ literal numeric key → the key IS the account (LocalStack pattern).
//  2. "ASIA", "LSIA", or "LKIA" prefix + length ≥ 20 → base32-decode embedded account.
//     JaisCloud issues ASIA-prefixed keys (AWS-compatible) with an embedded account ID.
//  3. "AKIA" prefix + JAISCLOUD_PARITY_AWS_ACCESS_KEY_ID=true → same decode.
//  4. Everything else (including "test", empty, malformed) → DefaultAccountID.
func AccountFromAccessKey(accessKey string) string {
	if TwelveDigit.MatchString(accessKey) {
		return accessKey
	}
	if len(accessKey) >= 20 {
		prefix := accessKey[:4]
		switch prefix {
		case "ASIA", "LSIA", "LKIA":
			if acct, ok := DecodeLSIA(accessKey); ok {
				return acct
			}
		case "AKIA":
			if parityEnabled() {
				if acct, ok := DecodeLSIA(accessKey); ok {
					return acct
				}
			}
		}
	}
	return DefaultAccountID
}
