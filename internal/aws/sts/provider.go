// Package sts implements the AWS STS provider for JaisCloud.
// It handles temporary credential issuance (AssumeRole variants, GetSessionToken,
// GetFederationToken) and session tag propagation, matching LocalStack behavioral parity.
package sts

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

var (
	// from LocalStack provider.py lines 39–41
	roleARNRegex           = regexp.MustCompile(`^arn:[^:]+:[^:]+:[^:]*:[^:]*:[^:]+$`)
	sessionNameRegex       = regexp.MustCompile(`^[\w+=,.@-]*$`)
	federationNameRegex    = regexp.MustCompile(`^[\w+=,.@-]+$`)
)

// STSProvider handles STS API operations.
type STSProvider struct {
	store SessionStore
}

// New constructs an STSProvider.
func New(store SessionStore) *STSProvider {
	return &STSProvider{store: store}
}

// Routes returns all STS handler registrations.
func (p *STSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"STS.GetCallerIdentity":          p.GetCallerIdentity,
		"STS.AssumeRole":                 p.AssumeRole,
		"STS.AssumeRoleWithWebIdentity":  p.AssumeRoleWithWebIdentity,
		"STS.AssumeRoleWithSAML":         p.AssumeRoleWithSAML,
		"STS.GetSessionToken":            p.GetSessionToken,
		"STS.GetFederationToken":         p.GetFederationToken,
		"STS.GetAccessKeyInfo":           p.GetAccessKeyInfo,
		"STS.DecodeAuthorizationMessage": p.DecodeAuthorizationMessage,
	}
}

// Reset wipes all session state. Implements admin.Resetter.
func (p *STSProvider) Reset() {
	p.store.Reset()
}

// ─── handlers ─────────────────────────────────────────────────────────────────

func (p *STSProvider) GetCallerIdentity(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Extract the access key from the Authorization header so we can distinguish
	// between static IAM credentials and temporary session credentials.
	var authHeader string
	if nr.Raw != nil {
		authHeader = nr.Raw.Header.Get("Authorization")
	}
	accessKey := extractAccessKeyFromAuth(authHeader)

	// ASIA* prefix indicates a temporary/session credential issued by AssumeRole.
	if strings.HasPrefix(accessKey, "ASIA") {
		if sess, ok := p.store.GetSession(accessKey); ok {
			// Best-effort: use tag context to reconstruct role/session names.
			roleName := "assumed-role"
			sessionName := "session"
			if rn, exists := sess.IAMContext["role_name"]; exists {
				if s, ok := rn.(string); ok && s != "" {
					roleName = s
				}
			}
			if sn, exists := sess.IAMContext["session_name"]; exists {
				if s, ok := sn.(string); ok && s != "" {
					sessionName = s
				}
			}
			return provider.OK(map[string]any{
				"Account": nr.AccountID,
				"Arn":     nr.ResourceID("sts-assumed-role", roleName+"/"+sessionName),
				"UserId":  accessKey + ":" + sessionName,
			}), nil
		}
		// ASIA key but no session found — return a generic assumed-role identity.
		return provider.OK(map[string]any{
			"Account": nr.AccountID,
			"Arn":     nr.ResourceID("sts-assumed-role", "assumed-role/session"),
			"UserId":  accessKey + ":session",
		}), nil
	}

	// Static credentials or no key — return root identity.
	return provider.OK(map[string]any{
		"Account": nr.AccountID,
		"Arn":     nr.ResourceID("iam-root", ""),
		"UserId":  nr.AccountID,
	}), nil
}

func (p *STSProvider) AssumeRole(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	sessionName := strParam(nr.Params, "RoleSessionName")
	durationSecs := intParam(nr.Params, "DurationSeconds", 3600)

	if durationSecs < 900 || durationSecs > 43200 {
		return nil, stsErr("ValidationError",
			"1 validation error detected: Value at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900", 400)
	}
	if !roleARNRegex.MatchString(roleArn) {
		return nil, stsErr("ValidationError", fmt.Sprintf("%s is invalid", roleArn), 400)
	}
	if !sessionNameRegex.MatchString(sessionName) {
		return nil, stsErr("ValidationError", fmt.Sprintf(
			"1 validation error detected: Value '%s' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*",
			sessionName), 400)
	}

	// extract caller's access key from Authorization header for tag propagation
	var authHeader string
	if nr.Raw != nil {
		authHeader = nr.Raw.Header.Get("Authorization")
	}
	callerKey := extractAccessKeyFromAuth(authHeader)
	existing, hasExisting := p.store.GetSession(callerKey)

	// process tags
	tags := parseTags(nr.Params, "Tags")
	transitiveKeys := parseStringList(nr.Params, "TransitiveTagKeys")

	if len(tags) > 0 {
		tagKeySet := make(map[string]bool, len(tags))
		for _, t := range tags {
			lk := strings.ToLower(t.Key)
			if tagKeySet[lk] {
				return nil, stsErr("InvalidParameterValue",
					"Duplicate tag keys found. Please note that Tag keys are case insensitive.", 400)
			}
			tagKeySet[lk] = true
		}
		if hasExisting {
			for _, tk := range existing.TransitiveTags {
				if tagKeySet[tk] {
					return nil, stsErr("InvalidParameterValue",
						"One of the specified transitive tag keys can't be set because it conflicts with a transitive tag key from the calling session.", 400)
				}
			}
		}
		if len(transitiveKeys) > 0 {
			tagMap := make(map[string]bool, len(tags))
			for _, t := range tags {
				tagMap[strings.ToLower(t.Key)] = true
			}
			for _, tk := range transitiveKeys {
				if !tagMap[strings.ToLower(tk)] {
					return nil, stsErr("InvalidParameterValue",
						"The specified transitive tag key must be included in the requested tags.", 400)
				}
			}
		}
	}

	creds := generateCredentials(durationSecs)

	// build merged session for tag propagation
	merged := make(map[string]Tag)
	mergedTransitive := make([]string, 0)
	if hasExisting {
		for _, tk := range existing.TransitiveTags {
			merged[tk] = existing.Tags[tk]
			mergedTransitive = append(mergedTransitive, tk)
		}
	}
	for _, t := range tags {
		lk := strings.ToLower(t.Key)
		merged[lk] = t
	}
	for _, tk := range transitiveKeys {
		lk := strings.ToLower(tk)
		mergedTransitive = append(mergedTransitive, lk)
	}
	if len(merged) > 0 {
		_ = p.store.StoreSession(creds.AccessKeyId, SessionConfig{
			Tags:           merged,
			TransitiveTags: mergedTransitive,
			IAMContext:     map[string]any{},
		})
	}

	// extract role name from ARN for the assumed-role user
	roleName := roleArn
	if parts := strings.Split(roleArn, "/"); len(parts) > 1 {
		roleName = parts[len(parts)-1]
	}

	return provider.OK(map[string]any{
		"Credentials": credMap(creds),
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": "AROA" + randID(16) + ":" + sessionName,
			"Arn":           nr.ResourceID("sts-assumed-role", roleName+"/"+sessionName),
		},
		"PackedPolicySize": 0,
	}), nil
}

func (p *STSProvider) AssumeRoleWithWebIdentity(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	sessionName := strParam(nr.Params, "RoleSessionName")
	webToken := strParam(nr.Params, "WebIdentityToken")
	durationSecs := intParam(nr.Params, "DurationSeconds", 3600)

	if !roleARNRegex.MatchString(roleArn) {
		return nil, stsErr("ValidationError", fmt.Sprintf("%s is invalid", roleArn), 400)
	}
	if !sessionNameRegex.MatchString(sessionName) {
		return nil, stsErr("ValidationError", fmt.Sprintf(
			"1 validation error detected: Value '%s' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*",
			sessionName), 400)
	}

	subject, err := extractJWTSubject(webToken)
	if err != nil {
		return nil, err
	}
	creds := generateCredentials(durationSecs)

	roleName := roleArn
	if parts := strings.Split(roleArn, "/"); len(parts) > 1 {
		roleName = parts[len(parts)-1]
	}

	return provider.OK(map[string]any{
		"Credentials": credMap(creds),
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": "AROA" + randID(16) + ":" + sessionName,
			"Arn":           nr.ResourceID("sts-assumed-role", roleName+"/"+sessionName),
		},
		"SubjectFromWebIdentityToken": subject,
		"PackedPolicySize":            0,
	}), nil
}

func (p *STSProvider) AssumeRoleWithSAML(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	durationSecs := intParam(nr.Params, "DurationSeconds", 3600)

	if !roleARNRegex.MatchString(roleArn) {
		return nil, stsErr("ValidationError", fmt.Sprintf("%s is invalid", roleArn), 400)
	}

	creds := generateCredentials(durationSecs)

	roleName := roleArn
	if parts := strings.Split(roleArn, "/"); len(parts) > 1 {
		roleName = parts[len(parts)-1]
	}

	return provider.OK(map[string]any{
		"Credentials": credMap(creds),
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": "AROA" + randID(16) + ":saml-session",
			"Arn":           nr.ResourceID("sts-assumed-role", roleName+"/saml-session"),
		},
		"Subject":       "saml-subject",
		"SubjectType":   "persistent",
		"Issuer":        "saml-issuer",
		"Audience":      "https://signin.aws.amazon.com/saml",
		"NameQualifier": "",
		"PackedPolicySize": 0,
	}), nil
}

func (p *STSProvider) GetSessionToken(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	durationSecs := intParam(nr.Params, "DurationSeconds", 43200)
	if durationSecs < 900 || durationSecs > 129600 {
		return nil, stsErr("ValidationError",
			"1 validation error detected: Value at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900", 400)
	}
	creds := generateCredentials(durationSecs)
	return provider.OK(map[string]any{"Credentials": credMap(creds)}), nil
}

func (p *STSProvider) GetFederationToken(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" || !federationNameRegex.MatchString(name) {
		return nil, stsErr("ValidationError",
			fmt.Sprintf("1 validation error detected: Value '%s' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]+", name), 400)
	}
	durationSecs := intParam(nr.Params, "DurationSeconds", 43200)
	creds := generateCredentials(durationSecs)

	policySize := 0
	if policy := strParam(nr.Params, "Policy"); policy != "" {
		policySize = (len(policy) * 100) / 2048
		if policySize < 1 {
			policySize = 1
		}
		if policySize > 100 {
			policySize = 100
		}
	}

	return provider.OK(map[string]any{
		"Credentials": credMap(creds),
		"FederatedUser": map[string]any{
			"FederatedUserId": fmt.Sprintf("%s:%s", nr.AccountID, name),
			"Arn":             nr.ResourceID("sts-federated-user", name),
		},
		"PackedPolicySize": policySize,
	}), nil
}

func (p *STSProvider) GetAccessKeyInfo(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"Account": nr.AccountID}), nil
}

func (p *STSProvider) DecodeAuthorizationMessage(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	encoded := strParam(nr.Params, "EncodedMessage")
	// In emulator context we don't encode authorization messages, return as-is
	return provider.OK(map[string]any{"DecodedMessage": encoded}), nil
}

// ─── credential generation ────────────────────────────────────────────────────

type credentials struct {
	AccessKeyId     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

func generateCredentials(durationSeconds int) credentials {
	return credentials{
		AccessKeyId:     "ASIA" + randAlphaNum(16),
		SecretAccessKey: randBase64(30),
		SessionToken:    randBase64(268), // 356-char base64 encoded
		Expiration:      time.Now().UTC().Add(time.Duration(durationSeconds) * time.Second),
	}
}

func credMap(c credentials) map[string]any {
	return map[string]any{
		"AccessKeyId":     c.AccessKeyId,
		"SecretAccessKey": c.SecretAccessKey,
		"SessionToken":    c.SessionToken,
		"Expiration":      c.Expiration.UTC().Format(time.RFC3339),
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

const alphaNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func randAlphaNum(n int) string {
	b := make([]byte, n)
	for i := range b {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphaNum))))
		b[i] = alphaNum[idx.Int64()]
	}
	return strings.ToUpper(string(b))
}

func randBase64(byteLen int) string {
	b := make([]byte, byteLen)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func randID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func intParam(params map[string]any, key string, def int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// parseTags reads Tags.member.N.Key / Tags.member.N.Value from Query params,
// or a JSON array [{Key,Value},...] for JSON-protocol callers.
func parseTags(params map[string]any, prefix string) []Tag {
	// JSON array form: [{Key:..., Value:...}]
	if raw, ok := params[prefix]; ok {
		if arr, ok := raw.([]any); ok {
			var tags []Tag
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					k, _ := m["Key"].(string)
					v, _ := m["Value"].(string)
					if k != "" {
						tags = append(tags, Tag{Key: k, Value: v})
					}
				}
			}
			return tags
		}
	}
	// Query-protocol: Tags.member.1.Key, Tags.member.1.Value
	var tags []Tag
	for i := 1; ; i++ {
		k, ok1 := params[fmt.Sprintf("%s.member.%d.Key", prefix, i)].(string)
		v, _ := params[fmt.Sprintf("%s.member.%d.Value", prefix, i)].(string)
		if !ok1 || k == "" {
			break
		}
		tags = append(tags, Tag{Key: k, Value: v})
	}
	return tags
}

// parseStringList reads TransitiveTagKeys.member.N from Query params or a JSON []string.
func parseStringList(params map[string]any, prefix string) []string {
	if raw, ok := params[prefix]; ok {
		if arr, ok := raw.([]any); ok {
			var out []string
			for _, item := range arr {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	var out []string
	for i := 1; ; i++ {
		v, ok := params[fmt.Sprintf("%s.member.%d", prefix, i)].(string)
		if !ok || v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

// extractAccessKeyFromAuth pulls the AccessKeyId from an AWS Authorization header.
// Returns "" when not present or unparseable.
func extractAccessKeyFromAuth(auth string) string {
	// AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
	const marker = "Credential="
	idx := strings.Index(auth, marker)
	if idx < 0 {
		return ""
	}
	rest := auth[idx+len(marker):]
	if slash := strings.Index(rest, "/"); slash > 0 {
		return rest[:slash]
	}
	return ""
}

// extractJWTSubject decodes the JWT payload segment, validates exp/iat claims,
// and returns the `sub` claim. Returns an error if the token has expired.
//
// NOTE: Cryptographic signature verification (JWKS chain) is deferred — it
// would require external HTTP calls to the OIDC discovery endpoint and is out
// of scope for the emulator. The token structure and expiry are validated here.
func extractJWTSubject(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return "unknown", nil
	}
	// add padding
	payload := parts[1]
	for len(payload)%4 != 0 {
		payload += "="
	}
	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return "unknown", nil
		}
	}

	// Parse as a generic JSON map so we can inspect exp/iat/sub.
	var claims map[string]any
	if jsonErr := json.Unmarshal(decoded, &claims); jsonErr != nil {
		return "unknown", nil
	}

	// Validate expiry.
	if expRaw, ok := claims["exp"]; ok {
		var expUnix int64
		switch v := expRaw.(type) {
		case float64:
			expUnix = int64(v)
		case json.Number:
			expUnix, _ = v.Int64()
		}
		if expUnix > 0 && expUnix < time.Now().Unix() {
			return "", model.NewProviderError("ExpiredTokenException", "Token has expired", 400)
		}
	}

	// Extract sub claim.
	if sub, ok := claims["sub"].(string); ok && sub != "" {
		return sub, nil
	}
	return "unknown", nil
}

func stsErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}
