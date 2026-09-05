package iam

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// rtServiceAccountKey is the generic ResourceStore type for service-account
// keys (RSA key pairs backing signBlob/signJwt).
const rtServiceAccountKey = "gcp_service_account_key"

// serviceAccountKeyMeta is the stored representation of a service-account key.
type serviceAccountKeyMeta struct {
	Email      string `json:"email"`
	KeyID      string `json:"keyId"`
	Algorithm  string `json:"algorithm"`
	PrivateDER string `json:"privateDer"` // base64 PKCS8 private key DER
	ValidAfter string `json:"validAfterTime"`
}

// privDER decodes the stored private key DER.
func (m *serviceAccountKeyMeta) privDER() ([]byte, error) {
	return base64.StdEncoding.DecodeString(m.PrivateDER)
}

// publicKeyData derives the base64 PKIX public-key PEM from the stored PKCS8
// private key DER. validBeforeTime stays omitted for non-expiring keys.
func (m *serviceAccountKeyMeta) publicKeyData() (string, bool) {
	privDER, err := m.privDER()
	if err != nil {
		return "", false
	}
	priv, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return "", false
	}
	signer, ok := priv.(crypto.Signer)
	if !ok {
		return "", false
	}
	pubDER, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return "", false
	}
	pemStr, err := kmsstore.PublicKeyPEM(pubDER)
	if err != nil {
		return "", false
	}
	return base64.StdEncoding.EncodeToString([]byte(pemStr)), true
}

func generateKeyID() string {
	b := make([]byte, 16)
	_, _ = io.ReadFull(rand.Reader, b)
	return hex.EncodeToString(b)
}

// parseKeyParent extracts the SA email from a ".../serviceAccounts/{email}/keys"
// collection name.
func parseKeyParent(name string) string {
	name = strings.TrimPrefix(name, "serviceAccounts/")
	return strings.TrimSuffix(name, "/keys")
}

// parseKeyName extracts (email, keyID) from ".../serviceAccounts/{email}/keys/{keyId}".
func parseKeyName(name string) (email, keyID string) {
	name = strings.TrimPrefix(name, "serviceAccounts/")
	if i := strings.LastIndex(name, "/keys/"); i >= 0 {
		return name[:i], name[i+len("/keys/"):]
	}
	return "", ""
}

// keyToMap renders a service-account key as its GCP response object (no
// privateKeyData — that is returned only on create).
func keyToMap(nr *model.NormalizedRequest, m serviceAccountKeyMeta) map[string]any {
	out := map[string]any{
		"name":           nr.ResourceID("service-account", m.Email) + "/keys/" + m.KeyID,
		"privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE",
		"keyAlgorithm":   m.Algorithm,
		"validAfterTime": m.ValidAfter,
	}
	if pub, ok := m.publicKeyData(); ok {
		out["publicKeyData"] = pub
	}
	return out
}

// createKey generates an RSA-2048 key pair and persists it.
func (p *Provider) createKey(ctx context.Context, account, email string) (*serviceAccountKeyMeta, error) {
	priv, _, err := kmsstore.GenerateRSAKeyPair(2048)
	if err != nil {
		return nil, err
	}
	m := serviceAccountKeyMeta{
		Email:      email,
		KeyID:      generateKeyID(),
		Algorithm:  "KEY_ALG_RSA_2048",
		PrivateDER: base64.StdEncoding.EncodeToString(priv),
		ValidAfter: clock.Now().UTC().Format(time.RFC3339Nano),
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, account, store.GlobalRegion, store.ResourceEntry{Type: rtServiceAccountKey, ID: m.KeyID, Data: data}); err != nil {
		return nil, err
	}
	return &m, nil
}

// ensureKey returns an existing key for the SA, or creates a system-managed one.
func (p *Provider) ensureKey(ctx context.Context, account, email string) (*serviceAccountKeyMeta, error) {
	entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var m serviceAccountKeyMeta
		if json.Unmarshal(e.Data, &m) == nil && m.Email == email {
			return &m, nil
		}
	}
	return p.createKey(ctx, account, email)
}

// loadKey loads a key by ID, verifying it belongs to the given SA.
func (p *Provider) loadKey(ctx context.Context, account, email, keyID string) (*serviceAccountKeyMeta, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtServiceAccountKey, keyID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "key not found", 404)
		}
		return nil, err
	}
	var m serviceAccountKeyMeta
	if json.Unmarshal(e.Data, &m) != nil || m.Email != email {
		return nil, model.NewProviderError("NotFound", "key not found", 404)
	}
	return &m, nil
}

func (p *Provider) ServiceAccountKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := parseKeyParent(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	m, err := p.createKey(ctx, nr.AccountID, email)
	if err != nil {
		return nil, err
	}
	privDER, _ := m.privDER()
	privPEM, _ := kmsstore.PrivateKeyPEM(privDER)
	creds, _ := json.Marshal(map[string]any{
		"type":            "service_account",
		"project_id":      nr.AccountID,
		"private_key_id":  m.KeyID,
		"private_key":     privPEM,
		"client_email":    email,
		"client_id":       "0",
		"auth_uri":        "https://accounts.google.com/o/oauth2/auth",
		"token_uri":       "https://oauth2.googleapis.com/token",
		"universe_domain": "googleapis.com",
	})
	resp := keyToMap(nr, *m)
	resp["privateKeyData"] = base64.StdEncoding.EncodeToString(creds)
	return provider.OK(resp), nil
}

func (p *Provider) ServiceAccountKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := parseKeyParent(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0)
	for _, e := range entries {
		var m serviceAccountKeyMeta
		if json.Unmarshal(e.Data, &m) == nil && m.Email == email {
			items = append(items, keyToMap(nr, m))
		}
	}
	return provider.OK(map[string]any{"keys": items}), nil
}

func (p *Provider) ServiceAccountKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	m, err := p.loadKey(ctx, nr.AccountID, email, keyID)
	if err != nil {
		return nil, err
	}
	return provider.OK(keyToMap(nr, *m)), nil
}

func (p *Provider) ServiceAccountKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	if _, err := p.loadKey(ctx, nr.AccountID, email, keyID); err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccountKey, keyID); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) ServiceAccountSignBlob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	m, err := p.ensureKey(ctx, nr.AccountID, email)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	// GCP's signBlob carries the payload under bytesToSign; accept payload too
	// for backward compatibility with earlier emulator callers.
	blob, _ := body["bytesToSign"].(string)
	if blob == "" {
		blob, _ = body["payload"].(string)
	}
	raw, err := base64.StdEncoding.DecodeString(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "payload must be base64", 400)
	}
	priv, _ := m.privDER()
	digest := sha256.Sum256(raw)
	sig, err := kmsstore.RSASign(priv, digest[:], "RSA_SIGN_PKCS1_2048_SHA256")
	if err != nil {
		return nil, model.NewProviderError("Internal", "sign failed", 500)
	}
	return provider.OK(map[string]any{
		"keyId":     m.KeyID,
		"signature": base64.StdEncoding.EncodeToString(sig),
	}), nil
}

func (p *Provider) ServiceAccountSignJwt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	m, err := p.ensureKey(ctx, nr.AccountID, email)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	payloadStr, _ := body["payload"].(string)
	priv, _ := m.privDER()

	header := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"alg":"RS256","typ":"JWT","kid":"%s"}`, m.KeyID)))
	payload := base64.RawURLEncoding.EncodeToString([]byte(payloadStr))
	signingInput := header + "." + payload
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := kmsstore.RSASign(priv, digest[:], "RSA_SIGN_PKCS1_2048_SHA256")
	if err != nil {
		return nil, model.NewProviderError("Internal", "sign failed", 500)
	}
	return provider.OK(map[string]any{
		"keyId":     m.KeyID,
		"signedJwt": signingInput + "." + base64.RawURLEncoding.EncodeToString(sig),
	}), nil
}
