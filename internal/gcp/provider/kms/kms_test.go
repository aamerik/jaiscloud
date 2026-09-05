package kms

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func TestKMSEncryptDecryptRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(kmsstore.NewMemoryStore())

	// Create keyring.
	nr := newNR(map[string]any{"location": "global", "keyRingId": "my-kr"})
	if _, err := p.KeyRingCreate(ctx, nr); err != nil {
		t.Fatalf("keyring create: %v", err)
	}

	// Create crypto key.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/my-kr", "cryptoKeyId": "my-key", "body": map[string]any{"purpose": "ENCRYPT_DECRYPT"}})
	if _, err := p.CryptoKeyCreate(ctx, nr); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}

	// Encrypt.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/my-kr/cryptoKeys/my-key", "body": map[string]any{"plaintext": "aGVsbG8="}})
	resp, err := p.CryptoKeyEncrypt(ctx, nr)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ciphertext, _ := resp.Data["ciphertext"].(string)
	if ciphertext == "" || ciphertext == "aGVsbG8=" {
		t.Fatalf("expected distinct ciphertext, got %q", ciphertext)
	}
	if name, _ := resp.Data["name"].(string); name != "projects/proj/locations/global/keyRings/my-kr/cryptoKeys/my-key/cryptoKeyVersions/1" {
		t.Errorf("encrypt name = %q, want full cryptoKeyVersion resource name", name)
	}

	// Decrypt.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/my-kr/cryptoKeys/my-key", "body": map[string]any{"ciphertext": ciphertext}})
	resp, err = p.CryptoKeyDecrypt(ctx, nr)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if plaintext, _ := resp.Data["plaintext"].(string); plaintext != "aGVsbG8=" {
		t.Errorf("expected plaintext aGVsbG8=, got %q", plaintext)
	}
}

// TestCryptoKeyPrimaryAlgorithm verifies the primary-version algorithm is
// surfaced in the CryptoKey response (the version-template algorithm, which is
// the primary version's algorithm).
func TestCryptoKeyPrimaryAlgorithm(t *testing.T) {
	ctx := context.Background()
	p := New(kmsstore.NewMemoryStore())

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}

	// MAC defaults to HMAC_SHA256 (a non-default algorithm).
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "mac-key",
		"body":        map[string]any{"purpose": "MAC"},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}

	resp, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/mac-key"}))
	if err != nil {
		t.Fatalf("cryptokey get: %v", err)
	}
	primary, _ := resp.Data["primary"].(map[string]any)
	if alg, _ := primary["algorithm"].(string); alg != "HMAC_SHA256" {
		t.Errorf("primary.algorithm = %q, want HMAC_SHA256", alg)
	}
	if state, _ := primary["state"].(string); state != "ENABLED" {
		t.Errorf("primary.state = %q, want ENABLED", state)
	}
	vt, _ := resp.Data["versionTemplate"].(map[string]any)
	if alg, _ := vt["algorithm"].(string); alg != "HMAC_SHA256" {
		t.Errorf("versionTemplate.algorithm = %q, want HMAC_SHA256", alg)
	}
	if pl, _ := vt["protectionLevel"].(string); pl != "SOFTWARE" {
		t.Errorf("versionTemplate.protectionLevel = %q, want SOFTWARE", pl)
	}
}
