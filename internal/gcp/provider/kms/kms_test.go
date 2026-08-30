package kms

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func TestKMSEncryptDecryptRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

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
