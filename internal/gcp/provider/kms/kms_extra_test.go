package kms

import (
	"context"
	"encoding/base64"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestKMSNegativesAndPagination(t *testing.T) {
	ctx := context.Background()
	p := New(kmsstore.NewMemoryStore())

	for _, kr := range []string{"kr-a", "kr-b", "kr-c"} {
		nr := newNR(map[string]any{"location": "global", "keyRingId": kr})
		if _, err := p.KeyRingCreate(ctx, nr); err != nil {
			t.Fatalf("create keyring %s: %v", kr, err)
		}
	}

	// KeyRing pagination.
	nr := newNR(map[string]any{"location": "global", "pageSize": "2"})
	resp, err := p.KeyRingList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	rings, _ := resp.Data["keyRings"].([]any)
	if len(rings) != 2 {
		t.Fatalf("page 1 expected 2 keyrings, got %d", len(rings))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}

	// 409 duplicate keyring.
	nr = newNR(map[string]any{"location": "global", "keyRingId": "kr-a"})
	if _, err := p.KeyRingCreate(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate keyring, got %v", err)
	}

	// Encrypt/decrypt response fields.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a", "cryptoKeyId": "key1"})
	if _, err := p.CryptoKeyCreate(ctx, nr); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/key1", "body": map[string]any{"plaintext": "aGVsbG8="}})
	enc, err := p.CryptoKeyEncrypt(ctx, nr)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	for _, field := range []string{"ciphertextCrc32c", "protectionLevel", "verifiedPlaintextCrc32c"} {
		if _, ok := enc.Data[field]; !ok {
			t.Errorf("encrypt response missing %s", field)
		}
	}
	if enc.Data["protectionLevel"] != "SOFTWARE" {
		t.Errorf("expected protectionLevel SOFTWARE, got %v", enc.Data["protectionLevel"])
	}

	ciphertext, _ := enc.Data["ciphertext"].(string)
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/key1", "body": map[string]any{"ciphertext": ciphertext}})
	dec, err := p.CryptoKeyDecrypt(ctx, nr)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	for _, field := range []string{"plaintextCrc32c", "protectionLevel", "usedPrimary"} {
		if _, ok := dec.Data[field]; !ok {
			t.Errorf("decrypt response missing %s", field)
		}
	}
	if dec.Data["plaintext"] != "aGVsbG8=" {
		t.Errorf("expected plaintext aGVsbG8=, got %v", dec.Data["plaintext"])
	}

	// 404 missing crypto key.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/nope"})
	if _, err := p.CryptoKeyGet(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing crypto key, got %v", err)
	}
}

// TestKMSCryptoKeyNotFound verifies encrypt/decrypt reject a non-existent key
// with 404 instead of silently operating on it.
func TestKMSCryptoKeyNotFound(t *testing.T) {
	ctx := context.Background()
	p := New(kmsstore.NewMemoryStore())

	nr := newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/missing", "body": map[string]any{"plaintext": "aGk="}})
	if _, err := p.CryptoKeyEncrypt(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on encrypt missing key, got %v", err)
	}
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/missing", "body": map[string]any{"ciphertext": "amFpc2Nsb3VkOmhp"}})
	if _, err := p.CryptoKeyDecrypt(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on decrypt missing key, got %v", err)
	}
}

// TestKMSEncryptDecryptRealCrypto verifies AES-GCM semantics: nondeterministic
// ciphertext and decryption failure on a wrong AAD.
func TestKMSEncryptDecryptRealCrypto(t *testing.T) {
	ctx := context.Background()
	p := New(kmsstore.NewMemoryStore())

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k"})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}

	enc := func(aad string) string {
		body := map[string]any{"plaintext": "aGVsbG8="}
		if aad != "" {
			body["additionalAuthenticatedData"] = aad
		}
		resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": body}))
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return resp.Data["ciphertext"].(string)
	}

	c1 := enc("")
	c2 := enc("")
	if c1 == c2 {
		t.Error("expected nondeterministic ciphertext")
	}

	// Decrypt with wrong AAD must fail.
	aad := base64.StdEncoding.EncodeToString([]byte("ctx"))
	_ = enc(aad) // encrypt with AAD
	resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"plaintext": "aGVsbG8=", "additionalAuthenticatedData": aad}}))
	if err != nil {
		t.Fatalf("encrypt with aad: %v", err)
	}
	if _, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"ciphertext": resp.Data["ciphertext"]}})); err == nil {
		t.Error("expected decrypt to fail without AAD")
	}
}

// TestKMSRotation verifies that after rotating the primary version, ciphertext
// encrypted under the old version still decrypts (version is embedded in the
// ciphertext blob).
func TestKMSRotation(t *testing.T) {
	ctx := context.Background()
	p := New(kmsstore.NewMemoryStore())

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k"})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}

	enc := func() string {
		resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"plaintext": "aGVsbG8="}}))
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return resp.Data["ciphertext"].(string)
	}
	dec := func(ct string) {
		resp, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"ciphertext": ct}}))
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		if resp.Data["plaintext"] != "aGVsbG8=" {
			t.Errorf("unexpected plaintext: %v", resp.Data["plaintext"])
		}
	}

	old := enc()

	// Create version 2 and rotate primary to it.
	if _, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions"})); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if _, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"cryptoKeyVersionId": "2"}})); err != nil {
		t.Fatalf("update primary: %v", err)
	}

	// Both old (v1) and new (v2) ciphertext decrypt correctly.
	dec(old)
	dec(enc())

	// Version list shows two versions.
	lr, err := p.CryptoKeyVersionList(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions"}))
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if vs, _ := lr.Data["cryptoKeyVersions"].([]any); len(vs) != 2 {
		t.Fatalf("expected 2 versions, got %v", lr.Data["cryptoKeyVersions"])
	}

	// Get + destroy version 1.
	if _, err := p.CryptoKeyVersionGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1"})); err != nil {
		t.Fatalf("get version: %v", err)
	}
	dr, err := p.CryptoKeyVersionDestroy(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1"}))
	if err != nil {
		t.Fatalf("destroy version: %v", err)
	}
	if dr.Data["state"] != "DESTROYED" {
		t.Errorf("expected DESTROYED, got %v", dr.Data["state"])
	}
}
