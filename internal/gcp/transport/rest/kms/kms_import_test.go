package kms

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
)

func TestRESTImportDecapsulate(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": kr, "cryptoKeyId": "sym", "body": map[string]any{}})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}

	// Import job.
	resp, err := p.ImportJobCreate(ctx, newNR(map[string]any{
		"name": kr, "importJobId": "job",
		"body": map[string]any{"importMethod": "RSA_OAEP_3072_SHA256_AES_256"},
	}))
	if err != nil {
		t.Fatalf("ImportJobCreate: %v", err)
	}
	pubMap, _ := resp.Data["publicKey"].(map[string]any)
	pemStr, _ := pubMap["pem"].(string)
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		t.Fatal("import job public key is not PEM")
	}
	pubAny, _ := x509.ParsePKIXPublicKey(block.Bytes)
	rpub := pubAny.(*rsa.PublicKey)

	material := []byte("0123456789abcdef0123456789abcdef")
	kwpKey := make([]byte, 32)
	rand.Read(kwpKey)
	enc, _ := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	wrappedKWP, _ := kmsstore.AESKeyWrapWithPadding(kwpKey, material)
	wrapped := append(enc, wrappedKWP...)

	if _, err := p.CryptoKeyVersionImport(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{
			"importJob":  kr + "/importJobs/job",
			"algorithm":  "GOOGLE_SYMMETRIC_ENCRYPTION",
			"wrappedKey": base64.StdEncoding.EncodeToString(wrapped),
		},
	})); err != nil {
		t.Fatalf("CryptoKeyVersionImport: %v", err)
	}

	// Encrypt/decrypt with the imported key (set it primary first).
	if _, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{"cryptoKeyVersionId": "2"},
	})); err != nil {
		t.Fatalf("UpdatePrimaryVersion: %v", err)
	}
	ctResp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{"plaintext": base64.StdEncoding.EncodeToString([]byte("hello"))},
	}))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	ctStr, _ := ctResp.Data["ciphertext"].(string)
	ptResp, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{"ciphertext": ctStr},
	}))
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	pt, _ := base64.StdEncoding.DecodeString(ptResp.Data["plaintext"].(string))
	if string(pt) != "hello" {
		t.Fatalf("decrypt = %q, want hello", pt)
	}

	// KEM decapsulate.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "kem",
		"body": map[string]any{
			"purpose":         "KEY_ENCAPSULATION",
			"versionTemplate": map[string]any{"algorithm": "ML_KEM_768"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(kem): %v", err)
	}
	kemVersion := kr + "/cryptoKeys/kem/cryptoKeyVersions/1"
	pkResp, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{"name": kemVersion + "/publicKey"}))
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	rawB64, _ := pkResp.Data["publicKey"].(string)
	raw, _ := base64.StdEncoding.DecodeString(rawB64)
	ss, kemCT, err := kmsstore.EncapsulateKEM("ML_KEM_768", raw)
	if err != nil {
		t.Fatalf("EncapsulateKEM: %v", err)
	}
	decResp, err := p.CryptoKeyVersionDecapsulate(ctx, newNR(map[string]any{
		"name": kemVersion,
		"body": map[string]any{"ciphertext": base64.StdEncoding.EncodeToString(kemCT)},
	}))
	if err != nil {
		t.Fatalf("Decapsulate: %v", err)
	}
	ssGot, _ := base64.StdEncoding.DecodeString(decResp.Data["sharedSecret"].(string))
	if string(ssGot) != string(ss) {
		t.Fatal("decapsulated shared secret mismatch")
	}
}
