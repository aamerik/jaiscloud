package kms

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

func newCore() (*Service, kmsstore.Store) {
	keys := kmsstore.NewMemoryStore()
	return NewService(keys, store.NewMemoryResourceStore(), "proj"), keys
}

func TestCoreImportCryptoKeyVersionRSA(t *testing.T) {
	ctx := context.Background()
	core, keys := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	const method = "RSA_OAEP_3072_SHA256_AES_256"
	job, err := core.CreateImportJob(ctx, "proj", "global", "kr", "job", ImportJobInput{ImportMethod: method})
	if err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}
	block, _ := pem.Decode([]byte(job.PublicKeyPEM))
	if block == nil {
		t.Fatal("import job public key is not PEM")
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	rpub := pubAny.(*rsa.PublicKey)

	material := []byte("0123456789abcdef0123456789abcdef")
	kwpKey := make([]byte, 32)
	rand.Read(kwpKey)
	enc, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	wrappedKWP, err := kmsstore.AESKeyWrapWithPadding(kwpKey, material)
	if err != nil {
		t.Fatal(err)
	}
	v, err := core.ImportCryptoKeyVersion(ctx, "proj", "global", "kr", "sym", ImportVersionRequest{
		ImportJob:  job.Name,
		Algorithm:  "GOOGLE_SYMMETRIC_ENCRYPTION",
		WrappedKey: append(enc, wrappedKWP...),
	})
	if err != nil {
		t.Fatalf("ImportCryptoKeyVersion: %v", err)
	}
	if v.State != "ENABLED" || v.ImportTime.IsZero() {
		t.Fatalf("imported version state=%q importTime zero=%v", v.State, v.ImportTime.IsZero())
	}
	got, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "sym", v.Version)
	if err != nil {
		t.Fatalf("KeyMaterial: %v", err)
	}
	if !bytes.Equal(got, material) {
		t.Fatalf("imported material = %x, want %x", got, material)
	}
}

func TestCoreImportCryptoKeyVersionHPKE(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{}); err != nil {
		t.Fatal(err)
	}
	const method = "HPKE_KEM_ML_KEM_768_HKDF_SHA256_AES_256_GCM"
	job, err := core.CreateImportJob(ctx, "proj", "global", "kr", "job", ImportJobInput{ImportMethod: method})
	if err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}
	if len(job.PublicKeyRaw) == 0 {
		t.Fatal("HPKE import job should expose a raw public key")
	}
	material := []byte("post-quantum-imported-material!")
	sealed, err := kmsstore.HPKESeal(method, job.PublicKeyRaw, material)
	if err != nil {
		t.Fatalf("HPKESeal: %v", err)
	}
	if _, err := core.ImportCryptoKeyVersion(ctx, "proj", "global", "kr", "sym", ImportVersionRequest{
		ImportJob: job.Name, Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION", WrappedKey: sealed,
	}); err != nil {
		t.Fatalf("ImportCryptoKeyVersion(HPKE): %v", err)
	}
}

func TestCoreDecapsulate(t *testing.T) {
	ctx := context.Background()
	core, keys := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "kem", CryptoKeyInput{
		Purpose: "KEY_ENCAPSULATION", Algorithm: "ML_KEM_768",
	}); err != nil {
		t.Fatal(err)
	}
	pub, err := keys.PublicKey(ctx, "proj", "global", "kr", "kem", "1")
	if err != nil {
		t.Fatalf("PublicKey: %v", err)
	}
	ss, ct, err := kmsstore.EncapsulateKEM("ML_KEM_768", pub)
	if err != nil {
		t.Fatalf("EncapsulateKEM: %v", err)
	}
	got, err := core.Decapsulate(ctx, "proj", "global", "kr", "kem", "1", ct)
	if err != nil {
		t.Fatalf("Decapsulate: %v", err)
	}
	if !bytes.Equal(got, ss) {
		t.Fatal("decapsulated shared secret mismatch")
	}
}

func TestCoreTrustedWrappingRoundTrip(t *testing.T) {
	ctx := context.Background()
	core, keys := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "wrap", CryptoKeyInput{
		Purpose: "AES_WRAPPING", Algorithm: "AES_256_KWP", ProtectionLevel: "HSM_SINGLE_TENANT",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{}); err != nil {
		t.Fatal(err)
	}
	wk, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "wrap", "1")
	if err != nil {
		t.Fatalf("wrapping key material: %v", err)
	}
	material := []byte("trusted-wrapped-key-material-1234")
	wrapped, err := kmsstore.WrapTrustedKey(wk, material)
	if err != nil {
		t.Fatal(err)
	}
	v, err := core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "sym", ImportVersionRequest{
		ImportingKey: VersionName("proj", "global", "kr", "wrap", "1"),
		Algorithm:    "GOOGLE_SYMMETRIC_ENCRYPTION",
		WrappedKey:   wrapped,
	})
	if err != nil {
		t.Fatalf("ImportTrusted: %v", err)
	}
	if !v.TrustedWrappingEnabled {
		t.Fatal("trusted import should set trustedWrappingEnabled")
	}
	exported, err := core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "sym", v.Version,
		VersionName("proj", "global", "kr", "wrap", "1"))
	if err != nil {
		t.Fatalf("ExportTrusted: %v", err)
	}
	back, err := kmsstore.UnwrapTrustedKey(wk, exported)
	if err != nil {
		t.Fatalf("UnwrapTrustedKey: %v", err)
	}
	if !bytes.Equal(back, material) {
		t.Fatal("trusted wrapping round-trip mismatch")
	}
}
