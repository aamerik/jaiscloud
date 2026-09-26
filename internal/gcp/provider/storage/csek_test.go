package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// csekMaterial returns the base64 key and base64 SHA-256 for a 32-byte AES-256
// key, matching the x-goog-encryption-key / -key-sha256 header values.
func csekMaterial(key []byte) (keyB64, shaB64 string) {
	sum := sha256.Sum256(key)
	return base64.StdEncoding.EncodeToString(key), base64.StdEncoding.EncodeToString(sum[:])
}

// newCSEKBucket creates a bucket and an object encrypted with the given CSEK
// material, returning the provider and the object's key/SHA headers.
func newCSEKBucket(t *testing.T, p *Provider, keyB64, shaB64 string) {
	t.Helper()
	ctx := context.Background()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.MediaKey] = []byte("classified")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert csek object: %v", err)
	}
}

func TestGCSCSEKMetadataAndMedia(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64, shaB64 := csekMaterial(key)
	newCSEKBucket(t, p, keyB64, shaB64)

	// Metadata surfaces the CSEK descriptor (JSON API customerEncryption).
	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	ce, _ := get.Data["customerEncryption"].(map[string]any)
	if ce == nil {
		t.Fatalf("expected customerEncryption on CSEK metadata, got %#v", get.Data)
	}
	if alg, _ := ce["encryptionAlgorithm"].(string); alg != "AES256" {
		t.Errorf("expected AES256, got %q", alg)
	}
	if sha, _ := ce["keySha256"].(string); sha != shaB64 {
		t.Errorf("expected keySha256 %q, got %q", shaB64, sha)
	}

	// Read without the key → 400 (real GCS: resourceIsEncryptedWithCustomerEncryptionKey).
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Fatal("expected error reading CSEK object without key")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}

	// Read with the key → original bytes, plus the XML CSEK response headers.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media with key: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
	if hdr, _ := media.Data[wire.HeadersKey].(map[string]string); hdr != nil {
		if hdr["x-goog-encryption-algorithm"] != "AES256" {
			t.Errorf("expected x-goog-encryption-algorithm AES256, got %q", hdr["x-goog-encryption-algorithm"])
		}
		if hdr["x-goog-encryption-key-sha256"] != shaB64 {
			t.Errorf("expected x-goog-encryption-key-sha256 %q, got %q", shaB64, hdr["x-goog-encryption-key-sha256"])
		}
	} else {
		t.Error("expected CSEK response headers on media download")
	}
}

func TestGCSCSEKReadWrongSHARejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	// Correct key but a sha256 that does not belong to it → 400.
	otherKey := []byte("fedcba9876543210fedcba9876543210")
	_, otherSHA := csekMaterial(otherKey)

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = otherSHA
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Fatal("expected error for mismatched caller-supplied sha256")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

func TestGCSCopySourceInvalidAlgorithmRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "copy.txt"
	nr.Params[wire.CopySourceCSEKAlgorithm] = "AES128"
	nr.Params[wire.CopySourceCSEKKey] = keyB64
	nr.Params[wire.CopySourceCSEKKeySHA256] = shaB64
	if _, err := p.ObjectsCopy(ctx, nr); err == nil {
		t.Fatal("expected error for non-AES256 copy-source algorithm")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

func TestGCSCSEKInvalidAlgorithmRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "x.txt"
	nr.Params[wire.MediaKey] = []byte("data")
	nr.Params[wire.CSEKAlgorithm] = "AES128"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	if _, err := p.ObjectsInsert(ctx, nr); err == nil {
		t.Fatal("expected error for non-AES256 algorithm")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

func TestGCSCSEKMissingSHARejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, _ := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "x.txt"
	nr.Params[wire.MediaKey] = []byte("data")
	nr.Params[wire.CSEKKey] = keyB64
	if _, err := p.ObjectsInsert(ctx, nr); err == nil {
		t.Fatal("expected error for CSEK key without sha256")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

// TestGCSCopyCSEKSourceRequiresSourceKey verifies a copy of a CSEK source
// without the copy-source key fails with 400 — the source cannot be decrypted.
func TestGCSCopyCSEKSourceRequiresSourceKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "plain.txt"
	if _, err := p.ObjectsCopy(ctx, nr); err == nil {
		t.Fatal("expected error copying CSEK source without the source key")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

// TestGCSCopyDropsSourceCSEKWhenNoDestinationKey verifies a copy of a CSEK
// source (with the copy-source key) re-encrypts with server-DEK and clears the
// source's customerEncryption metadata, so the destination is readable without
// a key.
func TestGCSCopyDropsSourceCSEKWhenNoDestinationKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "plain.txt"
	nr.Params[wire.CopySourceCSEKKey] = keyB64
	nr.Params[wire.CopySourceCSEKKeySHA256] = shaB64
	if _, err := p.ObjectsCopy(ctx, nr); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// Destination metadata must not claim CSEK.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "plain.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get destination: %v", err)
	}
	if ce, _ := get.Data["customerEncryption"].(map[string]any); ce != nil {
		t.Fatalf("destination must not inherit source customerEncryption: %#v", ce)
	}

	// Destination is readable without any key.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "plain.txt"
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get destination media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
}

// TestGCSCopyCSEKSourceWithKeys verifies a copy of a CSEK source decrypts the
// source with the copy-source key and re-encrypts the destination under the
// destination key.
func TestGCSCopyCSEKSourceWithKeys(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	srcKey := []byte("0123456789abcdef0123456789abcdef")
	srcB64, srcSHA := csekMaterial(srcKey)
	newCSEKBucket(t, p, srcB64, srcSHA)

	dstKey := []byte("fedcba9876543210fedcba9876543210")
	dstB64, dstSHA := csekMaterial(dstKey)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "copy.txt"
	nr.Params[wire.CopySourceCSEKKey] = srcB64
	nr.Params[wire.CopySourceCSEKKeySHA256] = srcSHA
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = dstB64
	nr.Params[wire.CSEKKeySHA256] = dstSHA
	if _, err := p.ObjectsCopy(ctx, nr); err != nil {
		t.Fatalf("copy: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "copy.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get destination: %v", err)
	}
	ce, _ := get.Data["customerEncryption"].(map[string]any)
	if ce == nil || ce["keySha256"] != dstSHA {
		t.Fatalf("expected destination keySha256 %q, got %#v", dstSHA, ce)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "copy.txt"
	nr.Params[wire.CSEKKey] = dstB64
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get destination media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
}
