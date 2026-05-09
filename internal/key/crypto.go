// Package key provides KMS key management: envelope encryption, key storage,
// and the KeyProvider that handles KMS API operations.
package key

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/asn1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
)

const (
	versionPlaintext byte = 0x00
	versionAESGCM    byte = 0x01

	ivLen  = 12
	tagLen = 16
)

// Generate32 returns a cryptographically random 32-byte key.
func Generate32() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	return key, nil
}

// ParseHexKey decodes a 32-byte hex-encoded KEK from a string.
// Returns an error if the string is empty, not valid hex, or not 32 bytes.
func ParseHexKey(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("kms: empty master key")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("kms: invalid master key hex: %w", err)
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("kms: master key must be 32 bytes, got %d", len(b))
	}
	return b, nil
}

// WrapDEK encrypts a 32-byte DEK with the KEK using AES-256-GCM.
// Produces: VERSION(1) || IV(12) || CIPHERTEXT+TAG.
func WrapDEK(kek, dek []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("wrap dek: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("wrap dek: new gcm: %w", err)
	}
	iv := make([]byte, ivLen)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("wrap dek: random iv: %w", err)
	}
	ct := gcm.Seal(nil, iv, dek, nil)
	blob := make([]byte, 1+ivLen+len(ct))
	blob[0] = versionAESGCM
	copy(blob[1:], iv)
	copy(blob[1+ivLen:], ct)
	return blob, nil
}

// UnwrapDEK decrypts a wrapped DEK blob produced by WrapDEK.
func UnwrapDEK(kek, blob []byte) ([]byte, error) {
	if len(blob) == 0 {
		return nil, errors.New("unwrap dek: empty blob")
	}
	if blob[0] == versionPlaintext {
		// Stored plaintext — return the raw bytes (dev mode, no KEK).
		return blob[1:], nil
	}
	if blob[0] != versionAESGCM {
		return nil, fmt.Errorf("unwrap dek: unknown version 0x%02x", blob[0])
	}
	if len(blob) < 1+ivLen+tagLen {
		return nil, errors.New("unwrap dek: blob too short")
	}
	iv := blob[1 : 1+ivLen]
	ct := blob[1+ivLen:]
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("unwrap dek: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("unwrap dek: new gcm: %w", err)
	}
	pt, err := gcm.Open(nil, iv, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("unwrap dek: decrypt: %w", err)
	}
	return pt, nil
}

// plaintextBlob wraps a plaintext DEK as a version-0x00 blob for storage.
func plaintextBlob(dek []byte) []byte {
	blob := make([]byte, 1+len(dek))
	blob[0] = versionPlaintext
	copy(blob[1:], dek)
	return blob
}

// encryptData encrypts plaintext with a key using AES-256-GCM and additional data.
// Returns: IV(12) || CIPHERTEXT+TAG.
func encryptData(key, pt, additionalData []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encrypt: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encrypt: new gcm: %w", err)
	}
	iv := make([]byte, ivLen)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("encrypt: random iv: %w", err)
	}
	ct := gcm.Seal(nil, iv, pt, additionalData)
	result := make([]byte, ivLen+len(ct))
	copy(result, iv)
	copy(result[ivLen:], ct)
	return result, nil
}

// ─── Asymmetric key generation ────────────────────────────────────────────────

// generateAsymmetricKey generates an RSA or ECC key pair and returns
// (DER PKCS8 private key, DER SubjectPublicKeyInfo public key).
func generateAsymmetricKey(keySpec string) (privDER, pubDER []byte, err error) {
	switch keySpec {
	case "RSA_2048":
		return genRSA(2048)
	case "RSA_3072":
		return genRSA(3072)
	case "RSA_4096":
		return genRSA(4096)
	case "ECC_NIST_P256":
		return genECC(elliptic.P256())
	case "ECC_NIST_P384":
		return genECC(elliptic.P384())
	case "ECC_NIST_P521":
		return genECC(elliptic.P521())
	case "ECC_SECG_P256K1":
		return nil, nil, errors.New("ECC_SECG_P256K1 is not supported in this build")
	default:
		return nil, nil, errors.New("unsupported asymmetric key spec: " + keySpec)
	}
}

func genRSA(bits int) ([]byte, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	return privDER, pubDER, nil
}

func genECC(curve elliptic.Curve) ([]byte, []byte, error) {
	priv, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	return privDER, pubDER, nil
}

// ─── Sign / Verify ────────────────────────────────────────────────────────────

// signData signs msg using the DER-encoded PKCS8 private key and the named algorithm.
// When messageType == "DIGEST" the caller passes a pre-hashed digest (not re-hashed).
func signData(privDER []byte, msg []byte, sigAlgo string, messageType string) ([]byte, error) {
	key, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return nil, err
	}
	hashAlgo, err := resolveHash(sigAlgo)
	if err != nil {
		return nil, err
	}
	digest, err := digestMsg(msg, hashAlgo, messageType)
	if err != nil {
		return nil, err
	}
	switch k := key.(type) {
	case *rsa.PrivateKey:
		if strings.Contains(sigAlgo, "PSS") {
			return rsa.SignPSS(rand.Reader, k, hashAlgo, digest, &rsa.PSSOptions{
				SaltLength: rsa.PSSSaltLengthEqualsHash,
			})
		}
		return rsa.SignPKCS1v15(rand.Reader, k, hashAlgo, digest)
	case *ecdsa.PrivateKey:
		r, s, err := ecdsa.Sign(rand.Reader, k, digest)
		if err != nil {
			return nil, err
		}
		return asn1.Marshal(struct{ R, S *big.Int }{r, s})
	}
	return nil, errors.New("unsupported key type for signing")
}

// verifySignature verifies sig against msg using the DER SubjectPublicKeyInfo public key.
func verifySignature(pubDER []byte, msg []byte, sig []byte, sigAlgo string, messageType string) error {
	pub, err := x509.ParsePKIXPublicKey(pubDER)
	if err != nil {
		return err
	}
	hashAlgo, err := resolveHash(sigAlgo)
	if err != nil {
		return err
	}
	digest, err := digestMsg(msg, hashAlgo, messageType)
	if err != nil {
		return err
	}
	switch k := pub.(type) {
	case *rsa.PublicKey:
		if strings.Contains(sigAlgo, "PSS") {
			return rsa.VerifyPSS(k, hashAlgo, digest, sig, &rsa.PSSOptions{
				SaltLength: rsa.PSSSaltLengthEqualsHash,
			})
		}
		return rsa.VerifyPKCS1v15(k, hashAlgo, digest, sig)
	case *ecdsa.PublicKey:
		var ecSig struct{ R, S *big.Int }
		if _, err := asn1.Unmarshal(sig, &ecSig); err != nil {
			return errors.New("invalid ECDSA signature encoding")
		}
		if !ecdsa.Verify(k, digest, ecSig.R, ecSig.S) {
			return errors.New("signature verification failed")
		}
		return nil
	}
	return errors.New("unsupported key type for verification")
}

func digestMsg(msg []byte, h crypto.Hash, messageType string) ([]byte, error) {
	if strings.ToUpper(messageType) == "DIGEST" {
		if len(msg) != h.Size() {
			return nil, fmt.Errorf("digest length %d does not match hash size %d", len(msg), h.Size())
		}
		return msg, nil
	}
	d := h.New()
	d.Write(msg)
	return d.Sum(nil), nil
}

func resolveHash(sigAlgo string) (crypto.Hash, error) {
	switch {
	case strings.HasSuffix(sigAlgo, "SHA_256"):
		return crypto.SHA256, nil
	case strings.HasSuffix(sigAlgo, "SHA_384"):
		return crypto.SHA384, nil
	case strings.HasSuffix(sigAlgo, "SHA_512"):
		return crypto.SHA512, nil
	}
	return 0, errors.New("unknown hash in signing algorithm: " + sigAlgo)
}

// isAsymmetricSpec returns true for RSA and ECC key specs.
func isAsymmetricSpec(keySpec string) bool {
	return strings.HasPrefix(keySpec, "RSA_") || strings.HasPrefix(keySpec, "ECC_")
}

// signingAlgorithmsForSpec returns the supported signing algorithms for a key spec.
func signingAlgorithmsForSpec(keySpec string) []string {
	switch {
	case strings.HasPrefix(keySpec, "RSA_"):
		return []string{
			"RSASSA_PKCS1_V1_5_SHA_256", "RSASSA_PKCS1_V1_5_SHA_384", "RSASSA_PKCS1_V1_5_SHA_512",
			"RSASSA_PSS_SHA_256", "RSASSA_PSS_SHA_384", "RSASSA_PSS_SHA_512",
		}
	case strings.HasPrefix(keySpec, "ECC_"):
		return []string{"ECDSA_SHA_256", "ECDSA_SHA_384", "ECDSA_SHA_512"}
	}
	return nil
}

// encryptionAlgorithmsForSpec returns supported encryption algorithms for a key spec.
func encryptionAlgorithmsForSpec(keySpec string) []string {
	if keySpec == "SYMMETRIC_DEFAULT" {
		return []string{"SYMMETRIC_DEFAULT"}
	}
	if strings.HasPrefix(keySpec, "RSA_") {
		return []string{"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"}
	}
	return nil
}

// Silence unused-import warnings for sha256/sha512 (used via crypto.Hash.Size()).
var _ = sha256.Size
var _ = sha512.Size384

// decryptData decrypts ciphertext produced by encryptData.
func decryptData(key, ct, additionalData []byte) ([]byte, error) {
	if len(ct) < ivLen+tagLen {
		return nil, errors.New("decrypt: ciphertext too short")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("decrypt: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("decrypt: new gcm: %w", err)
	}
	pt, err := gcm.Open(nil, ct[:ivLen], ct[ivLen:], additionalData)
	if err != nil {
		return nil, fmt.Errorf("decrypt: gcm open: %w", err)
	}
	return pt, nil
}
