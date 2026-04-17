// Package key provides KMS key management: envelope encryption, key storage,
// and the KeyProvider that handles KMS API operations.
package key

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
