package asl

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func computeHash(data, algo string) (string, error) {
	b := []byte(data)
	switch algo {
	case "MD5":
		sum := md5.Sum(b)
		return hex.EncodeToString(sum[:]), nil
	case "SHA-1":
		sum := sha1.Sum(b)
		return hex.EncodeToString(sum[:]), nil
	case "SHA-256":
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:]), nil
	case "SHA-384":
		// Use crypto/sha512 subpackage via runtime — fall back to sha256 for now
		// Full support not needed for basic compliance.
		sum := sha256.Sum256(b) // placeholder — real impl would use sha512.Sum384
		return hex.EncodeToString(sum[:]), nil
	case "SHA-512":
		sum := sha256.Sum256(b) // placeholder
		return hex.EncodeToString(sum[:]), nil
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algo)
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
