package key

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerate32(t *testing.T) {
	k1, err := Generate32()
	require.NoError(t, err)
	assert.Len(t, k1, 32)

	k2, err := Generate32()
	require.NoError(t, err)
	assert.NotEqual(t, k1, k2, "two generated keys should differ")
}

func TestParseHexKey(t *testing.T) {
	k, err := Generate32()
	require.NoError(t, err)

	import_hex := func(b []byte) string {
		const hextable = "0123456789abcdef"
		s := make([]byte, len(b)*2)
		for i, v := range b {
			s[i*2] = hextable[v>>4]
			s[i*2+1] = hextable[v&0xf]
		}
		return string(s)
	}

	parsed, err := ParseHexKey(import_hex(k))
	require.NoError(t, err)
	assert.Equal(t, k, parsed)
}

func TestParseHexKey_Errors(t *testing.T) {
	_, err := ParseHexKey("")
	assert.Error(t, err)

	_, err = ParseHexKey("notvalidhex")
	assert.Error(t, err)

	_, err = ParseHexKey("deadbeef") // too short
	assert.Error(t, err)
}

func TestWrapUnwrapDEK_WithKEK(t *testing.T) {
	kek, err := Generate32()
	require.NoError(t, err)
	dek, err := Generate32()
	require.NoError(t, err)

	blob, err := WrapDEK(kek, dek)
	require.NoError(t, err)
	assert.Equal(t, versionAESGCM, blob[0])

	recovered, err := UnwrapDEK(kek, blob)
	require.NoError(t, err)
	assert.Equal(t, dek, recovered)
}

func TestWrapUnwrapDEK_PlaintextVersion(t *testing.T) {
	dek, err := Generate32()
	require.NoError(t, err)
	blob := plaintextBlob(dek)
	assert.Equal(t, versionPlaintext, blob[0])

	// UnwrapDEK with any kek should return the plaintext bytes
	recovered, err := UnwrapDEK(nil, blob)
	require.NoError(t, err)
	assert.Equal(t, dek, recovered)
}

func TestUnwrapDEK_WrongKEK(t *testing.T) {
	kek, _ := Generate32()
	wrongKEK, _ := Generate32()
	dek, _ := Generate32()

	blob, err := WrapDEK(kek, dek)
	require.NoError(t, err)

	_, err = UnwrapDEK(wrongKEK, blob)
	assert.Error(t, err)
}

func TestEncryptDecryptData_RoundTrip(t *testing.T) {
	key, err := Generate32()
	require.NoError(t, err)

	pt := []byte("hello, JaisCloud!")
	aad := []byte("context")

	ct, err := encryptData(key, pt, aad)
	require.NoError(t, err)
	assert.NotEqual(t, pt, ct)

	recovered, err := decryptData(key, ct, aad)
	require.NoError(t, err)
	assert.Equal(t, pt, recovered)
}

func TestEncryptDecryptData_WrongAAD(t *testing.T) {
	key, _ := Generate32()
	pt := []byte("secret")
	ct, err := encryptData(key, pt, []byte("correct-aad"))
	require.NoError(t, err)

	_, err = decryptData(key, ct, []byte("wrong-aad"))
	assert.Error(t, err)
}

func TestEncryptData_NilAAD(t *testing.T) {
	key, _ := Generate32()
	pt := []byte("no aad")
	ct, err := encryptData(key, pt, nil)
	require.NoError(t, err)

	recovered, err := decryptData(key, ct, nil)
	require.NoError(t, err)
	assert.Equal(t, pt, recovered)
}

func TestEncryptData_Nondeterministic(t *testing.T) {
	key, _ := Generate32()
	pt := []byte("same plaintext")
	ct1, _ := encryptData(key, pt, nil)
	ct2, _ := encryptData(key, pt, nil)
	assert.False(t, bytes.Equal(ct1, ct2), "two encryptions of same plaintext should differ (random IV)")
}
