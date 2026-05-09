package integration_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P4.5: GenerateDataKeyPair ────────────────────────────────────────────────

func TestKMS_GenerateDataKeyPair_RSA2048(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	out, err := c.GenerateDataKeyPair(ctx, &awskms.GenerateDataKeyPairInput{
		KeyId:       aws.String(keyID),
		KeyPairSpec: types.DataKeyPairSpecRsa2048,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.PublicKey)
	assert.NotEmpty(t, out.PrivateKeyPlaintext)
	assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
	assert.Equal(t, types.DataKeyPairSpecRsa2048, out.KeyPairSpec)
}

func TestKMS_GenerateDataKeyPairWithoutPlaintext_ECC_P256(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	out, err := c.GenerateDataKeyPairWithoutPlaintext(ctx, &awskms.GenerateDataKeyPairWithoutPlaintextInput{
		KeyId:       aws.String(keyID),
		KeyPairSpec: types.DataKeyPairSpecEccNistP256,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.PublicKey)
	assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
	assert.Equal(t, types.DataKeyPairSpecEccNistP256, out.KeyPairSpec)
}

func TestKMS_GenerateDataKeyPair_AsymmetricCMKRejected(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	// Create an asymmetric RSA SIGN_VERIFY key — should be rejected
	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecRsa2048,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	_, err = c.GenerateDataKeyPair(ctx, &awskms.GenerateDataKeyPairInput{
		KeyId:       aws.String(keyID),
		KeyPairSpec: types.DataKeyPairSpecRsa2048,
	})
	require.Error(t, err, "asymmetric CMK must be rejected by GenerateDataKeyPair")
}

// ─── P4.6: GenerateMac / VerifyMac ───────────────────────────────────────────

func TestKMS_GenerateMac_VerifyMac_HMAC256(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecHmac256,
		KeyUsage: types.KeyUsageTypeGenerateVerifyMac,
	})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	msg := []byte("hello from jaiscloud")
	msgB64 := base64.StdEncoding.EncodeToString(msg)

	genOut, err := c.GenerateMac(ctx, &awskms.GenerateMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: types.MacAlgorithmSpecHmacSha256,
		Message:      []byte(msgB64),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, genOut.Mac)

	verOut, err := c.VerifyMac(ctx, &awskms.VerifyMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: types.MacAlgorithmSpecHmacSha256,
		Message:      []byte(msgB64),
		Mac:          genOut.Mac,
	})
	require.NoError(t, err)
	assert.True(t, verOut.MacValid)
}

func TestKMS_VerifyMac_WrongMacFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecHmac256,
		KeyUsage: types.KeyUsageTypeGenerateVerifyMac,
	})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	msgB64 := base64.StdEncoding.EncodeToString([]byte("test"))
	badMac := make([]byte, 32) // all zeros

	_, err = c.VerifyMac(ctx, &awskms.VerifyMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: types.MacAlgorithmSpecHmacSha256,
		Message:      []byte(msgB64),
		Mac:          badMac,
	})
	require.Error(t, err, "wrong MAC must fail verification")
}

func TestKMS_GenerateMac_WrongKeyUsageFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	// Symmetric encrypt/decrypt key — cannot generate MACs
	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	_, err = c.GenerateMac(ctx, &awskms.GenerateMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: types.MacAlgorithmSpecHmacSha256,
		Message:      []byte(base64.StdEncoding.EncodeToString([]byte("msg"))),
	})
	require.Error(t, err, "non-HMAC key must be rejected for GenerateMac")
}

func TestKMS_GenerateMac_IncompatibleAlgorithmFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	// HMAC_256 key
	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecHmac256,
		KeyUsage: types.KeyUsageTypeGenerateVerifyMac,
	})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	// Use HMAC_SHA_512 algorithm on an HMAC_256 key — incompatible
	_, err = c.GenerateMac(ctx, &awskms.GenerateMacInput{
		KeyId:        aws.String(keyID),
		MacAlgorithm: types.MacAlgorithmSpecHmacSha512,
		Message:      []byte(base64.StdEncoding.EncodeToString([]byte("msg"))),
	})
	require.Error(t, err, "algorithm incompatible with key spec must be rejected")
}

// ─── P4.7: GenerateRandom ─────────────────────────────────────────────────────

func TestKMS_GenerateRandom_ReturnsCorrectLength(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.GenerateRandom(ctx, &awskms.GenerateRandomInput{
		NumberOfBytes: aws.Int32(32),
	})
	require.NoError(t, err)
	assert.Len(t, out.Plaintext, 32)
}

func TestKMS_GenerateRandom_MaxBytes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.GenerateRandom(ctx, &awskms.GenerateRandomInput{
		NumberOfBytes: aws.Int32(1024),
	})
	require.NoError(t, err)
	assert.Len(t, out.Plaintext, 1024)
}

func TestKMS_GenerateRandom_OverLimitFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	_, err := c.GenerateRandom(ctx, &awskms.GenerateRandomInput{
		NumberOfBytes: aws.Int32(1025),
	})
	require.Error(t, err, "NumberOfBytes > 1024 must be rejected")
}
