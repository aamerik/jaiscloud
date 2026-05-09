package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P3.6: KMS Asymmetric Key Generation ─────────────────────────────────────

func TestKMS_Asymmetric_CreateRSA2048Key(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecRsa2048,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	assert.Equal(t, types.KeySpecRsa2048, out.KeyMetadata.KeySpec)
	assert.Equal(t, types.KeyUsageTypeSignVerify, out.KeyMetadata.KeyUsage)
}

func TestKMS_Asymmetric_CreateECCP256Key(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecEccNistP256,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	assert.Equal(t, types.KeySpecEccNistP256, out.KeyMetadata.KeySpec)
}

// ─── P3.7: KMS Sign / Verify ──────────────────────────────────────────────────

func TestKMS_Sign_Verify_RSAPKCS1SHA256(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecRsa2048,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	msg := []byte("hello asymmetric signing")
	signOut, err := c.Sign(ctx, &awskms.SignInput{
		KeyId:            aws.String(keyID),
		Message:          msg,
		MessageType:      types.MessageTypeRaw,
		SigningAlgorithm: types.SigningAlgorithmSpecRsassaPkcs1V15Sha256,
	})
	require.NoError(t, err)
	require.NotEmpty(t, signOut.Signature)

	_, err = c.Verify(ctx, &awskms.VerifyInput{
		KeyId:            aws.String(keyID),
		Message:          msg,
		MessageType:      types.MessageTypeRaw,
		Signature:        signOut.Signature,
		SigningAlgorithm: types.SigningAlgorithmSpecRsassaPkcs1V15Sha256,
	})
	require.NoError(t, err)
}

func TestKMS_Sign_Verify_RSAPSS(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecRsa2048,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	msg := []byte("pss signing test")
	signOut, err := c.Sign(ctx, &awskms.SignInput{
		KeyId:            aws.String(keyID),
		Message:          msg,
		MessageType:      types.MessageTypeRaw,
		SigningAlgorithm: types.SigningAlgorithmSpecRsassaPssSha256,
	})
	require.NoError(t, err)

	_, err = c.Verify(ctx, &awskms.VerifyInput{
		KeyId:            aws.String(keyID),
		Message:          msg,
		MessageType:      types.MessageTypeRaw,
		Signature:        signOut.Signature,
		SigningAlgorithm: types.SigningAlgorithmSpecRsassaPssSha256,
	})
	require.NoError(t, err)
}

func TestKMS_Sign_Verify_ECDSA_P256(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecEccNistP256,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	msg := []byte("ecdsa signing test")
	signOut, err := c.Sign(ctx, &awskms.SignInput{
		KeyId:            aws.String(keyID),
		Message:          msg,
		MessageType:      types.MessageTypeRaw,
		SigningAlgorithm: types.SigningAlgorithmSpecEcdsaSha256,
	})
	require.NoError(t, err)

	_, err = c.Verify(ctx, &awskms.VerifyInput{
		KeyId:            aws.String(keyID),
		Message:          msg,
		MessageType:      types.MessageTypeRaw,
		Signature:        signOut.Signature,
		SigningAlgorithm: types.SigningAlgorithmSpecEcdsaSha256,
	})
	require.NoError(t, err)
}

func TestKMS_Verify_WrongSignatureFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecRsa2048,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.Verify(ctx, &awskms.VerifyInput{
		KeyId:            aws.String(keyID),
		Message:          []byte("msg"),
		MessageType:      types.MessageTypeRaw,
		Signature:        []byte("invalidsig"),
		SigningAlgorithm: types.SigningAlgorithmSpecRsassaPkcs1V15Sha256,
	})
	require.Error(t, err)
}

// ─── P3.8: KMS GetPublicKey ───────────────────────────────────────────────────

func TestKMS_GetPublicKey_RSA(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecRsa2048,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	pubOut, err := c.GetPublicKey(ctx, &awskms.GetPublicKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.NotEmpty(t, pubOut.PublicKey, "public key DER bytes must be returned")
	assert.Equal(t, types.KeySpecRsa2048, pubOut.KeySpec)
}

func TestKMS_GetPublicKey_ECC(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecEccNistP384,
		KeyUsage: types.KeyUsageTypeSignVerify,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	pubOut, err := c.GetPublicKey(ctx, &awskms.GetPublicKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.NotEmpty(t, pubOut.PublicKey)
	assert.Equal(t, types.KeySpecEccNistP384, pubOut.KeySpec)
}

func TestKMS_GetPublicKey_SymmetricKeyReturnsError(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.GetPublicKey(ctx, &awskms.GetPublicKeyInput{KeyId: aws.String(keyID)})
	require.Error(t, err, "GetPublicKey on symmetric key must return error")
}

// ─── P3.9: KMS Key Rotation with Previous Material Decrypt ───────────────────

func TestKMS_RotateKeyOnDemand_DecryptWithPreviousMaterial(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	plaintext := []byte("data encrypted before rotation")
	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	ciphertext := encOut.CiphertextBlob

	// Rotate the key.
	_, err = c.RotateKeyOnDemand(ctx, &awskms.RotateKeyOnDemandInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	// Decrypt with old ciphertext must still work using previous key material.
	decOut, err := c.Decrypt(ctx, &awskms.DecryptInput{
		KeyId:          aws.String(keyID),
		CiphertextBlob: ciphertext,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

func TestKMS_RotateKeyOnDemand_NewEncryptUsesNewMaterial(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.RotateKeyOnDemand(ctx, &awskms.RotateKeyOnDemandInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	plaintext := []byte("data encrypted after rotation")
	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: plaintext,
	})
	require.NoError(t, err)

	decOut, err := c.Decrypt(ctx, &awskms.DecryptInput{
		KeyId:          aws.String(keyID),
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}
