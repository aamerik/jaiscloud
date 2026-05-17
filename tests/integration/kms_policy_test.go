package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/require"
)

// denyPolicy builds a KMS key policy JSON that explicitly denies the given
// action for all principals ("*").
func denyPolicy(action string) string {
	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":    "Deny",
				"Principal": "*",
				"Action":    action,
				"Resource":  "*",
			},
		},
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

// allowAllPolicy builds a KMS key policy JSON that allows all kms:* actions
// for all principals.
func allowAllPolicy() string {
	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":    "Allow",
				"Principal": "*",
				"Action":    "kms:*",
				"Resource":  "*",
			},
		},
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

// TestKMSKeyPolicyDenyDecrypt verifies that an explicit Deny on kms:Decrypt in
// a key policy causes Decrypt to return AccessDeniedException, while Encrypt
// (not denied) continues to succeed.
func TestKMSKeyPolicyDenyDecrypt(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	// Create a symmetric key.
	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	// Encrypt should succeed before applying the deny policy.
	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("sensitive data"),
	})
	require.NoError(t, err, "Encrypt must succeed before deny policy is applied")
	ciphertext := encOut.CiphertextBlob

	// Apply a policy that explicitly denies kms:Decrypt for all principals.
	_, err = c.PutKeyPolicy(ctx, &awskms.PutKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
		Policy:     aws.String(denyPolicy("kms:Decrypt")),
	})
	require.NoError(t, err)

	// Encrypt should still succeed — the policy only denies Decrypt.
	_, err = c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("another message"),
	})
	require.NoError(t, err, "Encrypt must succeed even when Decrypt is denied")

	// Decrypt must now fail with AccessDeniedException.
	_, err = c.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: ciphertext,
	})
	assertAWSError(t, err, "AccessDeniedException")
}

// TestKMSKeyPolicyDenyEncrypt verifies that an explicit Deny on kms:Encrypt
// causes Encrypt to return AccessDeniedException.
func TestKMSKeyPolicyDenyEncrypt(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	// Apply a policy that explicitly denies kms:Encrypt.
	_, err = c.PutKeyPolicy(ctx, &awskms.PutKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
		Policy:     aws.String(denyPolicy("kms:Encrypt")),
	})
	require.NoError(t, err)

	_, err = c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("blocked"),
	})
	assertAWSError(t, err, "AccessDeniedException")
}

// TestKMSKeyPolicyDenyGenerateDataKey verifies that an explicit Deny on
// kms:GenerateDataKey causes GenerateDataKey to return AccessDeniedException.
func TestKMSKeyPolicyDenyGenerateDataKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	// Apply deny policy for GenerateDataKey.
	_, err = c.PutKeyPolicy(ctx, &awskms.PutKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
		Policy:     aws.String(denyPolicy("kms:GenerateDataKey")),
	})
	require.NoError(t, err)

	_, err = c.GenerateDataKey(ctx, &awskms.GenerateDataKeyInput{
		KeyId:   aws.String(keyID),
		KeySpec: types.DataKeySpecAes256,
	})
	assertAWSError(t, err, "AccessDeniedException")
}

// TestKMSKeyPolicyAllowAll verifies that a policy with Allow * does not block
// any operations — Encrypt and Decrypt both succeed.
func TestKMSKeyPolicyAllowAll(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	// Apply an explicit allow-all policy.
	_, err = c.PutKeyPolicy(ctx, &awskms.PutKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
		Policy:     aws.String(allowAllPolicy()),
	})
	require.NoError(t, err)

	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("hello world"),
	})
	require.NoError(t, err, "Encrypt must succeed with allow-all policy")

	decOut, err := c.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err, "Decrypt must succeed with allow-all policy")
	require.Equal(t, []byte("hello world"), decOut.Plaintext)
}

// TestKMSKeyPolicyDenyWildcard verifies that a Deny with "kms:*" wildcard
// blocks all KMS crypto operations on the key.
func TestKMSKeyPolicyDenyWildcard(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	// Deny all KMS operations.
	_, err = c.PutKeyPolicy(ctx, &awskms.PutKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
		Policy:     aws.String(denyPolicy("kms:*")),
	})
	require.NoError(t, err)

	_, err = c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("should be denied"),
	})
	assertAWSError(t, err, "AccessDeniedException")
}

// TestKMSKeyPolicyNoCustomPolicy verifies that a key without a custom policy
// allows all operations (emulator permissive default).
func TestKMSKeyPolicyNoCustomPolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	// No PutKeyPolicy call — default permissive behaviour.
	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("no policy key"),
	})
	require.NoError(t, err)

	_, err = c.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
}
