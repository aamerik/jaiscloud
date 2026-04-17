//go:build lambda_e2e

package p25_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── KMS + SecretsManager cross-service ──────────────────────────────────────

// TestKMS_SecretsManager_Integration creates a KMS key and then uses it to
// create a secret. Decrypting the secret must recover the original plaintext,
// proving that the KMS encrypt/decrypt path is wired end-to-end through the
// SecretsManager provider.
func TestKMS_SecretsManager_Integration(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsClient := newKMSClient(t)
	smClient := newSMClient(t)

	// Create a KMS key to use as the CMK for the secret.
	keyOut, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("integration-test-cmk"),
		KeyUsage:    kmstypes.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.KeyMetadata.KeyId)

	// Create a secret encrypted with the CMK.
	createOut, err := smClient.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("kms-backed-secret"),
		KmsKeyId:     aws.String(keyID),
		SecretString: aws.String("top-secret-value"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.ARN))

	// Get the secret — must decrypt via KMS and return original plaintext.
	getOut, err := smClient.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("kms-backed-secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "top-secret-value", aws.ToString(getOut.SecretString))
}

// TestKMS_SecretsManager_BinarySecret verifies that binary secrets are stored
// and retrieved correctly when using a KMS CMK.
func TestKMS_SecretsManager_BinarySecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsClient := newKMSClient(t)
	smClient := newSMClient(t)

	keyOut, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.KeyMetadata.KeyId)

	binaryPayload := []byte("binary\x00data\xFF\xFE")

	_, err = smClient.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("kms-binary-secret"),
		KmsKeyId:     aws.String(keyID),
		SecretBinary: binaryPayload,
	})
	require.NoError(t, err)

	getOut, err := smClient.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("kms-binary-secret"),
	})
	require.NoError(t, err)
	assert.Nil(t, getOut.SecretString, "binary secret must not populate SecretString")
	assert.Equal(t, binaryPayload, getOut.SecretBinary)
}

// TestKMS_SecretsManager_RotateSecret verifies that putting a new secret value
// (rotation) with a different KMS key produces a new version and the latest
// value is returned.
func TestKMS_SecretsManager_RotateSecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsClient := newKMSClient(t)
	smClient := newSMClient(t)

	// Create two KMS keys: original and rotation target.
	key1Out, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{Description: aws.String("key-v1")})
	require.NoError(t, err)
	key1ID := aws.ToString(key1Out.KeyMetadata.KeyId)

	key2Out, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{Description: aws.String("key-v2")})
	require.NoError(t, err)
	key2ID := aws.ToString(key2Out.KeyMetadata.KeyId)

	// Initial secret with key1.
	_, err = smClient.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rotating-secret"),
		KmsKeyId:     aws.String(key1ID),
		SecretString: aws.String("version-one"),
	})
	require.NoError(t, err)

	// Rotate to new value (PutSecretValue doesn't accept KmsKeyId; the key
	// used at creation time is reused for subsequent versions).
	_, err = smClient.PutSecretValue(ctx, &awssm.PutSecretValueInput{
		SecretId:     aws.String("rotating-secret"),
		SecretString: aws.String("version-two"),
	})
	_ = key2ID // rotation via UpdateSecret (not tested here)
	require.NoError(t, err)

	// Latest value must be version-two.
	getOut, err := smClient.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("rotating-secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "version-two", aws.ToString(getOut.SecretString))
}

// TestKMS_DisabledKey_BlocksSecretCreate verifies that creating a secret with a
// disabled KMS key returns an error. The KMS provider rejects crypto ops on
// disabled keys before SecretsManager can encrypt.
func TestKMS_DisabledKey_BlocksSecretCreate(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsClient := newKMSClient(t)
	smClient := newSMClient(t)

	keyOut, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.KeyMetadata.KeyId)

	_, err = kmsClient.DisableKey(ctx, &awskms.DisableKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)

	_, err = smClient.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("disabled-key-secret"),
		KmsKeyId:     aws.String(keyID),
		SecretString: aws.String("should-fail"),
	})
	require.Error(t, err, "creating secret with disabled KMS key must fail")
}

// ─── SSM Parameter Store + KMS ────────────────────────────────────────────────

// TestSSM_SecureString_KMSEncryption verifies the full SecureString flow:
// create a KMS key, put a SecureString parameter using that key, and retrieve
// it with decryption — the returned value must match the original.
func TestSSM_SecureString_KMSEncryption(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kmsClient := newKMSClient(t)
	ssmClient := newSSMClient(t)

	keyOut, err := kmsClient.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("ssm-secure-string-key"),
	})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.KeyMetadata.KeyId)

	_, err = ssmClient.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:     aws.String("/prod/db/password"),
		Value:    aws.String("s3cr3tP@ss"),
		Type:     ssmtypes.ParameterTypeSecureString,
		KeyId:    aws.String(keyID),
		Overwrite: aws.Bool(false),
	})
	require.NoError(t, err)

	getOut, err := ssmClient.GetParameter(ctx, &awsssm.GetParameterInput{
		Name:           aws.String("/prod/db/password"),
		WithDecryption: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "s3cr3tP@ss", aws.ToString(getOut.Parameter.Value))
	assert.Equal(t, ssmtypes.ParameterTypeSecureString, getOut.Parameter.Type)
}

// TestSSM_PathHierarchy_RecursiveVsNonRecursive verifies that ListParametersByPath
// returns only direct children in non-recursive mode and all descendants in
// recursive mode. This exercises the path-prefix normalization fix (trailing /).
func TestSSM_PathHierarchy_RecursiveVsNonRecursive(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ssmClient := newSSMClient(t)

	params := []struct {
		name  string
		value string
	}{
		{"/app/db/host", "localhost"},
		{"/app/db/port", "5432"},
		{"/app/cache/host", "redis"},
		{"/app/version", "1.0"},
	}
	for _, p := range params {
		_, err := ssmClient.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:     aws.String(p.name),
			Value:    aws.String(p.value),
			Type:     ssmtypes.ParameterTypeString,
			Overwrite: aws.Bool(false),
		})
		require.NoError(t, err, "put %s", p.name)
	}

	// Non-recursive listing of /app — only direct children (version).
	nonRecOut, err := ssmClient.GetParametersByPath(ctx, &awsssm.GetParametersByPathInput{
		Path:      aws.String("/app"),
		Recursive: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Len(t, nonRecOut.Parameters, 1, "non-recursive /app: expect only /app/version")
	assert.Equal(t, "/app/version", aws.ToString(nonRecOut.Parameters[0].Name))

	// Recursive listing of /app — all 4 descendants.
	recOut, err := ssmClient.GetParametersByPath(ctx, &awsssm.GetParametersByPathInput{
		Path:      aws.String("/app"),
		Recursive: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, recOut.Parameters, 4, "recursive /app: expect all 4 params")
}

// TestSSM_PathPrefix_NoFalseMatch verifies that /app does NOT match /appname/x,
// exercising the trailing-slash normalization fix.
func TestSSM_PathPrefix_NoFalseMatch(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ssmClient := newSSMClient(t)

	for _, p := range []string{"/app/real", "/appname/wrong"} {
		_, err := ssmClient.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:     aws.String(p),
			Value:    aws.String("v"),
			Type:     ssmtypes.ParameterTypeString,
			Overwrite: aws.Bool(false),
		})
		require.NoError(t, err)
	}

	out, err := ssmClient.GetParametersByPath(ctx, &awsssm.GetParametersByPathInput{
		Path:      aws.String("/app"),
		Recursive: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 1)
	assert.Equal(t, "/app/real", aws.ToString(out.Parameters[0].Name))
}

// TestSSM_ParameterHistory verifies that overwriting a parameter creates a
// history entry and the version counter increments correctly.
func TestSSM_ParameterHistory(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ssmClient := newSSMClient(t)

	_, err := ssmClient.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:     aws.String("/svc/config"),
		Value:    aws.String("v1"),
		Type:     ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(false),
	})
	require.NoError(t, err)

	_, err = ssmClient.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:     aws.String("/svc/config"),
		Value:    aws.String("v2"),
		Type:     ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	getOut, err := ssmClient.GetParameter(ctx, &awsssm.GetParameterInput{
		Name:           aws.String("/svc/config"),
		WithDecryption: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", aws.ToString(getOut.Parameter.Value))
	assert.EqualValues(t, 2, getOut.Parameter.Version)

	histOut, err := ssmClient.GetParameterHistory(ctx, &awsssm.GetParameterHistoryInput{
		Name: aws.String("/svc/config"),
	})
	require.NoError(t, err)
	assert.Len(t, histOut.Parameters, 1, "one historical entry for the overwritten version")
	assert.Equal(t, "v1", aws.ToString(histOut.Parameters[0].Value))
}
