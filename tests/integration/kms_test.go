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

func TestKMS_CreateDescribeKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("integration test key"),
		KeyUsage:    types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	require.NotNil(t, out.KeyMetadata)
	keyID := aws.ToString(out.KeyMetadata.KeyId)
	assert.NotEmpty(t, keyID)
	assert.Equal(t, "integration test key", aws.ToString(out.KeyMetadata.Description))
	assert.True(t, out.KeyMetadata.Enabled)

	desc, err := c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Equal(t, keyID, aws.ToString(desc.KeyMetadata.KeyId))
}

func TestKMS_ListKeys(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	for i := 0; i < 3; i++ {
		_, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
		require.NoError(t, err)
	}

	out, err := c.ListKeys(ctx, &awskms.ListKeysInput{})
	require.NoError(t, err)
	assert.Len(t, out.Keys, 3)
}

func TestKMS_EnableDisableKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.DisableKey(ctx, &awskms.DisableKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)

	desc, err := c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.False(t, desc.KeyMetadata.Enabled)

	_, err = c.EnableKey(ctx, &awskms.EnableKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)

	desc, err = c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.True(t, desc.KeyMetadata.Enabled)
}

func TestKMS_EncryptDecryptRoundTrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	plaintext := []byte("top secret value")
	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	require.NotEmpty(t, encOut.CiphertextBlob)

	decOut, err := c.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

func TestKMS_GenerateDataKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	out, err := c.GenerateDataKey(ctx, &awskms.GenerateDataKeyInput{
		KeyId:   aws.String(keyID),
		KeySpec: types.DataKeySpecAes256,
	})
	require.NoError(t, err)
	assert.Len(t, out.Plaintext, 32)
	assert.NotEmpty(t, out.CiphertextBlob)
}

func TestKMS_CreateDeleteAlias(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.CreateAlias(ctx, &awskms.CreateAliasInput{
		AliasName:   aws.String("alias/my-integration-key"),
		TargetKeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	listOut, err := c.ListAliases(ctx, &awskms.ListAliasesInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Len(t, listOut.Aliases, 1)

	// Resolve via alias.
	desc, err := c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String("alias/my-integration-key")})
	require.NoError(t, err)
	assert.Equal(t, keyID, aws.ToString(desc.KeyMetadata.KeyId))

	_, err = c.DeleteAlias(ctx, &awskms.DeleteAliasInput{AliasName: aws.String("alias/my-integration-key")})
	require.NoError(t, err)

	listOut2, err := c.ListAliases(ctx, &awskms.ListAliasesInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Aliases)
}

func TestKMS_CreateGrant_RevokeGrant(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	grantOut, err := c.CreateGrant(ctx, &awskms.CreateGrantInput{
		KeyId:            aws.String(keyID),
		GranteePrincipal: aws.String("arn:aws:iam::000000000000:role/test"),
		Operations:       []types.GrantOperation{types.GrantOperationEncrypt, types.GrantOperationDecrypt},
	})
	require.NoError(t, err)
	grantID := aws.ToString(grantOut.GrantId)
	require.NotEmpty(t, grantID)

	listOut, err := c.ListGrants(ctx, &awskms.ListGrantsInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Len(t, listOut.Grants, 1)

	_, err = c.RevokeGrant(ctx, &awskms.RevokeGrantInput{
		KeyId:   aws.String(keyID),
		GrantId: aws.String(grantID),
	})
	require.NoError(t, err)

	listOut2, err := c.ListGrants(ctx, &awskms.ListGrantsInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Grants)
}

func TestKMS_ScheduleKeyDeletion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	delOut, err := c.ScheduleKeyDeletion(ctx, &awskms.ScheduleKeyDeletionInput{
		KeyId:               aws.String(keyID),
		PendingWindowInDays: aws.Int32(7),
	})
	require.NoError(t, err)
	assert.NotNil(t, delOut.DeletionDate)
}

func TestKMS_TagUntagResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.TagResource(ctx, &awskms.TagResourceInput{
		KeyId: aws.String(keyID),
		Tags:  []types.Tag{{TagKey: aws.String("env"), TagValue: aws.String("test")}},
	})
	require.NoError(t, err)

	listOut, err := c.ListResourceTags(ctx, &awskms.ListResourceTagsInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut.Tags[0].TagKey))

	_, err = c.UntagResource(ctx, &awskms.UntagResourceInput{
		KeyId:   aws.String(keyID),
		TagKeys: []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := c.ListResourceTags(ctx, &awskms.ListResourceTagsInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Tags)
}

// Ensure base64 round-trip works for raw encrypt/decrypt (tests the codec path).
func TestKMS_EncryptRawPlaintext_Base64Transport(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	raw := []byte("binary\x00data\xFF")
	b64 := base64.StdEncoding.EncodeToString(raw)
	_ = b64 // The SDK handles base64 encoding automatically

	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: raw,
	})
	require.NoError(t, err)

	decOut, err := c.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, raw, decOut.Plaintext)
}

func TestKMS_ReEncrypt(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	srcOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{Description: aws.String("src")})
	require.NoError(t, err)
	srcKeyID := aws.ToString(srcOut.KeyMetadata.KeyId)

	dstOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{Description: aws.String("dst")})
	require.NoError(t, err)
	dstKeyID := aws.ToString(dstOut.KeyMetadata.KeyId)

	plaintext := []byte("data to re-encrypt")
	encOut, err := c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(srcKeyID),
		Plaintext: plaintext,
	})
	require.NoError(t, err)

	reOut, err := c.ReEncrypt(ctx, &awskms.ReEncryptInput{
		CiphertextBlob:   encOut.CiphertextBlob,
		DestinationKeyId: aws.String(dstKeyID),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(reOut.KeyId), dstKeyID)

	// Decrypt with destination key must recover original plaintext.
	decOut, err := c.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: reOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

// ─── P1.1: KeyEntry new fields ────────────────────────────────────────────────

func TestKMS_CreateKey_ReturnsKeySpec(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		KeySpec:  types.KeySpecSymmetricDefault,
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
	})
	require.NoError(t, err)
	require.NotNil(t, out.KeyMetadata)
	assert.Equal(t, types.KeySpecSymmetricDefault, out.KeyMetadata.KeySpec)
	assert.Equal(t, types.KeyUsageTypeEncryptDecrypt, out.KeyMetadata.KeyUsage)
}

// ─── P1.5: ScheduleKeyDeletion validation ─────────────────────────────────────

func TestKMS_ScheduleKeyDeletion_InvalidWindow(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(out.KeyMetadata.KeyId)

	_, err = c.ScheduleKeyDeletion(ctx, &awskms.ScheduleKeyDeletionInput{
		KeyId:               aws.String(keyID),
		PendingWindowInDays: aws.Int32(6),
	})
	require.Error(t, err)
}

// ─── P1.6: CancelKeyDeletion ──────────────────────────────────────────────────

func TestKMS_CancelKeyDeletion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(out.KeyMetadata.KeyId)

	_, err = c.ScheduleKeyDeletion(ctx, &awskms.ScheduleKeyDeletionInput{
		KeyId:               aws.String(keyID),
		PendingWindowInDays: aws.Int32(7),
	})
	require.NoError(t, err)

	_, err = c.CancelKeyDeletion(ctx, &awskms.CancelKeyDeletionInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)

	// CancelKeyDeletion → Disabled state; must EnableKey to go Enabled.
	desc, err := c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Equal(t, types.KeyStateDisabled, desc.KeyMetadata.KeyState)
}

func TestKMS_DescribeKey_IncludesNewFields(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: aws.String("fields test"),
		KeySpec:     types.KeySpecSymmetricDefault,
	})
	require.NoError(t, err)
	keyID := aws.ToString(out.KeyMetadata.KeyId)

	desc, err := c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Equal(t, types.KeySpecSymmetricDefault, desc.KeyMetadata.KeySpec)
	assert.Equal(t, "fields test", aws.ToString(desc.KeyMetadata.Description))
	assert.False(t, aws.ToBool(desc.KeyMetadata.MultiRegion))
}

func TestKMS_ScheduleKeyDeletion_BlocksCryptoOps(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	createOut, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.KeyMetadata.KeyId)

	_, err = c.ScheduleKeyDeletion(ctx, &awskms.ScheduleKeyDeletionInput{
		KeyId:               aws.String(keyID),
		PendingWindowInDays: aws.Int32(7),
	})
	require.NoError(t, err)

	// DescribeKey should show PendingDeletion state.
	descOut, err := c.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Equal(t, types.KeyStatePendingDeletion, descOut.KeyMetadata.KeyState)

	// Encrypt on a pending-deletion key must fail.
	_, err = c.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte("should fail"),
	})
	require.Error(t, err)
}
