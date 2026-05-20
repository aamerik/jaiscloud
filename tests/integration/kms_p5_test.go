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

// ─── P5.4: KMS Grants Lifecycle ───────────────────────────────────────────────

func TestKMS_Grants_CreateAndList(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	grantOut, err := c.CreateGrant(ctx, &awskms.CreateGrantInput{
		KeyId:            aws.String(keyID),
		GranteePrincipal: aws.String("arn:aws:iam::000000000000:user/alice"),
		Operations:       []types.GrantOperation{types.GrantOperationEncrypt, types.GrantOperationDecrypt},
		Name:             aws.String("test-grant"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(grantOut.GrantId))
	assert.NotEmpty(t, aws.ToString(grantOut.GrantToken))

	listOut, err := c.ListGrants(ctx, &awskms.ListGrantsInput{
		KeyId: aws.String(keyID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Grants, 1)
	assert.Equal(t, "test-grant", aws.ToString(listOut.Grants[0].Name))
}

func TestKMS_Grants_CreateIdempotentByName(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	g1, err := c.CreateGrant(ctx, &awskms.CreateGrantInput{
		KeyId:            aws.String(keyID),
		GranteePrincipal: aws.String("arn:aws:iam::000000000000:user/bob"),
		Operations:       []types.GrantOperation{types.GrantOperationDecrypt},
		Name:             aws.String("idempotent-grant"),
	})
	require.NoError(t, err)

	// Second call with same name should return the same grant
	g2, err := c.CreateGrant(ctx, &awskms.CreateGrantInput{
		KeyId:            aws.String(keyID),
		GranteePrincipal: aws.String("arn:aws:iam::000000000000:user/bob"),
		Operations:       []types.GrantOperation{types.GrantOperationDecrypt},
		Name:             aws.String("idempotent-grant"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(g1.GrantId), aws.ToString(g2.GrantId))
}

func TestKMS_Grants_InvalidOperationFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	// "BadOperation" is not a valid grant operation
	_, err = c.CreateGrant(ctx, &awskms.CreateGrantInput{
		KeyId:            aws.String(keyID),
		GranteePrincipal: aws.String("arn:aws:iam::000000000000:user/carol"),
		Operations:       []types.GrantOperation{types.GrantOperation("BadOperation")},
	})
	require.Error(t, err, "invalid grant operation must be rejected")
}

func TestKMS_Grants_RevokeGrant(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	key, err := c.CreateKey(ctx, &awskms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := aws.ToString(key.KeyMetadata.KeyId)

	grantOut, err := c.CreateGrant(ctx, &awskms.CreateGrantInput{
		KeyId:            aws.String(keyID),
		GranteePrincipal: aws.String("arn:aws:iam::000000000000:user/dave"),
		Operations:       []types.GrantOperation{types.GrantOperationEncrypt},
	})
	require.NoError(t, err)

	_, err = c.RevokeGrant(ctx, &awskms.RevokeGrantInput{
		KeyId:   aws.String(keyID),
		GrantId: grantOut.GrantId,
	})
	require.NoError(t, err)

	listOut, err := c.ListGrants(ctx, &awskms.ListGrantsInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Empty(t, listOut.Grants)
}

// ─── P5.3: KMS Key Import stubs ───────────────────────────────────────────────

func TestKMS_CreateKey_ExternalOrigin(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	out, err := c.CreateKey(ctx, &awskms.CreateKeyInput{
		Origin: types.OriginTypeExternal,
	})
	require.NoError(t, err)
	assert.Equal(t, types.OriginTypeExternal, out.KeyMetadata.Origin)
}

func TestKMS_ImportKeyMaterial_ReturnsUnsupported(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newKMSClient(t)

	// ImportKeyMaterial should return UnsupportedOperationException
	_, err := c.ImportKeyMaterial(ctx, &awskms.ImportKeyMaterialInput{
		KeyId:                aws.String("fake-key-id"),
		ImportToken:          []byte("token"),
		EncryptedKeyMaterial: []byte("material"),
	})
	require.Error(t, err, "ImportKeyMaterial must return an error (not supported)")
}
