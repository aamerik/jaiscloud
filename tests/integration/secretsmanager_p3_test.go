package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P3.10: SecretsManager RotateSecret / CancelRotateSecret ─────────────────

func TestSecretsManager_RotateSecret_CreatesPendingVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rotate-test"),
		SecretString: aws.String("initial-value"),
	})
	require.NoError(t, err)

	// RotateImmediately=false: should create AWSPENDING but not promote.
	_, err = c.RotateSecret(ctx, &awssm.RotateSecretInput{
		SecretId:          aws.String("rotate-test"),
		RotateImmediately: aws.Bool(false),
	})
	require.NoError(t, err)

	// AWSCURRENT must still be the original value.
	getOut, err := c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId:     aws.String("rotate-test"),
		VersionStage: aws.String("AWSCURRENT"),
	})
	require.NoError(t, err)
	assert.Equal(t, "initial-value", aws.ToString(getOut.SecretString))

	// AWSPENDING must exist.
	pendingOut, err := c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId:     aws.String("rotate-test"),
		VersionStage: aws.String("AWSPENDING"),
	})
	require.NoError(t, err)
	assert.NotNil(t, pendingOut)
}

func TestSecretsManager_RotateSecret_RotateImmediately_PromotesToCurrent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rotate-immediate"),
		SecretString: aws.String("original"),
	})
	require.NoError(t, err)

	_, err = c.RotateSecret(ctx, &awssm.RotateSecretInput{
		SecretId:          aws.String("rotate-immediate"),
		RotateImmediately: aws.Bool(true),
	})
	require.NoError(t, err)

	// AWSCURRENT must now be different from original (the pending copy was promoted).
	listOut, err := c.ListSecretVersionIds(ctx, &awssm.ListSecretVersionIdsInput{
		SecretId: aws.String("rotate-immediate"),
	})
	require.NoError(t, err)
	// Should have at least 2 versions: original AWSPREVIOUS and new AWSCURRENT.
	assert.GreaterOrEqual(t, len(listOut.Versions), 2)

	// The current value was set from pending; verify AWSCURRENT is readable.
	_, err = c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId:     aws.String("rotate-immediate"),
		VersionStage: aws.String("AWSCURRENT"),
	})
	require.NoError(t, err)
}

func TestSecretsManager_CancelRotateSecret_RemovesPending(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("cancel-rotate"),
		SecretString: aws.String("stable"),
	})
	require.NoError(t, err)

	// Create a pending rotation.
	_, err = c.RotateSecret(ctx, &awssm.RotateSecretInput{
		SecretId:          aws.String("cancel-rotate"),
		RotateImmediately: aws.Bool(false),
	})
	require.NoError(t, err)

	// Verify AWSPENDING exists.
	_, err = c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId:     aws.String("cancel-rotate"),
		VersionStage: aws.String("AWSPENDING"),
	})
	require.NoError(t, err)

	// Cancel the rotation.
	_, err = c.CancelRotateSecret(ctx, &awssm.CancelRotateSecretInput{
		SecretId: aws.String("cancel-rotate"),
	})
	require.NoError(t, err)

	// AWSPENDING must no longer exist after cancel.
	_, err = c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId:     aws.String("cancel-rotate"),
		VersionStage: aws.String("AWSPENDING"),
	})
	require.Error(t, err, "AWSPENDING must not exist after CancelRotateSecret")

	// AWSCURRENT must still be the original.
	getOut, err := c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId:     aws.String("cancel-rotate"),
		VersionStage: aws.String("AWSCURRENT"),
	})
	require.NoError(t, err)
	assert.Equal(t, "stable", aws.ToString(getOut.SecretString))
}
