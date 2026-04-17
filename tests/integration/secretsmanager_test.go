package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecretsManager_CreateDescribeSecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	out, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:        aws.String("test/db-password"),
		Description: aws.String("database password"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.ARN))
	assert.Equal(t, "test/db-password", aws.ToString(out.Name))

	desc, err := c.DescribeSecret(ctx, &awssm.DescribeSecretInput{
		SecretId: aws.String("test/db-password"),
	})
	require.NoError(t, err)
	assert.Equal(t, "database password", aws.ToString(desc.Description))
}

func TestSecretsManager_CreateDuplicateFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("dup/secret")})
	require.NoError(t, err)

	_, err = c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("dup/secret")})
	require.Error(t, err, "duplicate create should fail")
}

func TestSecretsManager_PutGetSecretValue_String(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("app/api-key")})
	require.NoError(t, err)

	putOut, err := c.PutSecretValue(ctx, &awssm.PutSecretValueInput{
		SecretId:     aws.String("app/api-key"),
		SecretString: aws.String(`{"key":"sk-test-123"}`),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.VersionId))

	getOut, err := c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("app/api-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, `{"key":"sk-test-123"}`, aws.ToString(getOut.SecretString))
}

func TestSecretsManager_PutGetSecretValue_Binary(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("app/binary-secret")})
	require.NoError(t, err)

	binaryData := []byte("binary\x00secret\xFF")
	_, err = c.PutSecretValue(ctx, &awssm.PutSecretValueInput{
		SecretId:     aws.String("app/binary-secret"),
		SecretBinary: binaryData,
	})
	require.NoError(t, err)

	getOut, err := c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("app/binary-secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, binaryData, getOut.SecretBinary)
}

func TestSecretsManager_CreateWithInitialValue(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("app/with-value"),
		SecretString: aws.String("initial-secret"),
	})
	require.NoError(t, err)

	getOut, err := c.GetSecretValue(ctx, &awssm.GetSecretValueInput{
		SecretId: aws.String("app/with-value"),
	})
	require.NoError(t, err)
	assert.Equal(t, "initial-secret", aws.ToString(getOut.SecretString))
}

func TestSecretsManager_ListSecrets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	for _, n := range []string{"s1", "s2", "s3"} {
		_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String(n)})
		require.NoError(t, err)
	}

	out, err := c.ListSecrets(ctx, &awssm.ListSecretsInput{})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 3)
}

func TestSecretsManager_DeleteRestoreSecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("to-delete")})
	require.NoError(t, err)

	// Soft delete.
	_, err = c.DeleteSecret(ctx, &awssm.DeleteSecretInput{
		SecretId: aws.String("to-delete"),
	})
	require.NoError(t, err)

	// Restore.
	restoreOut, err := c.RestoreSecret(ctx, &awssm.RestoreSecretInput{
		SecretId: aws.String("to-delete"),
	})
	require.NoError(t, err)
	assert.Equal(t, "to-delete", aws.ToString(restoreOut.Name))

	// Describe should succeed after restore.
	_, err = c.DescribeSecret(ctx, &awssm.DescribeSecretInput{SecretId: aws.String("to-delete")})
	require.NoError(t, err)
}

func TestSecretsManager_ForceDeleteSecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("force-del")})
	require.NoError(t, err)

	_, err = c.DeleteSecret(ctx, &awssm.DeleteSecretInput{
		SecretId:                   aws.String("force-del"),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = c.DescribeSecret(ctx, &awssm.DescribeSecretInput{SecretId: aws.String("force-del")})
	require.Error(t, err, "force-deleted secret should not be found")
}

func TestSecretsManager_UpdateSecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:        aws.String("upd-secret"),
		Description: aws.String("original"),
	})
	require.NoError(t, err)

	_, err = c.UpdateSecret(ctx, &awssm.UpdateSecretInput{
		SecretId:    aws.String("upd-secret"),
		Description: aws.String("updated"),
	})
	require.NoError(t, err)

	desc, err := c.DescribeSecret(ctx, &awssm.DescribeSecretInput{SecretId: aws.String("upd-secret")})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.Description))
}

func TestSecretsManager_ListSecretVersionIds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{Name: aws.String("versioned")})
	require.NoError(t, err)

	for _, v := range []string{"v1", "v2", "v3"} {
		_, err = c.PutSecretValue(ctx, &awssm.PutSecretValueInput{
			SecretId:     aws.String("versioned"),
			SecretString: aws.String(v),
		})
		require.NoError(t, err)
	}

	listOut, err := c.ListSecretVersionIds(ctx, &awssm.ListSecretVersionIdsInput{
		SecretId: aws.String("versioned"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Versions, 3)
}
