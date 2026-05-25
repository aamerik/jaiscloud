package secret_test

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
		"jaiscloud/internal/aws/secret"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSecretStore() *secret.MemorySecretStore { return secret.NewMemorySecretStore() }

func TestMemorySecretStore_CreateGet(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()

	e := secret.SecretEntry{SecretID: "s1", Name: "my/secret", Description: "test"}
	require.NoError(t, s.CreateSecret(ctx, e))

	got, err := s.GetSecret(ctx, "s1")
	require.NoError(t, err)
	assert.Equal(t, "my/secret", got.Name)

	got, err = s.GetSecretByName(ctx, "", "my/secret")
	require.NoError(t, err)
	assert.Equal(t, "s1", got.SecretID)

	assert.ErrorIs(t, s.CreateSecret(ctx, e), secret.ErrAlreadyExists)
}

func TestMemorySecretStore_Update(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})

	err := s.UpdateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1", Description: "updated"})
	require.NoError(t, err)

	got, _ := s.GetSecret(ctx, "s1")
	assert.Equal(t, "updated", got.Description)
}

func TestMemorySecretStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})
	require.NoError(t, s.DeleteSecret(ctx, "s1"))
	_, err := s.GetSecret(ctx, "s1")
	assert.ErrorIs(t, err, secret.ErrSecretNotFound)
	_, err = s.GetSecretByName(ctx, "", "n1")
	assert.ErrorIs(t, err, secret.ErrSecretNotFound)
}

func TestMemorySecretStore_VersionStagePromotion(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})

	v1 := secret.VersionEntry{SecretID: "s1", VersionID: "v1", SecretBinary: []byte("val1"), Stages: []string{"AWSCURRENT"}}
	require.NoError(t, s.PutVersion(ctx, v1))

	v2 := secret.VersionEntry{SecretID: "s1", VersionID: "v2", SecretBinary: []byte("val2"), Stages: []string{"AWSCURRENT"}}
	require.NoError(t, s.PutVersion(ctx, v2))

	// v1 should have been demoted to AWSPREVIOUS.
	got, err := s.GetVersionByStage(ctx, "s1", "AWSCURRENT")
	require.NoError(t, err)
	assert.Equal(t, "v2", got.VersionID)

	prev, err := s.GetVersionByStage(ctx, "s1", "AWSPREVIOUS")
	require.NoError(t, err)
	assert.Equal(t, "v1", prev.VersionID)
}

func TestMemorySecretStore_GetVersionNotFound(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})
	_, err := s.GetVersion(ctx, "s1", "nonexistent")
	assert.ErrorIs(t, err, secret.ErrVersionNotFound)
}

func TestMemorySecretStore_ListVersions(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})
	s.PutVersion(ctx, secret.VersionEntry{SecretID: "s1", VersionID: "v1", Stages: []string{"AWSCURRENT"}})
	s.PutVersion(ctx, secret.VersionEntry{SecretID: "s1", VersionID: "v2", Stages: []string{"AWSCURRENT"}})

	versions, err := s.ListVersions(ctx, "s1")
	require.NoError(t, err)
	assert.Len(t, versions, 2)
}

// ─── P1.2: New SecretEntry fields ────────────────────────────────────────────

func TestSecretEntry_NewFieldsDefault(t *testing.T) {
	e := secret.SecretEntry{SecretID: "s1", Name: "n1"}
	assert.False(t, e.RotationEnabled)
	assert.Empty(t, e.RotationLambdaARN)
	assert.Zero(t, e.AutoRotateAfterDays)
	assert.Nil(t, e.LastRotatedDate)
	assert.Nil(t, e.NextRotationDate)
	assert.Empty(t, e.ResourcePolicy)
}

func TestSecretEntry_SerializeDeserialize(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()

	now := clock.RealNow().Truncate(time.Second)
	e := secret.SecretEntry{
		SecretID:            "s-rt",
		Name:                "rt/secret",
		RotationEnabled:     true,
		RotationLambdaARN:   "arn:aws:lambda:us-east-1:000000000000:function:rotate",
		AutoRotateAfterDays: 30,
		NextRotationDate:    &now,
		ResourcePolicy:      `{"Version":"2012-10-17"}`,
	}
	require.NoError(t, s.CreateSecret(ctx, e))

	got, err := s.GetSecret(ctx, "s-rt")
	require.NoError(t, err)
	assert.True(t, got.RotationEnabled)
	assert.Equal(t, e.RotationLambdaARN, got.RotationLambdaARN)
	assert.Equal(t, 30, got.AutoRotateAfterDays)
	require.NotNil(t, got.NextRotationDate)
	assert.Equal(t, now, got.NextRotationDate.UTC().Truncate(time.Second))
	assert.Equal(t, e.ResourcePolicy, got.ResourcePolicy)
}

func TestSecretEntry_BackwardsCompatible(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()

	e := secret.SecretEntry{SecretID: "s-old", Name: "old/secret", Description: "legacy"}
	require.NoError(t, s.CreateSecret(ctx, e))

	got, err := s.GetSecret(ctx, "s-old")
	require.NoError(t, err)
	assert.False(t, got.RotationEnabled)
	assert.Empty(t, got.RotationLambdaARN)
	assert.Zero(t, got.AutoRotateAfterDays)
	assert.Nil(t, got.NextRotationDate)
	assert.Empty(t, got.ResourcePolicy)
}

func TestMemorySecretStore_Reset(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})
	s.Reset(context.Background())
	secrets, _ := s.ListSecrets(ctx, "")
	assert.Empty(t, secrets)
}
