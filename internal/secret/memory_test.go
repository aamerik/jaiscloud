package secret_test

import (
	"context"
	"testing"

	"jaiscloud/internal/secret"

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

	got, err = s.GetSecretByName(ctx, "my/secret")
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
	_, err = s.GetSecretByName(ctx, "n1")
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

func TestMemorySecretStore_Reset(t *testing.T) {
	ctx := context.Background()
	s := newSecretStore()
	s.CreateSecret(ctx, secret.SecretEntry{SecretID: "s1", Name: "n1"})
	s.Reset()
	secrets, _ := s.ListSecrets(ctx)
	assert.Empty(t, secrets)
}
