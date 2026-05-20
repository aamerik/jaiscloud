package key_test

import (
	"context"
	"testing"

	"jaiscloud/internal/aws/key"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStore() *key.MemoryKeyStore { return key.NewMemoryKeyStore() }

func TestMemoryKeyStore_KeyCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	e := key.KeyEntry{KeyID: "k1", Enabled: true, Description: "test", KeyUsage: "ENCRYPT_DECRYPT"}
	require.NoError(t, s.CreateKey(ctx, e))

	got, err := s.GetKey(ctx, "k1")
	require.NoError(t, err)
	assert.Equal(t, e.KeyID, got.KeyID)
	assert.Equal(t, e.Description, got.Description)

	// Duplicate create should fail.
	assert.ErrorIs(t, s.CreateKey(ctx, e), key.ErrAlreadyExists)

	// Update.
	e.Description = "updated"
	require.NoError(t, s.UpdateKey(ctx, e))
	got, _ = s.GetKey(ctx, "k1")
	assert.Equal(t, "updated", got.Description)

	// Delete.
	require.NoError(t, s.DeleteKey(ctx, "k1"))
	_, err = s.GetKey(ctx, "k1")
	assert.ErrorIs(t, err, key.ErrKeyNotFound)
}

func TestMemoryKeyStore_ListKeys(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	for _, id := range []string{"k1", "k2", "k3"} {
		s.CreateKey(ctx, key.KeyEntry{KeyID: id, Enabled: true})
	}
	keys, err := s.ListKeys(ctx, "")
	require.NoError(t, err)
	assert.Len(t, keys, 3)
}

func TestMemoryKeyStore_AliasCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	s.CreateKey(ctx, key.KeyEntry{KeyID: "k1", Enabled: true})

	require.NoError(t, s.CreateAlias(ctx, key.AliasEntry{AliasName: "alias/mykey", TargetKeyID: "k1"}))

	a, err := s.GetAlias(ctx, "alias/mykey")
	require.NoError(t, err)
	assert.Equal(t, "k1", a.TargetKeyID)

	aliases, err := s.ListAliases(ctx, "k1")
	require.NoError(t, err)
	assert.Len(t, aliases, 1)

	assert.ErrorIs(t, s.CreateAlias(ctx, key.AliasEntry{AliasName: "alias/mykey", TargetKeyID: "k1"}), key.ErrAlreadyExists)
	require.NoError(t, s.DeleteAlias(ctx, "alias/mykey"))
	assert.ErrorIs(t, s.DeleteAlias(ctx, "alias/mykey"), key.ErrAliasNotFound)
}

func TestMemoryKeyStore_GrantCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	g := key.GrantEntry{GrantID: "g1", KeyID: "k1", Operations: []string{"Encrypt"}}
	require.NoError(t, s.CreateGrant(ctx, g))

	got, err := s.GetGrant(ctx, "g1")
	require.NoError(t, err)
	assert.Equal(t, "k1", got.KeyID)

	grants, err := s.ListGrants(ctx, "k1")
	require.NoError(t, err)
	assert.Len(t, grants, 1)

	require.NoError(t, s.RevokeGrant(ctx, "g1"))
	assert.ErrorIs(t, s.RevokeGrant(ctx, "g1"), key.ErrGrantNotFound)
}

func TestMemoryKeyStore_DEK(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	// Initially absent.
	_, err := s.LoadDEK(ctx)
	assert.ErrorIs(t, err, key.ErrKeyNotFound)

	blob := []byte{0x01, 0x02, 0x03}
	require.NoError(t, s.StoreDEK(ctx, blob))

	got, err := s.LoadDEK(ctx)
	require.NoError(t, err)
	assert.Equal(t, blob, got)
}

func TestMemoryKeyStore_Reset(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	s.CreateKey(ctx, key.KeyEntry{KeyID: "k1", Enabled: true})
	s.StoreDEK(ctx, []byte{1, 2, 3})

	s.Reset(context.Background())

	keys, _ := s.ListKeys(ctx, "")
	assert.Empty(t, keys)
	// DEK is wiped on Reset.
	_, err := s.LoadDEK(ctx)
	assert.ErrorIs(t, err, key.ErrKeyNotFound)
}

// ─── P1.1: New KeyEntry fields ───────────────────────────────────────────────

func TestKeyEntry_NewFieldsDefault(t *testing.T) {
	e := key.KeyEntry{KeyID: "k1", Enabled: true}
	assert.Nil(t, e.PrivateKey)
	assert.Nil(t, e.PublicKey)
	assert.False(t, e.MultiRegion)
	assert.False(t, e.RotationEnabled)
	assert.Zero(t, e.RotationPeriodInDays)
	assert.Nil(t, e.PreviousKeyMaterials)
	assert.Empty(t, e.Policy)
}

func TestKeyEntry_SerializeDeserialize(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	e := key.KeyEntry{
		KeyID:                "k-rt",
		Enabled:              true,
		PrivateKey:           []byte{1, 2, 3},
		PublicKey:            []byte{4, 5, 6},
		MultiRegion:          true,
		RotationEnabled:      true,
		RotationPeriodInDays: 90,
		PreviousKeyMaterials: [][]byte{{10, 11}, {12, 13}},
		Policy:               `{"Version":"2012-10-17"}`,
	}
	require.NoError(t, s.CreateKey(ctx, e))

	got, err := s.GetKey(ctx, "k-rt")
	require.NoError(t, err)
	assert.Equal(t, e.PrivateKey, got.PrivateKey)
	assert.Equal(t, e.PublicKey, got.PublicKey)
	assert.True(t, got.MultiRegion)
	assert.True(t, got.RotationEnabled)
	assert.Equal(t, 90, got.RotationPeriodInDays)
	assert.Equal(t, e.PreviousKeyMaterials, got.PreviousKeyMaterials)
	assert.Equal(t, e.Policy, got.Policy)
}

func TestKeyEntry_BackwardsCompatible(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	// Entry using only pre-P1.1 fields; new fields must remain at zero values.
	e := key.KeyEntry{
		KeyID:       "k-old",
		Enabled:     true,
		Description: "legacy key",
		KeyUsage:    "ENCRYPT_DECRYPT",
		KeySpec:     "SYMMETRIC_DEFAULT",
	}
	require.NoError(t, s.CreateKey(ctx, e))

	got, err := s.GetKey(ctx, "k-old")
	require.NoError(t, err)
	assert.Nil(t, got.PrivateKey)
	assert.Nil(t, got.PublicKey)
	assert.False(t, got.MultiRegion)
	assert.False(t, got.RotationEnabled)
	assert.Zero(t, got.RotationPeriodInDays)
	assert.Nil(t, got.PreviousKeyMaterials)
	assert.Empty(t, got.Policy)
}

func TestMemoryKeyStore_DeleteKey_CascadesAliasesGrants(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	s.CreateKey(ctx, key.KeyEntry{KeyID: "k1", Enabled: true})
	s.CreateAlias(ctx, key.AliasEntry{AliasName: "alias/a1", TargetKeyID: "k1"})
	s.CreateGrant(ctx, key.GrantEntry{GrantID: "g1", KeyID: "k1"})

	require.NoError(t, s.DeleteKey(ctx, "k1"))

	_, err := s.GetAlias(ctx, "alias/a1")
	assert.ErrorIs(t, err, key.ErrAliasNotFound)
	_, err = s.GetGrant(ctx, "g1")
	assert.ErrorIs(t, err, key.ErrGrantNotFound)
}
