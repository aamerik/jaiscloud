package key_test

import (
	"context"
	"encoding/base64"
	"testing"

	"jaiscloud/internal/key"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func newKeyProvider(t *testing.T) (*key.KeyProvider, *key.MemoryKeyStore) {
	t.Helper()
	dek, err := key.Generate32()
	require.NoError(t, err)
	store := key.NewMemoryKeyStore()
	p := key.New(store, nil, dek)
	return p, store
}

func nr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    "kms",
		Region:     "us-east-1",
		AccountID:  "000000000000",
		Params:     params,
		ResourceID: func(rtype, name string) string { return "arn:aws:kms:us-east-1:000000000000:key/" + name },
	}
}

func callKey(t *testing.T, routes map[string]provider.HandlerFunc, action string, params map[string]any) map[string]any {
	t.Helper()
	resp, err := routes["Key."+action](context.Background(), nr(params))
	require.NoError(t, err)
	return resp.Data
}

// ─── CreateKey / DescribeKey ──────────────────────────────────────────────────

func TestKeyProvider_CreateDescribeKey(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{
		"Description": "test key",
		"KeyUsage":    "ENCRYPT_DECRYPT",
	})
	meta := data["KeyMetadata"].(map[string]any)
	keyID, _ := meta["KeyId"].(string)
	require.NotEmpty(t, keyID)
	assert.Equal(t, "test key", meta["Description"])
	assert.Equal(t, true, meta["Enabled"])

	// DescribeKey by ID
	desc := callKey(t, routes, "DescribeKey", map[string]any{"KeyId": keyID})
	descMeta := desc["KeyMetadata"].(map[string]any)
	assert.Equal(t, keyID, descMeta["KeyId"])
}

// ─── Enable / Disable ─────────────────────────────────────────────────────────

func TestKeyProvider_EnableDisableKey(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	callKey(t, routes, "DisableKey", map[string]any{"KeyId": keyID})
	desc := callKey(t, routes, "DescribeKey", map[string]any{"KeyId": keyID})
	assert.Equal(t, false, desc["KeyMetadata"].(map[string]any)["Enabled"])

	callKey(t, routes, "EnableKey", map[string]any{"KeyId": keyID})
	desc = callKey(t, routes, "DescribeKey", map[string]any{"KeyId": keyID})
	assert.Equal(t, true, desc["KeyMetadata"].(map[string]any)["Enabled"])
}

// ─── Aliases ──────────────────────────────────────────────────────────────────

func TestKeyProvider_AliasCRUD(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	callKey(t, routes, "CreateAlias", map[string]any{
		"AliasName":   "alias/mykey",
		"TargetKeyId": keyID,
	})

	// DescribeKey via alias
	desc := callKey(t, routes, "DescribeKey", map[string]any{"KeyId": "alias/mykey"})
	assert.Equal(t, keyID, desc["KeyMetadata"].(map[string]any)["KeyId"])

	// ListAliases
	list := callKey(t, routes, "ListAliases", map[string]any{"KeyId": keyID})
	aliases := list["Aliases"].([]map[string]any)
	assert.Len(t, aliases, 1)
	assert.Equal(t, "alias/mykey", aliases[0]["AliasName"])

	// DeleteAlias
	callKey(t, routes, "DeleteAlias", map[string]any{"AliasName": "alias/mykey"})
	list2 := callKey(t, routes, "ListAliases", map[string]any{"KeyId": keyID})
	assert.Empty(t, list2["Aliases"])
}

// ─── Encrypt / Decrypt ────────────────────────────────────────────────────────

func TestKeyProvider_EncryptDecryptRoundTrip(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	plaintext := []byte("hello secret")
	// Encrypt: Plaintext param must be a base64 string; response CiphertextBlob is base64 string.
	encData := callKey(t, routes, "Encrypt", map[string]any{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString(plaintext),
	})
	ctB64, _ := encData["CiphertextBlob"].(string)
	require.NotEmpty(t, ctB64)

	// Decrypt: CiphertextBlob param is base64 string; Plaintext response is base64 string.
	decData := callKey(t, routes, "Decrypt", map[string]any{
		"CiphertextBlob": ctB64,
	})
	ptB64, _ := decData["Plaintext"].(string)
	require.NotEmpty(t, ptB64)
	decoded, err := base64.StdEncoding.DecodeString(ptB64)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decoded)
}

// ─── GenerateDataKey ──────────────────────────────────────────────────────────

func TestKeyProvider_GenerateDataKey(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	// KeySpec "AES_256" → 32-byte key; response values are base64 strings.
	gdkData := callKey(t, routes, "GenerateDataKey", map[string]any{
		"KeyId":   keyID,
		"KeySpec": "AES_256",
	})
	ptB64, _ := gdkData["Plaintext"].(string)
	ctB64, _ := gdkData["CiphertextBlob"].(string)
	require.NotEmpty(t, ptB64)
	require.NotEmpty(t, ctB64)
	ptBytes, err := base64.StdEncoding.DecodeString(ptB64)
	require.NoError(t, err)
	assert.Len(t, ptBytes, 32)
}

// ─── Grants ───────────────────────────────────────────────────────────────────

func TestKeyProvider_GrantCRUD(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	grantData := callKey(t, routes, "CreateGrant", map[string]any{
		"KeyId":            keyID,
		"GranteePrincipal": "arn:aws:iam::000000000000:role/test",
		"Operations":       []any{"Encrypt", "Decrypt"},
	})
	grantID, _ := grantData["GrantId"].(string)
	require.NotEmpty(t, grantID)

	listData := callKey(t, routes, "ListGrants", map[string]any{"KeyId": keyID})
	grants := listData["Grants"].([]map[string]any)
	assert.Len(t, grants, 1)

	callKey(t, routes, "RevokeGrant", map[string]any{"KeyId": keyID, "GrantId": grantID})
	listData2 := callKey(t, routes, "ListGrants", map[string]any{"KeyId": keyID})
	assert.Empty(t, listData2["Grants"])
}

// ─── ListKeys ─────────────────────────────────────────────────────────────────

func TestKeyProvider_ListKeys(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	for i := 0; i < 3; i++ {
		callKey(t, routes, "CreateKey", map[string]any{})
	}
	list := callKey(t, routes, "ListKeys", map[string]any{})
	keys := list["Keys"].([]map[string]any)
	assert.Len(t, keys, 3)
}

// ─── ScheduleKeyDeletion ──────────────────────────────────────────────────────

func TestKeyProvider_ScheduleKeyDeletion(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	delData := callKey(t, routes, "ScheduleKeyDeletion", map[string]any{
		"KeyId":               keyID,
		"PendingWindowInDays": float64(7),
	})
	assert.NotZero(t, delData["DeletionDate"])

	// Key still describable but in PendingDeletion state.
	descData, err := routes["Key.DescribeKey"](context.Background(), nr(map[string]any{"KeyId": keyID}))
	require.NoError(t, err)
	meta := descData.Data["KeyMetadata"].(map[string]any)
	assert.Equal(t, "PendingDeletion", meta["KeyState"])
}

// ─── P1.5: ScheduleKeyDeletion validation ────────────────────────────────────

func TestKeyProvider_ScheduleKeyDeletion_InvalidWindow(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	_, err := routes["Key.ScheduleKeyDeletion"](context.Background(), nr(map[string]any{
		"KeyId":               keyID,
		"PendingWindowInDays": float64(6), // too small
	}))
	require.Error(t, err)

	_, err = routes["Key.ScheduleKeyDeletion"](context.Background(), nr(map[string]any{
		"KeyId":               keyID,
		"PendingWindowInDays": float64(31), // too large
	}))
	require.Error(t, err)
}

// ─── P1.6: CancelKeyDeletion ─────────────────────────────────────────────────

func TestKeyProvider_CancelKeyDeletion(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	callKey(t, routes, "ScheduleKeyDeletion", map[string]any{
		"KeyId":               keyID,
		"PendingWindowInDays": float64(7),
	})

	// Verify it's pending.
	desc := callKey(t, routes, "DescribeKey", map[string]any{"KeyId": keyID})
	assert.Equal(t, "PendingDeletion", desc["KeyMetadata"].(map[string]any)["KeyState"])

	// Cancel.
	callKey(t, routes, "CancelKeyDeletion", map[string]any{"KeyId": keyID})

	// Now enabled again.
	desc2 := callKey(t, routes, "DescribeKey", map[string]any{"KeyId": keyID})
	assert.Equal(t, "Enabled", desc2["KeyMetadata"].(map[string]any)["KeyState"])
}

func TestKeyProvider_CancelKeyDeletion_NotPending(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	_, err := routes["Key.CancelKeyDeletion"](context.Background(), nr(map[string]any{"KeyId": keyID}))
	require.Error(t, err)
}

func TestKeyProvider_ScheduleKeyDeletion_AlreadyPending(t *testing.T) {
	p, _ := newKeyProvider(t)
	routes := p.Routes()

	data := callKey(t, routes, "CreateKey", map[string]any{})
	keyID := data["KeyMetadata"].(map[string]any)["KeyId"].(string)

	callKey(t, routes, "ScheduleKeyDeletion", map[string]any{
		"KeyId":               keyID,
		"PendingWindowInDays": float64(7),
	})

	// Second schedule on the same key must fail.
	_, err := routes["Key.ScheduleKeyDeletion"](context.Background(), nr(map[string]any{
		"KeyId":               keyID,
		"PendingWindowInDays": float64(7),
	}))
	require.Error(t, err)
}
