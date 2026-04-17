package secret_test

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/secret"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func newSecretProvider(t *testing.T) *secret.SecretProvider {
	t.Helper()
	store := secret.NewMemorySecretStore()
	return secret.New(store, &model.NoopKeyEncryptor{})
}

func snr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    "secretsmanager",
		Region:     "us-east-1",
		AccountID:  "000000000000",
		Params:     params,
		ResourceID: func(_, name string) string { return "arn:aws:secretsmanager:us-east-1:000000000000:secret:" + name },
	}
}

func callSecret(t *testing.T, routes map[string]provider.HandlerFunc, action string, params map[string]any) map[string]any {
	t.Helper()
	resp, err := routes["Secret."+action](context.Background(), snr(params))
	require.NoError(t, err)
	return resp.Data
}

// ─── Create / Describe ───────────────────────────────────────────────────────

func TestSecretProvider_CreateDescribe(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	data := callSecret(t, routes, "CreateSecret", map[string]any{
		"Name":        "my/secret",
		"Description": "test secret",
	})
	assert.NotEmpty(t, data["ARN"])
	assert.Equal(t, "my/secret", data["Name"])

	desc := callSecret(t, routes, "DescribeSecret", map[string]any{"SecretId": "my/secret"})
	assert.Equal(t, "test secret", desc["Description"])
}

func TestSecretProvider_CreateDuplicateFails(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "dup"})
	_, err := routes["Secret.CreateSecret"](context.Background(), snr(map[string]any{"Name": "dup"}))
	require.Error(t, err)
}

// ─── PutSecretValue / GetSecretValue ─────────────────────────────────────────

func TestSecretProvider_PutGetSecretValue(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "app/creds"})

	putData := callSecret(t, routes, "PutSecretValue", map[string]any{
		"SecretId":     "app/creds",
		"SecretString": `{"user":"admin","pass":"hunter2"}`,
	})
	require.NotEmpty(t, putData["VersionId"])

	getOut := callSecret(t, routes, "GetSecretValue", map[string]any{"SecretId": "app/creds"})
	assert.Equal(t, `{"user":"admin","pass":"hunter2"}`, getOut["SecretString"])
}

func TestSecretProvider_CreateWithInitialValue(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{
		"Name":         "init/val",
		"SecretString": "initial-value",
	})

	getOut := callSecret(t, routes, "GetSecretValue", map[string]any{"SecretId": "init/val"})
	assert.Equal(t, "initial-value", getOut["SecretString"])
}

// ─── ListSecrets ──────────────────────────────────────────────────────────────

func TestSecretProvider_ListSecrets(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	for _, n := range []string{"s1", "s2", "s3"} {
		callSecret(t, routes, "CreateSecret", map[string]any{"Name": n})
	}
	list := callSecret(t, routes, "ListSecrets", map[string]any{})
	secrets := list["SecretList"].([]map[string]any)
	assert.Len(t, secrets, 3)
}

// ─── Delete / Restore ─────────────────────────────────────────────────────────

func TestSecretProvider_DeleteRestoreSecret(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "del-me"})
	callSecret(t, routes, "DeleteSecret", map[string]any{
		"SecretId": "del-me", "ForceDeleteWithoutRecovery": true,
	})

	// After force-delete, DescribeSecret should fail.
	_, err := routes["Secret.DescribeSecret"](context.Background(), snr(map[string]any{"SecretId": "del-me"}))
	require.Error(t, err)
}

func TestSecretProvider_SoftDeleteRestore(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "soft-del"})
	callSecret(t, routes, "DeleteSecret", map[string]any{"SecretId": "soft-del"})

	// Restore brings it back.
	restoreOut := callSecret(t, routes, "RestoreSecret", map[string]any{"SecretId": "soft-del"})
	assert.Equal(t, "soft-del", restoreOut["Name"])

	desc := callSecret(t, routes, "DescribeSecret", map[string]any{"SecretId": "soft-del"})
	assert.Equal(t, "soft-del", desc["Name"])
}

// ─── UpdateSecret ─────────────────────────────────────────────────────────────

func TestSecretProvider_UpdateSecret(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "upd", "Description": "old"})
	callSecret(t, routes, "UpdateSecret", map[string]any{"SecretId": "upd", "Description": "new"})

	desc := callSecret(t, routes, "DescribeSecret", map[string]any{"SecretId": "upd"})
	assert.Equal(t, "new", desc["Description"])
}

// ─── ListSecretVersionIds ─────────────────────────────────────────────────────

func TestSecretProvider_ListSecretVersionIds(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "versions"})
	callSecret(t, routes, "PutSecretValue", map[string]any{"SecretId": "versions", "SecretString": "v1"})
	callSecret(t, routes, "PutSecretValue", map[string]any{"SecretId": "versions", "SecretString": "v2"})

	listV := callSecret(t, routes, "ListSecretVersionIds", map[string]any{"SecretId": "versions"})
	versions := listV["Versions"].([]map[string]any)
	assert.Len(t, versions, 2)
}
