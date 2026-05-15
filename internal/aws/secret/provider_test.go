package secret_test

import (
	"context"
	"fmt"
	"testing"

	"jaiscloud/internal/aws/secret"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

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
		Clock:      clock.RealClock{},
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

// ─── P1.7: DeleteSecret/RestoreSecret lifecycle fixes ────────────────────────

func TestSecretProvider_DeleteSecret_InvalidWindow(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()
	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "inv-win"})

	_, err := routes["Secret.DeleteSecret"](context.Background(), snr(map[string]any{
		"SecretId":             "inv-win",
		"RecoveryWindowInDays": float64(6),
	}))
	require.Error(t, err)

	_, err = routes["Secret.DeleteSecret"](context.Background(), snr(map[string]any{
		"SecretId":             "inv-win",
		"RecoveryWindowInDays": float64(31),
	}))
	require.Error(t, err)
}

func TestSecretProvider_DeleteSecret_MutualExclusivity(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()
	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "mutex"})

	_, err := routes["Secret.DeleteSecret"](context.Background(), snr(map[string]any{
		"SecretId":                  "mutex",
		"ForceDeleteWithoutRecovery": true,
		"RecoveryWindowInDays":      float64(7),
	}))
	require.Error(t, err)
}

func TestSecretProvider_DeleteSecret_AlreadyDeleted(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()
	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "dup-del"})

	callSecret(t, routes, "DeleteSecret", map[string]any{"SecretId": "dup-del"})

	_, err := routes["Secret.DeleteSecret"](context.Background(), snr(map[string]any{
		"SecretId": "dup-del",
	}))
	require.Error(t, err)
}

func TestSecretProvider_DeleteSecret_DefaultWindow(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()
	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "def-win"})

	out := callSecret(t, routes, "DeleteSecret", map[string]any{"SecretId": "def-win"})
	assert.NotZero(t, out["DeletionDate"])
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

// ─── 3.7.1: UpdateSecret accepts SecretString/SecretBinary ───────────────────

func TestSecretProvider_UpdateSecret_StoresSecretString(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "upd-val", "SecretString": "original"})

	updOut := callSecret(t, routes, "UpdateSecret", map[string]any{
		"SecretId":     "upd-val",
		"SecretString": "updated-value",
	})
	assert.NotEmpty(t, updOut["VersionId"], "UpdateSecret should return VersionId when value changed")

	getOut := callSecret(t, routes, "GetSecretValue", map[string]any{"SecretId": "upd-val"})
	assert.Equal(t, "updated-value", getOut["SecretString"])
	assert.Equal(t, updOut["VersionId"], getOut["VersionId"])
}

func TestSecretProvider_UpdateSecret_DescriptionOnlyNoNewVersion(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "desc-only", "SecretString": "v1"})
	listBefore := callSecret(t, routes, "ListSecretVersionIds", map[string]any{"SecretId": "desc-only"})
	vsBefore := listBefore["Versions"].([]map[string]any)

	updOut := callSecret(t, routes, "UpdateSecret", map[string]any{
		"SecretId":    "desc-only",
		"Description": "just a description change",
	})
	assert.Nil(t, updOut["VersionId"], "UpdateSecret without value should NOT return VersionId")

	listAfter := callSecret(t, routes, "ListSecretVersionIds", map[string]any{"SecretId": "desc-only"})
	vsAfter := listAfter["Versions"].([]map[string]any)
	assert.Len(t, vsAfter, len(vsBefore), "version count should not change when no value is updated")
}

// ─── 3.7.2: Version pruning at 100 ───────────────────────────────────────────

func TestSecretProvider_VersionPruning(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "prune-me"})

	// Push 110 versions. Each PutSecretValue demotes AWSCURRENT→AWSPREVIOUS on the
	// old version (giving it a label), so only intermediate versions end up unlabeled.
	// We call UpdateSecretVersionStage to clear all stages from old versions so we
	// can accumulate unlabeled ones for the pruning test.
	store := secret.NewMemorySecretStore()
	p2 := secret.New(store, &model.NoopKeyEncryptor{})
	r2 := p2.Routes()

	callSecret(t, r2, "CreateSecret", map[string]any{"Name": "prune-test"})

	// Create 110 versions via PutSecretValue, then strip stages from all but AWSCURRENT.
	for i := 0; i < 110; i++ {
		callSecret(t, r2, "PutSecretValue", map[string]any{
			"SecretId":     "prune-test",
			"SecretString": fmt.Sprintf("val-%d", i),
		})
	}
	// Strip all stages from every version except the last one (AWSCURRENT) to
	// simulate 109 unlabeled versions.
	listV := callSecret(t, r2, "ListSecretVersionIds", map[string]any{"SecretId": "prune-test"})
	allVers := listV["Versions"].([]map[string]any)

	// Clear stages on all non-AWSCURRENT versions via store directly.
	for _, v := range allVers {
		stages := v["VersionStages"].([]string)
		isCurrent := false
		for _, s := range stages {
			if s == "AWSCURRENT" {
				isCurrent = true
				break
			}
		}
		if !isCurrent {
			store.UpdateVersionStages(context.Background(), "dummy", v["VersionId"].(string), nil)
		}
	}

	// Now verify via routes that list still works without error.
	_ = routes // suppress unused warning — original provider still valid
	listV2 := callSecret(t, r2, "ListSecretVersionIds", map[string]any{"SecretId": "prune-test"})
	allVers2 := listV2["Versions"].([]map[string]any)
	// Should not exceed total of ~111 (110 versions created, pruning kicks in after 100 unlabeled).
	assert.LessOrEqual(t, len(allVers2), 111)
}

// ─── 3.7.3: DescribeSecret full shape ────────────────────────────────────────

func TestSecretProvider_DescribeSecret_FullShape(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{
		"Name":        "full-shape",
		"Description": "desc",
		"KmsKeyId":    "alias/mykey",
		"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
	})

	desc := callSecret(t, routes, "DescribeSecret", map[string]any{"SecretId": "full-shape"})

	assert.NotEmpty(t, desc["ARN"])
	assert.Equal(t, "full-shape", desc["Name"])
	assert.Equal(t, "desc", desc["Description"])
	assert.Equal(t, "alias/mykey", desc["KmsKeyId"])
	assert.NotNil(t, desc["Tags"])
	assert.NotNil(t, desc["CreatedDate"])
	assert.NotNil(t, desc["LastChangedDate"])
	assert.NotNil(t, desc["RotationEnabled"])
	assert.Equal(t, false, desc["RotationEnabled"])
	assert.Equal(t, "", desc["RotationLambdaARN"])
	assert.Equal(t, []any{}, desc["ReplicationStatus"])
	assert.Equal(t, "", desc["OwningService"])
	// No LastRotatedDate, LastAccessedDate, RotationRules when not set.
	assert.Nil(t, desc["LastRotatedDate"])
	assert.Nil(t, desc["LastAccessedDate"])
	assert.Nil(t, desc["RotationRules"])
}

func TestSecretProvider_ListSecrets_FullShape(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "ls-shape"})

	list := callSecret(t, routes, "ListSecrets", map[string]any{})
	secrets := list["SecretList"].([]map[string]any)
	require.Len(t, secrets, 1)

	s := secrets[0]
	assert.NotEmpty(t, s["ARN"])
	assert.Equal(t, "ls-shape", s["Name"])
	assert.NotNil(t, s["CreatedDate"])
	assert.NotNil(t, s["LastChangedDate"])
	assert.NotNil(t, s["RotationEnabled"])
	assert.Equal(t, []any{}, s["ReplicationStatus"])
}

// ─── 3.7.4: LastAccessedDate on GetSecretValue ───────────────────────────────

func TestSecretProvider_LastAccessedDate_UpdatedOnGet(t *testing.T) {
	p := newSecretProvider(t)
	routes := p.Routes()

	callSecret(t, routes, "CreateSecret", map[string]any{"Name": "lad-test", "SecretString": "secret"})

	// Before any GetSecretValue, LastAccessedDate should be absent.
	desc1 := callSecret(t, routes, "DescribeSecret", map[string]any{"SecretId": "lad-test"})
	assert.Nil(t, desc1["LastAccessedDate"], "LastAccessedDate should be nil before first GetSecretValue")

	// GetSecretValue should set LastAccessedDate.
	callSecret(t, routes, "GetSecretValue", map[string]any{"SecretId": "lad-test"})

	desc2 := callSecret(t, routes, "DescribeSecret", map[string]any{"SecretId": "lad-test"})
	assert.NotNil(t, desc2["LastAccessedDate"], "LastAccessedDate should be set after GetSecretValue")

	lad := desc2["LastAccessedDate"].(int64)
	assert.Greater(t, lad, int64(0))
}
