package parameter_test

import (
	"context"
	"encoding/base64"
	"testing"

	"jaiscloud/internal/aws/parameter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func newParamProvider(t *testing.T) *parameter.ParameterProvider {
	t.Helper()
	store := parameter.NewMemoryParameterStore()
	return parameter.New(store, &model.NoopKeyEncryptor{})
}

func pnr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    "ssm",
		Region:     "us-east-1",
		AccountID:  "000000000000",
		Params:     params,
		ResourceID: func(_, name string) string { return "arn:aws:ssm:us-east-1:000000000000:parameter" + name },
	}
}

func callParam(t *testing.T, routes map[string]provider.HandlerFunc, action string, params map[string]any) map[string]any {
	t.Helper()
	resp, err := routes["Parameter."+action](context.Background(), pnr(params))
	require.NoError(t, err)
	return resp.Data
}

// ─── PutParameter / GetParameter ─────────────────────────────────────────────

func TestParameterProvider_PutGet(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/app/db-url", "Type": "String", "Value": "postgres://localhost",
	})

	out := callParam(t, routes, "GetParameter", map[string]any{"Name": "/app/db-url"})
	param := out["Parameter"].(map[string]any)
	assert.Equal(t, "postgres://localhost", param["Value"])
	assert.Equal(t, "String", param["Type"])
	assert.Equal(t, int64(1), param["Version"])
}

func TestParameterProvider_PutRequired(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	_, err := routes["Parameter.PutParameter"](context.Background(), pnr(map[string]any{
		"Type": "String", "Value": "x",
	}))
	require.Error(t, err)
}

// ─── Overwrite ────────────────────────────────────────────────────────────────

func TestParameterProvider_Overwrite(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/p", "Type": "String", "Value": "v1",
	})

	// No overwrite → error.
	_, err := routes["Parameter.PutParameter"](context.Background(), pnr(map[string]any{
		"Name": "/p", "Type": "String", "Value": "v2",
	}))
	require.Error(t, err)

	// With overwrite → success.
	callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/p", "Type": "String", "Value": "v2", "Overwrite": true,
	})

	out := callParam(t, routes, "GetParameter", map[string]any{"Name": "/p"})
	param := out["Parameter"].(map[string]any)
	assert.Equal(t, "v2", param["Value"])
	assert.Equal(t, int64(2), param["Version"])
}

// ─── GetParameters ────────────────────────────────────────────────────────────

func TestParameterProvider_GetParameters(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	for _, n := range []string{"/a", "/b", "/c"} {
		callParam(t, routes, "PutParameter", map[string]any{
			"Name": n, "Type": "String", "Value": "val",
		})
	}

	out := callParam(t, routes, "GetParameters", map[string]any{
		"Names": []any{"/a", "/b", "/nosuch"},
	})
	params := out["Parameters"].([]map[string]any)
	invalid := out["InvalidParameters"].([]string)
	assert.Len(t, params, 2)
	assert.Equal(t, []string{"/nosuch"}, invalid)
}

// ─── GetParametersByPath ──────────────────────────────────────────────────────

func TestParameterProvider_GetParametersByPath(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	for _, n := range []string{"/app/db", "/app/api/key", "/app/api/secret", "/other/x"} {
		callParam(t, routes, "PutParameter", map[string]any{
			"Name": n, "Type": "String", "Value": "v",
		})
	}

	out := callParam(t, routes, "GetParametersByPath", map[string]any{
		"Path": "/app/", "Recursive": true,
	})
	params := out["Parameters"].([]map[string]any)
	assert.Len(t, params, 3)
}

// ─── DeleteParameter ──────────────────────────────────────────────────────────

func TestParameterProvider_DeleteParameter(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/del", "Type": "String", "Value": "x",
	})
	callParam(t, routes, "DeleteParameter", map[string]any{"Name": "/del"})

	_, err := routes["Parameter.GetParameter"](context.Background(), pnr(map[string]any{"Name": "/del"}))
	require.Error(t, err)
}

// ─── DeleteParameters ─────────────────────────────────────────────────────────

func TestParameterProvider_DeleteParameters(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	for _, n := range []string{"/x", "/y"} {
		callParam(t, routes, "PutParameter", map[string]any{"Name": n, "Type": "String", "Value": "v"})
	}

	out := callParam(t, routes, "DeleteParameters", map[string]any{
		"Names": []any{"/x", "/y", "/nosuch"},
	})
	deleted := out["DeletedParameters"].([]string)
	invalid := out["InvalidParameters"].([]string)
	assert.Len(t, deleted, 2)
	assert.Equal(t, []string{"/nosuch"}, invalid)
}

// ─── GetParameterHistory ─────────────────────────────────────────────────────

func TestParameterProvider_History(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	callParam(t, routes, "PutParameter", map[string]any{"Name": "/h", "Type": "String", "Value": "v1"})
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/h", "Type": "String", "Value": "v2", "Overwrite": true})
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/h", "Type": "String", "Value": "v3", "Overwrite": true})

	out := callParam(t, routes, "GetParameterHistory", map[string]any{"Name": "/h"})
	history := out["Parameters"].([]map[string]any)
	// History contains archived versions (all but current).
	assert.Len(t, history, 2)
}

// ─── DescribeParameters ───────────────────────────────────────────────────────

func TestParameterProvider_DescribeParameters(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	for _, n := range []string{"/a", "/b"} {
		callParam(t, routes, "PutParameter", map[string]any{"Name": n, "Type": "String", "Value": "v"})
	}

	out := callParam(t, routes, "DescribeParameters", map[string]any{})
	params := out["Parameters"].([]map[string]any)
	assert.Len(t, params, 2)
}

// ─── Item 3.6.1: SecureString ciphertext without WithDecryption ───────────────

func TestParameterProvider_SecureStringCiphertext(t *testing.T) {
	store := parameter.NewMemoryParameterStore()
	kms := &model.NoopKeyEncryptor{}
	p := parameter.New(store, kms)
	routes := p.Routes()

	// Put a SecureString parameter.
	callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/sec/key", "Type": "SecureString", "Value": "mysecretvalue",
	})

	// Get without WithDecryption — should get base64-encoded ciphertext, not "****".
	out := callParam(t, routes, "GetParameter", map[string]any{
		"Name": "/sec/key", "WithDecryption": false,
	})
	param := out["Parameter"].(map[string]any)
	val := param["Value"].(string)
	assert.NotEqual(t, "****", val, "should not return literal ****")
	// Must be valid base64.
	_, err := base64.StdEncoding.DecodeString(val)
	assert.NoError(t, err, "ciphertext must be valid base64")

	// Get with WithDecryption — should get plaintext.
	out2 := callParam(t, routes, "GetParameter", map[string]any{
		"Name": "/sec/key", "WithDecryption": true,
	})
	param2 := out2["Parameter"].(map[string]any)
	assert.Equal(t, "mysecretvalue", param2["Value"])
}

// ─── Item 3.6.2: Tier inference ───────────────────────────────────────────────

func TestParameterProvider_TierInference(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	// Short value → Standard.
	out := callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/tier/small", "Type": "String", "Value": "small",
	})
	assert.Equal(t, "Standard", out["Tier"])

	// Explicit Advanced.
	out2 := callParam(t, routes, "PutParameter", map[string]any{
		"Name": "/tier/adv", "Type": "String", "Value": "x", "Tier": "Advanced",
	})
	assert.Equal(t, "Advanced", out2["Tier"])
}

// ─── Item 3.6.2: ParameterFilters ─────────────────────────────────────────────

func TestParameterProvider_DescribeParameters_Filters(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	callParam(t, routes, "PutParameter", map[string]any{"Name": "/app/db", "Type": "String", "Value": "v"})
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/app/key", "Type": "SecureString", "Value": "v"})
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/other/x", "Type": "String", "Value": "v"})

	// Filter by Type=SecureString.
	out := callParam(t, routes, "DescribeParameters", map[string]any{
		"ParameterFilters": []any{
			map[string]any{"Key": "Type", "Option": "Equals", "Values": []any{"SecureString"}},
		},
	})
	params := out["Parameters"].([]map[string]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/app/key", params[0]["Name"])

	// Filter by Name BeginsWith /app/.
	out2 := callParam(t, routes, "DescribeParameters", map[string]any{
		"ParameterFilters": []any{
			map[string]any{"Key": "Name", "Option": "BeginsWith", "Values": []any{"/app/"}},
		},
	})
	params2 := out2["Parameters"].([]map[string]any)
	assert.Len(t, params2, 2)
}

// ─── Item 3.6.2: Pagination ───────────────────────────────────────────────────

func TestParameterProvider_DescribeParameters_Pagination(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	for i := 0; i < 5; i++ {
		callParam(t, routes, "PutParameter", map[string]any{
			"Name": "/pg/" + string(rune('a'+i)), "Type": "String", "Value": "v",
		})
	}

	// Page 1: max 2.
	out1 := callParam(t, routes, "DescribeParameters", map[string]any{"MaxResults": float64(2)})
	params1 := out1["Parameters"].([]map[string]any)
	assert.Len(t, params1, 2)
	nextToken, ok := out1["NextToken"].(string)
	assert.True(t, ok, "NextToken must be present")

	// Page 2: next 2.
	out2 := callParam(t, routes, "DescribeParameters", map[string]any{"MaxResults": float64(2), "NextToken": nextToken})
	params2 := out2["Parameters"].([]map[string]any)
	assert.Len(t, params2, 2)

	// Page 3: last 1.
	nextToken2 := out2["NextToken"].(string)
	out3 := callParam(t, routes, "DescribeParameters", map[string]any{"MaxResults": float64(2), "NextToken": nextToken2})
	params3 := out3["Parameters"].([]map[string]any)
	assert.Len(t, params3, 1)
	_, hasMore := out3["NextToken"]
	assert.False(t, hasMore, "no more pages expected")
}

func TestParameterProvider_GetParametersByPath_Pagination(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	for i := 0; i < 5; i++ {
		callParam(t, routes, "PutParameter", map[string]any{
			"Name": "/path/" + string(rune('a'+i)), "Type": "String", "Value": "v",
		})
	}

	out1 := callParam(t, routes, "GetParametersByPath", map[string]any{
		"Path": "/path/", "Recursive": true, "MaxResults": float64(3),
	})
	params1 := out1["Parameters"].([]map[string]any)
	assert.Len(t, params1, 3)
	nextToken := out1["NextToken"].(string)

	out2 := callParam(t, routes, "GetParametersByPath", map[string]any{
		"Path": "/path/", "Recursive": true, "MaxResults": float64(3), "NextToken": nextToken,
	})
	params2 := out2["Parameters"].([]map[string]any)
	assert.Len(t, params2, 2)
}

// ─── Item 3.6.3: Name normalisation ──────────────────────────────────────────

func TestParameterProvider_NameNormalization(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	// Put without leading slash.
	callParam(t, routes, "PutParameter", map[string]any{
		"Name": "no-slash", "Type": "String", "Value": "val",
	})

	// Get with or without slash should work (stored as /no-slash).
	out := callParam(t, routes, "GetParameter", map[string]any{"Name": "no-slash"})
	param := out["Parameter"].(map[string]any)
	assert.Equal(t, "/no-slash", param["Name"])
	assert.Equal(t, "val", param["Value"])

	// Also retrievable with leading slash.
	out2 := callParam(t, routes, "GetParameter", map[string]any{"Name": "/no-slash"})
	param2 := out2["Parameter"].(map[string]any)
	assert.Equal(t, "val", param2["Value"])
}

// ─── Item 3.6.3: Version selector ─────────────────────────────────────────────

func TestParameterProvider_VersionSelector(t *testing.T) {
	p := newParamProvider(t)
	routes := p.Routes()

	callParam(t, routes, "PutParameter", map[string]any{"Name": "/ver", "Type": "String", "Value": "v1"})
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/ver", "Type": "String", "Value": "v2", "Overwrite": true})
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/ver", "Type": "String", "Value": "v3", "Overwrite": true})

	// Get specific version 1.
	out := callParam(t, routes, "GetParameter", map[string]any{"Name": "/ver:1"})
	param := out["Parameter"].(map[string]any)
	assert.Equal(t, "v1", param["Value"])
	assert.Equal(t, int64(1), param["Version"])

	// Get specific version 2.
	out2 := callParam(t, routes, "GetParameter", map[string]any{"Name": "/ver:2"})
	param2 := out2["Parameter"].(map[string]any)
	assert.Equal(t, "v2", param2["Value"])

	// Get current (v3).
	out3 := callParam(t, routes, "GetParameter", map[string]any{"Name": "/ver"})
	param3 := out3["Parameter"].(map[string]any)
	assert.Equal(t, "v3", param3["Value"])
}

// ─── Item 3.6.4: EventBridge events ──────────────────────────────────────────

type testEventPublisher struct {
	events []map[string]any
}

func (e *testEventPublisher) InternalPutEvents(_ context.Context, entries []map[string]any) error {
	e.events = append(e.events, entries...)
	return nil
}

func TestParameterProvider_EventBridgeEvents(t *testing.T) {
	store := parameter.NewMemoryParameterStore()
	p := parameter.New(store, nil)
	pub := &testEventPublisher{}
	p.SetEventPublisher(pub)
	routes := p.Routes()

	// Create emits "Create".
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/ev/p", "Type": "String", "Value": "v1"})
	require.Len(t, pub.events, 1)
	detail := pub.events[0]["Detail"].(map[string]any)
	assert.Equal(t, "Create", detail["operation"])
	assert.Equal(t, "/ev/p", detail["name"])

	// Update emits "Update".
	callParam(t, routes, "PutParameter", map[string]any{"Name": "/ev/p", "Type": "String", "Value": "v2", "Overwrite": true})
	require.Len(t, pub.events, 2)
	detail2 := pub.events[1]["Detail"].(map[string]any)
	assert.Equal(t, "Update", detail2["operation"])

	// Delete emits "Delete".
	callParam(t, routes, "DeleteParameter", map[string]any{"Name": "/ev/p"})
	require.Len(t, pub.events, 3)
	detail3 := pub.events[2]["Detail"].(map[string]any)
	assert.Equal(t, "Delete", detail3["operation"])
}

// ─── Item 3.6.5: SecretsManager bridge ───────────────────────────────────────

type testSecretGetter struct {
	secrets map[string]string
}

func (s *testSecretGetter) InternalGetSecretValue(_ context.Context, _, secretID string) (string, error) {
	v, ok := s.secrets[secretID]
	if !ok {
		return "", context.DeadlineExceeded // any non-nil error
	}
	return v, nil
}

func TestParameterProvider_SMBridge(t *testing.T) {
	store := parameter.NewMemoryParameterStore()
	p := parameter.New(store, nil)
	sg := &testSecretGetter{secrets: map[string]string{"my-secret": "supersecret"}}
	p.SetSecretGetter(sg)
	routes := p.Routes()

	// GetParameter with /aws/reference/secretsmanager/ prefix.
	out := callParam(t, routes, "GetParameter", map[string]any{
		"Name": "/aws/reference/secretsmanager/my-secret",
	})
	param := out["Parameter"].(map[string]any)
	assert.Equal(t, "supersecret", param["Value"])
	assert.Equal(t, "String", param["Type"])

	// Missing secret → ParameterNotFound error.
	_, err := routes["Parameter.GetParameter"](context.Background(), pnr(map[string]any{
		"Name": "/aws/reference/secretsmanager/no-such-secret",
	}))
	require.Error(t, err)
}

// ─── Item 3.6.3: Normalize helper ─────────────────────────────────────────────

func TestNormalize(t *testing.T) {
	assert.Equal(t, "/foo", parameter.Normalize("foo"))
	assert.Equal(t, "/foo", parameter.Normalize("/foo"))
	assert.Equal(t, "", parameter.Normalize(""))
}

func TestParseSelector(t *testing.T) {
	base, ver, lbl := parameter.ParseSelector("/p/name:3")
	assert.Equal(t, "/p/name", base)
	assert.Equal(t, int64(3), ver)
	assert.Equal(t, "", lbl)

	base2, ver2, lbl2 := parameter.ParseSelector("/p/name:myLabel")
	assert.Equal(t, "/p/name", base2)
	assert.Equal(t, int64(0), ver2)
	assert.Equal(t, "myLabel", lbl2)

	base3, ver3, lbl3 := parameter.ParseSelector("/p/name")
	assert.Equal(t, "/p/name", base3)
	assert.Equal(t, int64(0), ver3)
	assert.Equal(t, "", lbl3)
}
