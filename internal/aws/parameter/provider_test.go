package parameter_test

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/aws/parameter"
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
