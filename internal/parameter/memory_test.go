package parameter_test

import (
	"context"
	"testing"

	"jaiscloud/internal/parameter"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newParamStore() *parameter.MemoryParameterStore { return parameter.NewMemoryParameterStore() }

func TestMemoryParameterStore_PutGet(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()

	e := parameter.ParameterEntry{Name: "/app/db-url", Type: "String", Value: []byte("postgres://localhost")}
	require.NoError(t, s.PutParameter(ctx, e, false))

	got, err := s.GetParameter(ctx, "/app/db-url")
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Version)
	assert.Equal(t, []byte("postgres://localhost"), got.Value)
}

func TestMemoryParameterStore_Overwrite(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()

	s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String", Value: []byte("v1")}, false)

	// No overwrite should fail.
	err := s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String", Value: []byte("v2")}, false)
	assert.ErrorIs(t, err, parameter.ErrAlreadyExists)

	// With overwrite should succeed and bump version.
	require.NoError(t, s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String", Value: []byte("v2")}, true))
	got, _ := s.GetParameter(ctx, "/p")
	assert.Equal(t, int64(2), got.Version)
	assert.Equal(t, []byte("v2"), got.Value)
}

func TestMemoryParameterStore_History(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()

	s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String", Value: []byte("v1")}, false)
	s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String", Value: []byte("v2")}, true)
	s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String", Value: []byte("v3")}, true)

	history, err := s.GetParameterHistory(ctx, "/p")
	require.NoError(t, err)
	// Two history entries (v1 and v2; v3 is current).
	assert.Len(t, history, 2)
	assert.Equal(t, int64(1), history[0].Version)
	assert.Equal(t, int64(2), history[1].Version)
}

func TestMemoryParameterStore_Delete(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()
	s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String"}, false)
	require.NoError(t, s.DeleteParameter(ctx, "/p"))
	_, err := s.GetParameter(ctx, "/p")
	assert.ErrorIs(t, err, parameter.ErrParameterNotFound)
}

func TestMemoryParameterStore_ListByPath_Recursive(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()
	for _, name := range []string{"/app/db", "/app/api/key", "/app/api/secret", "/other/x"} {
		s.PutParameter(ctx, parameter.ParameterEntry{Name: name, Type: "String"}, false)
	}

	results, err := s.ListParameters(ctx, "/app/", true)
	require.NoError(t, err)
	assert.Len(t, results, 3)
}

func TestMemoryParameterStore_ListByPath_NonRecursive(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()
	for _, name := range []string{"/app/db", "/app/api/key", "/app/api/secret"} {
		s.PutParameter(ctx, parameter.ParameterEntry{Name: name, Type: "String"}, false)
	}

	// Only direct children of /app/ (one level deep).
	results, err := s.ListParameters(ctx, "/app/", false)
	require.NoError(t, err)
	assert.Len(t, results, 1) // only /app/db
}

func TestMemoryParameterStore_Reset(t *testing.T) {
	ctx := context.Background()
	s := newParamStore()
	s.PutParameter(ctx, parameter.ParameterEntry{Name: "/p", Type: "String"}, false)
	s.Reset()
	results, _ := s.ListParameters(ctx, "", true)
	assert.Empty(t, results)
}
