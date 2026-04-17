package apigw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ─── pathMatches ──────────────────────────────────────────────────────────────

func TestPathMatches_ExactLiteral(t *testing.T) {
	assert.True(t, pathMatches("/items", "/items"))
}

func TestPathMatches_SingleParam(t *testing.T) {
	assert.True(t, pathMatches("/items/{id}", "/items/123"))
	assert.False(t, pathMatches("/items/{id}", "/items/123/extra"))
}

func TestPathMatches_MultiSegment(t *testing.T) {
	assert.True(t, pathMatches("/a/{b}/c/{d}", "/a/1/c/2"))
	assert.False(t, pathMatches("/a/{b}/c/{d}", "/a/1/c"))
}

func TestPathMatches_GreedyProxy(t *testing.T) {
	assert.True(t, pathMatches("/files/{proxy+}", "/files/a/b/c"))
	assert.True(t, pathMatches("/files/{proxy+}", "/files/x"))
	assert.False(t, pathMatches("/files/{proxy+}", "/files"))
}

func TestPathMatches_RootLiteral(t *testing.T) {
	assert.True(t, pathMatches("/", "/"))
	assert.False(t, pathMatches("/", "/anything"))
}

func TestPathMatches_Mismatch(t *testing.T) {
	assert.False(t, pathMatches("/a/b", "/a/c"))
	assert.False(t, pathMatches("/a/b/c", "/a/b"))
}

// ─── interpolateStageVars ─────────────────────────────────────────────────────

func TestInterpolateStageVars_Single(t *testing.T) {
	out := interpolateStageVars("http://${stageVariables.host}/api", map[string]string{"host": "example.com"})
	assert.Equal(t, "http://example.com/api", out)
}

func TestInterpolateStageVars_Multiple(t *testing.T) {
	out := interpolateStageVars("http://${stageVariables.host}:${stageVariables.port}/", map[string]string{
		"host": "localhost", "port": "8080",
	})
	assert.Equal(t, "http://localhost:8080/", out)
}

func TestInterpolateStageVars_NilVars(t *testing.T) {
	out := interpolateStageVars("http://static/", nil)
	assert.Equal(t, "http://static/", out)
}

func TestInterpolateStageVars_NoMatch(t *testing.T) {
	out := interpolateStageVars("http://static/", map[string]string{"other": "x"})
	assert.Equal(t, "http://static/", out)
}
