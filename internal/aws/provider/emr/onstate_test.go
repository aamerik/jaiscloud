package emr

import (
	"context"
	"testing"

	"jaiscloud/internal/store"
)

// newMinimalEMRProvider builds a bare-minimum EMRProvider for tests.
func newMinimalEMRProvider() *EMRProvider {
	return New(store.NewMemoryResourceStore(), nil)
}

// TestEMRProvider_Reset_IsNoOp verifies Reset does not panic.
func TestEMRProvider_Reset_IsNoOp(t *testing.T) {
	p := newMinimalEMRProvider()
	p.Reset(context.Background()) // must not panic
}

// TestEMRProvider_Shutdown_IsNoOp verifies Shutdown does not panic.
func TestEMRProvider_Shutdown_IsNoOp(t *testing.T) {
	p := newMinimalEMRProvider()
	p.Shutdown(nil) // must not panic
}
