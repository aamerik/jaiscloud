package emroneks

import (
	"testing"

	"jaiscloud/internal/store"
)

// newMinimalEMRCProvider builds a bare-minimum EMRContainersProvider for tests.
func newMinimalEMRCProvider() *EMRContainersProvider {
	return New(store.NewMemoryResourceStore(), nil)
}

// TestEMRContainersProvider_Reset_IsNoOp verifies Reset does not panic.
func TestEMRContainersProvider_Reset_IsNoOp(t *testing.T) {
	p := newMinimalEMRCProvider()
	p.Reset() // must not panic
}

// TestEMRContainersProvider_Shutdown_IsNoOp verifies Shutdown does not panic.
func TestEMRContainersProvider_Shutdown_IsNoOp(t *testing.T) {
	p := newMinimalEMRCProvider()
	p.Shutdown(nil) // must not panic
}
