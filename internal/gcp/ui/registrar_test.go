package ui

import (
	"testing"

	"jaiscloud/internal/model"
)

func TestRegistrar_GCPShell(t *testing.T) {
	reg := NewRegistrar()
	if got := reg.Cloud(); got != model.CloudGCP {
		t.Fatalf("Cloud() = %q, want %q", got, model.CloudGCP)
	}
	// Shell-only milestone: no service pages are advertised yet.
	if services := reg.Services(); len(services) != 0 {
		t.Fatalf("Services() = %d entries, want 0", len(services))
	}
}
