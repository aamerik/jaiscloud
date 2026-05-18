package identity_test

import (
	"testing"

	"jaiscloud/internal/aws/identity"
)

func TestNormaliseRegion_KnownRegion(t *testing.T) {
	regions := []string{
		"us-east-1", "us-east-2", "us-west-1", "us-west-2",
		"eu-west-1", "eu-central-1", "ap-northeast-1",
		"ca-central-1", "sa-east-1",
	}
	for _, r := range regions {
		got := identity.NormaliseRegion(r, "")
		if got != r {
			t.Errorf("NormaliseRegion(%q, ...) = %q, want unchanged", r, got)
		}
	}
}

func TestNormaliseRegion_EmptyFallback(t *testing.T) {
	got := identity.NormaliseRegion("", "eu-west-1")
	if got != "eu-west-1" {
		t.Errorf("got %q, want eu-west-1", got)
	}
}

func TestNormaliseRegion_EmptyNoFallback(t *testing.T) {
	got := identity.NormaliseRegion("", "")
	if got != identity.DefaultRegion {
		t.Errorf("got %q, want DefaultRegion %q", got, identity.DefaultRegion)
	}
}

func TestNormaliseRegion_UnknownRegion(t *testing.T) {
	// Without the env var, unknown regions fall back to DefaultRegion.
	t.Setenv("JAISCLOUD_ALLOW_NONSTANDARD_REGIONS", "false")
	got := identity.NormaliseRegion("xx-custom-1", "us-east-1")
	if got != identity.DefaultRegion {
		t.Errorf("got %q, want DefaultRegion", got)
	}
}

func TestNormaliseRegion_AllowNonstandard(t *testing.T) {
	// With the env var enabled, structurally valid but unknown regions are kept.
	t.Setenv("JAISCLOUD_ALLOW_NONSTANDARD_REGIONS", "true")
	got := identity.NormaliseRegion("xx-custom-1", "us-east-1")
	if got != "xx-custom-1" {
		t.Errorf("got %q, want xx-custom-1", got)
	}
}

func TestNormaliseRegion_AllowNonstandardInvalidShape(t *testing.T) {
	// Even with the env var, a completely malformed region falls back.
	t.Setenv("JAISCLOUD_ALLOW_NONSTANDARD_REGIONS", "true")
	got := identity.NormaliseRegion("not-a-region", "us-east-1")
	if got != identity.DefaultRegion {
		t.Errorf("got %q, want DefaultRegion", got)
	}
}
