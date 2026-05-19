package platform

import (
	"strings"
	"testing"
)

type fakeDetector struct {
	container bool
	cwd       string
	home      string
	hasHome   bool
}

func (f fakeDetector) IsContainer() bool        { return f.container }
func (f fakeDetector) WorkingDir() string        { return f.cwd }
func (f fakeDetector) HomeDir() (string, bool)   { return f.home, f.hasHome }

func TestResolveDataDir_FlagTakesPrecedence(t *testing.T) {
	det := fakeDetector{hasHome: true, home: "/home/user", cwd: "/app"}
	path, source := ResolveDataDir("/custom/dir", "ENV_DIR", "jaiscloud-aws", det)
	if source != "flag" {
		t.Errorf("expected source 'flag', got %q", source)
	}
	if path != "/custom/dir" {
		t.Errorf("expected /custom/dir, got %q", path)
	}
}

func TestResolveDataDir_EnvOverridesDefault(t *testing.T) {
	det := fakeDetector{hasHome: true, home: "/home/user", cwd: "/app"}
	path, source := ResolveDataDir("", "/env/data", "jaiscloud-aws", det)
	if source != "env" {
		t.Errorf("expected source 'env', got %q", source)
	}
	if path != "/env/data" {
		t.Errorf("expected /env/data, got %q", path)
	}
}

func TestResolveDataDir_HostDefault(t *testing.T) {
	det := fakeDetector{container: false, hasHome: true, home: "/home/user", cwd: "/app"}
	path, source := ResolveDataDir("", "", "jaiscloud-aws", det)
	if source != "home" {
		t.Errorf("expected source 'home', got %q", source)
	}
	if path != "/home/user/.jaiscloud/jaiscloud-aws" {
		t.Errorf("unexpected path: %q", path)
	}
}

func TestResolveDataDir_ContainerDefault(t *testing.T) {
	det := fakeDetector{container: true, cwd: "/app", hasHome: false}
	path, source := ResolveDataDir("", "", "jaiscloud-aws", det)
	if source != "container" {
		t.Errorf("expected source 'container', got %q", source)
	}
	if !strings.HasSuffix(path, "/.jaiscloud/jaiscloud-aws") {
		t.Errorf("unexpected path: %q", path)
	}
}

func TestResolveDataDir_NeverUsesTemp(t *testing.T) {
	for _, det := range []fakeDetector{
		{container: false, hasHome: true, home: "/home/user", cwd: "/app"},
		{container: true, cwd: "/app"},
		{container: false, hasHome: false, cwd: "/app"},
	} {
		path, _ := ResolveDataDir("", "", "jaiscloud-aws", det)
		if strings.HasPrefix(path, "/tmp") {
			t.Errorf("data dir must not start with /tmp, got %q", path)
		}
	}
}

func TestResolveDataDir_FallbackWhenNoHome(t *testing.T) {
	det := fakeDetector{container: false, hasHome: false, cwd: "/app"}
	path, source := ResolveDataDir("", "", "jaiscloud-aws", det)
	if source != "fallback" {
		t.Errorf("expected source 'fallback', got %q", source)
	}
	if !strings.HasSuffix(path, "/.jaiscloud/jaiscloud-aws") {
		t.Errorf("unexpected path: %q", path)
	}
}
