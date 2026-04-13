package plugin_test

import (
	"context"
	"testing"

	sdk "github.com/jaiscloud/plugin-sdk"
	"jaiscloud/internal/plugin"
	"jaiscloud/internal/provider"
)

// ─── PluginManager: LoadAll with no dir ──────────────────────────────────────

func TestPluginManager_LoadAll_EmptyDir_NoOp(t *testing.T) {
	mgr := plugin.NewPluginManager()
	reg := provider.NewRegistry()
	err := mgr.LoadAll(context.Background(), "", nil, nil, nil, reg)
	if err != nil {
		t.Fatalf("empty dir should be no-op, got: %v", err)
	}
	if len(mgr.Plugins()) != 0 {
		t.Errorf("expected 0 plugins, got %d", len(mgr.Plugins()))
	}
}

func TestPluginManager_LoadAll_NonexistentDir_NoOp(t *testing.T) {
	mgr := plugin.NewPluginManager()
	reg := provider.NewRegistry()
	err := mgr.LoadAll(context.Background(), "/does/not/exist", nil, nil, nil, reg)
	if err != nil {
		t.Fatalf("missing dir should be no-op, got: %v", err)
	}
}

// ─── Reset / Shutdown ────────────────────────────────────────────────────────

type recordPlugin struct {
	resets    int
	shutdowns int
}

func (p *recordPlugin) Init(_ context.Context, _ sdk.ResourceManager, _ sdk.ResourceStore, _ sdk.EventBus) error {
	return nil
}
func (p *recordPlugin) Manifest() sdk.ManifestInfo { return sdk.ManifestInfo{Name: "test"} }
func (p *recordPlugin) Handle(_ context.Context, _ sdk.HandleRequest) sdk.HandleResponse {
	return sdk.HandleResponse{}
}
func (p *recordPlugin) Shutdown(_ context.Context) error { p.shutdowns++; return nil }
func (p *recordPlugin) Reset()                           { p.resets++ }

func TestPluginManager_Reset_CallsAllPlugins(t *testing.T) {
	mgr := plugin.NewPluginManager()
	p1 := &recordPlugin{}
	p2 := &recordPlugin{}
	mgr.InjectForTest(p1, p2)

	mgr.Reset()

	if p1.resets != 1 || p2.resets != 1 {
		t.Errorf("expected each plugin Reset called once, got p1=%d p2=%d", p1.resets, p2.resets)
	}
}

func TestPluginManager_Shutdown_CallsAllPlugins(t *testing.T) {
	mgr := plugin.NewPluginManager()
	p1 := &recordPlugin{}
	p2 := &recordPlugin{}
	mgr.InjectForTest(p1, p2)

	mgr.Shutdown(context.Background())

	if p1.shutdowns != 1 || p2.shutdowns != 1 {
		t.Errorf("expected each plugin Shutdown called once, got p1=%d p2=%d", p1.shutdowns, p2.shutdowns)
	}
}

func TestPluginManager_Plugins_ReturnsSnapshot(t *testing.T) {
	mgr := plugin.NewPluginManager()
	p1 := &recordPlugin{}
	mgr.InjectForTest(p1)

	snap := mgr.Plugins()
	if len(snap) != 1 {
		t.Fatalf("expected 1 plugin, got %d", len(snap))
	}
}
