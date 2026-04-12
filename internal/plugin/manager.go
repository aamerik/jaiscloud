// Package plugin loads JaisCloud plugins from .so files at startup.
// Plugins are compiled with go build -buildmode=plugin and must export:
//
//	var Plugin sdk.SparkPlugin = &MyPlugin{}
//
// The host calls Init once, then Manifest to register routes, then routes
// matching requests to Handle. On reset, Reset() is called on every plugin.
package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	goplugin "plugin"
	"sync"

	sdk "github.com/jaiscloud/plugin-sdk"
	"jaiscloud/internal/provider"
)

// PluginManager loads and manages the lifecycle of all .so plugins.
type PluginManager struct {
	mu      sync.RWMutex
	plugins []sdk.SparkPlugin
}

// NewPluginManager creates an empty PluginManager.
func NewPluginManager() *PluginManager {
	return &PluginManager{}
}

// LoadAll opens every *.so file in dir, looks up the Plugin symbol,
// calls Init, and registers the plugin's routes in registry.
//
// rm and store are passed to each plugin's Init method.
// If dir is empty or does not exist, LoadAll is a no-op (not an error).
func (m *PluginManager) LoadAll(
	ctx context.Context,
	dir string,
	rm sdk.ResourceManager,
	store sdk.ResourceStore,
	registry *provider.Registry,
) error {
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("plugin: read dir %q: %w", dir, err)
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".so" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if err := m.load(ctx, path, rm, store, registry); err != nil {
			return fmt.Errorf("plugin: load %q: %w", path, err)
		}
	}
	return nil
}

// load opens a single .so, validates the Plugin symbol, calls Init, and
// registers the plugin's routes.
func (m *PluginManager) load(
	ctx context.Context,
	path string,
	rm sdk.ResourceManager,
	store sdk.ResourceStore,
	registry *provider.Registry,
) error {
	p, err := goplugin.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	sym, err := p.Lookup("Plugin")
	if err != nil {
		return fmt.Errorf("lookup Plugin symbol: %w", err)
	}

	sp, ok := sym.(*sdk.SparkPlugin)
	if !ok {
		return fmt.Errorf("Plugin symbol is %T, want *sdk.SparkPlugin", sym)
	}
	if sp == nil || *sp == nil {
		return fmt.Errorf("Plugin symbol is nil")
	}
	plugin := *sp

	manifest := plugin.Manifest()
	slog.Info("plugin loaded", "name", manifest.Name, "version", manifest.Version, "services", manifest.Services)

	if err := plugin.Init(ctx, rm, store); err != nil {
		return fmt.Errorf("Init: %w", err)
	}

	// Register the plugin's Handle method as a provider route for each service/action.
	// We register a catch-all per service; the plugin's Handle method dispatches internally.
	for _, svc := range manifest.Services {
		registerPluginRoutes(registry, svc, plugin)
	}

	m.mu.Lock()
	m.plugins = append(m.plugins, plugin)
	m.mu.Unlock()

	return nil
}

// Shutdown calls Shutdown on every loaded plugin in reverse load order.
func (m *PluginManager) Shutdown(ctx context.Context) {
	m.mu.RLock()
	plugins := m.plugins
	m.mu.RUnlock()

	for i := len(plugins) - 1; i >= 0; i-- {
		p := plugins[i]
		manifest := p.Manifest()
		if err := p.Shutdown(ctx); err != nil {
			slog.Error("plugin shutdown error", "name", manifest.Name, "err", err)
		}
	}
}

// Reset calls Reset on every loaded plugin.
// Called from POST /_jaiscloud/reset.
func (m *PluginManager) Reset() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.plugins {
		p.Reset()
	}
}

// Plugins returns the loaded plugins (read-only snapshot).
func (m *PluginManager) Plugins() []sdk.SparkPlugin {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]sdk.SparkPlugin, len(m.plugins))
	copy(out, m.plugins)
	return out
}

// InjectForTest adds plugins directly without loading from .so files.
// Used only in unit tests.
func (m *PluginManager) InjectForTest(plugins ...sdk.SparkPlugin) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.plugins = append(m.plugins, plugins...)
}
