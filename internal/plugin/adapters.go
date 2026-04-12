package plugin

import (
	"context"

	sdk "github.com/jaiscloud/plugin-sdk"
	"jaiscloud/internal/resourcemgr"
	"jaiscloud/internal/store"
)

// ─── SDK ResourceStore adapter ────────────────────────────────────────────────

// sdkStoreAdapter bridges store.ResourceStore → sdk.ResourceStore.
type sdkStoreAdapter struct {
	s store.ResourceStore
}

// NewSDKStoreAdapter wraps a store.ResourceStore for use with plugins.
func NewSDKStoreAdapter(s store.ResourceStore) sdk.ResourceStore {
	return &sdkStoreAdapter{s: s}
}

func (a *sdkStoreAdapter) Exists(ctx context.Context, resourceType, id string) (bool, error) {
	_, err := a.s.Get(ctx, resourceType, id)
	if err == nil {
		return true, nil
	}
	if err == store.ErrNotFound {
		return false, nil
	}
	return false, err
}

func (a *sdkStoreAdapter) Get(ctx context.Context, resourceType, id string) (sdk.ResourceEntry, error) {
	e, err := a.s.Get(ctx, resourceType, id)
	if err != nil {
		return sdk.ResourceEntry{}, err
	}
	return sdk.ResourceEntry{Type: e.Type, ID: e.ID, Data: []byte(e.Data)}, nil
}

func (a *sdkStoreAdapter) List(ctx context.Context, resourceType, prefix string) ([]sdk.ResourceEntry, error) {
	entries, err := a.s.List(ctx, resourceType, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]sdk.ResourceEntry, len(entries))
	for i, e := range entries {
		out[i] = sdk.ResourceEntry{Type: e.Type, ID: e.ID, Data: []byte(e.Data)}
	}
	return out, nil
}

func (a *sdkStoreAdapter) Create(ctx context.Context, entry sdk.ResourceEntry) error {
	return a.s.Create(ctx, store.ResourceEntry{Type: entry.Type, ID: entry.ID, Data: entry.Data})
}

func (a *sdkStoreAdapter) Update(ctx context.Context, entry sdk.ResourceEntry) error {
	return a.s.Update(ctx, store.ResourceEntry{Type: entry.Type, ID: entry.ID, Data: entry.Data})
}

func (a *sdkStoreAdapter) Delete(ctx context.Context, resourceType, id string) error {
	return a.s.Delete(ctx, resourceType, id)
}

// ─── SDK ResourceManager adapter ─────────────────────────────────────────────

// sdkRMAdapter bridges *resourcemgr.Manager → sdk.ResourceManager.
type sdkRMAdapter struct {
	m *resourcemgr.Manager
}

// NewSDKResourceManager wraps a *resourcemgr.Manager for use with plugins.
func NewSDKResourceManager(m *resourcemgr.Manager) sdk.ResourceManager {
	return &sdkRMAdapter{m: m}
}

func (a *sdkRMAdapter) CheckParent(ctx context.Context, parentType, parentID, notFoundCode, notFoundMsg string, httpStatus int) error {
	return a.m.CheckParent(ctx, parentType, parentID, notFoundCode, notFoundMsg, httpStatus)
}

func (a *sdkRMAdapter) AcquireDelete(ctx context.Context, resourceType, resourceID string) (sdk.DeletionHandle, error) {
	return a.m.AcquireDelete(ctx, resourceType, resourceID)
}

func (a *sdkRMAdapter) RegisterRules(rules []sdk.DeleteGuardRule) {
	converted := make([]resourcemgr.DeleteGuardRule, len(rules))
	for i, r := range rules {
		converted[i] = convertRule(r)
	}
	a.m.RegisterRules(converted)
}

// convertRule translates an sdk.DeleteGuardRule into a resourcemgr.DeleteGuardRule.
func convertRule(r sdk.DeleteGuardRule) resourcemgr.DeleteGuardRule {
	out := resourcemgr.DeleteGuardRule{
		ParentType:  r.ParentType,
		Policy:      resourcemgr.DeletionPolicy(r.Policy),
		FailCode:    r.FailCode,
		FailStatus:  r.FailStatus,
	}

	if r.FindChildren != nil {
		out.FindChildren = func(ctx context.Context, rs resourcemgr.ResourceStore, parentID string) ([]resourcemgr.ChildRef, error) {
			sdkStore := &rmStoreAdapter{rs: rs}
			children, err := r.FindChildren(ctx, sdkStore, parentID)
			if err != nil {
				return nil, err
			}
			out := make([]resourcemgr.ChildRef, len(children))
			for i, c := range children {
				out[i] = resourcemgr.ChildRef{Type: c.Type, ID: c.ID}
			}
			return out, nil
		}
	}

	if r.CascadeDelete != nil {
		out.CascadeDelete = func(ctx context.Context, rs resourcemgr.ResourceStore, child resourcemgr.ChildRef) error {
			sdkStore := &rmStoreAdapter{rs: rs}
			return r.CascadeDelete(ctx, sdkStore, sdk.ChildRef{Type: child.Type, ID: child.ID})
		}
	}

	if r.ForceTerminate != nil {
		out.ForceTerminate = func(ctx context.Context, rs resourcemgr.ResourceStore, child resourcemgr.ChildRef) error {
			sdkStore := &rmStoreAdapter{rs: rs}
			return r.ForceTerminate(ctx, sdkStore, sdk.ChildRef{Type: child.Type, ID: child.ID})
		}
	}

	if r.FailMessage != nil {
		out.FailMessage = func(parentID string, children []resourcemgr.ChildRef) string {
			sdkChildren := make([]sdk.ChildRef, len(children))
			for i, c := range children {
				sdkChildren[i] = sdk.ChildRef{Type: c.Type, ID: c.ID}
			}
			return r.FailMessage(parentID, sdkChildren)
		}
	}

	return out
}

// rmStoreAdapter wraps resourcemgr.ResourceStore → sdk.ResourceStore for use
// inside FindChildren / CascadeDelete / ForceTerminate callbacks.
type rmStoreAdapter struct {
	rs resourcemgr.ResourceStore
}

func (a *rmStoreAdapter) Exists(ctx context.Context, resourceType, id string) (bool, error) {
	return a.rs.Exists(ctx, resourceType, id)
}

func (a *rmStoreAdapter) Get(ctx context.Context, resourceType, id string) (sdk.ResourceEntry, error) {
	e, err := a.rs.Get(ctx, resourceType, id)
	if err != nil {
		return sdk.ResourceEntry{}, err
	}
	return sdk.ResourceEntry{Type: e.Type, ID: e.ID, Data: e.Data}, nil
}

func (a *rmStoreAdapter) List(ctx context.Context, resourceType, prefix string) ([]sdk.ResourceEntry, error) {
	entries, err := a.rs.List(ctx, resourceType, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]sdk.ResourceEntry, len(entries))
	for i, e := range entries {
		out[i] = sdk.ResourceEntry{Type: e.Type, ID: e.ID, Data: e.Data}
	}
	return out, nil
}

func (a *rmStoreAdapter) Create(ctx context.Context, entry sdk.ResourceEntry) error {
	return a.rs.Update(ctx, resourcemgr.ResourceEntry{Type: entry.Type, ID: entry.ID, Data: entry.Data})
}

func (a *rmStoreAdapter) Update(ctx context.Context, entry sdk.ResourceEntry) error {
	return a.rs.Update(ctx, resourcemgr.ResourceEntry{Type: entry.Type, ID: entry.ID, Data: entry.Data})
}

func (a *rmStoreAdapter) Delete(ctx context.Context, resourceType, id string) error {
	return a.rs.Delete(ctx, resourceType, id)
}
