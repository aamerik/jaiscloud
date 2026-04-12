package sdk

import "context"

// ChildRef identifies a single child resource found by a FindChildren function.
type ChildRef struct {
	Type string
	ID   string
}

// DeletionPolicy declares what to do when a parent has active children at delete time.
type DeletionPolicy int8

const (
	// PolicyFail blocks the parent delete and returns an error to the caller.
	PolicyFail DeletionPolicy = iota

	// PolicyForceTerminate marks each active child as CANCELLED/TERMINATED, then deletes.
	PolicyForceTerminate

	// PolicyCascade deletes each active child, then deletes the parent.
	PolicyCascade
)

// DeleteGuardRule declares how to find and handle children of a parent type
// when that parent is being deleted. Plugins register rules during Init.
type DeleteGuardRule struct {
	// ParentType is the ResourceStore type key of the parent, e.g. "emrc_virtual_cluster".
	ParentType string

	// FindChildren returns all active children of the given parent ID.
	FindChildren func(ctx context.Context, store ResourceStore, parentID string) ([]ChildRef, error)

	// Policy declares what happens when FindChildren returns a non-empty slice.
	Policy DeletionPolicy

	// CascadeDelete removes a single child. Required when Policy == PolicyCascade.
	// If nil, the host falls back to store.Delete(ctx, child.Type, child.ID).
	CascadeDelete func(ctx context.Context, store ResourceStore, child ChildRef) error

	// ForceTerminate marks a child as terminal. Required when Policy == PolicyForceTerminate.
	ForceTerminate func(ctx context.Context, store ResourceStore, child ChildRef) error

	// FailCode is returned when Policy == PolicyFail. Defaults to "ValidationException".
	FailCode string

	// FailMessage generates the error message. If nil a default message is used.
	FailMessage func(parentID string, children []ChildRef) string

	// FailStatus is the HTTP status code for PolicyFail. Defaults to 400.
	FailStatus int
}

// DeletionHandle is returned by ResourceManager.AcquireDelete.
// The caller MUST call Release after the delete (success or failure).
//
// In a loop over multiple IDs, do NOT use defer — defers stack until function return.
// Call Release explicitly after each delete.
type DeletionHandle interface {
	Release()
}

// ResourceManager is the interface the host passes to plugins via Init.
// It provides parent-existence checks and safe delete orchestration.
//
// Plugins use it to:
//   - Call CheckParent before creating child resources.
//   - Call AcquireDelete before deleting parent resources.
//   - Call RegisterRules during Init to register their own guard rules.
type ResourceManager interface {
	// CheckParent verifies the parent exists and is not currently being deleted.
	// Returns a non-nil error (with the supplied code/msg/httpStatus) on failure.
	CheckParent(
		ctx context.Context,
		parentType, parentID string,
		notFoundCode, notFoundMsg string,
		httpStatus int,
	) error

	// AcquireDelete acquires a deletion lock and runs all applicable child-guard rules.
	// Returns a DeletionHandle the caller must Release when the delete is complete.
	AcquireDelete(ctx context.Context, resourceType, resourceID string) (DeletionHandle, error)

	// RegisterRules adds DeleteGuardRules to the manager.
	// Called during Init to register the plugin's own resource dependency rules.
	RegisterRules(rules []DeleteGuardRule)
}
