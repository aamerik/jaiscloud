// Package resourcemgr provides parent–child resource lifecycle management:
// parent-existence checks before child creation, and safe deletion with
// configurable policies (fail, force-terminate, cascade).
package resourcemgr

import "context"

// DeletionPolicy declares what to do when a parent is deleted while active children exist.
type DeletionPolicy int8

const (
	// PolicyFail returns an error. The parent cannot be deleted until all active children
	// are removed. Example: EMR on EKS virtual cluster with RUNNING job runs.
	PolicyFail DeletionPolicy = iota

	// PolicyForceTerminate marks each active child as CANCELLED/TERMINATED in the
	// ResourceStore, then proceeds with parent deletion. More conservative than Cascade
	// because it preserves child records.
	// Example: EMR cluster with running steps.
	PolicyForceTerminate

	// PolicyCascade deletes all active children from the ResourceStore, then the parent.
	// Example: SNS topic deletes its subscriptions.
	PolicyCascade
)

// ChildRef identifies a single child resource instance found by FindChildren.
// ID must be the exact key used in ResourceStore (including composite keys).
type ChildRef struct {
	Type string
	ID   string
}

// DeleteGuardRule declares how to find and handle children of a given parent type
// when the parent is being deleted.
type DeleteGuardRule struct {
	// ParentType is the ResourceStore type key of the parent (e.g. "emrc_virtual_cluster").
	ParentType string

	// FindChildren returns all active children of the given parent ID.
	// "Active" means children that should block or trigger the policy — terminal children
	// (COMPLETED, FAILED) already in the store are typically excluded.
	// The ResourceStore passed is the host's store — no second store needed.
	FindChildren func(ctx context.Context, resources ResourceStore, parentID string) ([]ChildRef, error)

	// Policy declares what to do when FindChildren returns a non-empty list.
	Policy DeletionPolicy

	// CascadeDelete removes a single child from the ResourceStore.
	// Required when Policy == PolicyCascade.
	// If nil, defaults to: resources.Delete(ctx, child.Type, child.ID)
	CascadeDelete func(ctx context.Context, resources ResourceStore, child ChildRef) error

	// ForceTerminate updates a child's state to a terminal value in the ResourceStore.
	// Required when Policy == PolicyForceTerminate.
	ForceTerminate func(ctx context.Context, resources ResourceStore, child ChildRef) error

	// FailCode is the cloud-specific error code returned when Policy == PolicyFail
	// and a blocking child is found. Defaults to "ValidationException".
	FailCode string

	// FailMessage generates the user-visible error message when PolicyFail fires.
	// If nil, a default message is used.
	FailMessage func(parentID string, children []ChildRef) string

	// FailStatus is the HTTP status code for PolicyFail errors. Defaults to 400.
	FailStatus int
}
