package resourcemgr

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// OperationError is returned by all Manager methods.
// The caller converts it to a cloud-specific HTTP error response.
type OperationError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *OperationError) Error() string { return e.Message }

// DeletionHandle is returned by AcquireDelete. The caller MUST call Release()
// after the delete (success or failure) to remove the deletion lock.
//
// In a loop over multiple IDs, do NOT use defer — defers stack until function return.
// Call Release() explicitly after each delete.
type DeletionHandle struct {
	lock         *DeletionLock
	resourceType string
	resourceID   string
}

// Release removes the deletion lock for this resource.
func (h *DeletionHandle) Release() {
	h.lock.Release(h.resourceType, h.resourceID)
}

// Manager provides parent-existence checks and delete guards.
// Create once at startup with New(); inject into providers via constructor.
//
// Thread-safe: CheckParent uses RLock; AcquireDelete briefly holds write lock
// to atomically set the deletion mark, then releases it before the slow work.
type Manager struct {
	resources ResourceStore
	lock      *DeletionLock
	rules     []DeleteGuardRule
	ruleIdx   map[string][]int // parentType → indices into rules slice
	mu        sync.RWMutex    // guards lock.Acquire atomically with CheckParent reads
}

// New creates a Manager with the given rules.
func New(resources ResourceStore, rules []DeleteGuardRule) *Manager {
	idx := buildRuleIndex(rules)
	return &Manager{
		resources: resources,
		lock:      newDeletionLock(),
		rules:     rules,
		ruleIdx:   idx,
	}
}

func buildRuleIndex(rules []DeleteGuardRule) map[string][]int {
	idx := make(map[string][]int)
	for i, r := range rules {
		idx[r.ParentType] = append(idx[r.ParentType], i)
	}
	return idx
}

// RegisterRules adds additional DeleteGuardRules to this Manager.
// Called by plugins during Init to register their own resource dependency rules.
// Thread-safe.
func (m *Manager) RegisterRules(rules []DeleteGuardRule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := len(m.rules)
	m.rules = append(m.rules, rules...)
	for i, r := range rules {
		m.ruleIdx[r.ParentType] = append(m.ruleIdx[r.ParentType], start+i)
	}
}

// ─── Check 1: parent must exist and must not be deleting ──────────────────────

// CheckParent verifies:
//   (a) the named parent instance exists in the resource store
//   (b) the parent is not currently being deleted
//
// Holds m.mu.RLock() for the duration of both checks so that a concurrent
// AcquireDelete cannot set IsDeleting between the Exists and IsDeleting checks.
// Multiple concurrent CheckParent calls do not block each other (RLock).
//
// Returns *OperationError on failure; caller converts to cloud-specific error.
func (m *Manager) CheckParent(
	ctx context.Context,
	account, region string,
	parentType, parentID string,
	notFoundCode, notFoundMsg string,
	httpStatus int,
) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exists, err := m.resources.Exists(ctx, account, region, parentType, parentID)
	if err != nil {
		return err
	}
	if !exists {
		return &OperationError{Code: notFoundCode, Message: notFoundMsg, HTTPStatus: httpStatus}
	}
	if m.lock.IsDeleting(parentType, parentID) {
		return &OperationError{
			Code:       notFoundCode,
			Message:    notFoundMsg + " (currently being deleted)",
			HTTPStatus: httpStatus,
		}
	}
	return nil
}

// ─── Check 2: safe to delete ──────────────────────────────────────────────────

// AcquireDelete acquires a deletion lock and runs all applicable child-guard rules.
//
// Steps:
//  1. Acquires the deletion lock under m.mu.Lock() (briefly).
//     After release, concurrent CheckParent sees IsDeleting=true — TOCTOU window closed.
//  2. Releases m.mu. FindChildren runs outside the mutex (slow path).
//  3. Sorts results by policy priority: PolicyFail first, PolicyForceTerminate second,
//     PolicyCascade last — ensures a fail rule fires before any irreversible operation.
//  4. Applies each policy. Releases lock and returns error on first failure.
//  5. Returns DeletionHandle — caller MUST call handle.Release() when done.
//
// Usage:
//
//	handle, err := mgr.AcquireDelete(ctx, "emrc_virtual_cluster", id)
//	if err != nil { return nil, toHTTPError(err) }
//	defer handle.Release()
//	// ... perform the actual delete ...
func (m *Manager) AcquireDelete(ctx context.Context, account, region, resourceType, resourceID string) (*DeletionHandle, error) {
	// Step 1: acquire deletion lock under write-lock (briefly).
	m.mu.Lock()
	if !m.lock.Acquire(resourceType, resourceID) {
		m.mu.Unlock()
		return nil, &OperationError{
			Code:       "ConflictException",
			Message:    fmt.Sprintf("%s/%s is already being deleted", resourceType, resourceID),
			HTTPStatus: 409,
		}
	}
	m.mu.Unlock()
	// m.mu released. DeletionLock is set. CheckParent now returns "being deleted".

	handle := &DeletionHandle{lock: m.lock, resourceType: resourceType, resourceID: resourceID}

	// Step 2: collect children for all registered rules for this parentType.
	type ruleResult struct {
		rule     DeleteGuardRule
		children []ChildRef
		priority int // lower = higher priority
	}
	var results []ruleResult
	for _, i := range m.ruleIdx[resourceType] {
		rule := m.rules[i]
		children, err := rule.FindChildren(ctx, m.resources, account, region, resourceID)
		if err != nil {
			handle.Release()
			return nil, err
		}
		if len(children) > 0 {
			results = append(results, ruleResult{rule: rule, children: children, priority: int(rule.Policy)})
		}
	}

	// Step 3: sort by policy priority — PolicyFail(0) first.
	sort.Slice(results, func(i, j int) bool {
		return results[i].priority < results[j].priority
	})

	// Step 4: apply each policy.
	for _, r := range results {
		rule := r.rule
		switch rule.Policy {
		case PolicyFail:
			handle.Release()
			code := rule.FailCode
			if code == "" {
				code = "ValidationException"
			}
			status := rule.FailStatus
			if status == 0 {
				status = 400
			}
			msg := fmt.Sprintf("%s/%s has active children and cannot be deleted", resourceType, resourceID)
			if rule.FailMessage != nil {
				msg = rule.FailMessage(resourceID, r.children)
			}
			return nil, &OperationError{Code: code, Message: msg, HTTPStatus: status}

		case PolicyForceTerminate:
			if rule.ForceTerminate == nil {
				handle.Release()
				return nil, fmt.Errorf("resourcemgr: ForceTerminate is nil for rule on %s", resourceType)
			}
			for _, child := range r.children {
				if err := rule.ForceTerminate(ctx, m.resources, account, region, child); err != nil {
					handle.Release()
					return nil, err
				}
			}

		case PolicyCascade:
			deleteFn := rule.CascadeDelete
			if deleteFn == nil {
				deleteFn = func(ctx context.Context, resources ResourceStore, account, region string, child ChildRef) error {
					return resources.Delete(ctx, account, region, child.Type, child.ID)
				}
			}
			for _, child := range r.children {
				if err := deleteFn(ctx, m.resources, account, region, child); err != nil {
					handle.Release()
					return nil, err
				}
			}
		}
	}

	return handle, nil
}

// Reset clears all deletion locks. Called from POST /_jaiscloud/reset.
func (m *Manager) Reset() {
	m.lock.Reset()
}
