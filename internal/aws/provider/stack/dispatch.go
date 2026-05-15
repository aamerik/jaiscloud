package stack

import (
	"context"

	"jaiscloud/internal/model"
)

// ResourceHandler provisions and deprovisions a single CloudFormation resource type.
type ResourceHandler struct {
	// Create provisions the resource, returning a stable physical ID and a map of
	// resource attributes (e.g. "Arn", "QueueUrl") that can be referenced via
	// Fn::GetAtt in the template.
	Create func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (physicalID string, attrs map[string]any, err error)

	// Update modifies an existing resource in place. If replacement is true, the
	// caller must delete the old resource and create a new one.
	// Update may be nil — in that case the resource is always replaced (Delete + Create).
	Update func(ctx context.Context, logicalID string, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (newPhysicalID string, attrs map[string]any, replacement bool, err error)

	// Delete removes the resource identified by physicalID.
	// Errors are logged but do not abort stack rollback.
	Delete func(ctx context.Context, physicalID string, props map[string]any) error

	// RefAttr is the attribute name returned when a template uses Ref on this resource.
	// If empty, the physical resource ID is used.
	RefAttr string

	// GetAttAttrs lists the attribute names this resource type supports via Fn::GetAtt.
	GetAttAttrs []string

	// ReplacementRules controls which property changes require resource replacement.
	ReplacementRules ReplacementRules
}

// ReplacementRules describes which property changes trigger resource replacement.
type ReplacementRules struct {
	// RequireReplacement lists property paths that, if changed, require resource replacement.
	RequireReplacement []string
	// RequireUpdate lists property paths that can be updated in-place.
	RequireUpdate []string
}
