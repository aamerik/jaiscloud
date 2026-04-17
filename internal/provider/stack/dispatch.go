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

	// Delete removes the resource identified by physicalID.
	// Errors are logged but do not abort stack rollback.
	Delete func(ctx context.Context, physicalID string, props map[string]any) error
}
