// Package resourcemanager implements the slice of Cloud Resource Manager v1
// (cloudresourcemanager.googleapis.com/v1) that the hashicorp/google Terraform
// and Pulumi Google providers require: the project lookup
// (projects.get) and project-level IAM
// (projects.getIamPolicy / setIamPolicy / testIamPermissions).
//
// The emulator's multi-tenancy is keyed by project id and projects are never
// created or deleted, so every project id resolves to an ACTIVE project with a
// stable synthetic projectNumber. IAM policies are stored in the shared
// ResourceStore through internal/gcp/policy (etag optimistic concurrency
// control, fresh etag per set), so memory and PostgreSQL backends behave
// identically and no provider-level Reset/Snapshotter is needed. Bindings are
// not enforced — they never restrict access to emulated resources.
package resourcemanager

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource type for a project's IAM policy in the shared ResourceStore. The
// entry id is the project id.
const rtProjectPolicy = "gcp_resourcemanager_project_iam"

// Provider handles the Cloud Resource Manager v1 project surface.
type Provider struct {
	resources store.ResourceStore
}

// New returns a Provider backed by the shared ResourceStore.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes maps "ResourceManager.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ResourceManager.ProjectGet":                p.GetProject,
		"ResourceManager.ProjectGetIamPolicy":       p.GetIamPolicy,
		"ResourceManager.ProjectSetIamPolicy":       p.SetIamPolicy,
		"ResourceManager.ProjectTestIamPermissions": p.TestIamPermissions,
	}
}

// GetProject returns the project resource. `name` is the user-assigned display
// name in Cloud Resource Manager v1 (not a resource path); `projectNumber` is
// the stable synthesized number.
func (p *Provider) GetProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "project is required", 400)
	}
	return provider.OK(map[string]any{
		"projectId":      project,
		"projectNumber":  resource.ProjectNumber(project),
		"name":           project,
		"lifecycleState": "ACTIVE",
	}), nil
}

// GetIamPolicy returns the stored project policy (or an empty default policy).
func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "project is required", 400)
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtProjectPolicy, project))), nil
}

// SetIamPolicy stores the project policy, enforcing etag OCC (a stale etag is
// rejected with ABORTED/409).
func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "project is required", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtProjectPolicy, project, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

// TestIamPermissions echoes the requested permissions (the emulator treats the
// caller as owner — no authz enforcement).
func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "project is required", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}
