package grpcconformance

import (
	"context"
	"fmt"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
)

// resourceManagerChecks covers the Cloud Resource Manager v3 project surface
// (google.cloud.resourcemanager.v3.Projects) the emulator serves: the project
// lifecycle (CreateProject / GetProject / ListProjects / DeleteProject /
// UndeleteProject) and project IAM (GetIamPolicy / SetIamPolicy /
// TestIamPermissions). Search/Update/Move remain explicit Unimplemented stubs
// and are not probed.
//
// The lifecycle probes share one run-unique project id (rmCreatedProjectID) so
// create feeds list, then delete, then undelete. GetProject/IAM use their own
// run-unique synthesized ids, keeping the shared default project's IAM policy
// untouched and the run idempotent.
func resourceManagerChecks() []Check {
	return []Check{
		{Service: "resourcemanager", RPC: "CreateProject", Method: "CreateProject", KeyField: "operation done, project ACTIVE", Run: checkRMCreateProject},
		{Service: "resourcemanager", RPC: "GetProject", Method: "GetProject", KeyField: "name/projectId/state ACTIVE round-trip", Run: checkRMGetProject},
		{Service: "resourcemanager", RPC: "ListProjects", Method: "ListProjects", KeyField: "created project listed", Run: checkRMListProjects},
		{Service: "resourcemanager", RPC: "DeleteProject", Method: "DeleteProject", KeyField: "state DELETE_REQUESTED", Run: checkRMDeleteProject},
		{Service: "resourcemanager", RPC: "UndeleteProject", Method: "UndeleteProject", KeyField: "state ACTIVE restored", Run: checkRMUndeleteProject},
		{Service: "resourcemanager", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "policy etag present", Run: checkRMGetIamPolicy},
		{Service: "resourcemanager", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "binding stored + etag rotates", Run: checkRMSetIamPolicy},
		{Service: "resourcemanager", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions echoed", Run: checkRMTestIamPermissions},
	}
}

// newResourceManagerClient dials the emulator and returns the official
// Resource Manager v3 Projects client.
func newResourceManagerClient(ctx context.Context, cfg Config) (*resourcemanager.ProjectsClient, error) {
	return resourcemanager.NewProjectsClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// resourceManagerProject is the full resource name of a run-unique synthesized
// project.
func resourceManagerProject(cfg Config, prefix string) string {
	return "projects/" + cfg.ResourceName(prefix)
}

// rmCreatedProjectID is the run-unique id of the project the lifecycle probes
// create, list, delete and undelete. The v1/v3 project-id grammar caps an id at
// 30 characters, so a short prefix is required: cfg.Suffix is 12 hex chars and
// a "gcpc-grpc-rm-*" prefix would overflow.
func rmCreatedProjectID(cfg Config) string {
	return "rmp" + cfg.Suffix
}

// rmCreatedProjectName is the "projects/{id}" name of the lifecycled project.
func rmCreatedProjectName(cfg Config) string {
	return "projects/" + rmCreatedProjectID(cfg)
}

// Check 1: CreateProject registers the project and returns a done operation.
func checkRMCreateProject(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := rmCreatedProjectID(cfg)
	op, err := client.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: id, DisplayName: "Conformance Project"},
	})
	if err != nil {
		return fmt.Errorf("CreateProject: %w", err)
	}
	created, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("CreateProject wait: %w", err)
	}
	if created.GetProjectId() != id {
		return fmt.Errorf("created projectId = %q, want %q", created.GetProjectId(), id)
	}
	if created.GetState() != resourcemanagerpb.Project_ACTIVE {
		return fmt.Errorf("created state = %v, want ACTIVE", created.GetState())
	}
	return nil
}

// Check 3: ListProjects includes the created project. The required parent is
// ignored by the emulator (no org/folder hierarchy is modelled).
func checkRMListProjects(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	want := rmCreatedProjectID(cfg)
	it := client.ListProjects(ctx, &resourcemanagerpb.ListProjectsRequest{Parent: "projects/" + cfg.Project})
	for {
		p, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListProjects did not include created project %q", want)
		}
		if err != nil {
			return fmt.Errorf("ListProjects: %w", err)
		}
		if p.GetProjectId() == want {
			return nil
		}
	}
}

// Check 4: DeleteProject marks the project DELETE_REQUESTED.
func checkRMDeleteProject(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := rmCreatedProjectName(cfg)
	op, err := client.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteProject: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteProject wait: %w", err)
	}
	got, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetProject after delete: %w", err)
	}
	if got.GetState() != resourcemanagerpb.Project_DELETE_REQUESTED {
		return fmt.Errorf("state = %v, want DELETE_REQUESTED", got.GetState())
	}
	return nil
}

// Check 5: UndeleteProject restores the project to ACTIVE.
func checkRMUndeleteProject(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := rmCreatedProjectName(cfg)
	op, err := client.UndeleteProject(ctx, &resourcemanagerpb.UndeleteProjectRequest{Name: name})
	if err != nil {
		return fmt.Errorf("UndeleteProject: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UndeleteProject wait: %w", err)
	}
	got, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetProject after undelete: %w", err)
	}
	if got.GetState() != resourcemanagerpb.Project_ACTIVE {
		return fmt.Errorf("state = %v, want ACTIVE", got.GetState())
	}
	return nil
}

// Check 2: GetProject returns the synthesized ACTIVE project.
func checkRMGetProject(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := resourceManagerProject(cfg, "gcpc-grpc-rm-get")
	got, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetProject: %w", err)
	}
	if got.GetName() != name {
		return fmt.Errorf("GetProject name = %q, want %q", got.GetName(), name)
	}
	if got.GetProjectId() != cfg.ResourceName("gcpc-grpc-rm-get") {
		return fmt.Errorf("GetProject projectId = %q", got.GetProjectId())
	}
	if got.GetState() != resourcemanagerpb.Project_ACTIVE {
		return fmt.Errorf("GetProject state = %v, want ACTIVE", got.GetState())
	}
	if got.GetDisplayName() == "" {
		return fmt.Errorf("GetProject displayName is empty")
	}
	return nil
}

// Check 6: GetIamPolicy returns a policy with an etag for a synthesized project.
func checkRMGetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resource := resourceManagerProject(cfg, "gcpc-grpc-rm-iamget")
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if len(pol.GetEtag()) == 0 {
		return fmt.Errorf("GetIamPolicy returned no etag")
	}
	return nil
}

// Check 7: SetIamPolicy stores a binding and rotates the etag.
func checkRMSetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resource := resourceManagerProject(cfg, "gcpc-grpc-rm-iamset")
	before, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	after, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Etag:     before.GetEtag(),
			Bindings: []*iampb.Binding{{Role: "roles/owner", Members: []string{"user:conformance@example.com"}}},
		},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	if len(after.GetBindings()) != 1 || after.GetBindings()[0].GetRole() != "roles/owner" {
		return fmt.Errorf("SetIamPolicy bindings = %v", after.GetBindings())
	}
	if len(after.GetEtag()) == 0 {
		return fmt.Errorf("SetIamPolicy returned no etag")
	}
	return nil
}

// Check 8: TestIamPermissions echoes the requested permissions.
func checkRMTestIamPermissions(ctx context.Context, cfg Config) error {
	client, err := newResourceManagerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resource := resourceManagerProject(cfg, "gcpc-grpc-rm-test")
	want := []string{"resourcemanager.projects.get", "resourcemanager.projects.setIamPolicy"}
	got, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    resource,
		Permissions: want,
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	if len(got.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions = %v, want %v", got.GetPermissions(), want)
	}
	return nil
}
