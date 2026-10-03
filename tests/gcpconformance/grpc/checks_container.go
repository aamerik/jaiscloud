package grpcconformance

import (
	"context"
	"fmt"

	container "cloud.google.com/go/container/apiv1"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// containerLocation is a canonical GKE location; the emulator does not validate
// the location list.
const containerLocation = "us-central1"

// containerChecks covers the GKE v1 surface (google.container.v1.ClusterManager)
// via the official generated cloud.google.com/go/container/apiv1 client:
// ListClusters, GetCluster, CreateCluster, DeleteCluster, GetOperation,
// ListOperations. GKE CreateCluster/DeleteCluster return the
// google.container.v1.Operation inline (not a google.longrunning.Operation).
//
// Every probe is self-contained and run-unique (cfg.ResourceName).
func containerChecks() []Check {
	return []Check{
		{Service: "container", RPC: "CreateCluster", Method: "CreateCluster", KeyField: "CREATE_CLUSTER operation DONE", Run: checkContainerCreateCluster},
		{Service: "container", RPC: "GetCluster", Method: "GetCluster", KeyField: "name/status/currentMasterVersion round-trip", Run: checkContainerGetCluster},
		{Service: "container", RPC: "GetCluster (legacy project/zone)", Method: "GetCluster", KeyField: "legacy project_id/zone/cluster_id addressing", Run: checkContainerGetClusterLegacy},
		{Service: "container", RPC: "ListClusters", Method: "ListClusters", KeyField: "created cluster present", Run: checkContainerListClusters},
		{Service: "container", RPC: "GetOperation", Method: "GetOperation", KeyField: "operation name/type round-trip", Run: checkContainerGetOperation},
		{Service: "container", RPC: "ListOperations", Method: "ListOperations", KeyField: "created operation present", Run: checkContainerListOperations},
		{Service: "container", RPC: "DeleteCluster", Method: "DeleteCluster", KeyField: "DELETE_CLUSTER DONE + NotFound after", Run: checkContainerDeleteCluster},
	}
}

// newContainerClient dials the emulator and returns the official generated GKE
// client (gRPC transport).
func newContainerClient(ctx context.Context, cfg Config) (*container.ClusterManagerClient, error) {
	return container.NewClusterManagerClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func containerParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, containerLocation)
}

func containerName(cfg Config, prefix string) string {
	return containerParent(cfg) + "/clusters/" + cfg.ResourceName(prefix)
}

func operationName(cfg Config, parent, operationID string) string {
	return parent + "/operations/" + operationID
}

// createContainerCluster creates a run-unique cluster and returns the operation.
func createContainerCluster(ctx context.Context, client *container.ClusterManagerClient, cfg Config, prefix string) (*containerpb.Operation, error) {
	op, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cfg.ResourceName(prefix), InitialNodeCount: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("CreateCluster: %w", err)
	}
	return op, nil
}

// Check 1: CreateCluster returns a DONE CREATE_CLUSTER operation.
func checkContainerCreateCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-gke-create")
	if err != nil {
		return err
	}
	if op.GetOperationType() != containerpb.Operation_CREATE_CLUSTER {
		return fmt.Errorf("CreateCluster operationType = %v, want CREATE_CLUSTER", op.GetOperationType())
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("CreateCluster status = %v, want DONE", op.GetStatus())
	}
	if op.GetName() == "" || op.GetSelfLink() == "" {
		return fmt.Errorf("CreateCluster operation missing name/selfLink")
	}
	return nil
}

// Check 2: GetCluster returns the short name, RUNNING status and a master version.
func checkContainerGetCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := containerName(cfg, "gcpc-grpc-gke-get")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cfg.ResourceName("gcpc-grpc-gke-get"), InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetName() != cfg.ResourceName("gcpc-grpc-gke-get") {
		return fmt.Errorf("GetCluster name = %q", got.GetName())
	}
	if got.GetStatus() != containerpb.Cluster_RUNNING {
		return fmt.Errorf("GetCluster status = %v, want RUNNING", got.GetStatus())
	}
	if got.GetCurrentMasterVersion() == "" {
		return fmt.Errorf("GetCluster currentMasterVersion is empty")
	}
	return nil
}

// Check 3: the legacy project_id/zone/cluster_id addressing reaches the same
// cluster as the canonical name.
func checkContainerGetClusterLegacy(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-gke-legacy")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: id, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{
		ProjectId: cfg.Project, Zone: containerLocation, ClusterId: id,
	})
	if err != nil {
		return fmt.Errorf("GetCluster (legacy): %w", err)
	}
	if got.GetName() != id {
		return fmt.Errorf("legacy GetCluster name = %q, want %q", got.GetName(), id)
	}
	if got.GetLocation() != containerLocation {
		return fmt.Errorf("legacy GetCluster location = %q", got.GetLocation())
	}
	return nil
}

// Check 4: ListClusters includes the created cluster.
func checkContainerListClusters(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-gke-list")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: id, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	resp, err := client.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: containerParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListClusters: %w", err)
	}
	for _, c := range resp.GetClusters() {
		if c.GetName() == id {
			return nil
		}
	}
	return fmt.Errorf("ListClusters did not include %q", id)
}

// Check 5: GetOperation returns the operation the create produced.
func checkContainerGetOperation(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-gke-op")
	if err != nil {
		return err
	}
	got, err := client.GetOperation(ctx, &containerpb.GetOperationRequest{
		Name: operationName(cfg, containerParent(cfg), op.GetName()),
	})
	if err != nil {
		return fmt.Errorf("GetOperation: %w", err)
	}
	if got.GetName() != op.GetName() || got.GetOperationType() != containerpb.Operation_CREATE_CLUSTER {
		return fmt.Errorf("GetOperation = %q/%v", got.GetName(), got.GetOperationType())
	}
	return nil
}

// Check 6: ListOperations includes the create operation.
func checkContainerListOperations(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-gke-ops")
	if err != nil {
		return err
	}
	resp, err := client.ListOperations(ctx, &containerpb.ListOperationsRequest{Parent: containerParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListOperations: %w", err)
	}
	for _, o := range resp.GetOperations() {
		if o.GetName() == op.GetName() {
			return nil
		}
	}
	return fmt.Errorf("ListOperations did not include %q", op.GetName())
}

// Check 7: DeleteCluster returns a DONE DELETE_CLUSTER operation and the cluster
// is subsequently NotFound.
func checkContainerDeleteCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := containerName(cfg, "gcpc-grpc-gke-delete")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cfg.ResourceName("gcpc-grpc-gke-delete"), InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteCluster: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_DELETE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("DeleteCluster op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	if _, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCluster after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}
