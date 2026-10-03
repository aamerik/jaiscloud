// Package container is the gRPC transport for Google Kubernetes Engine (GKE) v1
// (google.container.v1.ClusterManager). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/container: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// The GKE v1 proto is the legacy API: every request carries the canonical
// name/parent fields AND the older project_id/zone/cluster_id (or operation_id)
// fields, and the Java/Go clients may use either. The adapter resolves both —
// name/parent wins when present, otherwise the legacy fields are folded into
// the core's project + location + resource tuple.
package container

import (
	"context"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/container"
	"jaiscloud/internal/model"
)

// Service implements containerpb.ClusterManagerServer over the shared core.
type Service struct {
	containerpb.UnimplementedClusterManagerServer

	core        *core.Service
	defaultProj string
}

// NewService returns a GKE gRPC service wrapping the core. defaultProj is the
// config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func invalid(msg string) error { return model.NewProviderError("InvalidArgument", msg, 400) }

// projectFor resolves the owning project for a request, falling back to the
// gRPC routing metadata then the configured default.
func (s *Service) projectFor(ctx context.Context, project string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// parentRef resolves the project + location a collection RPC targets. A
// canonical parent (projects/{p}/locations/{l}) wins; otherwise the legacy
// project_id + zone pair is used.
func parentRef(parent, projectID, zone string) (project, location string, err error) {
	if parent != "" {
		p, l, ok := core.ParseParent(parent)
		if !ok {
			return "", "", invalid("invalid parent: " + parent)
		}
		return p, l, nil
	}
	if projectID == "" || zone == "" {
		return "", "", invalid("parent (or project_id and zone) is required")
	}
	return projectID, zone, nil
}

// clusterRef resolves the project + location + cluster a cluster RPC targets. A
// canonical name (projects/{p}/locations/{l}/clusters/{c}) wins; otherwise the
// legacy project_id + zone + cluster_id triple is used.
func clusterRef(name, projectID, zone, clusterID string) (project, location, cluster string, err error) {
	if name != "" {
		p, l, c, ok := core.ParseClusterName(name)
		if !ok {
			return "", "", "", invalid("invalid cluster name: " + name)
		}
		return p, l, c, nil
	}
	if projectID == "" || zone == "" || clusterID == "" {
		return "", "", "", invalid("name (or project_id, zone and cluster_id) is required")
	}
	return projectID, zone, clusterID, nil
}

// operationRef resolves the project + location + operation an operation RPC
// targets. A canonical name (projects/{p}/locations/{l}/operations/{o}) wins;
// otherwise the legacy project_id + zone + operation_id triple is used.
func operationRef(name, projectID, zone, operationID string) (project, location, operation string, err error) {
	if name != "" {
		p, l, o, ok := core.ParseOperationName(name)
		if !ok {
			return "", "", "", invalid("invalid operation name: " + name)
		}
		return p, l, o, nil
	}
	if projectID == "" || zone == "" || operationID == "" {
		return "", "", "", invalid("name (or project_id, zone and operation_id) is required")
	}
	return projectID, zone, operationID, nil
}

// ─── Clusters ─────────────────────────────────────────────────────────────────

func (s *Service) ListClusters(ctx context.Context, req *containerpb.ListClustersRequest) (*containerpb.ListClustersResponse, error) {
	project, location, err := parentRef(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, mapError(err)
	}
	clusters, err := s.core.ListClusters(ctx, s.projectFor(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	out := &containerpb.ListClustersResponse{}
	for _, c := range clusters {
		out.Clusters = append(out.Clusters, clusterToProto(c))
	}
	return out, nil
}

func (s *Service) GetCluster(ctx context.Context, req *containerpb.GetClusterRequest) (*containerpb.Cluster, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	c, err := s.core.GetCluster(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return clusterToProto(c), nil
}

func (s *Service) CreateCluster(ctx context.Context, req *containerpb.CreateClusterRequest) (*containerpb.Operation, error) {
	project, location, err := parentRef(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.CreateCluster(ctx, s.projectFor(ctx, project), location, clusterFromProto(req.GetCluster()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) DeleteCluster(ctx context.Context, req *containerpb.DeleteClusterRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.DeleteCluster(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (s *Service) GetOperation(ctx context.Context, req *containerpb.GetOperationRequest) (*containerpb.Operation, error) {
	project, location, operation, err := operationRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetOperationId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.GetOperation(ctx, s.projectFor(ctx, project), location, operation)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) ListOperations(ctx context.Context, req *containerpb.ListOperationsRequest) (*containerpb.ListOperationsResponse, error) {
	project, location, err := parentRef(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, mapError(err)
	}
	ops, err := s.core.ListOperations(ctx, s.projectFor(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	out := &containerpb.ListOperationsResponse{}
	for _, op := range ops {
		out.Operations = append(out.Operations, operationToProto(op))
	}
	return out, nil
}

// compile-time assertion that Service implements the generated server.
var _ containerpb.ClusterManagerServer = (*Service)(nil)
