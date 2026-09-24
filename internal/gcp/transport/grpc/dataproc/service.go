// Package dataproc is the gRPC transport for Cloud Dataproc v1
// (google.cloud.dataproc.v1.ClusterController / JobController). It is a thin
// proto adapter over the transport-neutral core in
// internal/gcp/service/dataproc: it transcodes between the generated protobuf
// messages and the core's typed API, and maps core errors to gRPC status codes.
// It owns no business logic and no state beyond its default project.
//
// Cluster create/update/start/stop/delete and SubmitJobAsOperation return a
// google.longrunning.Operation whose metadata and response are packed as typed
// Any protos, so the generated client's Wait observes the result without
// polling. WorkflowTemplateService, BatchController, Session* and
// AutoscalingPolicy are deliberately not registered.
package dataproc

import (
	"context"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/dataproc"

	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements dataprocpb.ClusterControllerServer and
// dataprocpb.JobControllerServer over the shared core.
type Service struct {
	dataprocpb.UnimplementedClusterControllerServer
	dataprocpb.UnimplementedJobControllerServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Dataproc gRPC service wrapping the core. defaultProj is
// the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// resolveProject prefers the request's explicit project_id, falling back to the
// gRPC routing metadata then the configured default.
func (s *Service) resolveProject(ctx context.Context, projectID string) string {
	if projectID != "" {
		return projectID
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── ClusterController ────────────────────────────────────────────────────────

func (s *Service) CreateCluster(ctx context.Context, req *dataprocpb.CreateClusterRequest) (*longrunningpb.Operation, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	_, op, err := s.core.CreateCluster(ctx, project, req.GetRegion(), req.GetCluster().GetClusterName(), clusterInputFromProto(req.GetCluster()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

func (s *Service) UpdateCluster(ctx context.Context, req *dataprocpb.UpdateClusterRequest) (*longrunningpb.Operation, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	_, op, err := s.core.UpdateCluster(ctx, project, req.GetRegion(), req.GetClusterName(), clusterInputFromProto(req.GetCluster()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

func (s *Service) DeleteCluster(ctx context.Context, req *dataprocpb.DeleteClusterRequest) (*longrunningpb.Operation, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	op, err := s.core.DeleteCluster(ctx, project, req.GetRegion(), req.GetClusterName())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

func (s *Service) GetCluster(ctx context.Context, req *dataprocpb.GetClusterRequest) (*dataprocpb.Cluster, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	c, err := s.core.GetCluster(ctx, project, req.GetRegion(), req.GetClusterName())
	if err != nil {
		return nil, mapError(err)
	}
	return clusterToProto(c), nil
}

func (s *Service) ListClusters(ctx context.Context, req *dataprocpb.ListClustersRequest) (*dataprocpb.ListClustersResponse, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	page, next, err := s.core.ListClusters(ctx, project, req.GetRegion(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &dataprocpb.ListClustersResponse{NextPageToken: next}
	for _, c := range page {
		out.Clusters = append(out.Clusters, clusterToProto(c))
	}
	return out, nil
}

func (s *Service) StopCluster(ctx context.Context, req *dataprocpb.StopClusterRequest) (*longrunningpb.Operation, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	_, op, err := s.core.StopCluster(ctx, project, req.GetRegion(), req.GetClusterName())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

func (s *Service) StartCluster(ctx context.Context, req *dataprocpb.StartClusterRequest) (*longrunningpb.Operation, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	_, op, err := s.core.StartCluster(ctx, project, req.GetRegion(), req.GetClusterName())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

// DiagnoseCluster remains an unimplemented stub (the emulator models no agent
// diagnostics plane). It fails loud with codes.Unimplemented.
func (s *Service) DiagnoseCluster(_ context.Context, _ *dataprocpb.DiagnoseClusterRequest) (*longrunningpb.Operation, error) {
	return nil, mapError(s.core.DiagnoseCluster())
}

// ─── JobController ────────────────────────────────────────────────────────────

func (s *Service) SubmitJob(ctx context.Context, req *dataprocpb.SubmitJobRequest) (*dataprocpb.Job, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	j, err := s.core.SubmitJob(ctx, project, req.GetRegion(), jobInputFromProto(req.GetJob()))
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) SubmitJobAsOperation(ctx context.Context, req *dataprocpb.SubmitJobRequest) (*longrunningpb.Operation, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	op, err := s.core.SubmitJobAsOperation(ctx, project, req.GetRegion(), jobInputFromProto(req.GetJob()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

func (s *Service) GetJob(ctx context.Context, req *dataprocpb.GetJobRequest) (*dataprocpb.Job, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	j, err := s.core.GetJob(ctx, project, req.GetRegion(), req.GetJobId())
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) ListJobs(ctx context.Context, req *dataprocpb.ListJobsRequest) (*dataprocpb.ListJobsResponse, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	page, next, err := s.core.ListJobs(ctx, project, req.GetRegion(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &dataprocpb.ListJobsResponse{NextPageToken: next}
	for _, j := range page {
		out.Jobs = append(out.Jobs, jobToProto(j))
	}
	return out, nil
}

func (s *Service) UpdateJob(ctx context.Context, req *dataprocpb.UpdateJobRequest) (*dataprocpb.Job, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	j, err := s.core.UpdateJob(ctx, project, req.GetRegion(), req.GetJobId(), jobInputFromProto(req.GetJob()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) CancelJob(ctx context.Context, req *dataprocpb.CancelJobRequest) (*dataprocpb.Job, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	j, err := s.core.CancelJob(ctx, project, req.GetRegion(), req.GetJobId())
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) DeleteJob(ctx context.Context, req *dataprocpb.DeleteJobRequest) (*emptypb.Empty, error) {
	project := s.resolveProject(ctx, req.GetProjectId())
	if err := s.core.DeleteJob(ctx, project, req.GetRegion(), req.GetJobId()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// compile-time assertions that Service implements both generated servers.
var (
	_ dataprocpb.ClusterControllerServer = (*Service)(nil)
	_ dataprocpb.JobControllerServer     = (*Service)(nil)
)
