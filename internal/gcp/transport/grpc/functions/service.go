// Package functions is the gRPC transport for Cloud Functions v1 and v2
// (google.cloud.functions.v1.CloudFunctionsService and
// google.cloud.functions.v2.FunctionService). It is a thin proto adapter over
// the transport-neutral core in internal/gcp/service/functions: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// v1 and v2 are separate Go servers (Service and ServiceV2) because the two
// generated server interfaces declare methods with the same names but different
// request types, which a single Go type cannot satisfy. Both wrap the SAME core
// Service instance, so the two API versions cannot drift.
//
// Create/Update/Delete return a google.longrunning.Operation whose metadata and
// response are packed as typed Any protos (OperationMetadataV1 / OperationMetadata
// for v1 and v2 respectively, and the Function or google.protobuf.Empty as the
// response), so the generated client's Wait observes the result without
// polling. Runtime invocation (v1 CallFunction) and v2 ListRuntimes are out of
// scope for the control plane and fail loud with codes.Unimplemented.
package functions

import (
	"context"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/functions"
)

// Service implements functionspb.CloudFunctionsServiceServer (v1) over the
// shared core.
type Service struct {
	functionspb.UnimplementedCloudFunctionsServiceServer

	core        *core.Service
	defaultProj string
}

// ServiceV2 implements apiv2functionspb.FunctionServiceServer (v2) over the
// same shared core.
type ServiceV2 struct {
	apiv2functionspb.UnimplementedFunctionServiceServer

	core        *core.Service
	defaultProj string
}

// NewService returns the Cloud Functions v1 gRPC server wrapping the core.
// defaultProj is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// NewServiceV2 returns the Cloud Functions v2 gRPC server wrapping the core.
func NewServiceV2(c *core.Service, defaultProj string) *ServiceV2 {
	return &ServiceV2{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// resolveProject prefers an explicit project (parsed from a resource name),
// falling back to the gRPC routing metadata then the configured default.
func resolveProject(ctx context.Context, project, defaultProj string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, defaultProj)
}

// ─── v1 CloudFunctionsService ────────────────────────────────────────────────

func (s *Service) CreateFunction(ctx context.Context, req *functionspb.CreateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, err := core.ParseLocationParent(req.GetLocation())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V1)
	_, op, err := s.core.CreateFunction(ctx, project, location, "", in, core.V1)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV1(project, op)
}

func (s *Service) UpdateFunction(ctx context.Context, req *functionspb.UpdateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetFunction().GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V1)
	_, op, err := s.core.UpdateFunction(ctx, project, location, id, in, req.GetUpdateMask().GetPaths(), core.V1)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV1(project, op)
}

func (s *Service) DeleteFunction(ctx context.Context, req *functionspb.DeleteFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	op, err := s.core.DeleteFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV1(project, op)
}

func (s *Service) GetFunction(ctx context.Context, req *functionspb.GetFunctionRequest) (*functionspb.CloudFunction, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	f, err := s.core.GetFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return functionToProtoV1(project, f), nil
}

func (s *Service) ListFunctions(ctx context.Context, req *functionspb.ListFunctionsRequest) (*functionspb.ListFunctionsResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	page, next, err := s.core.ListFunctions(ctx, project, location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &functionspb.ListFunctionsResponse{NextPageToken: next}
	for _, f := range page {
		out.Functions = append(out.Functions, functionToProtoV1(project, f))
	}
	return out, nil
}

func (s *Service) GenerateUploadUrl(ctx context.Context, req *functionspb.GenerateUploadUrlRequest) (*functionspb.GenerateUploadUrlResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	return &functionspb.GenerateUploadUrlResponse{UploadUrl: s.core.GenerateUploadURL(project, location)}, nil
}

func (s *Service) GenerateDownloadUrl(ctx context.Context, req *functionspb.GenerateDownloadUrlRequest) (*functionspb.GenerateDownloadUrlResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	url, err := s.core.GenerateDownloadURL(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &functionspb.GenerateDownloadUrlResponse{DownloadUrl: url}, nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, err := core.ParseFunctionName(req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	pol, err := s.core.GetIamPolicy(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, err := core.ParseFunctionName(req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	pol, err := s.core.SetIamPolicy(ctx, project, location, id, protojsonToMap(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	perms, err := s.core.TestIamPermissions(ctx, project, location, id, req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// ─── v2 FunctionService ──────────────────────────────────────────────────────

func (s *ServiceV2) CreateFunction(ctx context.Context, req *apiv2functionspb.CreateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V2)
	_, op, err := s.core.CreateFunction(ctx, project, location, req.GetFunctionId(), in, core.V2)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV2(project, op)
}

func (s *ServiceV2) UpdateFunction(ctx context.Context, req *apiv2functionspb.UpdateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetFunction().GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V2)
	_, op, err := s.core.UpdateFunction(ctx, project, location, id, in, req.GetUpdateMask().GetPaths(), core.V2)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV2(project, op)
}

func (s *ServiceV2) DeleteFunction(ctx context.Context, req *apiv2functionspb.DeleteFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	op, err := s.core.DeleteFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV2(project, op)
}

func (s *ServiceV2) GetFunction(ctx context.Context, req *apiv2functionspb.GetFunctionRequest) (*apiv2functionspb.Function, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	f, err := s.core.GetFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return functionToProtoV2(project, f), nil
}

func (s *ServiceV2) ListFunctions(ctx context.Context, req *apiv2functionspb.ListFunctionsRequest) (*apiv2functionspb.ListFunctionsResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	page, next, err := s.core.ListFunctions(ctx, project, location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &apiv2functionspb.ListFunctionsResponse{NextPageToken: next}
	for _, f := range page {
		out.Functions = append(out.Functions, functionToProtoV2(project, f))
	}
	return out, nil
}

func (s *ServiceV2) GenerateUploadUrl(ctx context.Context, req *apiv2functionspb.GenerateUploadUrlRequest) (*apiv2functionspb.GenerateUploadUrlResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	return &apiv2functionspb.GenerateUploadUrlResponse{UploadUrl: s.core.GenerateUploadURL(project, location)}, nil
}

func (s *ServiceV2) GenerateDownloadUrl(ctx context.Context, req *apiv2functionspb.GenerateDownloadUrlRequest) (*apiv2functionspb.GenerateDownloadUrlResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	url, err := s.core.GenerateDownloadURL(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &apiv2functionspb.GenerateDownloadUrlResponse{DownloadUrl: url}, nil
}

// policyToProto renders an internal policy.Policy as an iampb.Policy.
func policyToProto(p policy.Policy) *iampb.Policy {
	var out iampb.Policy
	if err := mapToProto(policy.ToMap(p), &out); err != nil {
		return &iampb.Policy{}
	}
	return &out
}

// compile-time assertions that the servers implement both generated interfaces.
var (
	_ functionspb.CloudFunctionsServiceServer = (*Service)(nil)
	_ apiv2functionspb.FunctionServiceServer  = (*ServiceV2)(nil)
)
