// Package serviceusage is the gRPC transport for Service Usage v1
// (google.api.serviceusage.v1.ServiceUsage). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/serviceusage: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// EnableService, DisableService, and BatchEnableServices return a done
// google.longrunning.Operation with the response (a Service, or a list of
// Services) packed as a typed Any, so the generated client's Wait observes it
// without polling. BatchGetServices is not implemented (Unimplemented), matching
// the emulator's control-plane scope.
package serviceusage

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/serviceusage"
)

// Service implements serviceusagepb.ServiceUsageServer over the shared core.
type Service struct {
	serviceusagepb.UnimplementedServiceUsageServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Service Usage gRPC service wrapping the core. defaultProj
// is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func (s *Service) GetService(ctx context.Context, req *serviceusagepb.GetServiceRequest) (*serviceusagepb.Service, error) {
	project, service := parseName(req.GetName())
	api, err := s.core.GetAPI(ctx, s.projectFor(ctx, project), service)
	if err != nil {
		return nil, mapError(err)
	}
	return apiToProto(api), nil
}

func (s *Service) ListServices(ctx context.Context, req *serviceusagepb.ListServicesRequest) (*serviceusagepb.ListServicesResponse, error) {
	project, _ := parseName(req.GetParent())
	filter, err := core.ParseFilter(req.GetFilter())
	if err != nil {
		return nil, mapError(err)
	}
	page, next, err := s.core.ListAPIs(ctx, s.projectFor(ctx, project), filter, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &serviceusagepb.ListServicesResponse{NextPageToken: next}
	for _, a := range page {
		out.Services = append(out.Services, apiToProto(a))
	}
	return out, nil
}

func (s *Service) EnableService(ctx context.Context, req *serviceusagepb.EnableServiceRequest) (*longrunningpb.Operation, error) {
	project, service := parseName(req.GetName())
	api, op, err := s.core.EnableAPI(ctx, s.projectFor(ctx, project), service)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, &serviceusagepb.EnableServiceResponse{Service: apiToProto(api)})
}

func (s *Service) DisableService(ctx context.Context, req *serviceusagepb.DisableServiceRequest) (*longrunningpb.Operation, error) {
	project, service := parseName(req.GetName())
	api, op, err := s.core.DisableAPI(ctx, s.projectFor(ctx, project), service)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, &serviceusagepb.DisableServiceResponse{Service: apiToProto(api)})
}

func (s *Service) BatchEnableServices(ctx context.Context, req *serviceusagepb.BatchEnableServicesRequest) (*longrunningpb.Operation, error) {
	project, _ := parseName(req.GetParent())
	apis, op, err := s.core.BatchEnableAPIs(ctx, s.projectFor(ctx, project), req.GetServiceIds())
	if err != nil {
		return nil, mapError(err)
	}
	resp := &serviceusagepb.BatchEnableServicesResponse{}
	for _, a := range apis {
		resp.Services = append(resp.Services, apiToProto(a))
	}
	return operationToProto(op, resp)
}

// compile-time assertion that Service implements the generated server.
var _ serviceusagepb.ServiceUsageServer = (*Service)(nil)
