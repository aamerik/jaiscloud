package serviceusage

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/serviceusage"
)

// parseName extracts the project and (optional) service id from a Service Usage
// resource name: projects/{project}/services/{service}, or the projects/{project}
// parent when only listing.
func parseName(name string) (project, service string) {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		switch p {
		case "projects":
			if i+1 < len(parts) {
				project = parts[i+1]
			}
		case "services":
			if i+1 < len(parts) {
				service = parts[i+1]
			}
		}
	}
	return project, service
}

// projectFor resolves the owning project: the name's project when present, else
// the gRPC metadata routing header, else the configured default.
func (s *Service) projectFor(ctx context.Context, project string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// stateToProto maps a core State to the proto enum.
func stateToProto(st core.State) serviceusagepb.State {
	switch st {
	case core.StateEnabled:
		return serviceusagepb.State_ENABLED
	case core.StateDisabled:
		return serviceusagepb.State_DISABLED
	default:
		return serviceusagepb.State_STATE_UNSPECIFIED
	}
}

// apiToProto renders a core API as the proto Service.
func apiToProto(a core.API) *serviceusagepb.Service {
	return &serviceusagepb.Service{
		Name:   a.Name,
		Parent: a.Parent,
		Config: &serviceusagepb.ServiceConfig{Name: a.ConfigName},
		State:  stateToProto(a.State),
	}
}

// operationToProto renders a core Operation as the proto
// google.longrunning.Operation, packing the metadata and the caller-supplied
// response message (EnableServiceResponse, DisableServiceResponse, or
// BatchEnableServicesResponse) as typed Any values.
func operationToProto(op core.Operation, response proto.Message) (*longrunningpb.Operation, error) {
	metadata, err := anypb.New(&serviceusagepb.OperationMetadata{ResourceNames: op.ResourceNames})
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: op.Name, Metadata: metadata, Done: op.Done}
	if response != nil {
		resp, err := anypb.New(response)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: resp}
	}
	return out, nil
}
