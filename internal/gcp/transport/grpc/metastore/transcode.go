package metastore

import (
	"context"
	"encoding/json"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/metastore"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// protojsonOpts tolerates unknown fields so a REST-created resource body (which
// may carry fields the proto snapshot does not yet have) still transcodes.
var protojsonOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// unmarshalProtoJSON decodes a rendered wire map into a proto message, ignoring
// an (impossible for these plain shapes) error so a malformed stored body
// degrades to the typed zero value.
func unmarshalProtoJSON(m map[string]any, out proto.Message) {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	_ = protojsonOpts.Unmarshal(data, out)
}

// serviceToProto renders a stored service as the proto Service. It is driven by
// the core's ServiceJSON map (via protojson), so the REST and gRPC renders of
// the same stored record cannot drift.
func serviceToProto(svc metastorestore.Service, project string) *metastorepb.Service {
	out := &metastorepb.Service{}
	unmarshalProtoJSON(core.ServiceJSON(svc, project), out)
	return out
}

// backupToProto renders a stored backup as the proto Backup.
func backupToProto(b metastorestore.Backup, project string) *metastorepb.Backup {
	out := &metastorepb.Backup{}
	unmarshalProtoJSON(core.BackupJSON(b, project), out)
	return out
}

// metadataImportToProto renders a stored metadata import as the proto
// MetadataImport.
func metadataImportToProto(mi metastorestore.MetadataImport, project string) *metastorepb.MetadataImport {
	out := &metastorepb.MetadataImport{}
	unmarshalProtoJSON(core.MetadataImportJSON(mi, project), out)
	return out
}

// operationToProto renders a stored operation as the proto
// google.longrunning.Operation, packing the typed metadata and the
// caller-supplied response message (a Service/Backup/MetadataImport, or Empty
// for delete).
func operationToProto(op metastorestore.Operation, project string, response proto.Message) (*longrunningpb.Operation, error) {
	metadata, err := anypb.New(&metastorepb.OperationMetadata{
		CreateTime: timestamppb.New(op.CreateTime),
		EndTime:    timestamppb.New(op.EndTime),
		Target:     op.Target,
		Verb:       op.Verb,
		ApiVersion: "v1",
	})
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{
		Name:     core.OperationName(project, op.Location, op.ID),
		Metadata: metadata,
		Done:     true,
	}
	if response != nil {
		resp, err := anypb.New(response)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: resp}
	}
	return out, nil
}

// emptyResponse builds the google.protobuf.Empty result a delete operation
// carries.
func emptyResponse() proto.Message { return &emptypb.Empty{} }

// projectFor resolves the owning project for a name/parent, falling back to the
// gRPC metadata routing header then the configured default.
func (s *Service) projectFor(ctx context.Context, name string) string {
	if p := core.ProjectFromName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}
