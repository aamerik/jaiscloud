package functions

import (
	"encoding/json"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// protojsonOpts tolerates unknown fields so a REST-created resource body (whose
// Discovery shape may carry fields the proto snapshot does not yet have) still
// transcodes.
var protojsonOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// protojsonToMap renders a proto message to a Discovery-shaped map (camelCase
// field names), the inverse of mapToProto.
func protojsonToMap(m proto.Message) map[string]any {
	if m == nil {
		return nil
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

// mapToProto renders a Discovery-shaped map into a proto message.
func mapToProto(m map[string]any, out proto.Message) error {
	if m == nil {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return protojsonOpts.Unmarshal(b, out)
}

// functionToProtoV1 renders a stored function as the v1 proto CloudFunction via
// the shared Discovery renderer, so REST and gRPC cannot drift.
func functionToProtoV1(project string, f functionsstore.Function) *functionspb.CloudFunction {
	out := &functionspb.CloudFunction{}
	if err := mapToProto(core.FunctionJSON(core.V1, project, f), out); err != nil {
		return &functionspb.CloudFunction{Name: core.FunctionJSON(core.V1, project, f)["name"].(string)}
	}
	return out
}

// functionToProtoV2 renders a stored function as the v2 proto Function.
func functionToProtoV2(project string, f functionsstore.Function) *apiv2functionspb.Function {
	out := &apiv2functionspb.Function{}
	if err := mapToProto(core.FunctionJSON(core.V2, project, f), out); err != nil {
		return &apiv2functionspb.Function{Name: core.FunctionJSON(core.V2, project, f)["name"].(string)}
	}
	return out
}

// operationToProtoV1 packs a done v1 operation with typed Any metadata
// (OperationMetadataV1) and a typed Any response (CloudFunction, or Empty for a
// delete).
func operationToProtoV1(project string, op core.Operation) (*longrunningpb.Operation, error) {
	ts := timestamppb.New(clock.Now().UTC())
	metaAny, err := anypb.New(&functionspb.OperationMetadataV1{
		Target:     op.Target,
		Type:       operationTypeV1(op.Verb),
		UpdateTime: ts,
	})
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: core.OperationName(project, op), Metadata: metaAny, Done: true}
	respAny, err := operationResponseProtoV1(project, op)
	if err != nil {
		return nil, err
	}
	out.Result = &longrunningpb.Operation_Response{Response: respAny}
	return out, nil
}

// operationToProtoV2 packs a done v2 operation with typed Any metadata
// (OperationMetadata) and a typed Any response (Function, or Empty for a
// delete).
func operationToProtoV2(project string, op core.Operation) (*longrunningpb.Operation, error) {
	ts := timestamppb.New(clock.Now().UTC())
	metaAny, err := anypb.New(&apiv2functionspb.OperationMetadata{
		Target:        op.Target,
		Verb:          op.Verb,
		ApiVersion:    "v2",
		CreateTime:    ts,
		EndTime:       ts,
		OperationType: operationTypeV2(op.Verb),
	})
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: core.OperationName(project, op), Metadata: metaAny, Done: true}
	respAny, err := operationResponseProtoV2(project, op)
	if err != nil {
		return nil, err
	}
	out.Result = &longrunningpb.Operation_Response{Response: respAny}
	return out, nil
}

func operationResponseProtoV1(project string, op core.Operation) (*anypb.Any, error) {
	if op.Function != nil {
		return anypb.New(functionToProtoV1(project, *op.Function))
	}
	return anypb.New(&emptypb.Empty{})
}

func operationResponseProtoV2(project string, op core.Operation) (*anypb.Any, error) {
	if op.Function != nil {
		return anypb.New(functionToProtoV2(project, *op.Function))
	}
	return anypb.New(&emptypb.Empty{})
}

func operationTypeV1(verb string) functionspb.OperationType {
	switch verb {
	case "create":
		return functionspb.OperationType_CREATE_FUNCTION
	case "update":
		return functionspb.OperationType_UPDATE_FUNCTION
	case "delete":
		return functionspb.OperationType_DELETE_FUNCTION
	}
	return functionspb.OperationType_OPERATION_UNSPECIFIED
}

func operationTypeV2(verb string) apiv2functionspb.OperationType {
	switch verb {
	case "create":
		return apiv2functionspb.OperationType_CREATE_FUNCTION
	case "update":
		return apiv2functionspb.OperationType_UPDATE_FUNCTION
	case "delete":
		return apiv2functionspb.OperationType_DELETE_FUNCTION
	}
	return apiv2functionspb.OperationType_OPERATIONTYPE_UNSPECIFIED
}
