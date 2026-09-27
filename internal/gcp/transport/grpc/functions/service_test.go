package functions

import (
	"context"
	"errors"
	"testing"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	lambdaexec "jaiscloud/internal/executor/lambda"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

func newTestV1() *Service {
	return NewService(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore()), "proj")
}

func newTestV2() *ServiceV2 {
	return NewServiceV2(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore()), "proj")
}

func createV1Req(id string) *functionspb.CreateFunctionRequest {
	return &functionspb.CreateFunctionRequest{
		Location: "projects/proj/locations/us-central1",
		Function: &functionspb.CloudFunction{
			Name:       "projects/proj/locations/us-central1/functions/" + id,
			Runtime:    "nodejs20",
			EntryPoint: "handler",
		},
	}
}

func TestCreateFunctionV1_TypedOperation(t *testing.T) {
	s := newTestV1()
	op, err := s.CreateFunction(context.Background(), createV1Req("f1"))
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("operation not done")
	}
	var meta functionspb.OperationMetadataV1
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetTarget() != "projects/proj/locations/us-central1/functions/f1" || meta.GetType() != functionspb.OperationType_CREATE_FUNCTION {
		t.Fatalf("metadata = %+v", &meta)
	}
	var fn functionspb.CloudFunction
	if err := op.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f1" || fn.GetStatus() != functionspb.CloudFunctionStatus_ACTIVE {
		t.Fatalf("function = %+v", &fn)
	}
}

func TestGetListUpdateDeleteV1(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	fn, err := s.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: "projects/proj/locations/us-central1/functions/f1"})
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if fn.GetRuntime() != "nodejs20" || fn.GetEntryPoint() != "handler" {
		t.Fatalf("function = %+v", fn)
	}
	list, err := s.ListFunctions(ctx, &functionspb.ListFunctionsRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListFunctions: %v", err)
	}
	if len(list.GetFunctions()) != 1 {
		t.Fatalf("functions = %+v", list.GetFunctions())
	}
	op, err := s.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{
		Function:   &functionspb.CloudFunction{Name: "projects/proj/locations/us-central1/functions/f1", Runtime: "nodejs22"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"runtime"}},
	})
	if err != nil {
		t.Fatalf("UpdateFunction: %v", err)
	}
	var updated functionspb.CloudFunction
	if err := op.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if updated.GetRuntime() != "nodejs22" || updated.GetEntryPoint() != "handler" {
		t.Fatalf("updated = %+v", &updated)
	}
	delOp, err := s.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: "projects/proj/locations/us-central1/functions/f1"})
	if err != nil {
		t.Fatalf("DeleteFunction: %v", err)
	}
	if delOp.GetResponse().GetTypeUrl() != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("delete response type = %q, want Empty", delOp.GetResponse().GetTypeUrl())
	}
	if _, err := s.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: "projects/proj/locations/us-central1/functions/f1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetFunction after delete = %v, want NotFound", err)
	}
}

func TestCreateFunctionV2_TypedOperation(t *testing.T) {
	s := newTestV2()
	op, err := s.CreateFunction(context.Background(), &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: "f2",
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("operation not done")
	}
	var meta apiv2functionspb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetTarget() != "projects/proj/locations/us-central1/functions/f2" || meta.GetVerb() != "create" {
		t.Fatalf("metadata = %+v", &meta)
	}
	var fn apiv2functionspb.Function
	if err := op.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f2" || fn.GetState() != apiv2functionspb.Function_ACTIVE {
		t.Fatalf("function = %+v", &fn)
	}
	if fn.GetBuildConfig().GetRuntime() != "nodejs20" {
		t.Fatalf("buildConfig = %+v", fn.GetBuildConfig())
	}
}

func TestGenerateURLs(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	up, err := s.GenerateUploadUrl(ctx, &functionspb.GenerateUploadUrlRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil || up.GetUploadUrl() == "" {
		t.Fatalf("GenerateUploadUrl = %v, %v", up, err)
	}
	dl, err := s.GenerateDownloadUrl(ctx, &functionspb.GenerateDownloadUrlRequest{Name: "projects/proj/locations/us-central1/functions/f1"})
	if err != nil || dl.GetDownloadUrl() == "" {
		t.Fatalf("GenerateDownloadUrl = %v, %v", dl, err)
	}
	if _, err := s.GenerateDownloadUrl(ctx, &functionspb.GenerateDownloadUrlRequest{Name: "projects/proj/locations/us-central1/functions/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GenerateDownloadUrl missing = %v, want NotFound", err)
	}
}

func TestIAMV1(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	name := "projects/proj/locations/us-central1/functions/f1"
	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: name,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/cloudfunctions.invoker", Members: []string{"allUsers"}}}},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	pol, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 1 {
		t.Fatalf("bindings = %+v", pol.GetBindings())
	}
	resp, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name, Permissions: []string{"cloudfunctions.functions.invoke"}})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(resp.GetPermissions()) != 1 {
		t.Fatalf("permissions = %v", resp.GetPermissions())
	}
}

// errExecutor fails every invocation, to exercise the in-band error path.
type errExecutor struct{}

func (errExecutor) Invoke(context.Context, lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	return lambdaexec.InvokeResult{}, errors.New("boom")
}
func (errExecutor) DeleteFunction(context.Context, string) {}
func (errExecutor) Reset(context.Context)                  {}
func (errExecutor) Close() error                           { return nil }

// newTestV1WithExecutor returns a v1 server whose core uses the given executor.
func newTestV1WithExecutor(e lambdaexec.LambdaExecutor) *Service {
	return NewService(core.NewService(
		functionsstore.NewMemoryStore(),
		store.NewMemoryResourceStore(),
		core.WithExecutor(e),
	), "proj")
}

// TestCallFunctionV1 covers runtime invocation over the shared Lambda executor:
// the mock-echo success path, an in-band executor error (returned on the
// response, not as a gRPC status), NotFound for a missing function, and
// InvalidArgument for a malformed resource name.
func TestCallFunctionV1(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	resp, err := s.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/f1",
		Data: `{"hello":"world"}`,
	})
	if err != nil {
		t.Fatalf("CallFunction: %v", err)
	}
	if resp.GetExecutionId() == "" {
		t.Fatal("empty executionId")
	}
	if resp.GetError() != "" {
		t.Fatalf("unexpected in-band error: %q", resp.GetError())
	}
	if resp.GetResult() != `{"hello":"world"}` {
		t.Fatalf("result = %q, want echoed payload", resp.GetResult())
	}

	failing := newTestV1WithExecutor(errExecutor{})
	if _, err := failing.CreateFunction(ctx, createV1Req("f2")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	resp, err = failing.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/f2",
		Data: "ignored",
	})
	if err != nil {
		t.Fatalf("CallFunction (failing executor): %v", err)
	}
	if resp.GetExecutionId() == "" {
		t.Fatal("empty executionId on error path")
	}
	if resp.GetError() != "boom" || resp.GetResult() != "" {
		t.Fatalf("in-band error = %q, result = %q; want boom/empty", resp.GetError(), resp.GetResult())
	}

	if _, err := s.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/missing",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("CallFunction missing = %v, want NotFound", err)
	}
	if _, err := s.CallFunction(ctx, &functionspb.CallFunctionRequest{Name: "not-a-function"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CallFunction malformed name = %v, want InvalidArgument", err)
	}
}

// TestListRuntimesUnimplemented pins the remaining control-plane-only decision:
// v2 ListRuntimes has no emulator implementation and fails loud.
func TestListRuntimesUnimplemented(t *testing.T) {
	v2 := newTestV2()
	if _, err := v2.ListRuntimes(context.Background(), &apiv2functionspb.ListRuntimesRequest{Parent: "projects/proj/locations/us-central1"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("ListRuntimes = %v, want Unimplemented", err)
	}
}
