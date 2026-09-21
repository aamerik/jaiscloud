package operations

import (
	"context"
	"net"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// operationsTestService dials a real in-process gRPC server backed by the
// stub Operations service and returns the generated client.
func operationsTestService(t *testing.T) (longrunningpb.OperationsClient, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	longrunningpb.RegisterOperationsServer(srv, New())
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return longrunningpb.NewOperationsClient(conn), cleanup
}

const operationName = "projects/test/locations/global/operations/op-1"

// TestGetOperation verifies the stub reports any operation name as done.
func TestGetOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	op, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: operationName})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if op.GetName() != operationName {
		t.Errorf("GetOperation name = %q, want %q", op.GetName(), operationName)
	}
	if !op.GetDone() {
		t.Errorf("GetOperation done = false, want true")
	}
}

// TestListOperations verifies the stub returns a well-formed empty list for any
// parent rather than erroring.
func TestListOperations(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{
		Name: "projects/test/locations/global",
	})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if n := len(resp.GetOperations()); n != 0 {
		t.Errorf("ListOperations returned %d operations, want 0", n)
	}
	if resp.GetNextPageToken() != "" {
		t.Errorf("ListOperations next_page_token = %q, want empty", resp.GetNextPageToken())
	}
}

// TestDeleteOperation verifies the no-op delete succeeds.
func TestDeleteOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: operationName}); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
}

// TestCancelOperation verifies the no-op cancel succeeds.
func TestCancelOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: operationName}); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}
}

// TestWaitOperation verifies the synchronous stub returns the requested name
// already done (there is never an in-flight operation to block on).
func TestWaitOperation(t *testing.T) {
	client, cleanup := operationsTestService(t)
	defer cleanup()
	ctx := context.Background()

	op, err := client.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: operationName})
	if err != nil {
		t.Fatalf("WaitOperation: %v", err)
	}
	if op.GetName() != operationName {
		t.Errorf("WaitOperation name = %q, want %q", op.GetName(), operationName)
	}
	if !op.GetDone() {
		t.Errorf("WaitOperation done = false, want true")
	}
}
