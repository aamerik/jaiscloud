// Package operations provides a stub google.longrunning.Operations service. The
// emulator completes every operation synchronously (e.g. index creation returns
// a done=true operation directly), so there are never in-flight operations to
// track. The stub reports every operation as done with an empty response so
// SDK init paths that poll Operations for a terminal state terminate cleanly
// instead of erroring.
package operations

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements longrunningpb.OperationsServer.
type Service struct {
	longrunningpb.UnimplementedOperationsServer
}

// New returns an Operations stub service.
func New() *Service { return &Service{} }

// GetOperation reports any operation as done with no result body, so SDK init
// paths that poll Operations for a terminal state terminate cleanly.
func (s *Service) GetOperation(_ context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	return terminalOperation(req.GetName()), nil
}

// WaitOperation returns the same terminal operation GetOperation would, without
// blocking: every emulator operation completes synchronously, so there is never
// an in-flight operation to wait on and the requested name is already done. The
// request's timeout is ignored because the wait returns immediately.
func (s *Service) WaitOperation(_ context.Context, req *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	return terminalOperation(req.GetName()), nil
}

// terminalOperation is the shared shape every Operations RPC reports: the
// requested name marked done, with no result or error body.
func terminalOperation(name string) *longrunningpb.Operation {
	return &longrunningpb.Operation{
		Name: name,
		Done: true,
	}
}

// ListOperations reports no operations (the emulator persists none).
func (s *Service) ListOperations(_ context.Context, _ *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	return &longrunningpb.ListOperationsResponse{}, nil
}

// DeleteOperation is a no-op (there is no operation registry to delete from).
func (s *Service) DeleteOperation(_ context.Context, _ *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// CancelOperation is a no-op (operations are never left cancellable).
func (s *Service) CancelOperation(_ context.Context, _ *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
