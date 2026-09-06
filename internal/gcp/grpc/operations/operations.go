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
	return &longrunningpb.Operation{
		Name: req.GetName(),
		Done: true,
	}, nil
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
