// Package operations provides the google.longrunning.Operations service. Most
// emulator operations complete synchronously (e.g. index creation returns a
// done=true operation directly), so by default an operation is reported as done
// with an empty response, letting SDK init paths that poll Operations terminate
// cleanly instead of erroring.
//
// Services that model genuinely asynchronous operations register a Resolver,
// which is consulted first: Dataproc cluster create/update/start/stop/delete are
// returned done=false and are completed lazily when the SDK polls
// Operations.GetOperation.
package operations

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	"google.golang.org/protobuf/types/known/emptypb"
)

// Resolver resolves a fully-qualified operation name to its
// google.longrunning.Operation. handled reports whether the name belongs to the
// resolver's service; when false the caller falls through to the terminal stub.
// An error (e.g. NotFound) is returned to the client for names the resolver
// owns.
type Resolver interface {
	ResolveOperation(ctx context.Context, name string) (op *longrunningpb.Operation, handled bool, err error)
}

// Service implements longrunningpb.OperationsServer.
type Service struct {
	longrunningpb.UnimplementedOperationsServer
	resolvers []Resolver
}

// New returns an Operations service. Resolvers are consulted in order; pass
// none for the terminal stub used by synchronous services.
func New(resolvers ...Resolver) *Service { return &Service{resolvers: resolvers} }

// GetOperation first asks each registered resolver; if none owns the name it
// reports the operation as done with no result body, so SDK init paths that poll
// Operations for a terminal state terminate cleanly.
func (s *Service) GetOperation(ctx context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	for _, r := range s.resolvers {
		op, handled, err := r.ResolveOperation(ctx, req.GetName())
		if !handled {
			continue
		}
		if err != nil {
			return nil, err
		}
		return op, nil
	}
	return terminalOperation(req.GetName()), nil
}

// WaitOperation resolves the operation the same way GetOperation does, without
// blocking: the emulator's async transitions advance on read, so a single
// resolution already reflects the latest state. The request's timeout is
// ignored.
func (s *Service) WaitOperation(ctx context.Context, req *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	return s.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: req.GetName()})
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
