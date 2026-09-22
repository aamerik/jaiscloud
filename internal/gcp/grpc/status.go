package grpc

import (
	"errors"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCStatus converts a provider/service error into a gRPC status error.
// Provider errors are resolved through gcperr.Resolve, so the gRPC mapping uses
// the same precedence as the REST codecs: google.rpc Status name, then the
// provider Code alias, then the HTTP status; anything else is INTERNAL. This is
// the shared error-mapping used by every GCP gRPC service (Firestore, Pub/Sub,
// Secret Manager, KMS).
func GRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return status.Error(codes.Internal, err.Error())
	}
	name, _ := gcperr.Resolve(perr)
	if c, ok := statusToCode(name); ok {
		return status.Error(c, perr.Message)
	}
	return status.Error(codes.Internal, perr.Message)
}

// statusToCode maps a canonical google.rpc status name to its gRPC code. The
// bool is false for unrecognized names so the caller can fall back to INTERNAL.
func statusToCode(s string) (codes.Code, bool) {
	switch s {
	case gcperr.FailedPrecondition:
		return codes.FailedPrecondition, true
	case gcperr.OutOfRange:
		return codes.OutOfRange, true
	case gcperr.Aborted:
		return codes.Aborted, true
	case gcperr.NotFound:
		return codes.NotFound, true
	case gcperr.InvalidArgument:
		return codes.InvalidArgument, true
	case gcperr.AlreadyExists:
		return codes.AlreadyExists, true
	case gcperr.PermissionDenied:
		return codes.PermissionDenied, true
	case gcperr.Unauthenticated:
		return codes.Unauthenticated, true
	case gcperr.ResourceExhausted:
		return codes.ResourceExhausted, true
	case gcperr.Unimplemented:
		return codes.Unimplemented, true
	case gcperr.Internal:
		return codes.Internal, true
	case gcperr.Unknown:
		return codes.Unknown, true
	case gcperr.Unavailable:
		return codes.Unavailable, true
	case gcperr.Cancelled:
		return codes.Canceled, true
	case gcperr.DataLoss:
		return codes.DataLoss, true
	case gcperr.DeadlineExceeded:
		return codes.DeadlineExceeded, true
	}
	return codes.Unknown, false
}
