package functions

import (
	"context"

	"jaiscloud/internal/gcp/paging"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// persistOperation records a completed mutation operation so operations.get /
// list and REST :wait can read it back after the mutation returns (and across a
// restart under --dsn). The version-independent Function response snapshot is
// stored; rendering happens on read.
func (s *Service) persistOperation(ctx context.Context, project string, op Operation) error {
	return s.functions.CreateOperation(ctx, project, op.Location, functionsstore.Operation{
		ID:         op.ID,
		Location:   op.Location,
		Done:       true,
		Verb:       op.Verb,
		Target:     op.Target,
		Function:   op.Function,
		CreateTime: op.CreateTime,
		EndTime:    op.EndTime,
	})
}

// operationFromStore converts a persisted operation into the render-time
// Operation the shared OperationJSON expects.
func operationFromStore(stored functionsstore.Operation) Operation {
	return Operation{
		ID:         stored.ID,
		Location:   stored.Location,
		Verb:       stored.Verb,
		Target:     stored.Target,
		Function:   stored.Function,
		CreateTime: stored.CreateTime,
		EndTime:    stored.EndTime,
	}
}

// GetOperationJSON renders the persisted operation for the requested name. An
// unknown operation is NotFound (real GCP never synthesizes a terminal op for an
// id it did not hand out).
func (s *Service) GetOperationJSON(ctx context.Context, project, name string, v Version) (map[string]any, error) {
	op, err := s.loadOperation(ctx, project, name)
	if err != nil {
		return nil, err
	}
	return OperationJSON(v, project, op), nil
}

// WaitOperation is the read side of the REST :wait custom method. Every function
// operation completes synchronously, so it returns the same persisted, done
// operation GetOperation would.
func (s *Service) WaitOperation(ctx context.Context, project, name string, v Version) (map[string]any, error) {
	return s.GetOperationJSON(ctx, project, name, v)
}

// loadOperation parses an operation name and reads the persisted record. A
// top-level v1 name (operations/{id}) carries no location, so the location is
// recovered from the stored record.
func (s *Service) loadOperation(ctx context.Context, project, name string) (Operation, error) {
	location, id, err := ParseOperationName(name)
	if err != nil {
		return Operation{}, err
	}
	var stored functionsstore.Operation
	if location == "" {
		stored, err = s.functions.GetOperationByID(ctx, project, id)
	} else {
		stored, err = s.functions.GetOperation(ctx, project, location, id)
	}
	if err != nil {
		return Operation{}, mapErr(err)
	}
	return operationFromStore(stored), nil
}

// LoadOperation parses and loads a persisted operation for the gRPC
// google.longrunning.Operations resolver (which needs the typed core Operation
// to build the Any metadata/response).
func (s *Service) LoadOperation(ctx context.Context, project, name string) (Operation, error) {
	return s.loadOperation(ctx, project, name)
}

// ListOperations returns a cursor page of the persisted operations for a
// location.
func (s *Service) ListOperations(ctx context.Context, project, location string, pageSize int, pageToken string) ([]Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	ops, err := s.functions.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(ops, func(op functionsstore.Operation) string { return op.ID }, pageParams(pageSize, pageToken))
	out := make([]Operation, 0, len(page))
	for _, op := range page {
		out = append(out, operationFromStore(op))
	}
	return out, next, nil
}

// CancelOperation validates that the operation exists. Every emulated operation
// completes synchronously, so there is nothing to cancel: a known operation is a
// no-op success and an unknown one is NotFound.
func (s *Service) CancelOperation(ctx context.Context, project, name string) error {
	_, err := s.loadOperation(ctx, project, name)
	return err
}

// DeleteOperation removes a persisted operation. An unknown operation is
// NotFound (matching real google.longrunning.Operations.DeleteOperation).
func (s *Service) DeleteOperation(ctx context.Context, project, name string) error {
	location, id, err := ParseOperationName(name)
	if err != nil {
		return err
	}
	if location == "" {
		// v1 top-level name: recover the location from the stored record.
		op, err := s.functions.GetOperationByID(ctx, project, id)
		if err != nil {
			return mapErr(err)
		}
		location = op.Location
	}
	if err := s.functions.DeleteOperation(ctx, project, location, id); err != nil {
		return mapErr(err)
	}
	return nil
}
