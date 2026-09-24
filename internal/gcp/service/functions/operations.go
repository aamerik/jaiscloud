package functions

// GetOperationJSON renders a synthesized done operation for the requested
// operation name. Function mutations are synchronous and never persisted, so a
// polling client that somehow reaches this sees a terminal operation.
func (s *Service) GetOperationJSON(project, name string, v Version) (map[string]any, error) {
	location, id, err := ParseOperationName(name)
	if err != nil {
		return nil, err
	}
	return SynthesizedOperationJSON(v, project, location, id), nil
}

// ListOperations returns the long-running operations for a location. Function
// mutations are synchronous and not persisted, so the set is always empty.
func (s *Service) ListOperations() []any { return []any{} }

// CancelOperation validates an operation name and no-ops (mutations are already
// done).
func (s *Service) CancelOperation(name string) error {
	_, _, err := ParseOperationName(name)
	return err
}

// DeleteOperation validates an operation name and no-ops (nothing is persisted).
func (s *Service) DeleteOperation(name string) error {
	_, _, err := ParseOperationName(name)
	return err
}
