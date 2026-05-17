package lambda

import "context"

// MockExecutor echoes the request payload as the response.
// Used in lite mode and as the default when no executor mode is configured.
type MockExecutor struct{}

func (e *MockExecutor) Invoke(_ context.Context, req InvokeRequest) (InvokeResult, error) {
	return InvokeResult{Payload: req.Payload}, nil
}

func (e *MockExecutor) DeleteFunction(_ context.Context, _ string) {}
func (e *MockExecutor) Reset()                                     {}
func (e *MockExecutor) Close() error                               { return nil }
