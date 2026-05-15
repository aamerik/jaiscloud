package ecs

import "context"

// MockExecutor immediately reports tasks as STOPPED.
// Used in unit tests and when no executor mode is configured.
type MockExecutor struct{}

func (e *MockExecutor) Run(_ context.Context, _ TaskSpec) (TaskHandle, error) {
	return TaskHandle{Mode: ModeMock}, nil
}

func (e *MockExecutor) Wait(_ context.Context, _ TaskHandle) error { return nil }

func (e *MockExecutor) Stop(_ context.Context, _ TaskHandle) error { return nil }

func (e *MockExecutor) StatusOf(_ context.Context, _ TaskHandle) (Status, error) {
	return Status{LastStatus: "STOPPED"}, nil
}
