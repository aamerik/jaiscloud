// Package ecs provides pluggable ECS task executors.
package ecs

import (
	"context"

	"jaiscloud/internal/logstream"
)

// LogsIngestor is satisfied by the CloudWatch Logs provider via logstream.Ingestor.
type LogsIngestor = logstream.Ingestor

// Executor is the interface implemented by MockExecutor, dockerExecutor, and k8sExecutor.
type Executor interface {
	Run(ctx context.Context, spec TaskSpec) (TaskHandle, error)
	Wait(ctx context.Context, handle TaskHandle) error
	Stop(ctx context.Context, handle TaskHandle) error
	StatusOf(ctx context.Context, handle TaskHandle) (Status, error)
}

// New returns the appropriate executor for the given mode.
func New(mode Mode, logsAPI LogsIngestor) Executor {
	switch mode {
	case ModeDocker:
		return newDockerExecutor(logsAPI)
	case ModeK8s:
		return newK8sExecutor(logsAPI)
	default:
		return &MockExecutor{}
	}
}
