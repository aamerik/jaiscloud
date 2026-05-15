// Package lambda provides pluggable Lambda executors.
// The LambdaExecutor interface decouples the FunctionProvider from the
// underlying invocation mechanism (mock echo, Docker warm container, K8s Job).
package lambda

import "context"

// InvokeRequest carries everything needed to invoke a Lambda function.
type InvokeRequest struct {
	FunctionName string
	Runtime      string
	Handler      string
	Image        string            // override image; empty = derive from Runtime
	MemoryMB     int
	TimeoutSecs  int
	EnvVars      map[string]string
	Payload      []byte
	AccountID    string
}

// LambdaExecutor is the interface satisfied by MockExecutor, DockerExecutor,
// and K8sExecutor.
type LambdaExecutor interface {
	// Invoke executes a Lambda function synchronously and returns the response payload.
	Invoke(ctx context.Context, req InvokeRequest) ([]byte, error)
	// DeleteFunction tears down any warm container or pod for the named function.
	DeleteFunction(ctx context.Context, functionName string)
	// Reset tears down all warm containers or pods (called on /_jaiscloud/reset).
	Reset()
	// Close releases all resources held by the executor (containers, goroutines).
	Close() error
}
