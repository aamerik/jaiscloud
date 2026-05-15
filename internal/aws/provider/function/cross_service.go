package function

import (
	"context"
	"fmt"
)

// InvokeResult holds the result of an internal Lambda invocation.
type InvokeResult struct {
	StatusCode    int
	FunctionError string
	Payload       []byte
}

// InternalInvoker is the interface used by other providers to invoke Lambda functions.
type InternalInvoker interface {
	InternalInvoke(ctx context.Context, funcARNorName string, payload []byte, invocationType string) (*InvokeResult, error)
}

// RawInvoker is the interface used by providers that need fire-and-forget Lambda invocation.
type RawInvoker interface {
	InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error
}

// InternalInvoke invokes a Lambda function synchronously or asynchronously.
// invocationType is "RequestResponse" or "Event".
func (p *FunctionProvider) InternalInvoke(ctx context.Context, funcARNorName string, payload []byte, invocationType string) (*InvokeResult, error) {
	name := extractFunctionName(funcARNorName)
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("lambda: function %q not found: %w", name, err)
	}
	if invocationType == "Event" {
		return &InvokeResult{StatusCode: 202}, nil
	}
	result, err := p.invokeConfig(ctx, cfg, payload)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// InternalGetFunction retrieves function configuration by name or ARN.
func (p *FunctionProvider) InternalGetFunction(ctx context.Context, nameOrARN string) (*functionConfig, error) {
	name := extractFunctionName(nameOrARN)
	cfg, err := p.loadConfig(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("lambda: function %q not found: %w", name, err)
	}
	return &cfg, nil
}

// InternalInvokeRaw invokes a Lambda function asynchronously (fire-and-forget).
// Returns immediately after submitting the invocation.
func (p *FunctionProvider) InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error {
	_, err := p.InternalInvoke(ctx, funcARNorName, payload, "Event")
	return err
}

// invokeConfig executes the given functionConfig with the provided payload and returns an InvokeResult.
func (p *FunctionProvider) invokeConfig(ctx context.Context, cfg functionConfig, payload []byte) (*InvokeResult, error) {
	raw, err := p.InvokeInternal(ctx, cfg.FunctionName, payload)
	if err != nil {
		return &InvokeResult{StatusCode: 200, FunctionError: err.Error()}, nil
	}
	return &InvokeResult{StatusCode: 200, Payload: raw}, nil
}
