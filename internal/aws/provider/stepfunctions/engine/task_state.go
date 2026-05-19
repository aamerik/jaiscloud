package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
	sfnstore "jaiscloud/internal/aws/store/stepfunctions"
)

// evalTaskWithRetry evaluates a Task state, applying Retry rules on failure.
func (e *ExecutionEngine) evalTaskWithRetry(ctx context.Context, execARN, stateName string, state *asl.TaskState, input any) (any, error) {
	return e.executeWithRetry(ctx, state.Retry, func() (any, error) {
		return e.evalTask(ctx, execARN, stateName, state, input)
	})
}

func (e *ExecutionEngine) evalTask(ctx context.Context, execARN, stateName string, state *asl.TaskState, input any) (any, error) {
	effective := applyInputPath2(input, state.InputPath)

	// Apply Parameters
	var taskInput any = effective
	if len(state.Parameters) > 0 {
		var params any
		if err := json.Unmarshal(state.Parameters, &params); err != nil {
			return nil, err
		}
		v, err := asl.EvalParameters(params, effective, nil)
		if err != nil {
			return nil, err
		}
		taskInput = v
	}

	// Build timeout context
	taskCtx := ctx
	var taskCancel context.CancelFunc
	if state.TimeoutSeconds > 0 {
		taskCtx, taskCancel = context.WithTimeout(ctx, time.Duration(state.TimeoutSeconds)*time.Second)
		defer taskCancel()
	}

	// Record TaskScheduled
	if execARN != "" {
		_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
			Type: "TaskScheduled",
			TaskScheduledEventDetails: &sfnstore.TaskScheduledEventDetails{
				ResourceType: "task",
				Resource:     state.Resource,
				Parameters:   mustJSON(taskInput),
			},
		})
	}

	// Dispatch the task
	result, err := e.dispatchTask(taskCtx, execARN, state.Resource, taskInput, state.TimeoutSeconds)
	if err != nil {
		if execARN != "" {
			errCode := errorName(err)
			sfnErr := toSFNError(err)
			_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
				Type: "TaskFailed",
				TaskFailedEventDetails: &sfnstore.TaskFailedEventDetails{
					ResourceType: "task",
					Resource:     state.Resource,
					Error:        errCode,
					Cause:        sfnErr.cause,
				},
			})
		}
		// Wrap timeout as States.Timeout
		if taskCtx.Err() != nil {
			return nil, &sfnError{code: "States.Timeout", cause: fmt.Sprintf("task %s timed out", state.Resource)}
		}
		return nil, err
	}

	if execARN != "" {
		_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
			Type: "TaskSucceeded",
			TaskSucceededEventDetails: &sfnstore.TaskSucceededEventDetails{
				ResourceType: "task",
				Resource:     state.Resource,
				Output:       mustJSON(result),
			},
		})
	}

	// ResultSelector
	var finalResult any = result
	if len(state.ResultSelector) > 0 {
		var rs any
		if err := json.Unmarshal(state.ResultSelector, &rs); err != nil {
			return nil, err
		}
		v, err := asl.EvalParameters(rs, result, nil)
		if err != nil {
			return nil, err
		}
		finalResult = v
	}

	// ResultPath
	output, err := applyResultPath(effective, state.ResultPath, finalResult)
	if err != nil {
		return nil, err
	}
	return applyOutputPath2(output, state.OutputPath), nil
}

// dispatchTask routes a Task Resource ARN to the correct service.
func (e *ExecutionEngine) dispatchTask(ctx context.Context, execARN, resource string, input any, timeoutSec int) (any, error) {
	if e.dispatcher == nil {
		// Lite mode: return input as output (passthrough)
		return input, nil
	}

	svcKey, modifier, err := parseTaskResource(resource)
	if err != nil {
		return nil, &sfnError{code: "States.TaskFailed", cause: fmt.Sprintf("unsupported resource ARN: %s", resource)}
	}

	entry, ok := taskResourceMap[svcKey]
	if !ok {
		return nil, &sfnError{code: "States.TaskFailed", cause: fmt.Sprintf("no dispatcher mapping for resource key: %s", svcKey)}
	}

	paramsMap, _ := input.(map[string]any)
	if paramsMap == nil {
		paramsMap = make(map[string]any)
	}

	// For Lambda invocations, inject _function_name and _payload so that
	// FunctionProvider.InvokeFunction can locate and call the right function.
	if svcKey == "lambda:invoke" {
		if fnName := extractLambdaFunctionName(resource, paramsMap); fnName != "" {
			paramsMap["_function_name"] = fnName
		}
		payload, _ := json.Marshal(input)
		paramsMap["_payload"] = payload
	}

	if modifier == "waitForTaskToken" {
		token := generateToken()
		// Inject token — the TaskToken field path is determined by the FunctionName/etc.
		paramsMap["TaskToken"] = token

		// Dispatch (fire-and-forget the actual call)
		go func() {
			_, _ = e.dispatcher.Dispatch(ctx, entry.Service, entry.Action, paramsMap)
		}()

		// Block until SendTaskSuccess/Failure
		return e.waitForTaskCallback(ctx, token, timeoutSec)
	}

	return e.dispatcher.Dispatch(ctx, entry.Service, entry.Action, paramsMap)
}

// extractLambdaFunctionName extracts the function name from a Lambda ARN or
// from a FunctionName field in the params (set via Parameters in the state def).
func extractLambdaFunctionName(arn string, params map[string]any) string {
	// Check params first (set via Parameters field in the state definition).
	if fn, ok := params["FunctionName"].(string); ok && fn != "" {
		return fn
	}
	// Legacy Lambda ARN: arn:aws:lambda:region:account:function:FunctionName
	const fnMarker = ":function:"
	if idx := strings.LastIndex(arn, fnMarker); idx >= 0 {
		name := arn[idx+len(fnMarker):]
		// Strip any qualifier suffix (e.g. :QUALIFIER or :version-number)
		if col := strings.IndexByte(name, ':'); col >= 0 {
			name = name[:col]
		}
		return name
	}
	return ""
}

// taskResourceEntry maps a parsed resource key to a provider service+action.
type taskResourceEntry struct {
	Service string
	Action  string
}

var taskResourceMap = map[string]taskResourceEntry{
	"lambda:invoke":          {"Function", "InvokeFunction"},
	"dynamodb:putItem":       {"Table", "PutItem"},
	"dynamodb:getItem":       {"Table", "GetItem"},
	"dynamodb:updateItem":    {"Table", "UpdateItem"},
	"dynamodb:deleteItem":    {"Table", "DeleteItem"},
	"sqs:sendMessage":        {"Queue", "SendMessage"},
	"sns:publish":            {"Notification", "Publish"},
	"states:startExecution":  {"StepFunctions", "StartExecution"},
	"events:putEvents":       {"EventBridge", "PutEvents"},
	"glue:startJobRun":       {"Glue", "StartJobRun"},
	"ecs:runTask":            {"ECS", "RunTask"},
}

// parseTaskResource parses a Task Resource ARN into (key, modifier, error).
// Examples:
//   "arn:aws:states:::lambda:invoke"          → ("lambda:invoke", "", nil)
//   "arn:aws:states:::lambda:invoke.sync"     → ("lambda:invoke", "sync", nil)
//   "arn:aws:states:::lambda:invoke.waitForTaskToken" → ("lambda:invoke", "waitForTaskToken", nil)
//   "arn:aws:lambda:us-east-1:000:function:Fn" → ("lambda:invoke", "", nil)  [legacy]
func parseTaskResource(arn string) (key, modifier string, err error) {
	// Legacy Lambda ARN: arn:aws:lambda:...:function:Name
	if strings.Contains(arn, ":function:") && !strings.Contains(arn, ":::") {
		return "lambda:invoke", "", nil
	}

	// States SDK integration ARN prefix for pattern matching (not construction) — lint-ignore: arn-prefix-match
	const prefix = "arn:aws:states:::" //nolint:hardcoded-arn
	if !strings.HasPrefix(arn, prefix) {
		// Activity ARN: arn:aws:states:region:account:activity:name — not dispatched here
		if strings.Contains(arn, ":activity:") {
			return "", "", fmt.Errorf("activity resources not supported in direct dispatch")
		}
		return "", "", fmt.Errorf("unrecognised resource ARN pattern: %s", arn)
	}

	rest := arn[len(prefix):]
	// rest = "lambda:invoke" or "lambda:invoke.waitForTaskToken"
	parts := strings.SplitN(rest, ".", 2)
	rawKey := parts[0]
	if len(parts) == 2 {
		modifier = parts[1]
	}
	return rawKey, modifier, nil
}

// waitForTaskCallback blocks until SendTaskSuccess/Failure is called for token.
func (e *ExecutionEngine) waitForTaskCallback(ctx context.Context, token string, timeoutSec int) (any, error) {
	ch := make(chan taskCallback, 1)
	e.tokenMu.Lock()
	e.tokens[token] = ch
	e.tokenMu.Unlock()
	defer func() {
		e.tokenMu.Lock()
		delete(e.tokens, token)
		e.tokenMu.Unlock()
	}()

	var timeoutCh <-chan time.Time
	if timeoutSec > 0 {
		timeoutCh = time.After(time.Duration(timeoutSec) * time.Second)
	}

	select {
	case cb := <-ch:
		if cb.err != nil {
			return nil, cb.err
		}
		var out any
		_ = json.Unmarshal([]byte(cb.output), &out)
		return out, nil
	case <-timeoutCh:
		return nil, &sfnError{code: "States.Timeout", cause: "waitForTaskToken timed out"}
	case <-ctx.Done():
		return nil, &sfnError{code: "States.Timeout", cause: "execution cancelled while waiting for task token"}
	}
}

func generateToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
