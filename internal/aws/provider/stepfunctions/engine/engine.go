// Package engine implements the AWS States Language execution engine for
// JaisCloud's Step Functions emulation. Each execution runs in its own
// goroutine and progresses through states synchronously within that goroutine.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
	sfnstore "jaiscloud/internal/aws/store/stepfunctions"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/logstream"
	"jaiscloud/internal/provider"
)

// LogIngestor is the minimal interface the engine needs to forward logs to CloudWatch Logs.
type LogIngestor interface {
	InternalPutEvents(ctx context.Context, logGroupName, logStreamName string, events []logstream.Event) error
	InternalCreateLogGroup(ctx context.Context, logGroupName string) error
}

// ExecutionEngine runs ASL state machine executions.
type ExecutionEngine struct {
	store       *sfnstore.MemoryStepFunctionsStore
	dispatcher  provider.ServiceDispatcher
	clock       clock.Clock
	logIngestor LogIngestor

	mu      sync.Mutex
	running map[string]context.CancelFunc // execARN → cancel

	// task tokens: token → channel that unblocks when SendTaskSuccess/Failure called
	tokenMu sync.Mutex
	tokens  map[string]chan taskCallback

	wg sync.WaitGroup
}

// SetLogsIngestor wires a CloudWatch Logs ingestor for task log forwarding.
func (e *ExecutionEngine) SetLogsIngestor(ingestor LogIngestor) {
	e.logIngestor = ingestor
}

type taskCallback struct {
	output string
	err    error
}

// New creates an ExecutionEngine.
func New(store *sfnstore.MemoryStepFunctionsStore, dispatcher provider.ServiceDispatcher, clk clock.Clock) *ExecutionEngine {
	return &ExecutionEngine{
		store:      store,
		dispatcher: dispatcher,
		clock:      clk,
		running:    make(map[string]context.CancelFunc),
		tokens:     make(map[string]chan taskCallback),
	}
}

// Start launches an execution in a background goroutine.
func (e *ExecutionEngine) Start(execARN string, def *asl.StateMachineDefinition, input string) {
	var ctx context.Context
	var cancel context.CancelFunc
	if def.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(def.TimeoutSeconds)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

	e.mu.Lock()
	e.running[execARN] = cancel
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer cancel()
		defer func() {
			e.mu.Lock()
			delete(e.running, execARN)
			e.mu.Unlock()
		}()
		e.runExecution(ctx, execARN, def, input)
	}()
}

// Stop aborts a running execution.
func (e *ExecutionEngine) Stop(execARN, errCode, cause string) {
	e.mu.Lock()
	cancel, ok := e.running[execARN]
	e.mu.Unlock()
	if !ok {
		return
	}
	_ = e.store.FinalizeExecution(execARN, sfnstore.ExecutionStatusAborted, "", errCode, cause)
	_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
		Type: "ExecutionAborted",
		ExecutionAbortedEventDetails: &sfnstore.ExecutionAbortedEventDetails{
			Error: errCode,
			Cause: cause,
		},
	})
	cancel()
}

// SendTaskSuccess unblocks a .waitForTaskToken task with output.
func (e *ExecutionEngine) SendTaskSuccess(token, output string) error {
	e.tokenMu.Lock()
	ch, ok := e.tokens[token]
	e.tokenMu.Unlock()
	if !ok {
		return fmt.Errorf("TaskDoesNotExist: token not found")
	}
	ch <- taskCallback{output: output}
	return nil
}

// SendTaskFailure unblocks a .waitForTaskToken task with an error.
func (e *ExecutionEngine) SendTaskFailure(token, errCode, cause string) error {
	e.tokenMu.Lock()
	ch, ok := e.tokens[token]
	e.tokenMu.Unlock()
	if !ok {
		return fmt.Errorf("TaskDoesNotExist: token not found")
	}
	ch <- taskCallback{err: &sfnError{code: errCode, cause: cause}}
	return nil
}

// Shutdown cancels all running executions and waits for them to finish.
func (e *ExecutionEngine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	for _, cancel := range e.running {
		cancel()
	}
	e.mu.Unlock()

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reset cancels all running executions (used by admin reset).
func (e *ExecutionEngine) Reset(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.Shutdown(ctx)
}

// ─── Core execution loop ──────────────────────────────────────────────────────

func (e *ExecutionEngine) runExecution(ctx context.Context, execARN string, def *asl.StateMachineDefinition, input string) {
	_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
		Type: "ExecutionStarted",
		ExecutionStartedEventDetails: &sfnstore.ExecutionStartedEventDetails{
			Input: input,
		},
	})

	var currentDoc any
	if err := json.Unmarshal([]byte(input), &currentDoc); err != nil {
		currentDoc = map[string]any{}
	}

	currentStateName := def.StartAt
	for currentStateName != "" {
		select {
		case <-ctx.Done():
			// Cancelled externally — check if already finalized by Stop()
			_ = e.store.FinalizeExecution(execARN, sfnstore.ExecutionStatusAborted, "", "States.Timeout", ctx.Err().Error())
			return
		default:
		}

		state, ok := def.States[currentStateName]
		if !ok {
			e.failExecution(execARN, "States.Runtime", fmt.Sprintf("state %q not found", currentStateName))
			return
		}

		// Record StateEntered
		inputJSON := mustJSON(currentDoc)
		_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
			Type: stateEnteredType(state),
			StateEnteredEventDetails: &sfnstore.StateEnteredEventDetails{
				Name:  currentStateName,
				Input: inputJSON,
			},
		})

		nextStateName, output, execErr := e.evaluateState(ctx, execARN, currentStateName, state, currentDoc, def)

		if execErr != nil {
			sfnErr := toSFNError(execErr)
			e.failExecution(execARN, sfnErr.code, sfnErr.cause)
			return
		}

		// Record StateExited
		outputJSON := mustJSON(output)
		_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
			Type: stateExitedType(state),
			StateExitedEventDetails: &sfnstore.StateExitedEventDetails{
				Name:   currentStateName,
				Output: outputJSON,
			},
		})

		currentDoc = output
		currentStateName = nextStateName
	}

	// Execution succeeded
	outputJSON := mustJSON(currentDoc)
	_ = e.store.FinalizeExecution(execARN, sfnstore.ExecutionStatusSucceeded, outputJSON, "", "")
	_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
		Type: "ExecutionSucceeded",
		ExecutionSucceededEventDetails: &sfnstore.ExecutionSucceededEventDetails{
			Output: outputJSON,
		},
	})
	e.emitExecutionLogs(execARN, input, outputJSON, "Succeeded")
}

// evaluateState dispatches to the appropriate state evaluator.
// Returns (nextStateName, outputDoc, error).
// Empty nextStateName means execution is terminal (Succeed/Fail/End=true).
func (e *ExecutionEngine) evaluateState(
	ctx context.Context,
	execARN, stateName string,
	state asl.StateDef,
	input any,
	def *asl.StateMachineDefinition,
) (string, any, error) {
	switch s := state.(type) {
	case *asl.PassState:
		output, err := e.evalPass(s, input)
		if err != nil {
			return "", nil, err
		}
		if s.End || s.Next == "" {
			return "", output, nil
		}
		return s.Next, output, nil

	case *asl.SucceedState:
		output := applyIOPath(input, s.InputPath, s.OutputPath)
		return "", output, nil

	case *asl.FailState:
		errCode := s.Error
		cause := s.Cause
		if s.ErrorPath != "" {
			if v, err := asl.EvalPath(input, s.ErrorPath); err == nil {
				errCode, _ = v.(string)
			}
		}
		if s.CausePath != "" {
			if v, err := asl.EvalPath(input, s.CausePath); err == nil {
				cause, _ = v.(string)
			}
		}
		return "", nil, &sfnError{code: errCode, cause: cause}

	case *asl.WaitState:
		output, err := e.evalWait(ctx, s, input)
		if err != nil {
			return "", nil, err
		}
		if s.End || s.Next == "" {
			return "", output, nil
		}
		return s.Next, output, nil

	case *asl.ChoiceState:
		next, output, err := e.evalChoice(s, input)
		if err != nil {
			return "", nil, err
		}
		return next, output, nil

	case *asl.TaskState:
		output, err := e.evalTaskWithRetry(ctx, execARN, stateName, s, input)
		if err != nil {
			// Try Catch rules
			next, catchOutput, caught := e.handleCatch(s.Catch, err, input)
			if caught {
				return next, catchOutput, nil
			}
			return "", nil, err
		}
		if s.End || s.Next == "" {
			return "", output, nil
		}
		return s.Next, output, nil

	case *asl.ParallelState:
		output, err := e.evalParallelWithRetry(ctx, s, input)
		if err != nil {
			next, catchOutput, caught := e.handleCatch(s.Catch, err, input)
			if caught {
				return next, catchOutput, nil
			}
			return "", nil, err
		}
		if s.End || s.Next == "" {
			return "", output, nil
		}
		return s.Next, output, nil

	case *asl.MapState:
		output, err := e.evalMapWithRetry(ctx, s, input)
		if err != nil {
			next, catchOutput, caught := e.handleCatch(s.Catch, err, input)
			if caught {
				return next, catchOutput, nil
			}
			return "", nil, err
		}
		if s.End || s.Next == "" {
			return "", output, nil
		}
		return s.Next, output, nil
	}

	return "", nil, fmt.Errorf("States.Runtime: unknown state type %T", state)
}

func (e *ExecutionEngine) failExecution(execARN, errCode, cause string) {
	_ = e.store.FinalizeExecution(execARN, sfnstore.ExecutionStatusFailed, "", errCode, cause)
	_ = e.store.AppendHistory(execARN, sfnstore.HistoryEvent{
		Type: "ExecutionFailed",
		ExecutionFailedEventDetails: &sfnstore.ExecutionFailedEventDetails{
			Error: errCode,
			Cause: cause,
		},
	})
	e.emitExecutionLogs(execARN, "", "", "Failed")
}

// emitExecutionLogs forwards execution start/end events to the CW log group
// declared in the state machine's LoggingConfiguration, if any.
func (e *ExecutionEngine) emitExecutionLogs(execARN, input, output, status string) {
	if e.logIngestor == nil {
		return
	}
	exec, err := e.store.GetExecution(execARN)
	if err != nil {
		return
	}
	sm, err := e.store.GetStateMachine(exec.StateMachineARN)
	if err != nil {
		return
	}
	if sm.LoggingConfiguration == nil || sm.LoggingConfiguration.Level == "OFF" {
		return
	}
	logGroupName := ""
	for _, d := range sm.LoggingConfiguration.Destinations {
		if d.CloudWatchLogsLogGroup.LogGroupArn != "" {
			arn := d.CloudWatchLogsLogGroup.LogGroupArn
			if idx := strings.LastIndex(arn, ":log-group:"); idx >= 0 {
				logGroupName = arn[idx+len(":log-group:"):]
			} else {
				logGroupName = arn
			}
			break
		}
	}
	if logGroupName == "" {
		return
	}
	// Log stream name is the execution name (last segment of the execution ARN).
	// Using the full ARN is invalid - log stream names can not contain ":".
	execName := execARN
	if idx := strings.LastIndex(execARN, ":"); idx >= 0 {
		execName = execARN[idx+1:]
	}
	ctx := context.Background()
	_ = e.logIngestor.InternalCreateLogGroup(ctx, logGroupName)

	startMs := exec.StartDate.UnixMilli()
	endMs := time.Now().UnixMilli()
	if exec.StopDate != nil {
		endMs = exec.StopDate.UnixMilli()
	}

	startDetails := map[string]any{
		"input":        input,
		"inputDetails": map[string]any{"included": true, "truncated": false},
	}
	var endDetails map[string]any
	if status == "Succeeded" {
		endDetails = map[string]any{
			"output":        output,
			"outputDetails": map[string]any{"included": true, "truncated": false},
		}
	} else {
		endDetails = map[string]any{"error": exec.Error, "cause": exec.Cause}
	}
	events := []logstream.Event{
		{Timestamp: startMs, Message: buildSFNLogEvent("ExecutionStarted", startDetails)},
		{Timestamp: endMs, Message: buildSFNLogEvent("Execution"+status, endDetails)},
	}
	_ = e.logIngestor.InternalPutEvents(ctx, logGroupName, execName, events)
}

func buildSFNLogEvent(eventType string, details map[string]any) string {
	b, _ := json.Marshal(map[string]any{"type": eventType, "details": details})
	return string(b)
}

// ─── Pass state ───────────────────────────────────────────────────────────────

func (e *ExecutionEngine) evalPass(state *asl.PassState, input any) (any, error) {
	effective := applyInputPath2(input, state.InputPath)

	var resultValue any
	if len(state.Parameters) > 0 {
		var params any
		if err := json.Unmarshal(state.Parameters, &params); err != nil {
			return nil, err
		}
		v, err := asl.EvalParameters(params, effective, nil)
		if err != nil {
			return nil, err
		}
		resultValue = v
	} else if len(state.Result) > 0 {
		var result any
		if err := json.Unmarshal(state.Result, &result); err != nil {
			return nil, err
		}
		resultValue = result
	} else {
		resultValue = effective
	}

	output, err := applyResultPath(effective, state.ResultPath, resultValue)
	if err != nil {
		return nil, err
	}
	return applyOutputPath2(output, state.OutputPath), nil
}

// ─── RunBranch (shared by Parallel + Map) ────────────────────────────────────

// runBranch runs a sub-state-machine (branch/iterator) and returns the final output.
func (e *ExecutionEngine) runBranch(ctx context.Context, sm *asl.StateMachineDefinition, input any) (any, error) {
	currentDoc := input
	currentStateName := sm.StartAt
	for currentStateName != "" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		state, ok := sm.States[currentStateName]
		if !ok {
			return nil, fmt.Errorf("States.Runtime: branch state %q not found", currentStateName)
		}
		// Use "" as execARN for nested branches — history events are only recorded
		// for the top-level execution.
		next, output, err := e.evaluateState(ctx, "", currentStateName, state, currentDoc, sm)
		if err != nil {
			return nil, err
		}
		currentDoc = output
		currentStateName = next
	}
	return currentDoc, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func applyIOPath(input any, inputPath, outputPath *string) any {
	v := input
	if inputPath != nil && *inputPath != "$" && *inputPath != "" {
		if res, err := asl.EvalPath(input, *inputPath); err == nil {
			v = res
		}
	}
	if outputPath != nil && *outputPath != "$" && *outputPath != "" {
		if res, err := asl.EvalPath(v, *outputPath); err == nil {
			v = res
		}
	}
	return v
}

func applyInputPath2(input any, path *string) any {
	if path == nil || *path == "$" || *path == "" {
		return input
	}
	v, err := asl.EvalPath(input, *path)
	if err != nil {
		return input
	}
	return v
}

func applyOutputPath2(input any, path *string) any {
	if path == nil || *path == "$" || *path == "" {
		return input
	}
	v, err := asl.EvalPath(input, *path)
	if err != nil {
		return input
	}
	return v
}

func applyResultPath(effective any, resultPath *string, result any) (any, error) {
	if resultPath == nil {
		// Default: replace effective with result
		return result, nil
	}
	rp := *resultPath
	if rp == "$" || rp == "" {
		return result, nil
	}
	if rp == "null" {
		return effective, nil // discard result
	}
	return asl.SetPath(effective, rp, result)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func stateEnteredType(s asl.StateDef) string {
	return typeName(s) + "StateEntered"
}

func stateExitedType(s asl.StateDef) string {
	return typeName(s) + "StateExited"
}

func typeName(s asl.StateDef) string {
	t := s.GetType()
	return strings.ToUpper(t[:1]) + t[1:]
}

// sfnError is the internal error type for state machine failures.
type sfnError struct {
	code  string
	cause string
}

func (e *sfnError) Error() string {
	if e.cause != "" {
		return fmt.Sprintf("%s: %s", e.code, e.cause)
	}
	return e.code
}

func toSFNError(err error) *sfnError {
	if e, ok := err.(*sfnError); ok {
		return e
	}
	return &sfnError{code: "States.TaskFailed", cause: err.Error()}
}

func errorName(err error) string {
	return toSFNError(err).code
}
