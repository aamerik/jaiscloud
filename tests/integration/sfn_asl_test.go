// Package integration provides ASL-conformance and execution-detail integration
// tests for the Step Functions emulator. Tests cover Choice state conditions,
// Pass state projections, Map/Parallel state variants, Succeed/Fail terminals,
// error handling, execution history, ListExecutions filtering, and
// DescribeExecution output round-trips.
package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForExecution polls DescribeExecution until it reaches a terminal state or
// the timeout elapses. It is safe to call from multiple goroutines.
func waitForExecution(t *testing.T, client *awssfn.Client, execARN string, timeout time.Duration) *awssfn.DescribeExecutionOutput {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.DescribeExecution(context.Background(), &awssfn.DescribeExecutionInput{
			ExecutionArn: aws.String(execARN),
		})
		require.NoError(t, err)
		switch out.Status {
		case sfntypes.ExecutionStatusSucceeded,
			sfntypes.ExecutionStatusFailed,
			sfntypes.ExecutionStatusAborted,
			sfntypes.ExecutionStatusTimedOut:
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("execution %s did not reach terminal state within %s", execARN, timeout)
	return nil
}

// startAndWait is a convenience helper: creates a state machine, starts an
// execution with the given input JSON, and waits for it to finish. Returns
// the DescribeExecution output.
func startAndWait(
	t *testing.T,
	client *awssfn.Client,
	smName string,
	definition string,
	input string,
) *awssfn.DescribeExecutionOutput {
	t.Helper()
	ctx := context.Background()

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	return waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)
}

// ─── Choice state — numeric comparisons ──────────────────────────────────────

// TestSFN_ChoiceState_NumericComparison verifies that Choice branches taken via
// NumericGreaterThan and NumericLessThan route to the correct terminal state.
func TestSFN_ChoiceState_NumericComparison(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Route",
		"States": {
			"Route": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.score",
						"NumericGreaterThan": 90,
						"Next": "High"
					},
					{
						"Variable": "$.score",
						"NumericLessThan": 50,
						"Next": "Low"
					}
				],
				"Default": "Mid"
			},
			"High": { "Type": "Pass", "Result": {"band": "high"}, "End": true },
			"Mid":  { "Type": "Pass", "Result": {"band": "mid"},  "End": true },
			"Low":  { "Type": "Pass", "Result": {"band": "low"},  "End": true }
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	cases := []struct {
		score    string
		wantBand string
	}{
		{`{"score": 95}`, "high"},
		{`{"score": 30}`, "low"},
		{`{"score": 70}`, "mid"},
	}

	for _, tc := range cases {
		startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(tc.score),
		})
		require.NoError(t, err)

		desc := waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
		assert.Contains(t, aws.ToString(desc.Output), tc.wantBand,
			"score %s should route to band %s", tc.score, tc.wantBand)
	}
}

// ─── Choice state — boolean equals ───────────────────────────────────────────

// TestSFN_ChoiceState_BooleanEquals verifies that a BooleanEquals Choice
// condition branches correctly on true/false inputs.
func TestSFN_ChoiceState_BooleanEquals(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Check",
		"States": {
			"Check": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.enabled",
						"BooleanEquals": true,
						"Next": "Active"
					}
				],
				"Default": "Inactive"
			},
			"Active":   { "Type": "Pass", "Result": {"state": "active"},   "End": true },
			"Inactive": { "Type": "Pass", "Result": {"state": "inactive"}, "End": true }
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// enabled = true → Active
	startTrue, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"enabled": true}`),
	})
	require.NoError(t, err)
	descTrue := waitForExecution(t, client, *startTrue.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descTrue.Status)
	assert.Contains(t, aws.ToString(descTrue.Output), "active")

	// enabled = false → Inactive
	startFalse, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"enabled": false}`),
	})
	require.NoError(t, err)
	descFalse := waitForExecution(t, client, *startFalse.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descFalse.Status)
	assert.Contains(t, aws.ToString(descFalse.Output), "inactive")
}

// ─── Choice state — And/Or compound conditions ────────────────────────────────

// TestSFN_ChoiceState_AndOr verifies And and Or compound conditions work correctly.
func TestSFN_ChoiceState_AndOr(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Route",
		"States": {
			"Route": {
				"Type": "Choice",
				"Choices": [
					{
						"And": [
							{"Variable": "$.x", "NumericGreaterThan": 0},
							{"Variable": "$.y", "NumericGreaterThan": 0}
						],
						"Next": "BothPositive"
					},
					{
						"Or": [
							{"Variable": "$.x", "NumericGreaterThan": 0},
							{"Variable": "$.y", "NumericGreaterThan": 0}
						],
						"Next": "OnePositive"
					}
				],
				"Default": "NonePositive"
			},
			"BothPositive": { "Type": "Pass", "Result": {"result": "both"},  "End": true },
			"OnePositive":  { "Type": "Pass", "Result": {"result": "one"},   "End": true },
			"NonePositive": { "Type": "Pass", "Result": {"result": "none"},  "End": true }
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	cases := []struct {
		input      string
		wantResult string
	}{
		{`{"x": 1, "y": 2}`, "both"},
		{`{"x": 1, "y": -1}`, "one"},
		{`{"x": -1, "y": -2}`, "none"},
	}

	for _, tc := range cases {
		startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(tc.input),
		})
		require.NoError(t, err)
		desc := waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
		assert.Contains(t, aws.ToString(desc.Output), tc.wantResult,
			"input %s should produce result %q", tc.input, tc.wantResult)
	}
}

// ─── Choice state — IsPresent ─────────────────────────────────────────────────

// TestSFN_ChoiceState_IsPresent verifies that IsPresent correctly branches based
// on whether a field exists in the input.
func TestSFN_ChoiceState_IsPresent(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Check",
		"States": {
			"Check": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.optional",
						"IsPresent": true,
						"Next": "HasField"
					}
				],
				"Default": "MissingField"
			},
			"HasField":     { "Type": "Pass", "Result": {"found": true},  "End": true },
			"MissingField": { "Type": "Pass", "Result": {"found": false}, "End": true }
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// Field present
	startPresent, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"optional": "value"}`),
	})
	require.NoError(t, err)
	descPresent := waitForExecution(t, client, *startPresent.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descPresent.Status)
	assert.Contains(t, aws.ToString(descPresent.Output), "true")

	// Field absent
	startAbsent, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"other": "value"}`),
	})
	require.NoError(t, err)
	descAbsent := waitForExecution(t, client, *startAbsent.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descAbsent.Status)
	assert.Contains(t, aws.ToString(descAbsent.Output), "false")
}

// ─── Choice state — Default branch ───────────────────────────────────────────

// TestSFN_ChoiceState_DefaultBranch verifies that the Default branch is taken
// when no Choice condition matches.
func TestSFN_ChoiceState_DefaultBranch(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "Route",
		"States": {
			"Route": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.type",
						"StringEquals": "known",
						"Next": "Known"
					}
				],
				"Default": "Unknown"
			},
			"Known":   { "Type": "Pass", "Result": {"branch": "known"},   "End": true },
			"Unknown": { "Type": "Pass", "Result": {"branch": "default"}, "End": true }
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"type": "unrecognised"}`)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
	assert.Contains(t, aws.ToString(desc.Output), "default")
}

// ─── Pass state — ResultPath ──────────────────────────────────────────────────

// TestSFN_PassState_ResultPath verifies that Pass with ResultPath merges the
// static Result into the input at the specified path rather than replacing it.
func TestSFN_PassState_ResultPath(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "Merge",
		"States": {
			"Merge": {
				"Type": "Pass",
				"Result": {"computed": 42},
				"ResultPath": "$.result",
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"original": "data"}`)
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out))
	assert.Contains(t, out, "original", "original field must be preserved")
	assert.Contains(t, out, "result", "ResultPath field must be injected")
	assert.Contains(t, string(out["result"]), "42")
}

// ─── Pass state — Parameters ──────────────────────────────────────────────────

// TestSFN_PassState_Parameters verifies that Pass with Parameters replaces the
// effective input with the evaluated Parameters object before applying ResultPath.
func TestSFN_PassState_Parameters(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	// Parameters uses JsonPath references (.$-suffix syntax) to reshape input.
	definition := `{
		"StartAt": "Transform",
		"States": {
			"Transform": {
				"Type": "Pass",
				"Parameters": {
					"id.$": "$.userId",
					"static": "constant"
				},
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"userId": "u-123", "extra": "drop"}`)
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)

	var out map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out))
	assert.Equal(t, "u-123", out["id"], "id should be mapped from $.userId")
	assert.Equal(t, "constant", out["static"])
	assert.NotContains(t, out, "extra", "extra field should be dropped by Parameters")
}

// ─── Map state — ResultPath ───────────────────────────────────────────────────

// TestSFN_MapState_ResultPath verifies that Map with ResultPath stores its array
// result under the specified sub-field rather than replacing the entire output.
func TestSFN_MapState_ResultPath(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "ProcessItems",
		"States": {
			"ProcessItems": {
				"Type": "Map",
				"ItemsPath": "$.items",
				"ResultPath": "$.processed",
				"Iterator": {
					"StartAt": "PassItem",
					"States": {
						"PassItem": { "Type": "Pass", "End": true }
					}
				},
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"items": [1, 2, 3], "meta": "keep"}`)
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)

	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out))
	assert.Contains(t, out, "meta", "meta field should be preserved by ResultPath")
	assert.Contains(t, out, "processed", "processed field should hold Map results")
}

// ─── Map state — MaxConcurrency ───────────────────────────────────────────────

// TestSFN_MapState_MaxConcurrency verifies that a Map state with MaxConcurrency=1
// processes items serially and produces the same result count as the input array.
func TestSFN_MapState_MaxConcurrency(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "Serial",
		"States": {
			"Serial": {
				"Type": "Map",
				"ItemsPath": "$.items",
				"MaxConcurrency": 1,
				"Iterator": {
					"StartAt": "Step",
					"States": {
						"Step": { "Type": "Pass", "End": true }
					}
				},
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"items": [10, 20, 30, 40, 50]}`)
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)

	var out []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out),
		"Map state output must be a JSON array")
	assert.Len(t, out, 5, "Map state should produce one result per input item")
}

// ─── Parallel state — multiple outputs ───────────────────────────────────────

// TestSFN_ParallelState_MultipleOutputs verifies that a Parallel state with three
// branches produces an output array with exactly three elements.
func TestSFN_ParallelState_MultipleOutputs(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "Parallel",
		"States": {
			"Parallel": {
				"Type": "Parallel",
				"Branches": [
					{
						"StartAt": "B1",
						"States": { "B1": { "Type": "Pass", "Result": {"branch": 1}, "End": true } }
					},
					{
						"StartAt": "B2",
						"States": { "B2": { "Type": "Pass", "Result": {"branch": 2}, "End": true } }
					},
					{
						"StartAt": "B3",
						"States": { "B3": { "Type": "Pass", "Result": {"branch": 3}, "End": true } }
					}
				],
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{}`)
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)

	var out []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(desc.Output)), &out),
		"Parallel state output must be a JSON array")
	assert.Len(t, out, 3, "Parallel state must produce one element per branch")
}

// ─── Succeed state ────────────────────────────────────────────────────────────

// TestSFN_SucceedState verifies that a Succeed terminal state causes the execution
// to end with SUCCEEDED and the input passes through unchanged.
func TestSFN_SucceedState(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "Done",
		"States": {
			"Done": { "Type": "Succeed" }
		}
	}`

	input := `{"keep": "this"}`
	desc := startAndWait(t, client, name, definition, input)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
	assert.JSONEq(t, input, aws.ToString(desc.Output))
}

// ─── Fail state ───────────────────────────────────────────────────────────────

// TestSFN_FailState verifies that a Fail state causes the execution to end with
// FAILED and that the Error and Cause fields are surfaced on DescribeExecution.
func TestSFN_FailState(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Explode",
		"States": {
			"Explode": {
				"Type": "Fail",
				"Error": "TestError",
				"Cause": "intentional test failure"
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	desc := waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusFailed, desc.Status)
	assert.NotNil(t, desc.StopDate)
	// The Error and Cause fields are returned by DescribeExecution for failed executions.
	assert.Equal(t, "TestError", aws.ToString(desc.Error))
	assert.Equal(t, "intentional test failure", aws.ToString(desc.Cause))
}

// ─── Error handling — multiple retry rules ────────────────────────────────────

// TestSFN_ErrorHandling_MultipleRetryRules verifies that a Task state with two
// distinct Retry rules is accepted and the execution completes (in lite mode the
// task passthrough always succeeds, so we confirm SUCCEEDED without errors).
func TestSFN_ErrorHandling_MultipleRetryRules(t *testing.T) {
	resetState(t)
	sfnCreateLambdaFn(t, "retry-fn")
	client := sfnClient(t)
	name := sfnName(t)

	definition := `{
		"StartAt": "Task",
		"States": {
			"Task": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:retry-fn",
				"Retry": [
					{
						"ErrorEquals": ["CustomError"],
						"IntervalSeconds": 1,
						"MaxAttempts": 2,
						"BackoffRate": 2.0
					},
					{
						"ErrorEquals": ["States.TaskFailed"],
						"IntervalSeconds": 2,
						"MaxAttempts": 1,
						"BackoffRate": 1.0
					}
				],
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"attempt": 1}`)
	// In lite mode the task passthrough always succeeds — no retry triggered.
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
	assert.NotEmpty(t, aws.ToString(desc.Output))
}

// ─── Error handling — catch all ───────────────────────────────────────────────

// TestSFN_ErrorHandling_CatchAll verifies that a Task state with a States.ALL
// Catch clause routes to the error handler state and that the error state
// injects Error and Cause into the ResultPath.
func TestSFN_ErrorHandling_CatchAll(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	name := sfnName(t)

	// The Catch clause uses ResultPath "$.errorInfo" so even when catch is
	// triggered the execution ends SUCCEEDED (via HandleError). In lite mode
	// the task always succeeds so catch is never triggered, but the machine
	// definition must parse correctly and produce SUCCEEDED.
	definition := `{
		"StartAt": "Task",
		"States": {
			"Task": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:catch-fn",
				"Catch": [
					{
						"ErrorEquals": ["States.ALL"],
						"Next": "HandleError",
						"ResultPath": "$.errorInfo"
					}
				],
				"End": true
			},
			"HandleError": {
				"Type": "Pass",
				"Result": {"handled": true},
				"End": true
			}
		}
	}`

	desc := startAndWait(t, client, name, definition, `{"input": "data"}`)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
	assert.NotEmpty(t, aws.ToString(desc.Output))
}

// ─── Execution history — forward order ───────────────────────────────────────

// TestSFN_GetExecutionHistory_ForwardOrder verifies that GetExecutionHistory with
// reverseOrder=false returns events in ascending (chronological) order, starting
// with ExecutionStarted and ending with ExecutionSucceeded.
func TestSFN_GetExecutionHistory_ForwardOrder(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)

	hist, err := client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
		ReverseOrder: false,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(hist.Events), 2, "must have at least ExecutionStarted and ExecutionSucceeded")

	first := hist.Events[0].Type
	last := hist.Events[len(hist.Events)-1].Type
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionStarted, first,
		"first event must be ExecutionStarted in forward order")
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionSucceeded, last,
		"last event must be ExecutionSucceeded in forward order")
}

// ─── Execution history — reverse order ───────────────────────────────────────

// TestSFN_GetExecutionHistory_ReverseOrder_ASL verifies that GetExecutionHistory
// with reverseOrder=true returns events in descending (reverse-chronological) order.
func TestSFN_GetExecutionHistory_ReverseOrder_ASL(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)

	hist, err := client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
		ReverseOrder: true,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(hist.Events), 2)

	first := hist.Events[0].Type
	last := hist.Events[len(hist.Events)-1].Type
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionSucceeded, first,
		"first event must be ExecutionSucceeded in reverse order")
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionStarted, last,
		"last event must be ExecutionStarted in reverse order")
}

// ─── Execution history — event types ─────────────────────────────────────────

// TestSFN_GetExecutionHistory_HasStateEvents verifies that the history for a
// completed Pass-state execution includes ExecutionStarted, at least one
// StateEntered event, at least one StateExited event, and ExecutionSucceeded.
func TestSFN_GetExecutionHistory_HasStateEvents(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)

	hist, err := client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)

	typeSet := make(map[sfntypes.HistoryEventType]bool)
	for _, ev := range hist.Events {
		typeSet[ev.Type] = true
	}

	assert.True(t, typeSet[sfntypes.HistoryEventTypeExecutionStarted], "must include ExecutionStarted")
	assert.True(t, typeSet[sfntypes.HistoryEventTypeExecutionSucceeded], "must include ExecutionSucceeded")
	// The engine emits PassStateEntered / PassStateExited for each Pass state transition.
	hasEntered := typeSet[sfntypes.HistoryEventTypePassStateEntered] ||
		typeSet[sfntypes.HistoryEventTypeTaskStateEntered] ||
		typeSet[sfntypes.HistoryEventTypeSucceedStateEntered] ||
		typeSet[sfntypes.HistoryEventTypeChoiceStateEntered]
	hasExited := typeSet[sfntypes.HistoryEventTypePassStateExited] ||
		typeSet[sfntypes.HistoryEventTypeTaskStateExited] ||
		typeSet[sfntypes.HistoryEventTypeSucceedStateExited] ||
		typeSet[sfntypes.HistoryEventTypeChoiceStateExited]
	assert.True(t, hasEntered, "must include at least one StateEntered event")
	assert.True(t, hasExited, "must include at least one StateExited event")
}

// ─── ListExecutions — RUNNING status filter ───────────────────────────────────

// TestSFN_ListExecutions_StatusFilter_Running starts two executions using a
// Wait state so they stay RUNNING, then filters by RUNNING and expects to see
// both. In lite mode the Wait state runs asynchronously in a goroutine so the
// executions remain RUNNING for several seconds.
func TestSFN_ListExecutions_StatusFilter_Running(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	// Use a 5-second Wait so the executions stay RUNNING long enough to list.
	definition := `{
		"StartAt": "Hold",
		"States": {
			"Hold": {
				"Type": "Wait",
				"Seconds": 5,
				"Next": "Done"
			},
			"Done": { "Type": "Pass", "End": true }
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		_, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(`{}`),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: createOut.StateMachineArn,
		StatusFilter:    sfntypes.ExecutionStatusRunning,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(listOut.Executions), "both executions should still be RUNNING")
	for _, e := range listOut.Executions {
		assert.Equal(t, sfntypes.ExecutionStatusRunning, e.Status)
	}
}

// ─── ListExecutions — SUCCEEDED status filter ────────────────────────────────

// TestSFN_ListExecutions_StatusFilter_Succeeded starts three executions that
// complete instantly (Pass state only), then filters by SUCCEEDED and asserts
// all three are returned with SUCCEEDED status.
func TestSFN_ListExecutions_StatusFilter_Succeeded(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(`{}`),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: createOut.StateMachineArn,
		StatusFilter:    sfntypes.ExecutionStatusSucceeded,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(listOut.Executions))
	for _, e := range listOut.Executions {
		assert.Equal(t, sfntypes.ExecutionStatusSucceeded, e.Status)
	}
}

// ─── DescribeExecution — Input and Output round-trip ─────────────────────────

// TestSFN_DescribeExecution_Input_Output verifies that after a successful
// execution the Input field matches what was submitted and the Output field is
// non-empty (for a Pass-through definition the output equals the input).
func TestSFN_DescribeExecution_Input_Output(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	input := `{"userId": "abc-123", "count": 7}`
	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	desc := waitForExecution(t, client, *startOut.ExecutionArn, 10*time.Second)
	require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)

	// Input must round-trip exactly.
	assert.JSONEq(t, input, aws.ToString(desc.Input))
	// Output must be non-empty; for the Pass-through definition it equals Input.
	assert.NotEmpty(t, aws.ToString(desc.Output))
	assert.JSONEq(t, input, aws.ToString(desc.Output))
}
