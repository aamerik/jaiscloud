package engine_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"jaiscloud/internal/clock"
		"jaiscloud/internal/aws/provider/stepfunctions/asl"
	"jaiscloud/internal/aws/provider/stepfunctions/engine"
	sfnstore "jaiscloud/internal/aws/store/stepfunctions"
)

// mockDispatcher is a no-op dispatcher that echoes params as output.
type mockDispatcher struct {
	calls []dispatchCall
}

type dispatchCall struct {
	Service string
	Action  string
	Params  map[string]any
}

func (m *mockDispatcher) Dispatch(_ context.Context, svc, action string, params map[string]any) (map[string]any, error) {
	m.calls = append(m.calls, dispatchCall{svc, action, params})
	// Echo back params as output
	result := make(map[string]any, len(params))
	for k, v := range params {
		result[k] = v
	}
	result["_service"] = svc
	result["_action"] = action
	return result, nil
}

func newTestEngine(t *testing.T) (*engine.ExecutionEngine, *sfnstore.MemoryStepFunctionsStore, *mockDispatcher) {
	t.Helper()
	store := sfnstore.NewMemoryStepFunctionsStore()
	disp := &mockDispatcher{}
	eng := engine.New(store, disp)
	return eng, store, disp
}

func startExec(t *testing.T, store *sfnstore.MemoryStepFunctionsStore, eng *engine.ExecutionEngine, smARN, def, input string) string {
	t.Helper()
	execARN := "arn:aws:states:us-east-1:000000000000:execution:test:" + t.Name()
	exec := &sfnstore.Execution{
		Name: t.Name(), ARN: execARN,
		StateMachineARN: smARN, Status: sfnstore.ExecutionStatusRunning,
		StartDate: clock.RealNow(), Input: input, History: []sfnstore.HistoryEvent{},
	}
	if err := store.StartExecution(exec); err != nil {
		t.Fatalf("StartExecution: %v", err)
	}
	sm, err := asl.Parse(def)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eng.Start(execARN, sm, input)
	return execARN
}

func waitTerminal(t *testing.T, store *sfnstore.MemoryStepFunctionsStore, execARN string, timeout time.Duration) *sfnstore.Execution {
	t.Helper()
	deadline := clock.RealNow().Add(timeout)
	for clock.RealNow().Before(deadline) {
		exec, err := store.GetExecution(execARN)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if exec.Status != sfnstore.ExecutionStatusRunning {
			return exec
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("execution %s did not reach terminal state within %s", execARN, timeout)
	return nil
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestEngine_PassState_Passthrough(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{"x":42}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Errorf("status = %s, want SUCCEEDED", exec.Status)
	}
	if exec.Output != `{"x":42}` {
		t.Errorf("output = %s, want {\"x\":42}", exec.Output)
	}
}

func TestEngine_PassState_WithResult(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Pass","Result":{"answer":99},"End":true}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Fatalf("status = %s", exec.Status)
	}
	var out map[string]any
	_ = json.Unmarshal([]byte(exec.Output), &out)
	if out["answer"] != 99.0 {
		t.Errorf("answer = %v, want 99", out["answer"])
	}
}

func TestEngine_PassState_WithResultPath(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Pass","Result":{"y":2},"ResultPath":"$.result","End":true}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{"x":1}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	var out map[string]any
	_ = json.Unmarshal([]byte(exec.Output), &out)
	if out["x"] != 1.0 {
		t.Errorf("x = %v, want 1 (original field preserved)", out["x"])
	}
	result, _ := out["result"].(map[string]any)
	if result["y"] != 2.0 {
		t.Errorf("result.y = %v, want 2", result["y"])
	}
}

func TestEngine_SucceedState_Terminates(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{"v":"hello"}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Errorf("status = %s", exec.Status)
	}
}

func TestEngine_FailState_SetsError(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Fail","Error":"MyErr","Cause":"test cause"}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusFailed {
		t.Errorf("status = %s, want FAILED", exec.Status)
	}
	if exec.Error != "MyErr" {
		t.Errorf("error = %q, want MyErr", exec.Error)
	}
	if exec.Cause != "test cause" {
		t.Errorf("cause = %q, want 'test cause'", exec.Cause)
	}
}

func TestEngine_ChainedPasses(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{
		"StartAt":"A",
		"States":{
			"A":{"Type":"Pass","Next":"B"},
			"B":{"Type":"Pass","Next":"C"},
			"C":{"Type":"Succeed"}
		}
	}`
	execARN := startExec(t, store, eng, "sm1", def, `{"val":7}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Errorf("status = %s", exec.Status)
	}
}

func TestEngine_ChoiceState_MatchesRule(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{
		"StartAt":"Choose",
		"States":{
			"Choose":{
				"Type":"Choice",
				"Choices":[
					{"Variable":"$.x","NumericEquals":1,"Next":"One"},
					{"Variable":"$.x","NumericEquals":2,"Next":"Two"}
				],
				"Default":"Other"
			},
			"One":{"Type":"Pass","Result":{"matched":"one"},"End":true},
			"Two":{"Type":"Pass","Result":{"matched":"two"},"End":true},
			"Other":{"Type":"Pass","Result":{"matched":"other"},"End":true}
		}
	}`
	execARN := startExec(t, store, eng, "sm1", def, `{"x":2}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	var out map[string]any
	_ = json.Unmarshal([]byte(exec.Output), &out)
	if out["matched"] != "two" {
		t.Errorf("matched = %v, want two", out["matched"])
	}
}

func TestEngine_ChoiceState_UsesDefault(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{
		"StartAt":"C",
		"States":{
			"C":{"Type":"Choice","Choices":[{"Variable":"$.x","StringEquals":"yes","Next":"Y"}],"Default":"N"},
			"Y":{"Type":"Pass","Result":{"r":"yes"},"End":true},
			"N":{"Type":"Pass","Result":{"r":"no"},"End":true}
		}
	}`
	execARN := startExec(t, store, eng, "sm1", def, `{"x":"no"}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	var out map[string]any
	_ = json.Unmarshal([]byte(exec.Output), &out)
	if out["r"] != "no" {
		t.Errorf("r = %v, want no", out["r"])
	}
}

func TestEngine_Shutdown_CancelsInflight(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	// A Wait state that waits 60 seconds — will be cancelled by shutdown
	def := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":60,"End":true}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{}`)

	// Give execution time to start
	time.Sleep(50 * time.Millisecond)

	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := eng.Shutdown(shutCtx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	// Execution should now be terminal (aborted or timed-out)
	exec, _ := store.GetExecution(execARN)
	if exec.Status == sfnstore.ExecutionStatusRunning {
		t.Errorf("execution still running after shutdown")
	}
}

func TestEngine_History_HasEvents(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{}`)
	waitTerminal(t, store, execARN, 3*time.Second)

	history, err := store.GetExecutionHistory(execARN, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) < 2 {
		t.Errorf("history len = %d, want >= 2", len(history))
	}
	// First event: ExecutionStarted
	if history[0].Type != "ExecutionStarted" {
		t.Errorf("history[0].Type = %q, want ExecutionStarted", history[0].Type)
	}
	// Last event: ExecutionSucceeded
	last := history[len(history)-1]
	if last.Type != "ExecutionSucceeded" {
		t.Errorf("last event type = %q, want ExecutionSucceeded", last.Type)
	}
}

func TestEngine_ParallelState(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{
		"StartAt":"P",
		"States":{
			"P":{
				"Type":"Parallel",
				"Branches":[
					{"StartAt":"A","States":{"A":{"Type":"Pass","Result":{"branch":"a"},"End":true}}},
					{"StartAt":"B","States":{"B":{"Type":"Pass","Result":{"branch":"b"},"End":true}}}
				],
				"End":true
			}
		}
	}`
	execARN := startExec(t, store, eng, "sm1", def, `{}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Fatalf("status = %s", exec.Status)
	}
	var out []any
	_ = json.Unmarshal([]byte(exec.Output), &out)
	if len(out) != 2 {
		t.Errorf("parallel result len = %d, want 2", len(out))
	}
}

func TestEngine_MapState(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{
		"StartAt":"M",
		"States":{
			"M":{
				"Type":"Map",
				"ItemsPath":"$.items",
				"Iterator":{
					"StartAt":"I",
					"States":{"I":{"Type":"Pass","End":true}}
				},
				"End":true
			}
		}
	}`
	execARN := startExec(t, store, eng, "sm1", def, `{"items":[1,2,3]}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Fatalf("status = %s, cause = %s", exec.Status, exec.Cause)
	}
	var out []any
	_ = json.Unmarshal([]byte(exec.Output), &out)
	if len(out) != 3 {
		t.Errorf("map result len = %d, want 3", len(out))
	}
}

func TestEngine_WaitState_ShortDuration(t *testing.T) {
	eng, store, _ := newTestEngine(t)
	def := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":0,"Next":"Done"},"Done":{"Type":"Succeed"}}}`
	execARN := startExec(t, store, eng, "sm1", def, `{}`)
	exec := waitTerminal(t, store, execARN, 3*time.Second)
	if exec.Status != sfnstore.ExecutionStatusSucceeded {
		t.Errorf("status = %s", exec.Status)
	}
}
