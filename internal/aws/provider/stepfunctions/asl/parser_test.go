package asl_test

import (
	"testing"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

const allStateTypesDef = `{
  "Comment": "all state types",
  "StartAt": "PassStep",
  "States": {
    "PassStep": {"Type": "Pass", "Next": "TaskStep"},
    "TaskStep": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:000000000000:function:myFn", "Next": "ChoiceStep"},
    "ChoiceStep": {
      "Type": "Choice",
      "Choices": [
        {"Variable": "$.x", "NumericEquals": 1, "Next": "WaitStep"},
        {"Variable": "$.x", "NumericGreaterThan": 1, "Next": "SucceedStep"}
      ],
      "Default": "FailStep"
    },
    "WaitStep": {"Type": "Wait", "Seconds": 10, "Next": "ParallelStep"},
    "SucceedStep": {"Type": "Succeed"},
    "FailStep": {"Type": "Fail", "Error": "TooLow", "Cause": "x was too low"},
    "ParallelStep": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "InnerPass",
          "States": {"InnerPass": {"Type": "Pass", "End": true}}
        }
      ],
      "Next": "MapStep"
    },
    "MapStep": {
      "Type": "Map",
      "ItemProcessor": {
        "StartAt": "ProcessItem",
        "States": {"ProcessItem": {"Type": "Pass", "End": true}}
      },
      "End": true
    }
  }
}`

func TestParser_AllStateTypes(t *testing.T) {
	sm, err := asl.Parse(allStateTypesDef)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if sm.StartAt != "PassStep" {
		t.Errorf("StartAt = %q, want PassStep", sm.StartAt)
	}
	if len(sm.States) != 8 {
		t.Errorf("got %d states, want 8", len(sm.States))
	}
	wantTypes := map[string]string{
		"PassStep":     "Pass",
		"TaskStep":     "Task",
		"ChoiceStep":   "Choice",
		"WaitStep":     "Wait",
		"SucceedStep":  "Succeed",
		"FailStep":     "Fail",
		"ParallelStep": "Parallel",
		"MapStep":      "Map",
	}
	for name, want := range wantTypes {
		s, ok := sm.States[name]
		if !ok {
			t.Errorf("state %q missing", name)
			continue
		}
		if s.GetType() != want {
			t.Errorf("state %q type = %q, want %q", name, s.GetType(), want)
		}
	}
}

func TestParser_RetryAndCatch(t *testing.T) {
	def := `{
		"StartAt": "S1",
		"States": {
			"S1": {
				"Type": "Task",
				"Resource": "arn:aws:states:::lambda:invoke",
				"Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 3, "BackoffRate": 2.0}],
				"Catch": [{"ErrorEquals": ["States.TaskFailed"], "Next": "Fallback", "ResultPath": "$.error"}],
				"End": true
			},
			"Fallback": {"Type": "Fail", "Error": "caught"}
		}
	}`
	sm, err := asl.Parse(def)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	task, ok := sm.States["S1"].(*asl.TaskState)
	if !ok {
		t.Fatal("S1 is not TaskState")
	}
	if len(task.Retry) != 1 {
		t.Errorf("Retry len = %d, want 1", len(task.Retry))
	}
	if task.Retry[0].MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", task.Retry[0].MaxAttempts)
	}
	if len(task.Catch) != 1 {
		t.Errorf("Catch len = %d, want 1", len(task.Catch))
	}
	if task.Catch[0].Next != "Fallback" {
		t.Errorf("Catch.Next = %q, want Fallback", task.Catch[0].Next)
	}
}

func TestParser_NestedParallel(t *testing.T) {
	def := `{
		"StartAt": "P",
		"States": {
			"P": {
				"Type": "Parallel",
				"Branches": [
					{
						"StartAt": "A",
						"States": {
							"A": {"Type": "Pass", "Next": "B"},
							"B": {"Type": "Succeed"}
						}
					},
					{
						"StartAt": "X",
						"States": {"X": {"Type": "Fail", "Error": "err"}}
					}
				],
				"End": true
			}
		}
	}`
	sm, err := asl.Parse(def)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	parallel, ok := sm.States["P"].(*asl.ParallelState)
	if !ok {
		t.Fatal("P is not ParallelState")
	}
	if len(parallel.Branches) != 2 {
		t.Errorf("branches = %d, want 2", len(parallel.Branches))
	}
	if len(parallel.Branches[0].States) != 2 {
		t.Errorf("branch[0] states = %d, want 2", len(parallel.Branches[0].States))
	}
}

func TestParser_ChoiceWithCompoundConditions(t *testing.T) {
	def := `{
		"StartAt": "C",
		"States": {
			"C": {
				"Type": "Choice",
				"Choices": [
					{
						"And": [
							{"Variable": "$.x", "NumericGreaterThan": 0},
							{"Variable": "$.x", "NumericLessThan": 10}
						],
						"Next": "InRange"
					}
				],
				"Default": "OutRange"
			},
			"InRange":  {"Type": "Succeed"},
			"OutRange": {"Type": "Succeed"}
		}
	}`
	sm, err := asl.Parse(def)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	choice, ok := sm.States["C"].(*asl.ChoiceState)
	if !ok {
		t.Fatal("C is not ChoiceState")
	}
	if len(choice.Choices[0].And) != 2 {
		t.Errorf("And len = %d, want 2", len(choice.Choices[0].And))
	}
}

func TestParser_MissingStartAt(t *testing.T) {
	def := `{"States": {"S": {"Type": "Succeed"}}}`
	_, err := asl.Parse(def)
	if err == nil {
		t.Fatal("expected error for missing StartAt")
	}
}

func TestParser_InvalidJSON(t *testing.T) {
	_, err := asl.Parse("{not json}")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParser_UnknownStateType(t *testing.T) {
	def := `{"StartAt": "S", "States": {"S": {"Type": "Unknown", "End": true}}}`
	_, err := asl.Parse(def)
	if err == nil {
		t.Fatal("expected error for unknown state type")
	}
}

func TestParser_MapWithIterator(t *testing.T) {
	def := `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"Iterator": {
					"StartAt": "I",
					"States": {"I": {"Type": "Pass", "End": true}}
				},
				"End": true
			}
		}
	}`
	sm, err := asl.Parse(def)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	m, ok := sm.States["M"].(*asl.MapState)
	if !ok {
		t.Fatal("M is not MapState")
	}
	if m.Iterator == nil {
		t.Fatal("Iterator should be non-nil")
	}
	if len(m.Iterator.States) != 1 {
		t.Errorf("Iterator states = %d, want 1", len(m.Iterator.States))
	}
}
