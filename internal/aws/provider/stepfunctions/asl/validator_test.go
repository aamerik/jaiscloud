package asl_test

import (
	"testing"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

func mustParse(t *testing.T, def string) *asl.StateMachineDefinition {
	t.Helper()
	sm, err := asl.Parse(def)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return sm
}

func findDiag(diags []asl.ValidationDiagnostic, code string) *asl.ValidationDiagnostic {
	for i := range diags {
		if diags[i].Code == code {
			return &diags[i]
		}
	}
	return nil
}

func TestValidate_ValidDefinition_Empty(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "S",
		"States": {"S": {"Type": "Succeed"}}
	}`)
	diags := asl.Validate(sm)
	for _, d := range diags {
		if d.Severity == "ERROR" {
			t.Errorf("unexpected error: %+v", d)
		}
	}
}

func TestValidate_StartAtMissing_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "NonExistent",
		"States": {"S": {"Type": "Succeed"}}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "MISSING_TRANSITION_TARGET")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected MISSING_TRANSITION_TARGET error, got: %v", diags)
	}
}

func TestValidate_DanglingNext_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "S",
		"States": {
			"S": {"Type": "Pass", "Next": "DoesNotExist"}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "MISSING_TRANSITION_TARGET")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected MISSING_TRANSITION_TARGET error, got: %v", diags)
	}
}

func TestValidate_UnreachableState_Warning(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "S",
		"States": {
			"S": {"Type": "Succeed"},
			"Orphan": {"Type": "Succeed"}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "STATE_UNREACHABLE")
	if d == nil || d.Severity != "WARNING" {
		t.Errorf("expected STATE_UNREACHABLE warning, got: %v", diags)
	}
}

func TestValidate_ChoiceDefaultMissing_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "C",
		"States": {
			"C": {
				"Type": "Choice",
				"Choices": [{"Variable": "$.x", "NumericEquals": 1, "Next": "S"}],
				"Default": "NotHere"
			},
			"S": {"Type": "Succeed"}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "MISSING_TRANSITION_TARGET")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected MISSING_TRANSITION_TARGET for bad Default, got: %v", diags)
	}
}

func TestValidate_ChoiceWithNext_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "C",
		"States": {
			"C": {
				"Type": "Choice",
				"Choices": [{"Variable": "$.x", "NumericEquals": 1, "Next": "S"}],
				"Next": "S"
			},
			"S": {"Type": "Succeed"}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "INVALID_PROPERTY_VALUE")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected INVALID_PROPERTY_VALUE for Choice.Next, got: %v", diags)
	}
}

func TestValidate_TaskMissingResource_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "T",
		"States": {
			"T": {"Type": "Task", "Resource": "", "End": true}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "MISSING_REQUIRED_FIELD")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected MISSING_REQUIRED_FIELD for missing Resource, got: %v", diags)
	}
}

func TestValidate_WaitMultipleFields_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "W",
		"States": {
			"W": {"Type": "Wait", "Seconds": 5, "SecondsPath": "$.t", "End": true}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "INVALID_PROPERTY_VALUE")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected INVALID_PROPERTY_VALUE for multiple Wait fields, got: %v", diags)
	}
}

func TestValidate_ParallelNoBranches_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "P",
		"States": {
			"P": {"Type": "Parallel", "Branches": [], "End": true}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "MISSING_REQUIRED_FIELD")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected MISSING_REQUIRED_FIELD for Parallel no branches, got: %v", diags)
	}
}

func TestValidate_CatchDanglingNext_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "T",
		"States": {
			"T": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:::function:fn",
				"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Missing"}],
				"End": true
			}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "MISSING_TRANSITION_TARGET")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected MISSING_TRANSITION_TARGET for bad Catch.Next, got: %v", diags)
	}
}

func TestValidate_SucceedWithNext_Error(t *testing.T) {
	sm := mustParse(t, `{
		"StartAt": "S",
		"States": {
			"S": {"Type": "Succeed", "Next": "X"},
			"X": {"Type": "Succeed"}
		}
	}`)
	diags := asl.Validate(sm)
	d := findDiag(diags, "INVALID_PROPERTY_VALUE")
	if d == nil || d.Severity != "ERROR" {
		t.Errorf("expected INVALID_PROPERTY_VALUE for Succeed.Next, got: %v", diags)
	}
}
