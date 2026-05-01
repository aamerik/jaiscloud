package emr

import (
	"testing"

	"jaiscloud/internal/executor/spark"
)

func TestIsTerminalClusterState(t *testing.T) {
	terminal := []string{"TERMINATED", "TERMINATED_WITH_ERRORS"}
	for _, s := range terminal {
		if !isTerminalClusterState(s) {
			t.Errorf("%q should be terminal", s)
		}
	}
	nonTerminal := []string{"WAITING", "RUNNING", "BOOTSTRAPPING", "TERMINATING", ""}
	for _, s := range nonTerminal {
		if isTerminalClusterState(s) {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

func TestIsTerminalStepState(t *testing.T) {
	terminal := []string{"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}
	for _, s := range terminal {
		if !isTerminalStepState(s) {
			t.Errorf("%q should be terminal", s)
		}
	}
	nonTerminal := []string{"PENDING", "RUNNING", "CANCEL_PENDING", ""}
	for _, s := range nonTerminal {
		if isTerminalStepState(s) {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

func TestSparkStateFromStepState(t *testing.T) {
	cases := []struct {
		in   string
		want spark.SparkState
	}{
		{"RUNNING", spark.StateRunning},
		{"COMPLETED", spark.StateCompleted},
		{"FAILED", spark.StateFailed},
		{"CANCELLED", spark.StateFailed},
		{"INTERRUPTED", spark.StateFailed},
		{"PENDING", spark.StatePending},
		{"CANCEL_PENDING", spark.StatePending},
		{"", spark.StatePending},
	}
	for _, c := range cases {
		if got := sparkStateFromStepState(c.in); got != c.want {
			t.Errorf("sparkStateFromStepState(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
