package emr

import (
	"testing"
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
