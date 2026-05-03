package emroneks

import (
	"testing"
)

func TestIsTerminalJobRunState(t *testing.T) {
	terminal := []string{"COMPLETED", "FAILED", "CANCELLED"}
	for _, s := range terminal {
		if !isTerminalJobRunState(s) {
			t.Errorf("%q should be terminal", s)
		}
	}
	nonTerminal := []string{"PENDING", "SUBMITTED", "RUNNING", ""}
	for _, s := range nonTerminal {
		if isTerminalJobRunState(s) {
			t.Errorf("%q should not be terminal", s)
		}
	}
}
