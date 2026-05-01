package emroneks

import (
	"testing"

	"jaiscloud/internal/executor/spark"
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

func TestSparkStateFromJobRunState(t *testing.T) {
	cases := []struct {
		in   string
		want spark.SparkState
	}{
		{"RUNNING", spark.StateRunning},
		{"COMPLETED", spark.StateCompleted},
		{"FAILED", spark.StateFailed},
		{"CANCELLED", spark.StateFailed},
		{"PENDING", spark.StatePending},
		{"SUBMITTED", spark.StatePending},
		{"", spark.StatePending},
	}
	for _, c := range cases {
		if got := sparkStateFromJobRunState(c.in); got != c.want {
			t.Errorf("sparkStateFromJobRunState(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
