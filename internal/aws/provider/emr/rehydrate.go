package emr

// isTerminalClusterState reports whether an EMR cluster state is terminal.
func isTerminalClusterState(s string) bool {
	switch s {
	case "TERMINATED", "TERMINATED_WITH_ERRORS":
		return true
	}
	return false
}

// isTerminalStepState reports whether an EMR step state is terminal.
func isTerminalStepState(s string) bool {
	switch s {
	case "COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED":
		return true
	}
	return false
}
