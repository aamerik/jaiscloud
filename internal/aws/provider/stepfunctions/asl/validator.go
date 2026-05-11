package asl

import "fmt"

// ValidationDiagnostic represents a single validation finding.
type ValidationDiagnostic struct {
	Severity string `json:"severity"` // "ERROR" or "WARNING"
	Code     string `json:"code"`
	Message  string `json:"message"`
	Location string `json:"location"`
}

// Validate checks an ASL state machine definition for errors and warnings.
// Returns an empty slice (not nil) when the definition is valid.
func Validate(sm *StateMachineDefinition) []ValidationDiagnostic {
	var diags []ValidationDiagnostic

	// 1. StartAt must exist
	if _, ok := sm.States[sm.StartAt]; !ok {
		diags = append(diags, ValidationDiagnostic{
			Severity: "ERROR",
			Code:     "MISSING_TRANSITION_TARGET",
			Message:  fmt.Sprintf("State '%s' referenced by StartAt does not exist", sm.StartAt),
			Location: "StartAt",
		})
	}

	// 2–11. Per-state checks
	for name, state := range sm.States {
		diags = append(diags, validateState(name, state, sm.States)...)
	}

	// 12. Reachability (warning)
	reachable := computeReachable(sm)
	for name := range sm.States {
		if !reachable[name] {
			diags = append(diags, ValidationDiagnostic{
				Severity: "WARNING",
				Code:     "STATE_UNREACHABLE",
				Message:  fmt.Sprintf("State '%s' is not reachable from StartAt", name),
				Location: fmt.Sprintf("States.%s", name),
			})
		}
	}

	// 13. At least one terminal path must exist
	if !hasTerminalPath(sm) {
		diags = append(diags, ValidationDiagnostic{
			Severity: "ERROR",
			Code:     "NO_TERMINAL_STATE",
			Message:  "State machine has no terminal state (End=true, Succeed, or Fail)",
			Location: "States",
		})
	}

	if diags == nil {
		return []ValidationDiagnostic{}
	}
	return diags
}

func validateState(name string, state StateDef, allStates map[string]StateDef) []ValidationDiagnostic {
	var diags []ValidationDiagnostic
	loc := func(field string) string { return fmt.Sprintf("States.%s.%s", name, field) }

	switch s := state.(type) {
	case *PassState:
		diags = append(diags, checkTransition(name, s.Next, s.End, allStates)...)

	case *TaskState:
		if s.Resource == "" {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "MISSING_REQUIRED_FIELD",
				Message:  fmt.Sprintf("Task state '%s' is missing required field Resource", name),
				Location: loc("Resource"),
			})
		}
		diags = append(diags, checkTransition(name, s.Next, s.End, allStates)...)
		for i, c := range s.Catch {
			if _, ok := allStates[c.Next]; !ok {
				diags = append(diags, ValidationDiagnostic{
					Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
					Message:  fmt.Sprintf("State '%s' Catch[%d] transitions to nonexistent state '%s'", name, i, c.Next),
					Location: fmt.Sprintf("States.%s.Catch[%d].Next", name, i),
				})
			}
		}

	case *ChoiceState:
		// Choice must NOT have End or Next at top level
		if s.End {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "INVALID_PROPERTY_VALUE",
				Message:  fmt.Sprintf("Choice state '%s' cannot have End=true", name),
				Location: loc("End"),
			})
		}
		if s.Next != "" {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "INVALID_PROPERTY_VALUE",
				Message:  fmt.Sprintf("Choice state '%s' cannot have a Next field", name),
				Location: loc("Next"),
			})
		}
		if len(s.Choices) == 0 {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "MISSING_REQUIRED_FIELD",
				Message:  fmt.Sprintf("Choice state '%s' must have at least one rule", name),
				Location: loc("Choices"),
			})
		}
		for i, ch := range s.Choices {
			if ch.Next == "" {
				diags = append(diags, ValidationDiagnostic{
					Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
					Message:  fmt.Sprintf("Choice state '%s' rule[%d] is missing Next", name, i),
					Location: fmt.Sprintf("States.%s.Choices[%d].Next", name, i),
				})
			} else if _, ok := allStates[ch.Next]; !ok {
				diags = append(diags, ValidationDiagnostic{
					Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
					Message:  fmt.Sprintf("Choice state '%s' rule[%d] transitions to nonexistent state '%s'", name, i, ch.Next),
					Location: fmt.Sprintf("States.%s.Choices[%d].Next", name, i),
				})
			}
		}
		if s.Default != "" {
			if _, ok := allStates[s.Default]; !ok {
				diags = append(diags, ValidationDiagnostic{
					Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
					Message:  fmt.Sprintf("Choice state '%s' Default transitions to nonexistent state '%s'", name, s.Default),
					Location: loc("Default"),
				})
			}
		}

	case *WaitState:
		diags = append(diags, checkTransition(name, s.Next, s.End, allStates)...)
		// Exactly one of the four wait fields
		waitCount := 0
		if s.Seconds > 0 {
			waitCount++
		}
		if s.SecondsPath != "" {
			waitCount++
		}
		if s.Timestamp != "" {
			waitCount++
		}
		if s.TimestampPath != "" {
			waitCount++
		}
		if waitCount == 0 {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "MISSING_REQUIRED_FIELD",
				Message:  fmt.Sprintf("Wait state '%s' must specify one of Seconds/SecondsPath/Timestamp/TimestampPath", name),
				Location: fmt.Sprintf("States.%s", name),
			})
		} else if waitCount > 1 {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "INVALID_PROPERTY_VALUE",
				Message:  fmt.Sprintf("Wait state '%s' must specify only one of Seconds/SecondsPath/Timestamp/TimestampPath", name),
				Location: fmt.Sprintf("States.%s", name),
			})
		}

	case *SucceedState:
		// Succeed is terminal — must NOT have Next or End=true (redundant but allowed)
		if s.Next != "" {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "INVALID_PROPERTY_VALUE",
				Message:  fmt.Sprintf("Succeed state '%s' cannot have a Next field", name),
				Location: loc("Next"),
			})
		}

	case *FailState:
		if s.Next != "" {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "INVALID_PROPERTY_VALUE",
				Message:  fmt.Sprintf("Fail state '%s' cannot have a Next field", name),
				Location: loc("Next"),
			})
		}

	case *ParallelState:
		diags = append(diags, checkTransition(name, s.Next, s.End, allStates)...)
		if len(s.Branches) == 0 {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "MISSING_REQUIRED_FIELD",
				Message:  fmt.Sprintf("Parallel state '%s' must have at least one branch", name),
				Location: loc("Branches"),
			})
		}
		for i, branch := range s.Branches {
			for _, d := range Validate(&branch) {
				d.Location = fmt.Sprintf("States.%s.Branches[%d].%s", name, i, d.Location)
				diags = append(diags, d)
			}
		}
		for i, c := range s.Catch {
			if _, ok := allStates[c.Next]; !ok {
				diags = append(diags, ValidationDiagnostic{
					Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
					Message:  fmt.Sprintf("State '%s' Catch[%d] transitions to nonexistent state '%s'", name, i, c.Next),
					Location: fmt.Sprintf("States.%s.Catch[%d].Next", name, i),
				})
			}
		}

	case *MapState:
		diags = append(diags, checkTransition(name, s.Next, s.End, allStates)...)
		if s.ItemProcessor == nil && s.Iterator == nil {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "MISSING_REQUIRED_FIELD",
				Message:  fmt.Sprintf("Map state '%s' must have ItemProcessor or Iterator", name),
				Location: fmt.Sprintf("States.%s", name),
			})
		}
		if s.ItemProcessor != nil && s.Iterator != nil {
			diags = append(diags, ValidationDiagnostic{
				Severity: "WARNING", Code: "INVALID_PROPERTY_VALUE",
				Message:  fmt.Sprintf("Map state '%s' has both ItemProcessor and Iterator; ItemProcessor takes precedence", name),
				Location: fmt.Sprintf("States.%s", name),
			})
		}
		for i, c := range s.Catch {
			if _, ok := allStates[c.Next]; !ok {
				diags = append(diags, ValidationDiagnostic{
					Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
					Message:  fmt.Sprintf("State '%s' Catch[%d] transitions to nonexistent state '%s'", name, i, c.Next),
					Location: fmt.Sprintf("States.%s.Catch[%d].Next", name, i),
				})
			}
		}
	}

	return diags
}

// checkTransition verifies that a state has exactly one of End=true or a valid Next.
func checkTransition(name, next string, end bool, allStates map[string]StateDef) []ValidationDiagnostic {
	var diags []ValidationDiagnostic
	if !end && next == "" {
		diags = append(diags, ValidationDiagnostic{
			Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
			Message:  fmt.Sprintf("State '%s' must have either End=true or a Next transition", name),
			Location: fmt.Sprintf("States.%s", name),
		})
		return diags
	}
	if next != "" {
		if _, ok := allStates[next]; !ok {
			diags = append(diags, ValidationDiagnostic{
				Severity: "ERROR", Code: "MISSING_TRANSITION_TARGET",
				Message:  fmt.Sprintf("State '%s' transitions to nonexistent state '%s'", name, next),
				Location: fmt.Sprintf("States.%s.Next", name),
			})
		}
	}
	return diags
}

// computeReachable returns the set of state names reachable from StartAt.
func computeReachable(sm *StateMachineDefinition) map[string]bool {
	reachable := make(map[string]bool)
	var visit func(string)
	visit = func(name string) {
		if reachable[name] {
			return
		}
		reachable[name] = true
		state, ok := sm.States[name]
		if !ok {
			return
		}
		for _, next := range stateOutgoing(state) {
			visit(next)
		}
	}
	visit(sm.StartAt)
	return reachable
}

// stateOutgoing returns all state names that can be transitioned to from s.
func stateOutgoing(s StateDef) []string {
	var out []string
	if next := s.GetNext(); next != "" {
		out = append(out, next)
	}
	switch st := s.(type) {
	case *ChoiceState:
		for _, ch := range st.Choices {
			if ch.Next != "" {
				out = append(out, ch.Next)
			}
		}
		if st.Default != "" {
			out = append(out, st.Default)
		}
	case *TaskState:
		for _, c := range st.Catch {
			out = append(out, c.Next)
		}
	case *ParallelState:
		for _, c := range st.Catch {
			out = append(out, c.Next)
		}
	case *MapState:
		for _, c := range st.Catch {
			out = append(out, c.Next)
		}
	}
	return out
}

// hasTerminalPath returns true if any path from StartAt leads to a terminal state.
func hasTerminalPath(sm *StateMachineDefinition) bool {
	for _, state := range sm.States {
		switch s := state.(type) {
		case *SucceedState:
			return true
		case *FailState:
			return true
		case *PassState:
			if s.End {
				return true
			}
		case *TaskState:
			if s.End {
				return true
			}
		case *WaitState:
			if s.End {
				return true
			}
		case *ParallelState:
			if s.End {
				return true
			}
		case *MapState:
			if s.End {
				return true
			}
		}
	}
	return false
}
