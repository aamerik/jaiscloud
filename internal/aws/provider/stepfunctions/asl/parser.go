package asl

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Parse parses an ASL definition string into a StateMachineDefinition.
func Parse(definition string) (*StateMachineDefinition, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(definition), &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	sm := &StateMachineDefinition{
		States: make(map[string]StateDef),
	}

	if c, ok := raw["Comment"]; ok {
		_ = json.Unmarshal(c, &sm.Comment)
	}
	if s, ok := raw["StartAt"]; ok {
		_ = json.Unmarshal(s, &sm.StartAt)
	} else {
		return nil, errors.New("StartAt is required")
	}
	if sm.StartAt == "" {
		return nil, errors.New("StartAt must be non-empty")
	}
	if v, ok := raw["Version"]; ok {
		_ = json.Unmarshal(v, &sm.Version)
	}
	if t, ok := raw["TimeoutSeconds"]; ok {
		_ = json.Unmarshal(t, &sm.TimeoutSeconds)
	}
	if q, ok := raw["QueryLanguage"]; ok {
		_ = json.Unmarshal(q, &sm.QueryLanguage)
	}

	statesRaw, ok := raw["States"]
	if !ok {
		return nil, errors.New("States is required")
	}
	var statesMap map[string]json.RawMessage
	if err := json.Unmarshal(statesRaw, &statesMap); err != nil {
		return nil, fmt.Errorf("invalid States: %w", err)
	}
	if len(statesMap) == 0 {
		return nil, errors.New("States must contain at least one state")
	}

	for name, sRaw := range statesMap {
		state, err := parseState(sRaw)
		if err != nil {
			return nil, fmt.Errorf("state %q: %w", name, err)
		}
		sm.States[name] = state
	}

	return sm, nil
}

// parseState dispatches on the Type field to return the concrete state struct.
func parseState(raw json.RawMessage) (StateDef, error) {
	var typeProbe struct {
		Type string `json:"Type"`
	}
	if err := json.Unmarshal(raw, &typeProbe); err != nil {
		return nil, err
	}
	if typeProbe.Type == "" {
		return nil, errors.New("Type is required")
	}

	switch StateType(typeProbe.Type) {
	case StateTypePass:
		var s PassState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeTask:
		var s TaskState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeChoice:
		var s ChoiceState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeWait:
		var s WaitState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeSucceed:
		var s SucceedState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeFail:
		var s FailState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeParallel:
		var s ParallelState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		// Parse each branch's States field
		if err := parseParallelBranches(&s, raw); err != nil {
			return nil, err
		}
		return &s, nil
	case StateTypeMap:
		var s MapState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		// Parse ItemProcessor/Iterator sub-states
		if err := parseMapSubStates(&s, raw); err != nil {
			return nil, err
		}
		return &s, nil
	default:
		return nil, fmt.Errorf("unknown state type: %s", typeProbe.Type)
	}
}

// parseParallelBranches re-parses the Branches array to populate StateDef maps.
func parseParallelBranches(s *ParallelState, raw json.RawMessage) error {
	var probe struct {
		Branches []json.RawMessage `json:"Branches"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return err
	}
	s.Branches = make([]StateMachineDefinition, 0, len(probe.Branches))
	for i, brRaw := range probe.Branches {
		branch, err := parseBranchDefinition(brRaw)
		if err != nil {
			return fmt.Errorf("branch[%d]: %w", i, err)
		}
		s.Branches = append(s.Branches, *branch)
	}
	return nil
}

// parseMapSubStates parses ItemProcessor or Iterator state defs.
func parseMapSubStates(s *MapState, raw json.RawMessage) error {
	var probe struct {
		ItemProcessor *json.RawMessage `json:"ItemProcessor"`
		Iterator      *json.RawMessage `json:"Iterator"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return err
	}

	if probe.ItemProcessor != nil {
		ip, err := parseItemProcessor(*probe.ItemProcessor)
		if err != nil {
			return fmt.Errorf("ItemProcessor: %w", err)
		}
		s.ItemProcessor = ip
	}
	if probe.Iterator != nil {
		iter, err := parseBranchDefinition(*probe.Iterator)
		if err != nil {
			return fmt.Errorf("Iterator: %w", err)
		}
		s.Iterator = iter
	}
	return nil
}

// parseBranchDefinition parses a branch (same shape as top-level minus TimeoutSeconds).
func parseBranchDefinition(raw json.RawMessage) (*StateMachineDefinition, error) {
	var outer struct {
		Comment string                     `json:"Comment"`
		StartAt string                     `json:"StartAt"`
		States  map[string]json.RawMessage `json:"States"`
		Version string                     `json:"Version"`
	}
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, err
	}
	if outer.StartAt == "" {
		return nil, errors.New("branch StartAt is required")
	}

	sm := &StateMachineDefinition{
		Comment: outer.Comment,
		StartAt: outer.StartAt,
		Version: outer.Version,
		States:  make(map[string]StateDef, len(outer.States)),
	}
	for name, sRaw := range outer.States {
		state, err := parseState(sRaw)
		if err != nil {
			return nil, fmt.Errorf("state %q: %w", name, err)
		}
		sm.States[name] = state
	}
	return sm, nil
}

// parseItemProcessor parses an ItemProcessor (has ProcessorConfig + StartAt + States).
func parseItemProcessor(raw json.RawMessage) (*ItemProcessor, error) {
	var outer struct {
		ProcessorConfig *ProcessorConfig           `json:"ProcessorConfig"`
		StartAt         string                     `json:"StartAt"`
		States          map[string]json.RawMessage `json:"States"`
	}
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, err
	}
	if outer.StartAt == "" {
		return nil, errors.New("ItemProcessor StartAt is required")
	}

	ip := &ItemProcessor{
		ProcessorConfig: outer.ProcessorConfig,
		StartAt:         outer.StartAt,
		States:          make(map[string]StateDef, len(outer.States)),
	}
	for name, sRaw := range outer.States {
		state, err := parseState(sRaw)
		if err != nil {
			return nil, fmt.Errorf("state %q: %w", name, err)
		}
		ip.States[name] = state
	}
	return ip, nil
}
