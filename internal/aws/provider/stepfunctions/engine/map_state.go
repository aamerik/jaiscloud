package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

func (e *ExecutionEngine) evalMapWithRetry(ctx context.Context, state *asl.MapState, input any) (any, error) {
	return e.executeWithRetry(ctx, state.Retry, func() (any, error) {
		return e.evalMap(ctx, state, input)
	})
}

func (e *ExecutionEngine) evalMap(ctx context.Context, state *asl.MapState, input any) (any, error) {
	effective := applyInputPath2(input, state.InputPath)

	// Extract items from ItemsPath (default: $)
	itemsPath := state.ItemsPath
	if itemsPath == "" {
		itemsPath = "$"
	}
	rawItems, err := asl.EvalPath(effective, itemsPath)
	if err != nil {
		return nil, err
	}
	items, ok := rawItems.([]any)
	if !ok {
		return nil, errors.New("States.Runtime: ItemsPath did not resolve to an array")
	}

	// Determine processor
	processor := state.ItemProcessor
	if processor == nil && state.Iterator != nil {
		processor = &asl.ItemProcessor{
			StartAt: state.Iterator.StartAt,
			States:  state.Iterator.States,
		}
	}
	if processor == nil {
		return nil, errors.New("States.Runtime: Map state has no ItemProcessor or Iterator")
	}

	// MaxConcurrency
	maxConc := state.MaxConcurrency
	if maxConc <= 0 {
		maxConc = len(items)
		if maxConc == 0 {
			// Empty array → empty result
			return e.applyMapResultPath(effective, state, []any{})
		}
	}
	if state.MaxConcurrencyPath != "" {
		v, _ := asl.EvalPath(effective, state.MaxConcurrencyPath)
		if n, ok2 := toNumber(v); ok2 && n > 0 {
			maxConc = int(n)
		}
	}

	sem := make(chan struct{}, maxConc)
	results := make([]any, len(items))
	errs := make([]error, len(items))
	var wg sync.WaitGroup
	mapCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, item := range items {
		wg.Add(1)
		go func(idx int, it any) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Apply ItemSelector (new style) or Parameters (legacy)
			iterInput := it
			var selectorRaw []byte
			if len(state.ItemSelector) > 0 {
				selectorRaw = state.ItemSelector
			} else if len(state.Parameters) > 0 {
				selectorRaw = state.Parameters
			}

			if len(selectorRaw) > 0 {
				ctxObj := map[string]any{
					"Map": map[string]any{
						"Item": map[string]any{
							"Index": float64(idx),
							"Value": it,
						},
					},
				}
				var sel any
				if err := json.Unmarshal(selectorRaw, &sel); err == nil {
					v, err := asl.EvalParameters(sel, it, ctxObj)
					if err == nil {
						iterInput = v
					}
				}
			}

			// Run iteration as a sub-state-machine
			r, err := e.runBranch(mapCtx, &asl.StateMachineDefinition{
				StartAt: processor.StartAt,
				States:  processor.States,
			}, iterInput)
			results[idx] = r
			errs[idx] = err
		}(i, item)
	}
	wg.Wait()

	// Check tolerated failure threshold
	failureCount := 0
	for _, err := range errs {
		if err != nil {
			failureCount++
		}
	}

	if failureCount > 0 {
		tolerated := false
		if state.ToleratedFailureCount > 0 && failureCount <= state.ToleratedFailureCount {
			tolerated = true
		}
		if state.ToleratedFailurePercentage > 0 {
			pct := float64(failureCount) / float64(len(items)) * 100
			if pct <= state.ToleratedFailurePercentage {
				tolerated = true
			}
		}
		if !tolerated {
			for _, err := range errs {
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return e.applyMapResultPath(effective, state, results)
}

func (e *ExecutionEngine) applyMapResultPath(effective any, state *asl.MapState, results []any) (any, error) {
	var result any = results

	if len(state.ResultSelector) > 0 {
		var rs any
		if err := json.Unmarshal(state.ResultSelector, &rs); err != nil {
			return nil, err
		}
		v, err := asl.EvalParameters(rs, results, nil)
		if err != nil {
			return nil, err
		}
		result = v
	}

	output, err := applyResultPath(effective, state.ResultPath, result)
	if err != nil {
		return nil, err
	}
	return applyOutputPath2(output, state.OutputPath), nil
}
