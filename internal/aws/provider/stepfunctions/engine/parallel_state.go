package engine

import (
	"context"
	"encoding/json"
	"sync"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

func (e *ExecutionEngine) evalParallelWithRetry(ctx context.Context, state *asl.ParallelState, input any) (any, error) {
	return e.executeWithRetry(ctx, state.Retry, func() (any, error) {
		return e.evalParallel(ctx, state, input)
	})
}

func (e *ExecutionEngine) evalParallel(ctx context.Context, state *asl.ParallelState, input any) (any, error) {
	effective := applyInputPath2(input, state.InputPath)

	var branchInput any = effective
	if len(state.Parameters) > 0 {
		var params any
		if err := json.Unmarshal(state.Parameters, &params); err != nil {
			return nil, err
		}
		v, err := asl.EvalParameters(params, effective, nil)
		if err != nil {
			return nil, err
		}
		branchInput = v
	}

	results := make([]any, len(state.Branches))
	errs := make([]error, len(state.Branches))
	var wg sync.WaitGroup
	branchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := range state.Branches {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, err := e.runBranch(branchCtx, &state.Branches[idx], branchInput)
			results[idx] = r
			errs[idx] = err
			if err != nil {
				cancel()
			}
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	// Apply ResultSelector / ResultPath / OutputPath
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
