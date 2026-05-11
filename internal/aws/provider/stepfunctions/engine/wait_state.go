package engine

import (
	"context"
	"fmt"
	"time"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

func (e *ExecutionEngine) evalWait(ctx context.Context, state *asl.WaitState, input any) (any, error) {
	effective := applyInputPath2(input, state.InputPath)

	var waitDuration time.Duration
	switch {
	case state.Seconds > 0:
		waitDuration = time.Duration(state.Seconds) * time.Second
	case state.SecondsPath != "":
		v, err := asl.EvalPath(effective, state.SecondsPath)
		if err != nil {
			return nil, err
		}
		secs, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("States.Runtime: SecondsPath %q did not resolve to a number", state.SecondsPath)
		}
		waitDuration = time.Duration(secs * float64(time.Second))
	case state.Timestamp != "":
		t, err := time.Parse(time.RFC3339, state.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("States.Runtime: invalid Timestamp %q: %w", state.Timestamp, err)
		}
		waitDuration = time.Until(t)
	case state.TimestampPath != "":
		v, err := asl.EvalPath(effective, state.TimestampPath)
		if err != nil {
			return nil, err
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("States.Runtime: TimestampPath did not resolve to a string")
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, fmt.Errorf("States.Runtime: invalid timestamp %q: %w", s, err)
		}
		waitDuration = time.Until(t)
	}

	if waitDuration < 0 {
		waitDuration = 0
	}

	timer := time.NewTimer(waitDuration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return applyOutputPath2(effective, state.OutputPath), nil
	case <-ctx.Done():
		return nil, &sfnError{code: "States.Timeout", cause: "execution cancelled during Wait state"}
	}
}
