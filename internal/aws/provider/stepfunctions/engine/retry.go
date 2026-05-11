package engine

import (
	"context"
	"math"
	"math/rand"
	"time"

	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

// executeWithRetry runs fn, retrying on matching errors per the retry rules.
func (e *ExecutionEngine) executeWithRetry(
	ctx context.Context,
	rules []asl.RetryRule,
	fn func() (any, error),
) (any, error) {
	attempts := make([]int, len(rules)) // per-rule attempt counter

	for {
		result, err := fn()
		if err == nil {
			return result, nil
		}

		ruleIdx := matchRetryRule(rules, err)
		if ruleIdx < 0 {
			return nil, err
		}

		rule := rules[ruleIdx]
		attempts[ruleIdx]++

		maxAttempts := rule.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 3 // AWS default
		}
		if attempts[ruleIdx] >= maxAttempts {
			return nil, err
		}

		delay := computeBackoff(rule, attempts[ruleIdx])

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

func matchRetryRule(rules []asl.RetryRule, err error) int {
	errName := errorName(err)
	for i, rule := range rules {
		for _, e := range rule.ErrorEquals {
			if e == "States.ALL" || e == errName {
				return i
			}
		}
	}
	return -1
}

func computeBackoff(rule asl.RetryRule, attempt int) time.Duration {
	interval := rule.IntervalSeconds
	if interval <= 0 {
		interval = 1
	}
	rate := rule.BackoffRate
	if rate <= 0 {
		rate = 2.0
	}

	delay := time.Duration(float64(time.Duration(interval)*time.Second) * math.Pow(rate, float64(attempt-1)))

	if rule.MaxDelaySeconds > 0 {
		maxDelay := time.Duration(rule.MaxDelaySeconds) * time.Second
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	if rule.JitterStrategy == "FULL" {
		delay = time.Duration(rand.Int63n(int64(delay) + 1))
	}

	return delay
}
