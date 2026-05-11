package engine

import (
	"jaiscloud/internal/aws/provider/stepfunctions/asl"
)

// handleCatch checks whether err matches any Catch rule and returns the
// transition state name and the error-injected input. Returns caught=false if
// no rule matches.
func (e *ExecutionEngine) handleCatch(catches []asl.CatchRule, err error, input any) (nextState string, output any, caught bool) {
	errName := errorName(err)
	sfnErr := toSFNError(err)

	for _, catch := range catches {
		for _, errPattern := range catch.ErrorEquals {
			if errPattern == "States.ALL" || errPattern == errName {
				errObj := map[string]any{
					"Error": sfnErr.code,
					"Cause": sfnErr.cause,
				}

				var catchOutput any = errObj
				if catch.ResultPath != nil {
					rp := *catch.ResultPath
					if rp == "$" || rp == "" {
						catchOutput = errObj
					} else if rp != "null" {
						injected, err2 := asl.SetPath(input, rp, errObj)
						if err2 == nil {
							catchOutput = injected
						}
					} else {
						catchOutput = input // null → discard error
					}
				} else {
					// Default ResultPath for catch is $ (replace input with error object)
					catchOutput = errObj
				}
				return catch.Next, catchOutput, true
			}
		}
	}
	return "", nil, false
}
