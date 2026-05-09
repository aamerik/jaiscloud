package emr

import (
	"context"
	"strings"
)

// runStep dispatches a single step to the appropriate runner.
// Called in a goroutine from AddJobFlowSteps.
func (p *EMRProvider) runStep(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	argv := extractStepArgv(stepCfg)
	if isSparkSubmitStep(argv) {
		p.runSparkSubmitStep(ctx, h, clusterID, stepID, stepCfg)
	} else {
		p.runGenericStepStub(ctx, h, clusterID, stepID, stepCfg)
	}
}

// isSparkSubmitStep returns true when the step's first arg is spark-submit
// (bare name or full path).
func isSparkSubmitStep(argv []string) bool {
	if len(argv) == 0 {
		return false
	}
	base := argv[0]
	// Accept "spark-submit" or any path ending in "/spark-submit".
	return base == "spark-submit" || strings.HasSuffix(base, "/spark-submit")
}

// runGenericStepStub transitions non-Spark steps to RUNNING then FAILED,
// since JaisCloud does not yet support arbitrary Hadoop steps.
func (p *EMRProvider) runGenericStepStub(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	actionOnFailure, _ := stepCfg["ActionOnFailure"].(string)
	if actionOnFailure == "" {
		actionOnFailure = "CONTINUE"
	}
	p.emitStepStateChange(h, clusterID, stepID, "RUNNING", "")
	p.emitStepStateChange(h, clusterID, stepID, "FAILED", "non-Spark step types deferred to a future phase")
	p.cascadeOnStepFailure(ctx, h, clusterID, stepID, actionOnFailure)
}
