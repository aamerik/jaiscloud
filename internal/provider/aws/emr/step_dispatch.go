package emr

import (
	"context"
	"strings"
)

// runStep dispatches a single step to the appropriate runner.
// Called in a goroutine from AddJobFlowSteps.
func (p *EMRProvider) runStep(ctx context.Context, clusterID, stepID string, stepCfg map[string]any) {
	argv := extractStepArgv(stepCfg)
	if isSparkSubmitStep(argv) {
		p.runSparkSubmitStep(ctx, clusterID, stepID, stepCfg)
	} else {
		p.runGenericStepStub(ctx, clusterID, stepID, stepCfg)
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
func (p *EMRProvider) runGenericStepStub(ctx context.Context, clusterID, stepID string, _ map[string]any) {
	p.emitStepStateChange(clusterID, stepID, "RUNNING", "")
	p.emitStepStateChange(clusterID, stepID, "FAILED", "non-Spark step types deferred to Phase 2")
}
