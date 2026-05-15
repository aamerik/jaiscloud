package emr

import (
	"context"
	"fmt"
	"strings"
)

// runStep dispatches a single step to the appropriate runner.
// Called in a goroutine from AddJobFlowSteps.
func (p *EMRProvider) runStep(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	jar, _, _ := extractHadoopJarStep(stepCfg)
	argv := extractStepArgv(stepCfg)

	switch {
	case isCommandRunnerJar(jar):
		p.runCommandRunnerStep(ctx, h, clusterID, stepID, stepCfg)
	case isSparkSubmitStep(argv):
		p.runSparkSubmitStep(ctx, h, clusterID, stepID, stepCfg)
	default:
		p.runGenericStepStub(ctx, h, clusterID, stepID, stepCfg)
	}
}

// isCommandRunnerJar returns true when the step uses command-runner.jar, which
// is the EMR mechanism for running shell commands (aws-cli, s3-dist-cp, etc.).
func isCommandRunnerJar(jar string) bool {
	return jar == "command-runner.jar"
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

// runCommandRunnerStep handles command-runner.jar steps.
// In mock mode (no k8s client): marks the step COMPLETED immediately.
// In k8s/docker mode: the step would run args[0] in a busybox container;
// that is deferred — for now it also completes instantly in all executor modes.
func (p *EMRProvider) runCommandRunnerStep(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	sink := p.LogSinkForStep(clusterID, stepID, "")
	p.emitStepStateChange(h, clusterID, stepID, "RUNNING", "")
	// Mock mode: instant completion.
	// TODO(future k8s): run HadoopJarStep.Args[0] in a busybox pod.
	fmt.Fprintf(sink, "Step %s (command-runner.jar): COMPLETED\n", stepID)
	p.emitStepStateChange(h, clusterID, stepID, "COMPLETED", "")
	p.flushStepLogs(ctx, clusterID, stepID, sink)
}

// runGenericStepStub transitions non-Spark, non-command-runner steps to RUNNING
// then FAILED, since JaisCloud does not yet support arbitrary Hadoop steps.
func (p *EMRProvider) runGenericStepStub(ctx context.Context, h handlerCtx, clusterID, stepID string, stepCfg map[string]any) {
	actionOnFailure, _ := stepCfg["ActionOnFailure"].(string)
	if actionOnFailure == "" {
		actionOnFailure = "CONTINUE"
	}
	sink := p.LogSinkForStep(clusterID, stepID, "")
	p.emitStepStateChange(h, clusterID, stepID, "RUNNING", "")
	fmt.Fprintf(sink, "Step %s: FAILED (non-Spark step types deferred)\n", stepID)
	p.emitStepStateChange(h, clusterID, stepID, "FAILED", "non-Spark step types deferred to a future phase")
	p.flushStepLogs(ctx, clusterID, stepID, sink)
	p.cascadeOnStepFailure(ctx, h, clusterID, stepID, actionOnFailure)
}
