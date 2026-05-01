package emroneks

import (
	"testing"

	"jaiscloud/internal/executor/spark"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newMinimalEMRCProvider builds a bare-minimum EMRContainersProvider for
// OnStateChange tests.
func newMinimalEMRCProvider(executor spark.SparkExecutor) *EMRContainersProvider {
	return &EMRContainersProvider{
		resources: store.NewMemoryResourceStore(),
		executor:  executor,
	}
}

// TestEMRContainersProvider_OnStateChange_NonTerminal_KeepsJobRef verifies that
// a non-terminal state change does not remove the job from jobRefs.
func TestEMRContainersProvider_OnStateChange_NonTerminal_KeepsJobRef(t *testing.T) {
	p := newMinimalEMRCProvider(nil)
	p.jobRefs.Store("jr-1", jobRef{vcID: "vc1", jrID: "jr1", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "jr-1",
		OldState: spark.StatePending,
		NewState: spark.StateRunning,
	})

	if _, ok := p.jobRefs.Load("jr-1"); !ok {
		t.Error("jobRefs entry must remain for a non-terminal state change")
	}
}

// TestEMRContainersProvider_OnStateChange_Terminal_RemovesJobRef verifies that a
// terminal state change removes the job from jobRefs.
func TestEMRContainersProvider_OnStateChange_Terminal_RemovesJobRef(t *testing.T) {
	p := newMinimalEMRCProvider(nil)
	p.jobRefs.Store("jr-2", jobRef{vcID: "vc2", jrID: "jr2", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "jr-2",
		NewState: spark.StateCompleted,
	})

	if _, ok := p.jobRefs.Load("jr-2"); ok {
		t.Error("jobRefs entry must be removed on terminal state")
	}
}

// TestEMRContainersProvider_OnStateChange_UnknownJob_NoOp verifies no panic for
// a job not tracked in jobRefs.
func TestEMRContainersProvider_OnStateChange_UnknownJob_NoOp(t *testing.T) {
	p := newMinimalEMRCProvider(nil)

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "unknown",
		NewState: spark.StateCompleted,
	})
}

// TestEMRContainersProvider_OnStateChange_Terminal_K8sExecutor verifies that
// jobRefs is cleaned up on terminal state when using K8sExecutor.
func TestEMRContainersProvider_OnStateChange_Terminal_K8sExecutor(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	cfg.Cloud = model.CloudAWS
	k8sExec := spark.NewK8sExecutor(cfg, nil)

	p := newMinimalEMRCProvider(k8sExec)
	p.jobRefs.Store("jr-k8s", jobRef{vcID: "vck8s", jrID: "jrk8s", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "jr-k8s",
		NewState: spark.StateFailed,
	})

	if _, ok := p.jobRefs.Load("jr-k8s"); ok {
		t.Error("jobRefs entry must be removed after terminal OnStateChange with K8sExecutor")
	}
}

// TestEMRContainersProvider_OnStateChange_Terminal_MockExecutor verifies that
// jobRefs is cleaned up on terminal state when using MockExecutor.
func TestEMRContainersProvider_OnStateChange_Terminal_MockExecutor(t *testing.T) {
	mockExec := spark.NewMockExecutor()
	p := newMinimalEMRCProvider(mockExec)
	p.jobRefs.Store("jr-mock", jobRef{vcID: "vcmock", jrID: "jrmock", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "jr-mock",
		NewState: spark.StateCompleted,
	})

	if _, ok := p.jobRefs.Load("jr-mock"); ok {
		t.Error("jobRefs entry must be removed even with MockExecutor")
	}
}
