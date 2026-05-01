package emr

import (
	"testing"

	"jaiscloud/internal/executor/spark"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newMinimalEMRProvider builds a bare-minimum EMRProvider for OnStateChange tests.
// resources must be non-nil; executor may be nil (instant completion mode).
func newMinimalEMRProvider(executor spark.SparkExecutor) *EMRProvider {
	p := &EMRProvider{
		resources: store.NewMemoryResourceStore(),
		executor:  executor,
	}
	return p
}

// TestEMRProvider_OnStateChange_NonTerminal_KeepsJobRef verifies that a
// non-terminal state change does not remove the job from jobRefs.
func TestEMRProvider_OnStateChange_NonTerminal_KeepsJobRef(t *testing.T) {
	p := newMinimalEMRProvider(nil)
	p.jobRefs.Store("job-1", jobRef{clusterID: "c1", resourceID: "s1", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "job-1",
		OldState: spark.StatePending,
		NewState: spark.StateRunning,
	})

	if _, ok := p.jobRefs.Load("job-1"); !ok {
		t.Error("jobRefs entry must remain for a non-terminal state change")
	}
}

// TestEMRProvider_OnStateChange_Terminal_RemovesJobRef verifies that a terminal
// state change removes the job from jobRefs.
func TestEMRProvider_OnStateChange_Terminal_RemovesJobRef(t *testing.T) {
	p := newMinimalEMRProvider(nil)
	p.jobRefs.Store("job-2", jobRef{clusterID: "c2", resourceID: "s2", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "job-2",
		NewState: spark.StateCompleted,
	})

	if _, ok := p.jobRefs.Load("job-2"); ok {
		t.Error("jobRefs entry must be removed on terminal state")
	}
}

// TestEMRProvider_OnStateChange_UnknownJob_NoOp verifies no panic for a job
// that is not tracked in jobRefs.
func TestEMRProvider_OnStateChange_UnknownJob_NoOp(t *testing.T) {
	p := newMinimalEMRProvider(nil)

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "unknown",
		NewState: spark.StateCompleted,
	})
}

// TestEMRProvider_OnStateChange_Terminal_K8sExecutor verifies that when the
// executor is a *K8sExecutor, jobRefs is cleaned up on terminal state.
func TestEMRProvider_OnStateChange_Terminal_K8sExecutor(t *testing.T) {
	cfg := spark.SparkConfigFrom("k8s", spark.SizeSmall)
	cfg.Cloud = model.CloudAWS
	k8sExec := spark.NewK8sExecutor(cfg, nil)

	p := newMinimalEMRProvider(k8sExec)
	p.jobRefs.Store("job-k8s", jobRef{clusterID: "ck8s", resourceID: "sk8s", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "job-k8s",
		NewState: spark.StateFailed,
	})

	if _, ok := p.jobRefs.Load("job-k8s"); ok {
		t.Error("jobRefs entry must be removed after terminal OnStateChange with K8sExecutor")
	}
}

// TestEMRProvider_OnStateChange_Terminal_MockExecutor verifies that jobRefs is
// cleaned up on terminal state when using MockExecutor.
func TestEMRProvider_OnStateChange_Terminal_MockExecutor(t *testing.T) {
	mockExec := spark.NewMockExecutor()
	p := newMinimalEMRProvider(mockExec)
	p.jobRefs.Store("job-mock", jobRef{clusterID: "cmock", resourceID: "smock", cloud: model.CloudAWS})

	p.OnStateChange(spark.StateChangeEvent{
		JobID:    "job-mock",
		NewState: spark.StateCompleted,
	})

	if _, ok := p.jobRefs.Load("job-mock"); ok {
		t.Error("jobRefs entry must be removed even with MockExecutor")
	}
}
