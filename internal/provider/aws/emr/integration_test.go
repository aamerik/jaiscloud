//go:build integration

package emr

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/store"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func makeManagedFailedJob(name, ns string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "jaiscloud",
			},
		},
		Status: batchv1.JobStatus{
			Failed: 1,
			Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Message: "BackoffLimitExceeded",
			}},
		},
	}
}

func makeSuspendedJob(name, ns string) *batchv1.Job {
	suspended := true
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "jaiscloud",
			},
		},
		Spec: batchv1.JobSpec{
			Suspend: &suspended,
		},
	}
}

func makeOOMPod(name, ns, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"job-name": jobName},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "spark-submit",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   137,
						Reason:     "OOMKilled",
						StartedAt:  metav1.NewTime(time.Now().Add(-5 * time.Second)),
						FinishedAt: metav1.NewTime(time.Now()),
					},
				},
			}},
		},
	}
}

// memTerminalStore satisfies k8shelpers.TerminalStore using MemoryResourceStore.
type memTerminalStore struct {
	inner *store.MemoryResourceStore
}

func newMemTerminalStore() *memTerminalStore {
	return &memTerminalStore{inner: store.NewMemoryResourceStore()}
}

func (m *memTerminalStore) Create(ctx context.Context, entry store.ResourceEntry) error {
	return m.inner.Create(ctx, entry)
}

func (m *memTerminalStore) Get(ctx context.Context, resourceType, id string) (store.ResourceEntry, error) {
	return m.inner.Get(ctx, resourceType, id)
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestSparkJobOrphanCleanup: a pre-existing terminal (failed) Job is swept on
// startup. OnTerminalJob is invoked with state "FAILED" and the Job is deleted.
func TestSparkJobOrphanCleanup(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	job := makeManagedFailedJob("jc-spark-orphan-failed", "jaiscloud")
	if _, err := client.BatchV1().Jobs("jaiscloud").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	var callbackName, callbackState string
	cfg := k8shelpers.CleanupConfig{
		Namespace: "jaiscloud",
		OnTerminalJob: func(name, state, reason string) {
			callbackName = name
			callbackState = state
		},
	}

	if err := k8shelpers.CleanupOrphans(ctx, client, cfg); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}

	// Callback must have fired.
	if callbackName == "" {
		t.Fatal("expected OnTerminalJob callback to be called, but it was not")
	}
	if callbackState != "FAILED" {
		t.Errorf("expected state FAILED, got %q", callbackState)
	}

	// Job must have been deleted.
	_, err := client.BatchV1().Jobs("jaiscloud").Get(ctx, job.Name, metav1.GetOptions{})
	if err == nil {
		t.Error("expected terminal job to be deleted after CleanupOrphans, but it still exists")
	}
}

// TestDriverOOMNoOrphan: a pod that exited with code 137 (OOMKilled) is
// correctly classified as non-successful with ExitCode 137.
func TestDriverOOMNoOrphan(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	pod := makeOOMPod("oom-driver-pod", "jaiscloud", "jc-spark-oom-job")
	if _, err := client.CoreV1().Pods("jaiscloud").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	// Use a fake watcher so WaitTerminal doesn't block on polling ticks.
	fw := watch.NewFake()
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		return true, fw, nil
	})

	go func() {
		time.Sleep(10 * time.Millisecond)
		fw.Add(pod)
	}()

	handle := k8shelpers.JobHandle{
		JobID:     "jc-spark-oom-job",
		Namespace: "jaiscloud",
		JobName:   "jc-spark-oom-job",
		PodName:   pod.Name,
	}

	final, err := k8shelpers.WaitTerminal(ctx, client, handle)
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	if final.Succeeded {
		t.Error("expected Succeeded=false for OOMKilled pod")
	}
	if final.ExitCode != 137 {
		t.Errorf("expected ExitCode=137, got %d", final.ExitCode)
	}
}

// TestJaisCloudRestartReapsOrphans: a suspended Job is unsuspended on restart
// (re-adopted) and OnTerminalJob is NOT called for it.
func TestJaisCloudRestartReapsOrphans(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	job := makeSuspendedJob("jc-spark-suspended", "jaiscloud")
	if _, err := client.BatchV1().Jobs("jaiscloud").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create suspended job: %v", err)
	}

	terminalCalled := false
	cfg := k8shelpers.CleanupConfig{
		Namespace: "jaiscloud",
		OnTerminalJob: func(name, state, reason string) {
			terminalCalled = true
		},
	}

	if err := k8shelpers.CleanupOrphans(ctx, client, cfg); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}

	// OnTerminalJob must NOT have been called — suspended is not terminal.
	if terminalCalled {
		t.Error("OnTerminalJob must not be called for a suspended (non-terminal) Job")
	}

	// The Job must have been unsuspended (spec.suspend == false or nil).
	updated, err := client.BatchV1().Jobs("jaiscloud").Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job after cleanup: %v", err)
	}
	if updated.Spec.Suspend != nil && *updated.Spec.Suspend {
		t.Error("expected job to be unsuspended after CleanupOrphans adopted it")
	}
}

// TestDescribeStepAfterTTL: a terminal snapshot persisted to the store can be
// loaded back, simulating describe-step queries after the K8s Job is GC'd.
func TestDescribeStepAfterTTL(t *testing.T) {
	s := newMemTerminalStore()
	ctx := context.Background()

	snap := k8shelpers.Snapshot{
		State:     "COMPLETED",
		Reason:    "finished",
		StartTime: time.Now().Add(-30 * time.Second),
		EndTime:   time.Now(),
		ExitCode:  0,
	}

	const prefix = "emr/steps"
	const jobID = "step-ttl-001"

	if err := k8shelpers.PersistTerminalSnapshot(ctx, s, prefix, jobID, snap); err != nil {
		t.Fatalf("PersistTerminalSnapshot: %v", err)
	}

	loaded, found, err := k8shelpers.LoadTerminalSnapshot(ctx, s, prefix, jobID)
	if err != nil {
		t.Fatalf("LoadTerminalSnapshot: %v", err)
	}
	if !found {
		t.Fatal("expected found=true after persisting snapshot")
	}
	if loaded.State != "COMPLETED" {
		t.Errorf("expected State=COMPLETED, got %q", loaded.State)
	}
}
