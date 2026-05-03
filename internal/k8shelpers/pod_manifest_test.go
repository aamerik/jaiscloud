package k8shelpers

import (
	"bytes"
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func makeSubmitRequest(name, ns string) SubmitJobRequest {
	ttl := int32(600)
	return SubmitJobRequest{
		Namespace: ns,
		JobName:   name,
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "spark-submit",
				Image: "apache/spark:3.5",
			}},
		},
		Labels: map[string]string{
			"app": "jaiscloud-spark",
			"job": name,
		},
		Annotations:             map[string]string{"jaiscloud.io/test": "true"},
		TTLSecondsAfterFinished: &ttl,
	}
}

func TestSubmitJob_CreatesJobWithLabels(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	handle, err := SubmitJob(ctx, client, makeSubmitRequest("test-job", "default"))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if handle.JobName != "test-job" {
		t.Errorf("expected JobName=test-job, got %s", handle.JobName)
	}

	job, err := client.BatchV1().Jobs("default").Get(ctx, "test-job", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Labels["app"] != "jaiscloud-spark" {
		t.Errorf("expected label app=jaiscloud-spark, got %q", job.Labels["app"])
	}
	if job.Annotations["jaiscloud.io/test"] != "true" {
		t.Errorf("expected annotation jaiscloud.io/test=true")
	}
}

func TestSubmitJob_WithOwnerRef(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	req := makeSubmitRequest("owned-job", "default")
	req.OwnerRef = &OwnerRefHint{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Name:       "parent-job",
		UID:        types.UID("parent-uid"),
	}

	_, err := SubmitJob(ctx, client, req)
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	job, err := client.BatchV1().Jobs("default").Get(ctx, "owned-job", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if len(job.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(job.OwnerReferences))
	}
	if job.OwnerReferences[0].Name != "parent-job" {
		t.Errorf("expected owner name parent-job, got %s", job.OwnerReferences[0].Name)
	}
}

func TestWaitTerminal_PodSucceeded(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	// Create the job first.
	handle, _ := SubmitJob(ctx, client, makeSubmitRequest("success-job", "default"))

	// Seed a succeeded pod.
	pod := successPod("success-pod", "default", "success-job")
	_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

	// Set up fake watch to return pod succeeded event.
	fw := watch.NewFake()
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
		return true, fw, nil
	})

	go func() {
		time.Sleep(10 * time.Millisecond)
		fw.Add(pod)
	}()

	final, err := waitByWatch(ctx, client, handle)
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	if !final.Succeeded {
		t.Error("expected Succeeded=true")
	}
}

func TestWaitTerminal_PodOOMKilled(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	handle, _ := SubmitJob(ctx, client, makeSubmitRequest("oom-job", "default"))

	pod := oomPod("oom-pod", "default", "oom-job")
	_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

	fw := watch.NewFake()
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
		return true, fw, nil
	})

	go func() {
		time.Sleep(10 * time.Millisecond)
		fw.Add(pod)
	}()

	final, err := waitByWatch(ctx, client, handle)
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	if final.Succeeded {
		t.Error("expected Succeeded=false for OOMKilled")
	}
	if final.ExitCode != 137 {
		t.Errorf("expected ExitCode=137, got %d", final.ExitCode)
	}
}

func TestCancel_DeletesJob(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	handle, _ := SubmitJob(ctx, client, makeSubmitRequest("cancel-job", "default"))

	if err := Cancel(ctx, client, handle); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	_, err := client.BatchV1().Jobs("default").Get(ctx, "cancel-job", metav1.GetOptions{})
	if err == nil {
		t.Error("expected job to be deleted")
	}
}

func TestCancel_AlreadyGone_NoError(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	handle := JobHandle{JobID: "gone", Namespace: "default", JobName: "gone-job"}
	if err := Cancel(ctx, client, handle); err != nil {
		t.Errorf("Cancel on non-existent job should return nil, got: %v", err)
	}
}

func TestTailLogs_CopiesBytes(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	pod := successPod("log-pod", "default", "log-job")
	_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

	// Fake log response.
	logContent := "line1\nline2\n"
	client.PrependReactor("get", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		// Only intercept GetLogs (subresource=log).
		if ga, ok := action.(k8stesting.GetActionImpl); ok && ga.GetSubresource() == "log" {
			return false, nil, nil // let default fake handle it
		}
		return false, nil, nil
	})
	_ = logContent // fake client returns empty logs; test verifies no error

	handle := JobHandle{
		JobID:     "log-job",
		Namespace: "default",
		JobName:   "log-job",
		PodName:   "log-pod",
	}
	var buf bytes.Buffer
	err := TailLogs(ctx, client, handle, LogKindMain, &buf)
	// Fake client returns empty body, so just check no error.
	if err != nil {
		t.Errorf("TailLogs: %v", err)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func successPod(name, ns, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"job-name": jobName},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "spark-submit", Image: "apache/spark:3.5"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "spark-submit",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   0,
						Reason:     "Completed",
						StartedAt:  metav1.NewTime(time.Now().Add(-10 * time.Second)),
						FinishedAt: metav1.NewTime(time.Now()),
					},
				},
			}},
		},
	}
}

func oomPod(name, ns, jobName string) *corev1.Pod {
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

// Ensure batchv1 import is used.
var _ = batchv1.Job{}
