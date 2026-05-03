package k8shelpers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

const (
	defaultPollInterval = 5 * time.Second
	maxPollInterval     = 30 * time.Second
)

// WaitTerminal polls the Job's single pod until it reaches a terminal
// container state (Succeeded, Failed) or the context is cancelled.
// Returns pod-level Final; workload-specific success classification
// is the caller's responsibility (see sparkhelpers.WaitTerminal).
func WaitTerminal(ctx context.Context, k8s kubernetes.Interface, handle JobHandle) (Final, error) {
	// Try watch first; fall back to polling on any watch error.
	result, err := waitByWatch(ctx, k8s, handle)
	if err == nil {
		return result, nil
	}
	return waitByPolling(ctx, k8s, handle)
}

func waitByWatch(ctx context.Context, k8s kubernetes.Interface, handle JobHandle) (Final, error) {
	watcher, err := k8s.CoreV1().Pods(handle.Namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", handle.JobName),
		Watch:         true,
	})
	if err != nil {
		return Final{}, err
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return Final{}, ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return Final{}, fmt.Errorf("watch channel closed")
			}
			if event.Type == watch.Error {
				return Final{}, fmt.Errorf("watch error: %v", event.Object)
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if f, done := podToFinal(pod); done {
				return f, nil
			}
		}
	}
}

func waitByPolling(ctx context.Context, k8s kubernetes.Interface, handle JobHandle) (Final, error) {
	interval := defaultPollInterval
	for {
		select {
		case <-ctx.Done():
			return Final{}, ctx.Err()
		case <-time.After(interval):
		}

		pods, err := k8s.CoreV1().Pods(handle.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", handle.JobName),
		})
		if err != nil {
			interval = min(interval*2, maxPollInterval)
			continue
		}
		for i := range pods.Items {
			if f, done := podToFinal(&pods.Items[i]); done {
				return f, nil
			}
		}
		interval = min(interval*2, maxPollInterval)
	}
}

// podToFinal extracts Final from a pod if it is in a terminal state.
// Returns (Final{}, false) when the pod is still running.
func podToFinal(pod *corev1.Pod) (Final, bool) {
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		f := Final{Succeeded: true}
		fillTimes(&f, pod)
		return f, true
	case corev1.PodFailed:
		f := Final{Succeeded: false}
		fillTimes(&f, pod)
		fillTermination(&f, pod)
		return f, true
	}

	// Check all container statuses for terminal state (handles Unknown phase).
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			t := cs.State.Terminated
			f := Final{
				Succeeded: t.ExitCode == 0,
				ExitCode:  t.ExitCode,
				Reason:    t.Reason,
				Message:   truncate(t.Message, 256),
			}
			if !t.StartedAt.IsZero() {
				f.StartTime = t.StartedAt.Time
			}
			if !t.FinishedAt.IsZero() {
				f.EndTime = t.FinishedAt.Time
			}
			return f, true
		}
	}
	return Final{}, false
}

func fillTimes(f *Final, pod *corev1.Pod) {
	if pod.Status.StartTime != nil {
		f.StartTime = pod.Status.StartTime.Time
	}
	f.EndTime = time.Now()
}

func fillTermination(f *Final, pod *corev1.Pod) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			t := cs.State.Terminated
			f.ExitCode = t.ExitCode
			f.Reason = t.Reason
			f.Message = truncate(t.Message, 256)
			if !t.StartedAt.IsZero() {
				f.StartTime = t.StartedAt.Time
			}
			if !t.FinishedAt.IsZero() {
				f.EndTime = t.FinishedAt.Time
			}
			return
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
