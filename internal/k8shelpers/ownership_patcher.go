package k8shelpers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// PatcherConfig specifies which pods the ownership patcher watches.
type PatcherConfig struct {
	// LabelSelector is the k8s label-selector string applied to the Watch.
	// Typically "spark-role in (driver,executor)" for Spark callers.
	LabelSelector string

	// ResolveOwner returns the OwnerRef that should be attached to a pod
	// matching LabelSelector. Called once per observed pod. May return
	// (nil, nil) to skip the pod.
	ResolveOwner func(pod *corev1.Pod) (*OwnerRefHint, error)

	// Namespace to watch.
	Namespace string
}

// StartOwnershipPatcher launches a controller goroutine that watches pods
// matching cfg.LabelSelector. When a pod is created with missing/empty
// OwnerReferences, the patcher calls cfg.ResolveOwner and issues a JSON-patch
// to backfill ownerReferences.
//
// On startup (before returning) the patcher runs a reconcile sweep: LIST all
// matching pods and patch any that have empty OwnerReferences. This closes the
// gap for pods created during a JaisCloud crash.
//
// Returned cancel() stops the controller. If the caller context is cancelled
// the controller also stops.
func StartOwnershipPatcher(ctx context.Context, k8s kubernetes.Interface, cfg PatcherConfig) (cancel func(), err error) {
	// Startup reconcile sweep.
	if err := reconcileSweep(ctx, k8s, cfg); err != nil {
		slog.Warn("k8shelpers: ownership patcher startup sweep failed", "err", err)
		// Non-fatal — proceed.
	}

	patchCtx, patchCancel := context.WithCancel(ctx)

	go func() {
		backoff := time.Second
		const maxBackoff = 30 * time.Second
		for {
			if err := watchAndPatch(patchCtx, k8s, cfg); err != nil {
				if patchCtx.Err() != nil {
					return // cancelled
				}
				slog.Warn("k8shelpers: ownership patcher watch error, restarting", "err", err, "backoff", backoff)
				select {
				case <-patchCtx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			} else {
				backoff = time.Second // reset on clean exit
			}
		}
	}()

	return patchCancel, nil
}

// reconcileSweep lists all matching pods and patches those with empty OwnerReferences.
func reconcileSweep(ctx context.Context, k8s kubernetes.Interface, cfg PatcherConfig) error {
	pods, err := k8s.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: cfg.LabelSelector,
	})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if len(pod.OwnerReferences) > 0 {
			continue
		}
		if err := patchOwner(ctx, k8s, pod, cfg.ResolveOwner); err != nil {
			slog.Warn("k8shelpers: reconcile sweep patch failed", "pod", pod.Name, "err", err)
		}
	}
	return nil
}

// watchAndPatch watches pods and patches new pods with empty OwnerReferences.
func watchAndPatch(ctx context.Context, k8s kubernetes.Interface, cfg PatcherConfig) error {
	watcher, err := k8s.CoreV1().Pods(cfg.Namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: cfg.LabelSelector,
		Watch:         true,
	})
	if err != nil {
		return err
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if len(pod.OwnerReferences) > 0 {
				continue
			}
			if err := patchOwner(ctx, k8s, pod, cfg.ResolveOwner); err != nil {
				slog.Warn("k8shelpers: patch owner failed", "pod", pod.Name, "err", err)
			}
		}
	}
}

// patchOwner resolves and applies ownerReferences to the pod.
func patchOwner(ctx context.Context, k8s kubernetes.Interface, pod *corev1.Pod, resolve func(*corev1.Pod) (*OwnerRefHint, error)) error {
	if resolve == nil {
		return nil
	}
	hint, err := resolve(pod)
	if err != nil {
		return err
	}
	if hint == nil {
		return nil
	}

	isController := true
	owner := metav1.OwnerReference{
		APIVersion: hint.APIVersion,
		Kind:       hint.Kind,
		Name:       hint.Name,
		UID:        hint.UID,
		Controller: &isController,
	}

	ownerJSON, err := json.Marshal([]metav1.OwnerReference{owner})
	if err != nil {
		return err
	}

	patch := fmt.Sprintf(`{"metadata":{"ownerReferences":%s}}`, ownerJSON)
	_, err = k8s.CoreV1().Pods(pod.Namespace).Patch(
		ctx, pod.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{},
	)
	if err != nil && isNotFound(err) {
		return nil // pod gone between list and patch
	}
	return err
}
