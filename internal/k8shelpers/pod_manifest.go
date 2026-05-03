package k8shelpers

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// SubmitJob creates a batchv1.Job with the supplied pod spec and returns a
// JobHandle. The Job's OwnerReferences field is populated from req.OwnerRef
// if non-nil.
func SubmitJob(ctx context.Context, k8s kubernetes.Interface, req SubmitJobRequest) (JobHandle, error) {
	parallelism := req.Parallelism
	if parallelism == 0 {
		parallelism = 1
	}
	backoffLimit := req.BackoffLimit

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        req.JobName,
			Namespace:   req.Namespace,
			Labels:      req.Labels,
			Annotations: req.Annotations,
		},
		Spec: batchv1.JobSpec{
			Parallelism:             &parallelism,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: req.TTLSecondsAfterFinished,
			ActiveDeadlineSeconds:   req.ActiveDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      req.Labels,
					Annotations: req.Annotations,
				},
				Spec: req.Spec,
			},
		},
	}

	if req.OwnerRef != nil {
		isController := true
		job.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: req.OwnerRef.APIVersion,
			Kind:       req.OwnerRef.Kind,
			Name:       req.OwnerRef.Name,
			UID:        req.OwnerRef.UID,
			Controller: &isController,
		}}
	}

	created, err := k8s.BatchV1().Jobs(req.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return JobHandle{}, fmt.Errorf("k8shelpers.SubmitJob: create job %q: %w", req.JobName, err)
	}

	return JobHandle{
		JobID:     req.JobName, // using job name as handle ID; provider may override
		Namespace: req.Namespace,
		JobName:   created.Name,
		JobUID:    created.UID,
		CreatedAt: created.CreationTimestamp.Time,
	}, nil
}

// Cancel deletes the Job (and its pods via cascade). Returns nil on
// success or if the Job was already gone.
func Cancel(ctx context.Context, k8s kubernetes.Interface, handle JobHandle) error {
	propagation := metav1.DeletePropagationForeground
	err := k8s.BatchV1().Jobs(handle.Namespace).Delete(ctx, handle.JobName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

// isNotFound returns true if err is a k8s "not found" error.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	// k8s errors package
	type statusErr interface {
		Status() metav1.Status
	}
	if se, ok := err.(statusErr); ok {
		return se.Status().Code == 404
	}
	// apierrors
	e := err.Error()
	return strings.Contains(e, "not found") || strings.Contains(e, "NotFound")
}

// now is a variable for test injection.
var now = time.Now
