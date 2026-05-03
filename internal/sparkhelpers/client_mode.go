package sparkhelpers

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
)

const (
	defaultMaster         = "k8s://kubernetes.default.svc"
	executorTemplateMount = "/jaiscloud/spark"
	executorTemplateKey   = "executor-template.yaml"
	executorTemplatePath  = "/jaiscloud/spark/executor-template.yaml"
)

// sanitizeJobID lowercases the jobID, replaces non-alphanumeric chars with '-',
// and truncates to 52 chars (to stay within k8s 63-char limit with prefix).
func sanitizeJobID(jobID string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(jobID) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	s := sb.String()
	// Collapse consecutive dashes and trim leading/trailing dashes.
	re := regexp.MustCompile(`-+`)
	s = re.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 52 {
		s = s[:52]
		s = strings.TrimRight(s, "-")
	}
	return s
}

// BuildClientModeArgs returns the argv for spark-submit (excluding the binary name itself).
// Argv order:
//  1. --master k8s://kubernetes.default.svc
//  2. --deploy-mode client
//  3. --conf spark.kubernetes.namespace=<job.Namespace>
//  4. --conf spark.kubernetes.driver.pod.name=$(SPARK_DRIVER_POD_NAME)
//  5. --conf spark.kubernetes.executor.podTemplateFile=file:///jaiscloud/spark/executor-template.yaml
//  6. --conf spark.driver.bindAddress=0.0.0.0
//  7. caller's SparkSubmitArgs verbatim
//  8. entry-point pre-args (e.g. --class MainClass)
//  9. jarOrScript
//  10. JarArgs
func BuildClientModeArgs(job ClientModeJob) []string {
	args := []string{
		"--master", defaultMaster,
		"--deploy-mode", "client",
		"--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", job.Namespace),
		"--conf", "spark.kubernetes.driver.pod.name=$(SPARK_DRIVER_POD_NAME)",
		"--conf", fmt.Sprintf("spark.kubernetes.executor.podTemplateFile=file://%s", executorTemplatePath),
		"--conf", "spark.driver.bindAddress=0.0.0.0",
	}

	// Caller's extra spark-submit args.
	args = append(args, job.SparkSubmitArgs...)

	// Entry-point args.
	if job.EntryPoint != nil {
		preJarArgs, jarOrScript := EntryPointArgs(job.EntryPoint)
		args = append(args, preJarArgs...)
		if jarOrScript != "" {
			args = append(args, jarOrScript)
		}
	}

	// Positional jar/script args.
	args = append(args, job.JarArgs...)

	return args
}

// SubmitClientMode builds and submits a spark-submit k8s Job in client mode.
// Steps:
//  1. Builds executor pod template via BuildExecutorPodTemplate
//  2. Creates ConfigMap spark-exec-tpl-<sanitized-jobID> with template YAML
//  3. Builds main container (spark-submit) with driver resources
//  4. Mounts ConfigMap via projected volume
//  5. Calls k8shelpers.BuildPodSpec for platform overlay + driver template
//  6. Calls k8shelpers.SubmitJob and returns the handle
func SubmitClientMode(ctx context.Context, k8s kubernetes.Interface, job ClientModeJob) (k8shelpers.JobHandle, error) {
	// 1. Build executor pod template YAML.
	execTplYAML, err := BuildExecutorPodTemplate(ctx, k8s, job.PlatformOverlay, job.CallerExecutorPodTpl)
	if err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: build executor pod template: %w", err)
	}

	// 2. Create ConfigMap for executor template.
	cmName := "spark-exec-tpl-" + sanitizeJobID(job.JobID)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: job.Namespace,
			Labels:    job.Labels,
		},
		Data: map[string]string{
			executorTemplateKey: string(execTplYAML),
		},
	}
	if job.OwnerHint != nil {
		isController := true
		cm.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: job.OwnerHint.APIVersion,
			Kind:       job.OwnerHint.Kind,
			Name:       job.OwnerHint.Name,
			UID:        job.OwnerHint.UID,
			Controller: &isController,
		}}
	}
	if _, err := k8s.CoreV1().ConfigMaps(job.Namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: create executor template configmap: %w", err)
	}
	// If anything after this point fails, delete the ConfigMap so it doesn't
	// linger without an owning Job. The deferred delete is a no-op on success
	// because the Job's OwnerRef causes K8s GC to handle it instead.
	cmCreated := true
	defer func() {
		if cmCreated {
			// Job was created successfully — K8s GC via OwnerRef handles cleanup.
			return
		}
		_ = k8s.CoreV1().ConfigMaps(job.Namespace).Delete(context.Background(), cmName, metav1.DeleteOptions{})
	}()

	// 3. Build spark-submit binary path.
	sparkSubmitPath := job.SparkSubmitPath
	if sparkSubmitPath == "" {
		sparkSubmitPath = "spark-submit"
	}

	// Build main container.
	mainContainer := corev1.Container{
		Name:    "spark-submit",
		Command: []string{sparkSubmitPath},
		Args:    BuildClientModeArgs(job),
		Env: []corev1.EnvVar{
			{
				Name: "SPARK_DRIVER_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			{
				Name: "SPARK_DRIVER_BIND_ADDRESS",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
				},
			},
		},
	}

	// Set driver resources if specified.
	if job.DriverResources.CPU != "" || job.DriverResources.Memory != "" {
		reqs := corev1.ResourceList{}
		lims := corev1.ResourceList{}
		if job.DriverResources.CPU != "" {
			q := resource.MustParse(job.DriverResources.CPU)
			reqs[corev1.ResourceCPU] = q
			lims[corev1.ResourceCPU] = q
		}
		if job.DriverResources.Memory != "" {
			q := resource.MustParse(job.DriverResources.Memory)
			reqs[corev1.ResourceMemory] = q
			lims[corev1.ResourceMemory] = q
		}
		mainContainer.Resources = corev1.ResourceRequirements{
			Requests: reqs,
			Limits:   lims,
		}
	}

	// 4. Mount ConfigMap via projected volume.
	volumeName := "spark-exec-tpl"
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: executorTemplateMount,
	})

	// 5. Build pod spec via k8shelpers.BuildPodSpec.
	base := k8shelpers.PodSpecInput{
		MainContainer: mainContainer,
		Namespace:     job.Namespace,
		Labels:        job.Labels,
		Annotations:   job.Annotations,
	}

	tpl, err := k8shelpers.BuildPodSpec(ctx, k8s, base, job.PlatformOverlay, job.CallerDriverPodTpl, job.IdentityMutator)
	if err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: build pod spec: %w", err)
	}

	// Add projected volume for executor template ConfigMap.
	tpl.Spec.Volumes = append(tpl.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{
					{
						ConfigMap: &corev1.ConfigMapProjection{
							LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
							Items: []corev1.KeyToPath{
								{
									Key:  executorTemplateKey,
									Path: executorTemplateKey,
								},
							},
						},
					},
				},
			},
		},
	})

	// 6. Submit the job.
	jobName := "jc-spark-cm-" + sanitizeJobID(job.JobID)
	req := k8shelpers.SubmitJobRequest{
		Namespace:               job.Namespace,
		JobName:                 jobName,
		Spec:                    tpl.Spec,
		Labels:                  job.Labels,
		Annotations:             job.Annotations,
		Parallelism:             1,
		BackoffLimit:            0,
		TTLSecondsAfterFinished: job.TTLSecondsAfterFinished,
		OwnerRef:                job.OwnerHint,
	}

	// Signal the deferred cleanup that Job creation is in progress.
	// If SubmitJob fails, cmCreated stays false and the defer deletes the ConfigMap.
	cmCreated = false
	handle, err := k8shelpers.SubmitJob(ctx, k8s, req)
	if err != nil {
		return k8shelpers.JobHandle{}, fmt.Errorf("sparkhelpers: submit job: %w", err)
	}
	cmCreated = true // Job owns the ConfigMap via OwnerRef; K8s GC handles cleanup.
	return handle, nil
}
