package spark

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
)

// dnsNameRe matches characters that are NOT allowed in a K8s DNS label.
var dnsNameRe = regexp.MustCompile(`[^a-z0-9-]`)

// K8sExecutor runs Spark jobs as Kubernetes batch/v1 Jobs.
// spark-submit runs in cluster deploy-mode inside a container using the
// configured Spark image; the driver Pod it spawns manages executor Pods.
//
// Auth priority (first match wins):
//  1. JAISCLOUD_K8S_TOKEN  (literal token or path to token file)
//     + JAISCLOUD_K8S_CA_FILE (path to PEM CA cert)
//  2. In-cluster service account at /var/run/secrets/kubernetes.io/serviceaccount/
//  3. Unauthenticated (development only)
type K8sExecutor struct {
	cfg    SparkConfig
	client *k8sClient
	jobs   sync.Map // jobID (string) → k8s Job name (string)
}

// NewK8sExecutor builds a K8sExecutor. Auth and TLS config are resolved from
// env vars and in-cluster service account files.
func NewK8sExecutor(cfg SparkConfig) *K8sExecutor {
	apiURL := cfg.APIServer
	if apiURL == "" {
		apiURL = DefaultAPIServer
	}

	tokenSource := resolveTokenSource()
	caFile := resolveCAFile()
	clientCertFile := resolveClientCertFile()
	clientKeyFile := resolveClientKeyFile()

	client, err := newK8sClient(apiURL, tokenSource, caFile, clientCertFile, clientKeyFile, cfg.Namespace)
	if err != nil {
		// Non-fatal: log and fall through with a nil client.
		// Submit will return an error if the client is nil.
		fmt.Fprintf(os.Stderr, "[K8sExecutor] init error: %v\n", err)
		return &K8sExecutor{cfg: cfg}
	}
	return &K8sExecutor{cfg: cfg, client: client}
}

// resolveTokenSource returns the bearer token source (env var or in-cluster file).
func resolveTokenSource() string {
	if v := os.Getenv("JAISCLOUD_K8S_TOKEN"); v != "" {
		return v
	}
	if _, err := os.Stat(inClusterTokenFile); err == nil {
		return inClusterTokenFile
	}
	return ""
}

// resolveCAFile returns the CA cert file path (env var or in-cluster file).
func resolveCAFile() string {
	if v := os.Getenv("JAISCLOUD_K8S_CA_FILE"); v != "" {
		return v
	}
	if _, err := os.Stat(inClusterCAFile); err == nil {
		return inClusterCAFile
	}
	return ""
}

// resolveClientCertFile returns the client certificate file path for mTLS auth.
func resolveClientCertFile() string {
	return os.Getenv("JAISCLOUD_K8S_CLIENT_CERT_FILE")
}

// resolveClientKeyFile returns the client key file path for mTLS auth.
func resolveClientKeyFile() string {
	return os.Getenv("JAISCLOUD_K8S_CLIENT_KEY_FILE")
}

// Submit creates a K8s batch/v1 Job that runs spark-submit in cluster mode.
func (e *K8sExecutor) Submit(ctx context.Context, job SparkJob) error {
	if e.client == nil {
		return fmt.Errorf("K8sExecutor: client not initialised (check init logs)")
	}

	jobName := k8sJobName(job.JobID)
	manifest := e.buildJobManifest(jobName, job)

	if err := e.client.createJob(ctx, manifest); err != nil {
		return fmt.Errorf("K8sExecutor: create job %s: %w", jobName, err)
	}

	e.jobs.Store(job.JobID, jobName)
	fmt.Fprintf(os.Stderr, "[K8sExecutor] submitted job %s (k8s name: %s)\n", job.JobID, jobName)
	return nil
}

// Status polls the K8s Job and maps its state to a SparkState.
func (e *K8sExecutor) Status(ctx context.Context, jobID string) (SparkStatus, error) {
	jobNameVal, ok := e.jobs.Load(jobID)
	if !ok {
		return SparkStatus{JobID: jobID, State: StatePending}, nil
	}
	jobName := jobNameVal.(string)

	if e.client == nil {
		return SparkStatus{}, fmt.Errorf("K8sExecutor: client not initialised")
	}

	k8sJob, err := e.client.getJob(ctx, jobName)
	if err != nil {
		return SparkStatus{}, fmt.Errorf("K8sExecutor: get job %s: %w", jobName, err)
	}

	state := mapJobStatus(k8sJob.Status)
	return SparkStatus{
		JobID:   jobID,
		State:   state,
		Message: jobFailureMessage(k8sJob.Status),
	}, nil
}

// Cancel deletes the K8s Job (propagates to driver and executor Pods).
func (e *K8sExecutor) Cancel(ctx context.Context, jobID string) error {
	jobNameVal, ok := e.jobs.Load(jobID)
	if !ok {
		return nil // already gone or never submitted
	}
	jobName := jobNameVal.(string)

	if e.client == nil {
		return fmt.Errorf("K8sExecutor: client not initialised")
	}

	if err := e.client.deleteJob(ctx, jobName); err != nil {
		return fmt.Errorf("K8sExecutor: delete job %s: %w", jobName, err)
	}
	e.jobs.Delete(jobID)
	return nil
}

// Close is a no-op — the HTTP client has no persistent connections to release.
func (e *K8sExecutor) Close() error { return nil }

// Reset clears the in-memory job map. Real K8s Jobs are left running so that
// a JaisCloud reset does not affect live cluster workloads.
func (e *K8sExecutor) Reset() {
	e.jobs.Range(func(k, _ any) bool {
		e.jobs.Delete(k)
		return true
	})
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// k8sJobName converts an arbitrary job ID to a valid K8s DNS label name.
// Format: "spark-<sanitized>", max 63 characters.
func k8sJobName(jobID string) string {
	lower := strings.ToLower(jobID)
	safe := dnsNameRe.ReplaceAllString(lower, "-")
	safe = strings.Trim(safe, "-")
	prefixed := "spark-" + safe
	if len(prefixed) > 63 {
		prefixed = prefixed[:63]
		prefixed = strings.TrimRight(prefixed, "-")
	}
	return prefixed
}

// mapJobStatus converts a K8s Job status to a SparkState.
func mapJobStatus(s batchJobStatus) SparkState {
	for _, c := range s.Conditions {
		if c.Status != "True" {
			continue
		}
		switch c.Type {
		case "Complete":
			return StateCompleted
		case "Failed":
			return StateFailed
		}
	}
	if s.Active > 0 || s.StartTime != "" {
		return StateRunning
	}
	return StatePending
}

// jobFailureMessage extracts a human-readable failure reason from a K8s Job status.
// Returns the message from the Failed condition, falling back to the reason field.
func jobFailureMessage(s batchJobStatus) string {
	for _, c := range s.Conditions {
		if c.Type == "Failed" && c.Status == "True" {
			if c.Message != "" {
				return c.Message
			}
			if c.Reason != "" {
				return c.Reason
			}
		}
	}
	return ""
}

// buildJobManifest constructs a batch/v1 Job that runs spark-submit.
func (e *K8sExecutor) buildJobManifest(jobName string, job SparkJob) batchJob {
	backoffLimit := 0
	ttl := 3600

	// Determine the container command and args.
	//
	// EMR Containers style (StartJobRun):
	//   job.JarURI = EntryPoint  = "/opt/spark/bin/spark-submit"  (executable path)
	//   job.Args   = EntryPointArguments = ["--master", "local[2]", "--class", ...]
	//   → use JarURI as the command and Args as-is; calling SparkSubmitArgs would
	//     re-append JarURI as a JAR and duplicate the user-supplied K8s conf flags.
	//
	// EMR classic style (AddJobFlowSteps with a real JAR):
	//   job.JarURI = "s3://bucket/app.jar"
	//   job.Args   = application arguments (no spark-submit flags)
	//   → build the full spark-submit arg list via SparkSubmitArgs.
	var cmd, containerArgs []string
	if strings.HasPrefix(job.JarURI, "/") && len(job.Args) > 0 {
		cmd = []string{job.JarURI}
		containerArgs = job.Args
	} else {
		cmd = []string{"/opt/spark/bin/spark-submit"}
		containerArgs = SparkSubmitArgs(job)
	}

	spec := podSpec{
		RestartPolicy: "Never",
		Containers: []container{{
			Name:    "spark-submit",
			Image:   e.cfg.Image,
			Command: cmd,
			Args:    containerArgs,
		}},
	}
	if e.cfg.ServiceAccount != "" {
		spec.ServiceAccountName = e.cfg.ServiceAccount
	}

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "jaiscloud",
	}

	return batchJob{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: jobMeta{
			Name:      jobName,
			Namespace: e.cfg.Namespace,
			Labels:    labels,
		},
		Spec: jobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: podTemplate{
				Metadata: podMeta{Labels: labels},
				Spec:     spec,
			},
		},
	}
}
