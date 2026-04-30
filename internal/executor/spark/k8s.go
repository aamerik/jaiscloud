package spark

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/platform"
)

// dnsNameRe matches characters that are NOT allowed in a K8s DNS label.
var dnsNameRe = regexp.MustCompile(`[^a-z0-9-]`)

// jobEntry tracks a submitted K8s batch Job alongside metadata needed for
// executor-template cleanup on terminal state.
type jobEntry struct {
	name          string // K8s Job name
	cleanupKey    string // opaque blob key for DeleteTemplate; empty when no template was uploaded
	transform     CloudSparkTransform
	isClusterMode bool // true when the job was submitted in Spark K8s cluster deploy-mode
}

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
	cfg                     SparkConfig
	platform                *platform.PlatformConfig
	client                  *k8sClient
	blobs                   blobfs.BlobStore
	jobEntries              sync.Map // jobID (string) → *jobEntry
	onClusterModeOrphanDelete func(jobID, reason string)
}

// NewK8sExecutor builds a K8sExecutor. Auth and TLS config are resolved from
// env vars and in-cluster service account files. plat may be nil; blobs may be nil
// (cluster mode will error if nil and a template upload is attempted).
func NewK8sExecutor(cfg SparkConfig, plat *platform.PlatformConfig, blobs blobfs.BlobStore) *K8sExecutor {
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
		return &K8sExecutor{cfg: cfg, blobs: blobs}
	}
	e := &K8sExecutor{cfg: cfg, platform: plat, client: client, blobs: blobs}
	slog.Info("spark k8s: executor initialised",
		"cluster_mode", cfg.ClusterMode,
		"strip_scheduling", cfg.StripScheduling,
		"cluster_shutdown", cfg.ClusterShutdown,
		"namespace", cfg.Namespace,
		"service_account", cfg.ServiceAccount,
		"image", cfg.Image,
		"template_bucket", cfg.TemplateBucket,
		"s3_endpoint_set", cfg.S3Endpoint != "",
	)
	go e.cleanupOrphans()
	return e
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
	manifest, transform, cleanupKey, err := e.buildJobManifest(ctx, jobName, job)
	if err != nil {
		return fmt.Errorf("K8sExecutor: build manifest for %s: %w", jobName, err)
	}

	if err := e.client.createJob(ctx, manifest); err != nil {
		// Clean up the uploaded executor template blob so it doesn't leak.
		if cleanupKey != "" && transform != nil {
			if derr := transform.DeleteTemplate(ctx, e.blobs, cleanupKey); derr != nil {
				slog.Warn("spark k8s: template cleanup failed after createJob error",
					"job", jobName, "cleanup_key", cleanupKey, "err", derr)
			}
		}
		return fmt.Errorf("K8sExecutor: create job %s: %w", jobName, err)
	}

	clusterModeActive := job.AllowClusterMode && e.cfg.Mode == "k8s"
	e.jobEntries.Store(job.JobID, &jobEntry{
		name:          jobName,
		cleanupKey:    cleanupKey,
		transform:     transform,
		isClusterMode: clusterModeActive,
	})
	slog.Info("spark k8s: submitted job",
		"job_id", job.JobID,
		"k8s_name", jobName,
		"cluster_mode", clusterModeActive,
		"namespace", e.cfg.Namespace,
		"image", manifest.Spec.Template.Spec.Containers[0].Image,
		"service_account", manifest.Spec.Template.Spec.ServiceAccountName,
		"executor_template_uploaded", cleanupKey != "",
	)
	return nil
}

// Status polls the K8s Job and maps its state to a SparkState.
func (e *K8sExecutor) Status(ctx context.Context, jobID string) (SparkStatus, error) {
	entryVal, ok := e.jobEntries.Load(jobID)
	if !ok {
		return SparkStatus{JobID: jobID, State: StatePending}, nil
	}
	jobName := entryVal.(*jobEntry).name

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
	entryVal, ok := e.jobEntries.Load(jobID)
	if !ok {
		return nil // already gone or never submitted
	}
	jobName := entryVal.(*jobEntry).name

	if e.client == nil {
		return fmt.Errorf("K8sExecutor: client not initialised")
	}

	if err := e.client.deleteJob(ctx, jobName); err != nil {
		return fmt.Errorf("K8sExecutor: delete job %s: %w", jobName, err)
	}
	e.forgetAndDeleteExecutorTemplate(ctx, jobID)
	return nil
}

// OnClusterModeOrphanDelete registers a callback fired when cleanupOrphans deletes
// a cluster-mode Job at startup. Providers use this to emit a terminal state event.
func (e *K8sExecutor) OnClusterModeOrphanDelete(cb func(jobID, reason string)) {
	e.onClusterModeOrphanDelete = cb
}

// forgetAndDeleteExecutorTemplate is called on terminal job state (completion,
// failure, cancel). Loads the jobEntry, dispatches DeleteTemplate, and removes
// the entry. Best-effort: errors log WARN but never propagate.
func (e *K8sExecutor) forgetAndDeleteExecutorTemplate(ctx context.Context, jobID string) {
	v, ok := e.jobEntries.LoadAndDelete(jobID)
	if !ok {
		return
	}
	entry, _ := v.(*jobEntry)
	if entry == nil || entry.cleanupKey == "" || entry.transform == nil {
		return
	}
	if err := entry.transform.DeleteTemplate(ctx, e.blobs, entry.cleanupKey); err != nil {
		slog.Warn("spark k8s: template cleanup failed on terminal state",
			"job_id", jobID, "cleanup_key", entry.cleanupKey, "err", err)
	}
}

// Close suspends non-cluster-mode Jobs so they survive a server restart.
// Cluster-mode Jobs are either left running (ClusterShutdown=="leave") or
// deleted (ClusterShutdown=="delete") per cfg.
func (e *K8sExecutor) Close() error {
	if e.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e.jobEntries.Range(func(k, v any) bool {
		entry, ok := v.(*jobEntry)
		if !ok {
			return true
		}
		if entry.isClusterMode {
			if e.cfg.ClusterShutdown == "delete" {
				if err := e.client.deleteJob(ctx, entry.name); err != nil {
					slog.Warn("spark k8s: failed to delete cluster-mode job on shutdown", "job", entry.name, "err", err)
				} else {
					slog.Info("spark k8s: deleted cluster-mode job on shutdown", "job", entry.name)
				}
			} else {
				slog.Info("spark k8s: leaving cluster-mode job running on shutdown", "job", entry.name)
			}
			return true
		}
		// Local-mode Job: suspend so it can be resumed on restart.
		if err := e.client.suspendJob(ctx, entry.name); err != nil {
			slog.Warn("spark k8s: failed to suspend job", "job", entry.name, "err", err)
		} else {
			slog.Info("spark k8s: suspended job", "job", entry.name)
		}
		return true
	})
	return nil
}

// cleanupOrphans runs at startup to re-adopt or delete jobs from previous runs.
func (e *K8sExecutor) cleanupOrphans() {
	if e.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	items, err := e.client.listJobs(ctx, "app.kubernetes.io%2Fmanaged-by%3Djaiscloud")
	if err != nil {
		slog.Warn("spark k8s: cleanupOrphans: list jobs failed", "err", err)
		return
	}

	for _, item := range items {
		name := item.Metadata.Name
		labels := item.Metadata.Labels
		jobID := labels["jaiscloud-job-id"]
		if jobID == "" {
			jobID = name
		}

		// Cluster-mode Jobs: always delete on restart (driver pod is gone; no resume).
		if labels["jaiscloud.io/spark-deploy-mode"] == "cluster" {
			if err := e.client.deleteJob(ctx, name); err != nil {
				slog.Warn("spark k8s: cleanupOrphans: delete cluster-mode job failed", "job", name, "err", err)
				continue
			}
			slog.Info("spark k8s: cleanupOrphans: deleted cluster-mode job", "job", name)

			// Decode blob cleanup key (encoded with "_" → "/" at write time).
			if encoded := labels["jaiscloud.io/executor-template-key"]; encoded != "" {
				cleanupKey := strings.ReplaceAll(encoded, "_", "/")
				if transform, terr := selectTransform(e.cfg.Cloud); terr == nil {
					if err := transform.DeleteTemplate(ctx, e.blobs, cleanupKey); err != nil {
						slog.Warn("spark k8s: cleanupOrphans: template cleanup failed",
							"job", name, "cleanup_key", cleanupKey, "err", err)
					}
				}
			}

			if e.onClusterModeOrphanDelete != nil {
				e.onClusterModeOrphanDelete(jobID, "JaisCloud restarted — cluster-mode job reaped")
			}
			continue
		}

		isTerminal := item.Status.Succeeded > 0 || item.Status.Failed > 0
		isSuspended := item.Spec.Suspend != nil && *item.Spec.Suspend

		switch {
		case isTerminal:
			if err := e.client.deleteJob(ctx, name); err != nil {
				slog.Warn("spark k8s: cleanupOrphans: delete terminal job failed", "job", name, "err", err)
			} else {
				slog.Info("spark k8s: cleanupOrphans: deleted terminal job", "job", name)
			}
		case isSuspended:
			if err := e.client.unsuspendJob(ctx, name); err != nil {
				slog.Warn("spark k8s: cleanupOrphans: unsuspend job failed", "job", name, "err", err)
			} else {
				e.jobEntries.Store(jobID, &jobEntry{name: name})
				slog.Info("spark k8s: cleanupOrphans: resumed suspended job", "job", name)
			}
		default:
			e.jobEntries.Store(jobID, &jobEntry{name: name})
			slog.Info("spark k8s: cleanupOrphans: re-adopted running job", "job", name)
		}
	}
}

// sanitizeLabel converts a string to a valid K8s label value (max 63 chars).
func sanitizeLabel(s string) string {
	var b strings.Builder
	for _, ch := range strings.ToLower(s) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' {
			b.WriteRune(ch)
		} else {
			b.WriteByte('-')
		}
	}
	result := strings.Trim(b.String(), "-")
	if len(result) > 63 {
		result = strings.TrimRight(result[:63], "-")
	}
	return result
}

// Reset clears the in-memory job map. Real K8s Jobs are left running so that
// a JaisCloud reset does not affect live cluster workloads.
func (e *K8sExecutor) Reset() {
	e.jobEntries.Range(func(k, _ any) bool {
		e.jobEntries.Delete(k)
		return true
	})
}

// ── Helpers ──────────────────────────────────────────────────────────────────
// resolveContainerBinary maps a user-supplied command name to the absolute
// path in the Spark image.
func resolveContainerBinary(name string) string {
	switch name {
	case "spark-submit":
		return "/opt/spark/bin/spark-submit"
	case "spark-sql":
		return "/opt/spark/bin/spark-sql"
	case "spark-shell":
		return "/opt/spark/bin/spark-shell"
	default:
		return name // assume it's a path or a well-known binary in the image
	}
}

// rewriteSparkMaster rewrites spark-submit args for execution inside a container.
// Any --master that is not already local[*]/local[N] is replaced with local[*].
// --deploy-mode cluster is rewritten to --deploy-mode client.
// If no --master is present, --master local[*] --deploy-mode client is prepended.
func rewriteSparkMaster(args []string) []string {
	out := make([]string, 0, len(args))
	skip := false
	hasMaster := false
	for i, a := range args {
		if skip {
			skip = false
			continue
		}
		// Two-token form: --master <value>
		if a == "--master" && i+1 < len(args) {
			hasMaster = true
			master := args[i+1]
			if strings.HasPrefix(master, "local") {
				out = append(out, "--master", master)
			} else {
				out = append(out, "--master", "local[*]")
			}
			skip = true
			continue
		}
		// Single-token form: --master=<value>
		if strings.HasPrefix(a, "--master=") {
			hasMaster = true
			master := strings.TrimPrefix(a, "--master=")
			if strings.HasPrefix(master, "local") {
				out = append(out, a)
			} else {
				out = append(out, "--master=local[*]")
			}
			continue
		}
		if a == "--deploy-mode" && i+1 < len(args) && args[i+1] == "cluster" {
			out = append(out, "--deploy-mode", "client")
			skip = true
			continue
		}
		out = append(out, a)
	}
	if !hasMaster {
		out = append([]string{"--master", "local[*]", "--deploy-mode", "client"}, out...)
	}
	return out
}

// insertBeforeJar inserts extra args before the first .jar argument in a
// spark-submit arg list. If no .jar is found, appends at the end.
func insertBeforeJar(args, extra []string) []string {
	for i, a := range args {
		if strings.HasSuffix(a, ".jar") && (i == 0 || args[i-1] != "--jars") {
			out := make([]string, 0, len(args)+len(extra))
			out = append(out, args[:i]...)
			out = append(out, extra...)
			out = append(out, args[i:]...)
			return out
		}
	}
	return append(args, extra...)
}

// stripSparkConfs removes --conf entries whose key matches any of the given prefixes.
func stripSparkConfs(args []string, keys ...string) []string {
	out := make([]string, 0, len(args))
	skip := false
	for i, a := range args {
		if skip {
			skip = false
			continue
		}
		if a == "--conf" && i+1 < len(args) {
			val := args[i+1]
			stripped := false
			for _, k := range keys {
				if strings.HasPrefix(val, k+"=") {
					skip = true
					stripped = true
					break
				}
			}
			if stripped {
				continue
			}
		}
		out = append(out, a)
	}
	return out
}

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
// Returns the manifest, the cloud transform used, and an executor-template cleanup key
// (non-empty only when an executor template was uploaded in cluster mode).
func (e *K8sExecutor) buildJobManifest(ctx context.Context, jobName string, job SparkJob) (batchJob, CloudSparkTransform, string, error) {
	backoffLimit := 0
	ttl := 3600

	transform, err := selectTransform(e.cfg.Cloud)
	if err != nil {
		return batchJob{}, nil, "", err
	}

	cmd, err := transform.ResolveCommand(job, e.cfg)
	if err != nil {
		return batchJob{}, nil, "", fmt.Errorf("K8sExecutor: resolve command for %s: %w", jobName, err)
	}

	// URI rewriting applied once across all argument sources.
	cmd.Args = rewriteURIs(transform, cmd.Args, e.cfg)
	cloudConfs := rewriteURIs(transform, transform.SparkConfs(e.cfg), e.cfg)
	extraConfs := rewriteURIs(transform, e.cfg.ExtraSparkConfs, e.cfg)

	cmd.Args = insertBeforeJar(cmd.Args, cloudConfs)
	cmd.Args = insertBeforeJar(cmd.Args, extraConfs)

	cloudVols, cloudMounts := transform.PodVolumes(e.cfg)

	ctr := container{
		Name:            "spark-submit",
		Image:           cmd.Image,
		Command:         []string{cmd.Binary},
		Args:            cmd.Args,
		ImagePullPolicy: "Never",
		Env:             transform.PodEnv(e.cfg),
		VolumeMounts:    cloudMounts,
	}

	spec := podSpec{RestartPolicy: "Never"}
	if e.cfg.ServiceAccount != "" {
		spec.ServiceAccountName = e.cfg.ServiceAccount
	}
	spec.Volumes = append(spec.Volumes, cloudVols...)

	// Platform layer: TLS, generic volumes, env — applied after cloud contributions.
	if err := platform.ApplyK8s(&spec, &ctr, e.platform); err != nil {
		return batchJob{}, nil, "", fmt.Errorf("platform apply: %w", err)
	}

	spec.Containers = []container{ctr}

	// Inject provider-resolved bootstrap fragments (classic EMR bootstrap actions).
	// Only present when the EMR provider called Resolve; zero cost otherwise.
	if len(job.ExtraInitContainers) > 0 {
		if err := platform.CheckVolumeConflicts(spec.Volumes, job.ExtraVolumes); err != nil {
			return batchJob{}, nil, "", fmt.Errorf("bootstrap volume conflict: %w", err)
		}
		// Prepend: bootstrap runs before any cloud-transform or platform init containers.
		spec.InitContainers = append(job.ExtraInitContainers, spec.InitContainers...)
		spec.Volumes = append(spec.Volumes, job.ExtraVolumes...)
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, job.ExtraMainMounts...)
	}

	// ── Cluster-mode block ─────────────────────────────────────────────────────
	clusterModeActive := job.AllowClusterMode && e.cfg.Mode == "k8s"
	if job.AllowClusterMode && !clusterModeActive {
		slog.Info("spark: cluster mode requested but executor mode is not k8s; running local",
			"job_id", job.JobID, "executor_mode", e.cfg.Mode)
	}

	// Validate cluster-mode config early so misconfiguration is visible before
	// the Job reaches Kubernetes and fails with an opaque container-level error.
	if clusterModeActive {
		if e.cfg.ServiceAccount == "" {
			slog.Warn("spark k8s: cluster-mode job has no ServiceAccount — "+
				"driver pod will lack RBAC to create executor pods; "+
				"set JAISCLOUD_K8S_SERVICE_ACCOUNT or spark.kubernetes.authenticate.driver.serviceAccountName",
				"job_id", job.JobID)
		}
		if e.cfg.Image == DefaultImage {
			slog.Warn("spark k8s: cluster-mode job using default image with ImagePullPolicy=Never — "+
				"ensure \""+DefaultImage+"\" is pre-loaded in the cluster, "+
				"or override via JAISCLOUD_K8S_SPARK_IMAGE / spark.kubernetes.container.image",
				"job_id", job.JobID, "image", e.cfg.Image)
		}
		if e.cfg.S3Endpoint == "" && strings.Contains(job.JarURI, "s3://") {
			slog.Warn("spark k8s: cluster-mode job references an s3:// JAR URI but "+
				"JAISCLOUD_K8S_S3_ENDPOINT is unset — "+
				"Spark pods will not be able to resolve the s3:// filesystem",
				"job_id", job.JobID, "jar", job.JarURI)
		}
	}

	// Local-mode fallback: scrub user podTemplateFile confs so Spark doesn't
	// try to fetch them in local[*] mode.
	if !clusterModeActive {
		spec.Containers[0].Args = stripTemplateFileConfs(spec.Containers[0].Args)
	}

	var execTemplateKey string

	// Driver template merge (cluster mode only).
	if clusterModeActive && len(job.DriverTemplateBytes) > 0 {
		driverTmpl, terr := unmarshalTemplateBytes(job.DriverTemplateBytes)
		if terr != nil {
			return batchJob{}, nil, "", fmt.Errorf("parse driver template: %w", terr)
		}
		if terr := MergeDriver(&spec, &spec.Containers[0], driverTmpl); terr != nil {
			return batchJob{}, nil, "", fmt.Errorf("merge driver template: %w", terr)
		}
	}

	// Executor template merge + upload via cloud transform (cluster mode only).
	if clusterModeActive && len(job.ExecutorTemplateBytes) > 0 {
		execTmpl, terr := unmarshalTemplateBytes(job.ExecutorTemplateBytes)
		if terr != nil {
			return batchJob{}, nil, "", fmt.Errorf("parse executor template: %w", terr)
		}
		mergedExec, terr := MergeExecutor(execTmpl)
		if terr != nil {
			return batchJob{}, nil, "", fmt.Errorf("merge executor template: %w", terr)
		}
		if e.cfg.StripScheduling {
			StripSchedulingFields(mergedExec)
		}
		if len(mergedExec.Containers) > 0 {
			ApplyResourceProfile(mergedExec, &mergedExec.Containers[0], nil, e.cfg.Resources)
		}
		body, jerr := json.Marshal(mergedExec)
		if jerr != nil {
			return batchJob{}, nil, "", fmt.Errorf("marshal merged executor spec: %w", jerr)
		}
		execURI, cleanupKey, uerr := transform.UploadTemplate(ctx, e.blobs, e.cfg, job.JobID, body)
		if uerr != nil {
			return batchJob{}, nil, "", fmt.Errorf("upload executor template: %w", uerr)
		}
		execTemplateKey = cleanupKey
		spec.Containers[0].Args = setOrAppendConf(
			spec.Containers[0].Args,
			"spark.kubernetes.executor.podTemplateFile",
			execURI,
		)
	}

	// Driver fetch env: cloud-specific credentials the driver's Spark scheduler
	// needs to fetch the uploaded executor template. First-wins against PodEnv.
	if clusterModeActive {
		fetchEnv := transform.DriverFetchEnv(e.cfg)
		spec.Containers[0].Env = appendIfAbsent(spec.Containers[0].Env, fetchEnv)
	}

	// Driver fitness: strip scheduling fields + apply resource cap.
	if clusterModeActive {
		if e.cfg.StripScheduling {
			StripSchedulingFields(&spec)
		}
		spec.Containers[0].Args = ApplyResourceProfile(
			&spec, &spec.Containers[0], spec.Containers[0].Args, e.cfg.Resources,
		)
	}

	// ── Labels ────────────────────────────────────────────────────────────────
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "jaiscloud",
		"jaiscloud-cloud":              string(e.cfg.Cloud),
		"jaiscloud-job-id":             sanitizeLabel(job.JobID),
	}
	if clusterModeActive {
		labels["jaiscloud.io/spark-deploy-mode"] = "cluster"
	}
	if execTemplateKey != "" {
		labels["jaiscloud.io/executor-template-key"] = strings.ReplaceAll(execTemplateKey, "/", "_")
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
	}, transform, execTemplateKey, nil
}
