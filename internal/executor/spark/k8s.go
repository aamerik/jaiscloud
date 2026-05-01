package spark

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/platform"
)

// dnsNameRe matches characters that are NOT allowed in a K8s DNS label.
var dnsNameRe = regexp.MustCompile(`[^a-z0-9-]`)

// jobEntry tracks a submitted K8s batch Job.
type jobEntry struct {
	name          string // K8s Job name
	isClusterMode bool   // true when the job was submitted in Spark K8s cluster deploy-mode
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
	cfg               SparkConfig
	platform          *platform.PlatformConfig
	client            k8sClientInterface
	jobEntries        sync.Map // jobID (string) → *jobEntry
	onRestartTerminal func(jobID string, state SparkState, message string)
	onJobAdopted      func(jobID string, state SparkState)
}

// NewK8sExecutor builds a K8sExecutor. Auth and TLS config are resolved from
// env vars and in-cluster service account files. plat may be nil.
func NewK8sExecutor(cfg SparkConfig, plat *platform.PlatformConfig) *K8sExecutor {
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
	e := &K8sExecutor{cfg: cfg, platform: plat, client: client}
	slog.Info("spark k8s: executor initialised",
		"cluster_mode", cfg.ClusterMode,
		"cluster_shutdown", cfg.ClusterShutdown,
		"cluster_restart_policy", cfg.ClusterRestartPolicy,
		"namespace", cfg.Namespace,
		"service_account", cfg.ServiceAccount,
		"image", cfg.Image,
		"s3_endpoint_set", cfg.S3Endpoint != "",
	)
	// Synchronous: must complete before NewK8sExecutor returns so providers can
	// safely call Submit without racing the orphan re-adoption scan (G3).
	e.cleanupOrphans()
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
	manifest, err := e.buildJobManifest(ctx, jobName, job)
	if err != nil {
		return fmt.Errorf("K8sExecutor: build manifest for %s: %w", jobName, err)
	}

	if err := e.client.createJob(ctx, manifest); err != nil {
		return fmt.Errorf("K8sExecutor: create job %s: %w", jobName, err)
	}

	clusterModeActive := job.AllowClusterMode && e.cfg.SparkMode == "k8s"
	e.jobEntries.Store(job.JobID, &jobEntry{
		name:          jobName,
		isClusterMode: clusterModeActive,
	})
	slog.Info("spark k8s: submitted job",
		"job_id", job.JobID,
		"k8s_name", jobName,
		"cluster_mode", clusterModeActive,
		"namespace", e.cfg.Namespace,
		"image", manifest.Spec.Template.Spec.Containers[0].Image,
		"service_account", manifest.Spec.Template.Spec.ServiceAccountName,
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
		// Propagate ErrJobNotFound unwrapped so the poller can use errors.Is.
		if errors.Is(err, ErrJobNotFound) {
			return SparkStatus{}, fmt.Errorf("K8sExecutor: get job %s: %w", jobName, ErrJobNotFound)
		}
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
	e.jobEntries.Delete(jobID)
	return nil
}

// OnRestartTerminal registers a callback fired when cleanupOrphans observes a
// terminal or cluster-mode-reaped Job at startup. Providers use this to
// dispatch terminal state via their OnStateChange chain.
func (e *K8sExecutor) OnRestartTerminal(cb func(jobID string, state SparkState, message string)) {
	e.onRestartTerminal = cb
}

// OnJobAdopted registers a callback fired when cleanupOrphans re-adopts a
// running or unsuspended Job at startup. Providers use this to call
// poller.Track for the adopted jobID.
func (e *K8sExecutor) OnJobAdopted(cb func(jobID string, state SparkState)) {
	e.onJobAdopted = cb
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

// cleanupOrphans runs synchronously at startup to re-adopt or delete jobs from
// previous runs. It must complete before NewK8sExecutor returns (G3).
func (e *K8sExecutor) cleanupOrphans() {
	if e.client == nil {
		return
	}
	// No fixed timeout: use a background context so a slow apiserver doesn't
	// truncate the list. Individual per-job ops use a short deadline (G4).
	ctx := context.Background()

	selector := encodeLabelSelector("app.kubernetes.io/managed-by=jaiscloud")
	if e.cfg.InstanceID != "" {
		selector = encodeLabelSelector(
			"app.kubernetes.io/managed-by=jaiscloud,jaiscloud.io/instance-id=" + e.cfg.InstanceID)
	}
	items, err := e.client.listJobs(ctx, selector)
	if err != nil {
		slog.Warn("spark k8s: cleanupOrphans: list jobs failed", "err", err)
		return
	}

	for _, item := range items {
		name := item.Metadata.Name
		labels := item.Metadata.Labels

		// Raw job ID from annotation (G1); fall back to sanitized label for pre-Phase-7 Jobs.
		jobID := item.Metadata.Annotations["jaiscloud.io/job-id-raw"]
		if jobID == "" {
			jobID = labels["jaiscloud-job-id"]
		}
		if jobID == "" {
			jobID = name
		}

		opCtx, opCancel := context.WithTimeout(ctx, 10*time.Second)

		if labels["jaiscloud.io/spark-deploy-mode"] == "cluster" {
			// Belt-and-suspenders: skip jobs from other instances even if the
			// selector didn't filter them (e.g. pre-Phase-7 jobs, test fakes).
			if e.cfg.InstanceID != "" && labels["jaiscloud.io/instance-id"] != e.cfg.InstanceID {
				slog.Debug("spark k8s: cleanupOrphans: skipping cluster-mode job from other instance",
					"job", name,
					"their_id", labels["jaiscloud.io/instance-id"],
					"our_id", e.cfg.InstanceID)
				opCancel()
				continue
			}
			e.handleClusterModeOrphan(opCtx, name, jobID, item)
			opCancel()
			continue
		}

		// Step-mode (non-cluster) Job.
		isTerminal := item.Status.Succeeded > 0 || item.Status.Failed > 0
		isSuspended := item.Spec.Suspend != nil && *item.Spec.Suspend

		switch {
		case isTerminal:
			state := mapJobStatus(item.Status)
			msg := jobFailureMessage(item.Status)
			if e.onRestartTerminal != nil {
				e.onRestartTerminal(jobID, state, msg)
			}
			if err := e.client.deleteJob(opCtx, name); err != nil {
				slog.Warn("spark k8s: cleanupOrphans: delete terminal job failed", "job", name, "err", err)
			} else {
				slog.Info("spark k8s: cleanupOrphans: deleted terminal job", "job", name, "state", state)
			}
		case isSuspended:
			if err := e.client.unsuspendJob(opCtx, name); err != nil {
				slog.Warn("spark k8s: cleanupOrphans: unsuspend job failed", "job", name, "err", err)
			} else {
				e.jobEntries.Store(jobID, &jobEntry{name: name})
				if e.onJobAdopted != nil {
					e.onJobAdopted(jobID, StateRunning)
				}
				slog.Info("spark k8s: cleanupOrphans: resumed suspended job", "job", name)
			}
		default:
			e.jobEntries.Store(jobID, &jobEntry{name: name})
			if e.onJobAdopted != nil {
				e.onJobAdopted(jobID, StateRunning)
			}
			slog.Info("spark k8s: cleanupOrphans: re-adopted running job", "job", name)
		}
		opCancel()
	}
}

// handleClusterModeOrphan applies ClusterRestartPolicy to a cluster-mode Job
// found at startup. Terminal Jobs always dispatch real state; running Jobs are
// adopted or reaped depending on cfg.ClusterRestartPolicy.
func (e *K8sExecutor) handleClusterModeOrphan(ctx context.Context, name, jobID string, item jobListItem) {
	isTerminal := item.Status.Succeeded > 0 || item.Status.Failed > 0
	if isTerminal {
		// R6/R7: dispatch the real terminal state, not a hard-coded FAILED.
		state := mapJobStatus(item.Status)
		msg := jobFailureMessage(item.Status)
		if e.onRestartTerminal != nil {
			e.onRestartTerminal(jobID, state, msg)
		}
		if err := e.client.deleteJob(ctx, name); err != nil {
			slog.Warn("spark k8s: cleanupOrphans: delete terminal cluster-mode job failed", "job", name, "err", err)
		} else {
			slog.Info("spark k8s: cleanupOrphans: deleted terminal cluster-mode job", "job", name, "state", state)
		}
		return
	}

	// R5: still running — honour the restart policy.
	switch e.cfg.ClusterRestartPolicy {
	case "reap":
		if e.onRestartTerminal != nil {
			e.onRestartTerminal(jobID, StateFailed,
				"reaped on restart per JAISCLOUD_SPARK_K8S_CLUSTER_RESTART_POLICY=reap")
		}
		if err := e.client.deleteJob(ctx, name); err != nil {
			slog.Warn("spark k8s: cleanupOrphans: reap cluster-mode job failed", "job", name, "err", err)
		} else {
			slog.Info("spark k8s: cleanupOrphans: reaped running cluster-mode job", "job", name)
		}
	default: // "adopt"
		e.jobEntries.Store(jobID, &jobEntry{name: name, isClusterMode: true})
		if e.onJobAdopted != nil {
			e.onJobAdopted(jobID, StateRunning)
		}
		slog.Info("spark k8s: cleanupOrphans: re-adopted running cluster-mode job", "job", name)
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

// Reset deletes every tracked K8s Job and clears the in-memory map, returning
// the cluster to a known-empty baseline. It also sweeps for any instance-owned
// Jobs that escaped the map (e.g. Submit crashed mid-store).
func (e *K8sExecutor) Reset() {
	if e.client == nil {
		e.jobEntries.Range(func(k, _ any) bool { e.jobEntries.Delete(k); return true })
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e.jobEntries.Range(func(k, v any) bool {
		if entry, ok := v.(*jobEntry); ok && entry != nil {
			if err := e.client.deleteJob(ctx, entry.name); err != nil {
				slog.Warn("spark k8s: Reset: delete job failed", "job", entry.name, "err", err)
			}
		}
		e.jobEntries.Delete(k)
		return true
	})

	// Sweep for instance-owned Jobs that escaped the map.
	if e.cfg.InstanceID != "" {
		selector := encodeLabelSelector(
			"app.kubernetes.io/managed-by=jaiscloud,jaiscloud.io/instance-id=" + e.cfg.InstanceID)
		if items, err := e.client.listJobs(ctx, selector); err == nil {
			for _, item := range items {
				if err := e.client.deleteJob(ctx, item.Metadata.Name); err != nil {
					slog.Warn("spark k8s: Reset: sweep delete failed", "job", item.Metadata.Name, "err", err)
				}
			}
		}
	}
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
func (e *K8sExecutor) buildJobManifest(ctx context.Context, jobName string, job SparkJob) (batchJob, error) {
	backoffLimit := 0
	ttl := 3600

	transform, err := selectTransform(e.cfg.Cloud)
	if err != nil {
		return batchJob{}, err
	}

	cmd, err := transform.ResolveCommand(job, e.cfg)
	if err != nil {
		return batchJob{}, fmt.Errorf("K8sExecutor: resolve command for %s: %w", jobName, err)
	}

	// Validate URIs: fail fast if storage schemes don't match the configured cloud.
	if err := transform.ValidateURIs(cmd.Args, e.cfg); err != nil {
		return batchJob{}, err
	}

	cloudConfs := transform.SparkConfs(e.cfg)
	extraConfs := e.cfg.ExtraSparkConfs

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
		return batchJob{}, fmt.Errorf("platform apply: %w", err)
	}

	spec.Containers = []container{ctr}

	// Inject provider-resolved bootstrap fragments (classic EMR bootstrap actions).
	// Only present when the EMR provider called Resolve; zero cost otherwise.
	if len(job.ExtraInitContainers) > 0 {
		if err := platform.CheckVolumeConflicts(spec.Volumes, job.ExtraVolumes); err != nil {
			return batchJob{}, fmt.Errorf("bootstrap volume conflict: %w", err)
		}
		// Prepend: bootstrap runs before any cloud-transform or platform init containers.
		spec.InitContainers = append(job.ExtraInitContainers, spec.InitContainers...)
		spec.Volumes = append(spec.Volumes, job.ExtraVolumes...)
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, job.ExtraMainMounts...)
	}

	// ── Cluster-mode block ─────────────────────────────────────────────────────
	clusterModeActive := job.AllowClusterMode && e.cfg.SparkMode == "k8s"
	if job.AllowClusterMode && !clusterModeActive {
		slog.Info("spark: cluster mode requested but executor mode is not k8s; running local",
			"job_id", job.JobID, "executor_mode", e.cfg.SparkMode)
	}

	// Validate cluster-mode config early so misconfiguration is visible before
	// the Job reaches Kubernetes and fails with an opaque container-level error.
	if clusterModeActive {
		if e.cfg.ServiceAccount == "" {
			slog.Warn("spark k8s: cluster-mode job has no ServiceAccount — "+
				"driver pod will lack RBAC to create executor pods; "+
				"set JAISCLOUD_K8S_SA or spark.kubernetes.authenticate.driver.serviceAccountName",
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

	// ── Labels + annotations ──────────────────────────────────────────────────
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "jaiscloud",
		"jaiscloud-cloud":              string(e.cfg.Cloud),
		"jaiscloud-job-id":             sanitizeLabel(job.JobID),
	}
	if e.cfg.InstanceID != "" {
		labels["jaiscloud.io/instance-id"] = e.cfg.InstanceID
	}
	if clusterModeActive {
		labels["jaiscloud.io/spark-deploy-mode"] = "cluster"
	}
	// Store the raw job ID in an annotation so cleanupOrphans can recover it
	// even when sanitizeLabel has collapsed characters (G1).
	annotations := map[string]string{
		"jaiscloud.io/job-id-raw": job.JobID,
	}

	return batchJob{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: jobMeta{
			Name:        jobName,
			Namespace:   e.cfg.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: jobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: podTemplate{
				Metadata: podMeta{Labels: labels},
				Spec:     spec,
			},
		},
	}, nil
}
