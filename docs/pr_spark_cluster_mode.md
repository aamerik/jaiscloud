# PR Summary — Spark K8s Cluster Deploy-Mode (Phase 2.5 patch 3)

## Overview

This PR implements Spark K8s **cluster deploy-mode** with per-cloud pod-template merging for both EMR on EC2 (`AddJobFlowSteps`) and EMR on EKS (`StartJobRun`). When enabled, the Spark driver runs as a real Kubernetes Pod (cluster mode) rather than inside the spark-submit Job container (local mode), matching production EMR-on-EKS behaviour.

---

## Key changes

### New executor interfaces (`internal/executor/spark/`)

- **`CloudSparkTransform` → `CloudExecutorTemplateIO`** sub-interface — `UploadTemplate`, `DeleteTemplate`, `DriverFetchEnv` — cloud-owned blob upload/cleanup for executor pod templates
- **`buildJobManifest`** now returns `(batchJob, CloudSparkTransform, cleanupKey string, error)` — the transform is returned so `Submit` owns blob cleanup on `createJob` failure, eliminating the previous blob-leak path
- **`jobEntry`** struct gains `isClusterMode bool` — set at `Submit` time so `Close()` applies different shutdown behaviour (leave vs delete) without any K8s API calls
- **`resolveMasterArgs`** (`cluster_mode.go`) — whitelist-validates `--master` values; returns `ErrIncompatibleMasterInClusterMode` for incompatible values; logs structured `WARN` when cluster mode is active but no `--master` is present
- **`rewriteSparkMaster`** — handles both `--master X` (two-token) and `--master=X` (single-token) forms
- **`ApplyResourceProfile`** — uses `ExecutorCPU`/`ExecutorMemory` when `args == nil` (executor-side merge), not driver values
- **`AWSSparkTransform.PodEnv`** — deterministic `[]EnvVar` slice instead of map iteration to avoid non-deterministic ordering

### Cluster-mode policy (`JAISCLOUD_SPARK_K8S_CLUSTER_MODE`)

| Value | Behaviour |
|---|---|
| `auto` (default) | Engage cluster mode when job supplies pod-template `--conf` entries |
| `always` | Always engage; diagnostic `WARN` logs if SA/image/endpoint/`--master` are missing |
| `never` | Always local mode; template `--conf` entries stripped |

### Diagnostic logging at submission time

Structured `WARN` logs emitted when cluster mode is active with common misconfigs:

- No `ServiceAccount` (`JAISCLOUD_K8S_SA` unset)
- Default image (`apache/spark:3.5.0`) with `ImagePullPolicy=Never`
- `s3://` JAR URI with no `JAISCLOUD_K8S_S3_ENDPOINT`
- No `--master` arg in `sparkSubmitParameters`

### Poller improvement

`pollAll` logs failed-state transitions at `WARN` (not `INFO`) and includes the failure `Message` field.

### Orphan callback wiring (`cmd/jaiscloud/main.go`)

`K8sExecutor.OnClusterModeOrphanDelete` callback wired so orphaned cluster-mode Jobs detected at startup propagate `FAILED` state to both `EMRProvider` and `EMRContainersProvider`.

### EMR Containers (`internal/provider/emroneks/`)

`StartJobRun` skips `spark.kubernetes.{driver,executor}.podTemplateFile` keys when rebuilding `configOverrides` — prevents double-injection of template URIs that are already managed by the executor transform.

---

## New environment variables

| Variable | Default | Description |
|---|---|---|
| `JAISCLOUD_SPARK_K8S_CLUSTER_MODE` | `auto` | `auto` / `always` / `never` |
| `JAISCLOUD_SPARK_K8S_STRIP_SCHEDULING` | `true` | Strip scheduling fields from merged templates |
| `JAISCLOUD_SPARK_K8S_CLUSTER_SHUTDOWN` | `leave` | `leave` (suspend) or `delete` on `Close()` |
| `JAISCLOUD_SPARK_K8S_POD_TEMPLATE_MAX_BYTES` | `262144` | Max pod-template YAML size (256 KiB) |
| `JAISCLOUD_SPARK_K8S_TEMPLATE_BUCKET` | `jaiscloud-spark-templates` | S3 bucket for merged executor templates |

---

## Files changed

| File | Change |
|---|---|
| `internal/executor/spark/k8s.go` | `buildJobManifest` 4-value return; blob cleanup on failure; `jobEntry.isClusterMode`; diagnostic `WARN` logs; startup config summary log |
| `internal/executor/spark/cluster_mode.go` | `resolveMasterArgs` with `--master` whitelist + `WARN`; single-token `--master=X` handling |
| `internal/executor/spark/pod_template_merge.go` | `ApplyResourceProfile` executor-side CPU/mem fix |
| `internal/executor/spark/aws_transform.go` | `PodEnv` deterministic slice |
| `internal/executor/spark/poller.go` | Failed-state `WARN` logging |
| `internal/executor/spark/k8s_test.go` | Updated 7 `buildJobManifest` call sites to 4-value return |
| `internal/provider/emroneks/emrcontainers.go` | Skip template URI keys in `configOverrides` rebuild |
| `cmd/jaiscloud/main.go` | `OnClusterModeOrphanDelete` wired to both providers |
| `DEVELOPER_GUIDE.md` | Cluster-mode section, env var table, troubleshooting guide |
| `CLAUDE.md` | Phase 2.5 patch 3 entry; cluster-mode env vars; updated Spark executor description |
| `README.md` | Spark K8s cluster-mode configuration subsection |
| `plan_docs/lld-multicloud-spark-executor.md` | §8 template-conf ordering; §9.1 `S3BlobFetcher` rewrite |
| `internal/blobfs/blobfetch.go` | Package-level doc comment with extension point guidance |

---

## Testing

- All existing unit tests pass: `go test -race ./internal/...`
- `buildJobManifest` call sites in `k8s_test.go` updated for the 4-value return
- Cluster-mode e2e coverage via `make test-e2e-emrcontainers-k8s` (`spark_e2e` build tag)
