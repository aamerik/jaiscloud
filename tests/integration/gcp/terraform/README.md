# GCP Terraform / OpenTofu compatibility suite

Opt-in integration suite that drives the real `hashicorp/google` Terraform
provider against a running `jaiscloud-gcp`, then asserts the resulting
resources through the emulator's REST API. It exists because Terraform is a
*stateful orchestrator*: it exercises cross-operation behavior (IAM
read-modify-write with etag OCC, update/patch semantics, read-after-write
consistency, and destroy/replacement lifecycle) that single-operation SDK tests
do not.

## Scope

Only services `jaiscloud-gcp` implements are declared in `main.tf`:

- Cloud Storage (bucket + object)
- IAM service account
- Secret Manager (secret + version + IAM member)
- Cloud KMS (key ring + crypto key)
- Pub/Sub (topic + subscription + topic/subscription IAM members)
- Cloud SQL for PostgreSQL (instance + database + user)
- Dataproc Metastore (federation + database IAM member)
- Dataproc (a Dataproc-on-GKE virtual cluster with two node-pool targets)
- Cloud Resource Manager project-level IAM
  (`/v1/projects/{project}:getIamPolicy`) and project lookup
- Service Usage (`/v1/projects/{project}/services`)

Deliberately **excluded** because it makes `apply` fail (and abort the run):

- **Cloud Run v2** — not implemented; declared out of scope in
  `docs/gcp/wire-contract.md` §5.
- **`google_container_cluster`** — the provider's cluster read dereferences
  nested response messages the GKE mock does not emit (`legacyAbac`,
  `networkConfig`, `shieldedNodes`, `defaultMaxPodsConstraint`) and panics
  (`SIGSEGV` in `resourceContainerClusterRead`). `run.sh` therefore creates the
  Dataproc-on-GKE prerequisite cluster itself over the GKE REST API and passes
  its name in (`var.gke_cluster`); the mock gap is recorded as **DPG37** in
  `plan_docs/gcp-dataproc-gke-wave-plan.md`, and the GKE half of the cross-API
  contract is gated by the conformance, gRPC and SDK suites instead.
- **`google_dataproc_cluster` with a plain `cluster_config`** (Compute-Engine
  clusters) — the emulator's GCE cluster read omits `config.softwareConfig`,
  which the provider flattens without a nil check and so panics
  (`flattenSoftwareConfig`); recorded as **DPG38**. Only the
  `virtual_cluster_config` shape is exercised here.

Keep `main.tf` in sync with the emulator's supported surface: one unimplemented
resource aborts the whole `apply`, so nothing else runs.

## Requirements

- `terraform` (validated with 1.16.x) or `tofu` (validated with 1.12.x)
- `curl`

The provider is fetched from the registry on first `init` (`hashicorp/google`
`~> 7.36`; validated with 7.46.1).

## Run

Preferred — the Makefile starts and stops the emulator for you:

```bash
make test-gcp-terraform     # requires terraform
make test-gcp-opentofu      # requires tofu
```

Both targets are opt-in: if the binary is not installed they print `SKIP` and
exit 0. They build `jaiscloud-gcp`, start it ephemeral (REST `:8080`,
gRPC `:8081`), run the suite, and stop it.

Against an already-running emulator:

```bash
tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project
TF_BIN=tofu tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project
```

## What it does

`run.sh` runs `init -> validate -> plan -> apply`, performs the spot checks
below, then `destroy` (also on exit via a trap, so failures still clean up):

- GCS bucket exists and its `labels` round-trip (`"env":"compat-test"`)
- GCS object exists with `contentType: text/plain`
- IAM service account exists
- Secret Manager secret exists with automatic replication; version 1 is
  `ENABLED`
- Secret Manager CMEK secret reads back its `global`-location key under
  `replication.automatic.customerManagedEncryption.kmsKeyName`, and the
  user-managed secret reads back both replica locations and its per-replica
  CMEK key verbatim (DC45)
- KMS key ring + crypto key (`purpose: ENCRYPT_DECRYPT`) exist, plus the
  `global` key ring/key the automatic-replication CMEK secret requires
- Pub/Sub topic + subscription exist
- Pub/Sub topic IAM keeps **both** member grants across the provider's
  `getIamPolicy -> merge -> setIamPolicy` flow (fresh-etag OCC)
- Pub/Sub subscription and Secret Manager IAM grants are present
- Cloud SQL instance is `RUNNABLE`, database and user exist
- The CRM project lookup resolves (`lifecycleState: ACTIVE`)
- The project IAM policy keeps the `google_project_iam_member` grant across
  the provider's `getIamPolicy -> merge -> setIamPolicy` flow (fresh-etag OCC),
  and project `:getIamPolicy` accepts the provider's POST
- The `google_project_service` API is `ENABLED` and appears in
  `GET .../services?filter=state:ENABLED`
- The Dataproc-on-GKE virtual cluster is `RUNNING`, echoes both `nodePoolTarget`
  names and the GKE target, and its read carries **no** `nodePoolConfig`
  (input-only in the real API)
- The node pools it named exist through the GKE API
  (`GET /container/v1/…/nodePools`): the caller-shaped one with `machineType`
  `n1-standard-4`, `maxNodeCount: 3` and the `goog-dataproc-cluster-name` label,
  the named-only one with the 1/10 autoscaler defaults
- After `destroy` removes the virtual cluster, the pool is still readable —
  real Dataproc does not delete a virtual cluster's node pools

## Provenance

Originally adapted from the floci-gcp `compat-terraform` / `compat-opentofu`
suites, pruned to the implemented services and made self-contained (no BATS, no
external compat repo). It is now an independent Terraform/OpenTofu provider
conformance suite; floci-gcp is not its spec (real GCP is). See
`plan_docs/gcp-terraform-compat-2026-09-22.md` for the findings that motivated
it.
