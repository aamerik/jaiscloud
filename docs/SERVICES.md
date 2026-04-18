# JaisCloud — Service Reference

Detailed operation coverage, executor modes, fidelity notes, and configuration for every service supported by JaisCloud.

---

## Amazon S3

| Operation | Supported |
|---|---|
| CreateBucket / DeleteBucket / ListBuckets / HeadBucket | ✅ |
| PutObject / GetObject / HeadObject / DeleteObject | ✅ |
| ListObjectsV1 / ListObjectsV2 (prefix, delimiter, pagination) | ✅ |
| CopyObject | ✅ |
| DeleteObjects (batch) | ✅ |
| CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload | ✅ |
| GetBucketLocation | ✅ |
| Object/Bucket tagging, ACLs, versioning stubs | ✅ (stubs — no error) |
| AWS chunked transfer encoding (`x-amz-content-sha256: STREAMING-*`) | ✅ |
| S3A Hadoop connector compatibility (FileOutputCommitter, flat-key semantics) | ✅ |

**Blob storage:** Pass `--blob-dir <path>` to persist object bytes to disk. Without it, blobs are held in memory and lost on restart even in full mode.

---

## Amazon SQS

| Operation | Supported |
|---|---|
| CreateQueue / DeleteQueue / ListQueues / GetQueueUrl | ✅ |
| GetQueueAttributes / SetQueueAttributes | ✅ |
| SendMessage / ReceiveMessage / DeleteMessage | ✅ |
| SendMessageBatch / DeleteMessageBatch | ✅ |
| ChangeMessageVisibility / ChangeMessageVisibilityBatch | ✅ |
| PurgeQueue | ✅ |
| TagQueue / UntagQueue / ListQueueTags | ✅ |
| FIFO queues (deduplication, message groups) | ✅ |
| Dead-letter queues | ✅ |
| JSON protocol (`X-Amz-Target`) + Query/XML protocol | ✅ |

---

## Amazon DynamoDB

| Operation | Supported |
|---|---|
| CreateTable / DescribeTable / DeleteTable / ListTables | ✅ |
| PutItem / GetItem / DeleteItem | ✅ |
| UpdateItem (SET, REMOVE, ADD expressions) | ✅ |
| Scan (with FilterExpression) | ✅ |
| Query (KeyConditionExpression, begins_with, between) | ✅ |
| BatchWriteItem / BatchGetItem | ✅ |
| Composite primary keys (hash + range) | ✅ |
| Conditional writes (ConditionExpression) | ✅ |
| DynamoDB Streams (GetShardIterator, GetRecords) | ✅ |

---

## Amazon SNS

| Operation | Supported |
|---|---|
| CreateTopic / DeleteTopic / ListTopics | ✅ |
| GetTopicAttributes / SetTopicAttributes | ✅ |
| Subscribe / Unsubscribe / ListSubscriptions | ✅ |
| Publish (fan-out to SQS subscriptions with MessageAttributes) | ✅ |
| DeleteTopic removes all subscriptions | ✅ |

---

## Amazon EventBridge

| Operation | Supported |
|---|---|
| PutRule / DescribeRule / DeleteRule / ListRules | ✅ |
| PutTargets / RemoveTargets / ListTargetsByRule | ✅ |
| EnableRule / DisableRule | ✅ |
| PutEvents (inject arbitrary events into the matching pipeline) | ✅ |
| Event delivery to SQS targets | ✅ |
| EventPattern matching | ✅ |

---

## AWS IAM + STS

| Operation | Supported |
|---|---|
| CreateRole / GetRole / DeleteRole / ListRoles | ✅ |
| CreatePolicy / GetPolicy / DeletePolicy / ListPolicies | ✅ |
| AttachRolePolicy / DetachRolePolicy / ListAttachedRolePolicies | ✅ |
| PutRolePolicy / GetRolePolicy / DeleteRolePolicy (inline) | ✅ |
| CreateUser / GetUser / DeleteUser / ListUsers | ✅ |
| CreateAccessKey / DeleteAccessKey / ListAccessKeys | ✅ |
| GetCallerIdentity | ✅ |
| AssumeRole (returns mock credentials) | ✅ |

> IAM policy evaluation is not enforced — all authenticated requests are accepted regardless of attached policies.

---

## AWS Lambda

| Operation | Supported |
|---|---|
| CreateFunction / GetFunction / DeleteFunction / ListFunctions | ✅ |
| UpdateFunctionConfiguration / UpdateFunctionCode | ✅ |
| Invoke (echo mode, Docker warm pool, or K8s job) | ✅ |

### Executor modes

Controlled by `JAISCLOUD_EXECUTOR_MODE`:

| Mode | Behaviour |
|---|---|
| _(unset)_ / `mock` | Echo mode — payload returned unchanged. No subprocess. Ideal for testing infrastructure wiring. |
| `docker` | Each function runs in a **warm Docker container** (one per function, reused across invocations). Cold start on first invoke; container kept alive for `JAISCLOUD_LAMBDA_KEEPALIVE_SECS` (default 300 s). |
| `k8s` | Each invocation creates a **one-shot `batch/v1 Job`** in Kubernetes. No warm pool. Result read from pod logs after job completion. |

**Runtime → image mapping** (override with `JAISCLOUD_LAMBDA_IMAGE` or per-function `ImageUri`):

| Runtime | Default image |
|---|---|
| `python3.12` | `public.ecr.aws/lambda/python:3.12` |
| `nodejs20.x` | `public.ecr.aws/lambda/nodejs:20` |
| `java21` | `public.ecr.aws/lambda/java:21` |
| `go1.x` / `provided.al2` | `public.ecr.aws/lambda/provided:al2` |

---

## AWS Glue Data Catalog

| Operation | Supported |
|---|---|
| CreateDatabase / GetDatabase / GetDatabases / UpdateDatabase / DeleteDatabase | ✅ |
| CreateTable / GetTable / GetTables / UpdateTable / DeleteTable | ✅ |
| CreatePartition / GetPartition / GetPartitions / UpdatePartition / DeletePartition | ✅ |
| BatchCreatePartition / BatchDeletePartition | ✅ |
| Iceberg `metadata_location` CAS (conditional update used by Iceberg commits) | ✅ |

**Apache Iceberg support:** JaisCloud passes as a Glue Catalog endpoint for real Apache Iceberg 1.5+ workloads running in Spark. Iceberg reads and writes table metadata via the Glue API and stores data files in S3 — both backed by JaisCloud. This enables full Iceberg integration testing locally, including schema evolution, time travel, partitioning, and multi-batch appends.

---

## Amazon EMR (on EC2)

| Operation | Supported |
|---|---|
| RunJobFlow / DescribeCluster / ListClusters / TerminateJobFlows | ✅ |
| ModifyCluster / SetTerminationProtection / SetVisibleToAllUsers | ✅ |
| AddJobFlowSteps / DescribeStep / ListSteps / CancelSteps | ✅ |
| AddInstanceFleet / ListInstanceFleets / ModifyInstanceFleet | ✅ |
| AddInstanceGroups / ListInstanceGroups / ModifyInstanceGroups | ✅ |
| ListBootstrapActions | ✅ |
| AddTags / RemoveTags | ✅ |
| GetBlockPublicAccessConfiguration / PutBlockPublicAccessConfiguration | ✅ |
| PutManagedScalingPolicy / GetManagedScalingPolicy / RemoveManagedScalingPolicy | ✅ |

### Executor modes

Controlled by `JAISCLOUD_EXECUTOR_MODE` (same flag as Lambda):

| Mode | Behaviour |
|---|---|
| _(unset)_ / `mock` | Steps complete immediately with `COMPLETED`. No external process. Ideal for CI. |
| `k8s` | Each step submits a real `batch/v1 Job` running `spark-submit --master k8s://`. Result polled every 5 s. |
| `docker` | Each step runs in a Docker container. |

### K8s executor configuration

| Variable | Default | Description |
|---|---|---|
| `JAISCLOUD_K8S_APISERVER` | `https://kubernetes.default.svc` | K8s API server URL |
| `JAISCLOUD_K8S_TOKEN` | in-cluster token file | Bearer token: literal value or path to a token file |
| `JAISCLOUD_K8S_CA_FILE` | in-cluster CA path | PEM CA certificate for TLS verification |
| `JAISCLOUD_K8S_NAMESPACE` | `default` | Namespace for spark-submit Jobs |
| `JAISCLOUD_K8S_SA` | _(none)_ | Kubernetes service account for the spark-submit Pod |

When running **inside a K8s pod**, only `JAISCLOUD_EXECUTOR_MODE=k8s` is required — token, CA, and API server URL are auto-detected from the pod mount.

### Required RBAC

JaisCloud service account (to manage Jobs):
```yaml
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "delete"]
```

Spark service account (set via `JAISCLOUD_K8S_SA`, to manage Pods):
```yaml
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log", "services", "configmaps"]
  verbs: ["create", "get", "list", "delete"]
```

### Resource profiles

| Size | Driver | Executors |
|---|---|---|
| `small` | 500m / 1Gi | 1 × 500m / 1Gi |
| `medium` | 1 / 2Gi | 2 × 1 / 2Gi |
| `large` | 2 / 4Gi | 4 × 2 / 4Gi |

---

## Amazon EMR on EKS (EMR Containers)

| Operation | Supported |
|---|---|
| CreateVirtualCluster / DescribeVirtualCluster / DeleteVirtualCluster / ListVirtualClusters | ✅ |
| StartJobRun / DescribeJobRun / CancelJobRun / ListJobRuns | ✅ |
| CreateManagedEndpoint / DescribeManagedEndpoint / DeleteManagedEndpoint / ListManagedEndpoints | ✅ |
| TagResource / UntagResource / ListTagsForResource | ✅ |

Uses the same executor modes and K8s configuration as EMR on EC2 above.

---

## AWS KMS

| Operation | Supported |
|---|---|
| CreateKey / DescribeKey / ListKeys / EnableKey / DisableKey | ✅ |
| Encrypt / Decrypt / ReEncrypt | ✅ |
| GenerateDataKey / GenerateDataKeyWithoutPlaintext | ✅ |
| CreateAlias / DeleteAlias / ListAliases | ✅ |
| ScheduleKeyDeletion / CancelKeyDeletion | ✅ |
| EnableKeyRotation / DisableKeyRotation / GetKeyRotationStatus | ✅ |
| CreateGrant / RetireGrant / RevokeGrant / ListGrants | ✅ |
| TagResource / UntagResource / ListResourceTags | ✅ |

KMS uses AES-256-GCM envelope encryption. When `JAISCLOUD_KMS_MASTER_KEY` is set to a 32-byte hex KEK, all DEKs are wrapped with it. Without it, DEKs are stored in plaintext (dev mode only).

---

## AWS Secrets Manager

| Operation | Supported |
|---|---|
| CreateSecret / DescribeSecret / UpdateSecret / DeleteSecret / RestoreSecret | ✅ |
| GetSecretValue / PutSecretValue | ✅ (SecretString and SecretBinary) |
| ListSecrets / ListSecretVersionIds | ✅ |
| RotateSecret | ✅ |
| TagResource / UntagResource | ✅ |

Secret values are encrypted at rest using KMS envelope encryption when a key is provided.

---

## AWS SSM Parameter Store

| Operation | Supported |
|---|---|
| PutParameter / GetParameter / GetParameters | ✅ |
| GetParametersByPath (recursive, with filters) | ✅ |
| GetParameterHistory | ✅ |
| DeleteParameter / DeleteParameters | ✅ |
| LabelParameterVersion | ✅ |
| AddTagsToResource / ListTagsForResource | ✅ |
| String, StringList, SecureString types | ✅ |

---

## AWS API Gateway (REST)

| Operation | Supported |
|---|---|
| CreateRestApi / GetRestApi / GetRestApis / UpdateRestApi / DeleteRestApi | ✅ |
| GetResources / GetResource / CreateResource / DeleteResource | ✅ |
| PutMethod / GetMethod / DeleteMethod | ✅ |
| PutIntegration / GetIntegration / DeleteIntegration | ✅ |
| PutMethodResponse / PutIntegrationResponse | ✅ |
| CreateDeployment / GetDeployments / DeleteDeployment | ✅ |
| CreateStage / GetStage / GetStages / UpdateStage / DeleteStage | ✅ |
| InvokeApi — MOCK, AWS_PROXY (→ Lambda), HTTP_PROXY integrations | ✅ |

---

## AWS CloudFormation

| Operation | Supported |
|---|---|
| CreateStack / UpdateStack / DeleteStack | ✅ |
| DescribeStacks / ListStacks | ✅ |
| DescribeStackResources | ✅ |
| ValidateTemplate / GetTemplate | ✅ |
| Intrinsics: Ref, Fn::GetAtt, Fn::Sub, Fn::Join, Fn::If, Fn::Select, Fn::Split, Fn::FindInMap, Fn::Base64, Fn::Not, Fn::And, Fn::Or, Fn::Equals, Fn::Length | ✅ |
| DependsOn (explicit) + implicit Ref/GetAtt dependency ordering | ✅ |
| Real resource provisioning: SQS Queue, SNS Topic, S3 Bucket, DynamoDB Table, IAM Role, Lambda Function, SSM Parameter, SecretsManager Secret, KMS Key | ✅ |

Other `AWS::*` resource types are recorded in stack metadata but not provisioned.

---

## Stub services

The following services are registered and respond with well-formed empty responses so SDK calls don't fail during infrastructure setup. Full implementations are planned.

| Service | Status |
|---|---|
| Amazon EC2 | Stub |
| Amazon Route 53 | Stub |
| Amazon RDS | Stub |
| Amazon ElastiCache | Stub |
| Amazon ECS | Stub |

---

## Full Mode (PostgreSQL Persistence)

Start with `--mode full --dsn <postgres DSN>` to persist all state across restarts.

```bash
./jaiscloud start --mode full \
  --dsn "postgres://user:pass@localhost:5432/jaiscloud" \
  --blob-dir /var/lib/jaiscloud/blobs
```

### What persists

| Service | PostgreSQL table(s) | Blob storage |
|---|---|---|
| All resource metadata (queues, topics, tables, roles, functions, Glue, EMR…) | `jc_resources` | — |
| SQS messages | `jc_sqs_messages`, `jc_sqs_dedup` | — |
| DynamoDB items | `jc_dynamodb_items` | — |
| S3 object metadata | `jc_s3_objects` | `--blob-dir` |
| S3 object bytes | — | `--blob-dir` |

### Startup retry

JaisCloud retries the initial database ping up to 10 times with exponential backoff (500 ms → 8 s), so it starts cleanly before Postgres is ready — useful in `docker-compose` or Kubernetes init ordering.

---

## Fidelity Notes

JaisCloud prioritises **protocol correctness** over breadth:

- All responses use the exact XML/JSON envelope the AWS SDK expects.
- Error codes match AWS (`NoSuchBucket`, `ResourceNotFoundException`, etc.).
- `Last-Modified` headers use RFC 1123 GMT format, not UTC — the SDK parses these strictly.
- SQS supports both **JSON** (`X-Amz-Target`) and **Query/XML** protocols in the same server.
- DynamoDB key hash is computed from key attributes only (in schema order), matching AWS semantics.
- DynamoDB `x-amz-crc32` header is computed and returned on every response.
- S3 ETag is the MD5 of the stored bytes, including correct handling of AWS chunked transfer encoding.
- S3 implements flat-key semantics: `foo/bar` and `foo/bar/baz` coexist as independent objects.

**Known limitations:**

- No IAM policy evaluation — all requests are accepted regardless of attached policies.
- S3 versioning and object locking are stubbed (no error, no actual versioning).
- No cross-region or cross-account semantics.
- Azure and GCP cloud modes are scaffolded only (return 501).
- CloudFormation resource dispatch covers 9 resource types; other `AWS::*` resources are not provisioned.
