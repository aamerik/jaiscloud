//go:build gcloud_conformance

package gcloud

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Expect is the status a command is expected to reach against a healthy
// emulator. It is compared against the observed status to decide whether a
// result is a regression: only an expected-pass command that fails is fatal.
type Expect string

const (
	ExpectPass        Expect = "pass"
	ExpectFail        Expect = "fail"
	ExpectUnsupported Expect = "unsupported"
)

// Command is one row of the conformance matrix. Args and Assert are already
// resolved against a run's fixtures (see fixtures), so they can be executed
// verbatim.
type Command struct {
	// Name is the stable, human-readable matrix key.
	Name string
	// Service groups the command in the report (storage, pubsub, ...).
	Service string
	// Args is the fully-resolved gcloud argument vector (excluding the binary).
	Args []string
	// Assert validates stdout when gcloud exits 0. A nil Assert means "exit 0
	// is sufficient".
	Assert func(stdout string) error
	// Expect is the required status for a non-regression outcome.
	Expect Expect
	// Gap documents why Expect is not ExpectPass. Required for known gaps.
	Gap string
}

// ConformanceText is uploaded to storage and stored as a secret, and asserted
// on the way back out.
const ConformanceText = "jaiscloud gcloud conformance"

// WorkflowSource is a minimal, valid workflow definition accepted by the
// emulator's workflows provider.
const WorkflowSource = "main:\n  steps:\n  - return: ok\n"

// fixtures are the per-run values baked into the command table. Resource names
// are suffixed with a run-unique ID so the suite is safe to run against a
// non-ephemeral emulator.
type fixtures struct {
	sid string // run-unique, lowercase alphanumeric
	tmp string // path to a small text file containing ConformanceText
	wf  string // path to a minimal workflow YAML source
}

// commands returns the curated matrix in execution order. Create commands
// precede the describe/list commands that assert on them.
func commands(f fixtures) []Command {
	bucket := "gs://" + f.sid + "-bucket"
	topic := f.sid + "-topic"
	sub := f.sid + "-sub"
	secret := f.sid + "-secret"
	ring := f.sid + "-ring"
	key := f.sid + "-key"
	zone := f.sid + "-zone"
	record := "www." + f.sid + ".example.com."
	sqlInstance := f.sid + "-sql"
	network := f.sid + "-net"
	vm := f.sid + "-vm"
	workflow := f.sid + "-wf"
	dataset := f.sid + "_ds"
	cluster := f.sid + "-dp"

	return []Command{
		// ── Cloud Storage ────────────────────────────────────────────────────
		{
			Name: "storage buckets list", Service: "storage",
			Args:   []string{"storage", "buckets", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "storage buckets create", Service: "storage",
			Args:   []string{"storage", "buckets", "create", bucket, "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "storage buckets describe", Service: "storage",
			Args:   []string{"storage", "buckets", "describe", bucket, "--format=json"},
			Assert: jsonObject, Expect: ExpectPass,
		},
		{
			Name: "storage objects upload", Service: "storage",
			Args:   []string{"storage", "cp", f.tmp, bucket + "/hello.txt"},
			Expect: ExpectPass,
		},
		{
			Name: "storage objects list", Service: "storage",
			Args:   []string{"storage", "objects", "list", bucket, "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "storage objects cat", Service: "storage",
			Args:   []string{"storage", "cat", bucket + "/hello.txt"},
			Assert: contains(ConformanceText), Expect: ExpectPass,
		},

		// ── Pub/Sub ──────────────────────────────────────────────────────────
		{
			Name: "pubsub topics list", Service: "pubsub",
			Args:   []string{"pubsub", "topics", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "pubsub topics create", Service: "pubsub",
			Args:   []string{"pubsub", "topics", "create", topic, "--format=json"},
			Assert: contains(topic), Expect: ExpectPass,
		},
		{
			Name: "pubsub topics describe", Service: "pubsub",
			Args:   []string{"pubsub", "topics", "describe", topic, "--format=json"},
			Assert: contains(topic), Expect: ExpectPass,
		},
		{
			Name: "pubsub subscriptions create", Service: "pubsub",
			Args:   []string{"pubsub", "subscriptions", "create", sub, "--topic=" + topic, "--format=json"},
			Assert: contains(sub), Expect: ExpectPass,
		},
		{
			Name: "pubsub subscriptions list", Service: "pubsub",
			Args:   []string{"pubsub", "subscriptions", "list", "--format=json"},
			Assert: contains(sub), Expect: ExpectPass,
		},
		{
			Name: "pubsub topics publish", Service: "pubsub",
			Args:   []string{"pubsub", "topics", "publish", topic, "--message=hello", "--format=json"},
			Assert: contains("messageIds"), Expect: ExpectPass,
		},

		// ── Secret Manager ───────────────────────────────────────────────────
		{
			Name: "secrets list", Service: "secrets",
			Args:   []string{"secrets", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "secrets create", Service: "secrets",
			Args:   []string{"secrets", "create", secret, "--replication-policy=automatic", "--format=json"},
			Assert: contains(secret), Expect: ExpectPass,
		},
		{
			Name: "secrets describe", Service: "secrets",
			Args:   []string{"secrets", "describe", secret, "--format=json"},
			Assert: contains(secret), Expect: ExpectPass,
		},
		{
			Name: "secrets versions add", Service: "secrets",
			Args:   []string{"secrets", "versions", "add", secret, "--data-file=" + f.tmp, "--format=json"},
			Expect: ExpectFail,
			Gap: "emulator AddVersion response omits the checksum.crc32c field, so gcloud " +
				"exits 1 with 'payload data corruption may have occurred' even though the " +
				"version is created and readable (verified by the following access command). " +
				"Secret Manager data integrity requires the create response to echo the " +
				"CRC32C of the payload.",
		},
		{
			// Depends on the version created above (which gcloud reports as
			// corrupt). If it returns the plaintext, the payload is intact and
			// the corruption warning is purely the missing checksum field.
			Name: "secrets versions access", Service: "secrets",
			Args:   []string{"secrets", "versions", "access", "latest", "--secret=" + secret},
			Assert: contains(ConformanceText), Expect: ExpectPass,
		},

		// ── Cloud KMS ────────────────────────────────────────────────────────
		{
			Name: "kms keyrings list", Service: "kms",
			Args:   []string{"kms", "keyrings", "list", "--location=global", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "kms keyrings create", Service: "kms",
			Args:   []string{"kms", "keyrings", "create", ring, "--location=global", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "kms keys create", Service: "kms",
			Args:   []string{"kms", "keys", "create", key, "--location=global", "--keyring=" + ring, "--purpose=encryption", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "kms keys describe", Service: "kms",
			Args:   []string{"kms", "keys", "describe", key, "--location=global", "--keyring=" + ring, "--format=json"},
			Assert: contains(key), Expect: ExpectPass,
		},
		{
			Name: "kms keys list", Service: "kms",
			Args:   []string{"kms", "keys", "list", "--location=global", "--keyring=" + ring, "--format=json"},
			Assert: contains(key), Expect: ExpectFail,
			Gap: "emulator KMS CryptoKeyList parses the keyring parent " +
				"(.../keyRings/{ring}) with parseCryptoKey, which needs five path " +
				"segments; it returns empty loc/ring and the store lookup finds " +
				"nothing, so the list is empty (exit 0). CryptoKeyGet returns the " +
				"same key correctly, so the key exists and only the list is broken.",
		},

		// ── Cloud DNS ────────────────────────────────────────────────────────
		{
			Name: "dns managed-zones list", Service: "dns",
			Args:   []string{"dns", "managed-zones", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "dns managed-zones create", Service: "dns",
			Args:   []string{"dns", "managed-zones", "create", zone, "--dns-name=" + f.sid + ".example.com.", "--description=conformance", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "dns record-sets create", Service: "dns",
			Args:   []string{"dns", "record-sets", "create", record, "--zone=" + zone, "--type=A", "--ttl=300", "--rrdatas=1.2.3.4", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "dns record-sets list", Service: "dns",
			Args:   []string{"dns", "record-sets", "list", "--zone=" + zone, "--format=json"},
			Assert: contains(record), Expect: ExpectPass,
		},

		// ── Cloud SQL ────────────────────────────────────────────────────────
		{
			Name: "sql instances list", Service: "sql",
			Args:   []string{"sql", "instances", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "sql instances create", Service: "sql",
			Args:   []string{"sql", "instances", "create", sqlInstance, "--database-version=POSTGRES_15", "--tier=db-f1-micro", "--region=us-central1", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "sql instances describe", Service: "sql",
			Args:   []string{"sql", "instances", "describe", sqlInstance, "--format=json"},
			Assert: contains(sqlInstance), Expect: ExpectPass,
		},
		{
			Name: "sql instances list (after create)", Service: "sql",
			Args:   []string{"sql", "instances", "list", "--format=json"},
			Assert: contains(sqlInstance), Expect: ExpectPass,
		},

		// ── Compute Engine ───────────────────────────────────────────────────
		{
			Name: "compute instances list", Service: "compute",
			Args:   []string{"compute", "instances", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "compute networks create", Service: "compute",
			Args:   []string{"compute", "networks", "create", network, "--subnet-mode=custom", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "compute networks list", Service: "compute",
			Args:   []string{"compute", "networks", "list", "--format=json"},
			Assert: contains(network), Expect: ExpectPass,
		},
		{
			Name: "compute instances create", Service: "compute",
			Args:   []string{"compute", "instances", "create", vm, "--zone=us-central1-a", "--machine-type=e2-micro", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "compute instances list (after create)", Service: "compute",
			Args:   []string{"compute", "instances", "list", "--format=json"},
			Assert: contains(vm), Expect: ExpectPass,
		},

		// ── Cloud Functions ──────────────────────────────────────────────────
		{
			Name: "functions list", Service: "functions",
			Args:   []string{"functions", "list", "--format=json"},
			Expect: ExpectUnsupported,
			Gap: "gcloud 585 uses the Cloud Functions v2 API (GET " +
				"/v2/projects/{p}/locations/-/functions), but jaiscloud only routes " +
				"the v1 surface (/v1/projects/{p}/locations/{l}/functions). The v2 " +
				"path falls through to the raw-GCS media handler and returns HTTP 404 " +
				"'object not found'. gcloud 585 no longer exposes a --v1/--no-gen2 " +
				"switch, so no gcloud functions command can reach the emulator.",
		},

		// ── Workflows ────────────────────────────────────────────────────────
		{
			Name: "workflows list", Service: "workflows",
			Args:   []string{"workflows", "list", "--location=us-central1", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "workflows deploy", Service: "workflows",
			Args:   []string{"workflows", "deploy", workflow, "--location=us-central1", "--source=" + f.wf, "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "workflows describe", Service: "workflows",
			Args:   []string{"workflows", "describe", workflow, "--location=us-central1", "--format=json"},
			Assert: contains(workflow), Expect: ExpectPass,
		},
		{
			Name: "workflows list (after deploy)", Service: "workflows",
			Args:   []string{"workflows", "list", "--location=us-central1", "--format=json"},
			Assert: contains(workflow), Expect: ExpectPass,
		},

		// ── BigQuery (alpha bq surface; `bq` itself cannot reach the emulator) ─
		{
			Name: "bigquery datasets list", Service: "bigquery",
			Args:   []string{"alpha", "bq", "datasets", "list", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "bigquery datasets create", Service: "bigquery",
			Args:   []string{"alpha", "bq", "datasets", "create", dataset, "--format=json"},
			Assert: contains(dataset), Expect: ExpectPass,
		},
		{
			Name: "bigquery datasets list (after create)", Service: "bigquery",
			Args:   []string{"alpha", "bq", "datasets", "list", "--format=json"},
			Assert: contains(dataset), Expect: ExpectPass,
		},

		// ── Dataproc ─────────────────────────────────────────────────────────
		{
			Name: "dataproc clusters list", Service: "dataproc",
			Args:   []string{"dataproc", "clusters", "list", "--region=us-central1", "--format=json"},
			Assert: jsonArray, Expect: ExpectPass,
		},
		{
			Name: "dataproc clusters create", Service: "dataproc",
			Args:   []string{"dataproc", "clusters", "create", cluster, "--region=us-central1", "--format=json"},
			Expect: ExpectPass,
		},
		{
			Name: "dataproc clusters describe", Service: "dataproc",
			Args:   []string{"dataproc", "clusters", "describe", cluster, "--region=us-central1", "--format=json"},
			Assert: contains(cluster), Expect: ExpectPass,
		},
		{
			Name: "dataproc clusters list (after create)", Service: "dataproc",
			Args:   []string{"dataproc", "clusters", "list", "--region=us-central1", "--format=json"},
			Assert: contains(cluster), Expect: ExpectPass,
		},
	}
}

// commandTimeout lets long-running create/deploy operations (SQL, Dataproc,
// Workflows) take longer than a plain list.
func commandTimeout(name string) time.Duration {
	switch {
	case strings.Contains(name, "create"), strings.Contains(name, "deploy"), strings.Contains(name, "add"):
		return 150 * time.Second
	default:
		return 60 * time.Second
	}
}

// ── Assert helpers ───────────────────────────────────────────────────────────

func jsonArray(stdout string) error {
	var v []any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &v); err != nil {
		return fmt.Errorf("stdout is not a JSON array: %w", err)
	}
	return nil
}

func jsonObject(stdout string) error {
	var v map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &v); err != nil {
		return fmt.Errorf("stdout is not a JSON object: %w", err)
	}
	return nil
}

// contains asserts the needle appears in stdout.
func contains(needle string) func(stdout string) error {
	return func(stdout string) error {
		if !strings.Contains(stdout, needle) {
			return fmt.Errorf("stdout does not contain %q", needle)
		}
		return nil
	}
}
