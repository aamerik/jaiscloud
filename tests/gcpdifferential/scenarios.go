//go:build gcp_differential

// Package gcpdifferential is an offline-first differential (record/replay)
// conformance harness: it records the responses of a curated, idempotent set of
// requests from REAL GCP into committed goldens, then replays the exact same
// requests against the jaiscloud GCP emulator and diffs the two.
//
// It is deliberately stdlib-only. The recorder authenticates with Application
// Default Credentials via the `gcloud` CLI (never printed, never written to
// disk by this package); the offline replay touches no network beyond the
// local emulator and requires no credentials.
//
// Scope: the recorded set covers the REST services both real GCP and the
// emulator expose (storage, pubsub, secretmanager, kms, bigquery). The curated
// Scenarios list may also contain operations that have not been recorded yet:
// a scenario with no committed golden is reported as "pending recording" and
// skipped by TestReplay, so new breadth can land in the tree without breaking
// the offline gate before the next real-GCP capture. Once the user records,
// every scenario gains a golden and starts being diffed.
//
// gRPC-only surfaces (Firestore, Datastore, Logging, Monitoring, Operations)
// are out of scope for this REST differential.
package gcpdifferential

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// EmulatorProjectDefault matches the emulator's default project.
const EmulatorProjectDefault = "jaiscloud-project"

// RealProjectDefault is the verified real-GCP parity project. It is only a
// default; record mode reads GCP_DIFFERENTIAL_PROJECT to override it.
const RealProjectDefault = "parity-diff-jaiscloud"

// RealProjectNumberDefault is the numeric id of RealProjectDefault.
const RealProjectNumberDefault = "978644905877"

// Fixed KMS resource names. KMS keyRings/cryptoKeys cannot be deleted by GCP
// (a cryptoKey can only be scheduled for destruction, ~30 days), so the harness
// uses stable, reusable names instead of per-run names: at most one keyring and
// one key exist, they are re-used across runs, and they are never destroyed.
const (
	FixedKMSKeyRing   = "jaiscloud-differential"
	FixedKMSCryptoKey = "differential-key"
)

// Scenario is one curated request. Path and Body may reference ${name}
// variables captured from earlier responses via Save. The same Scenario is sent
// to real GCP (record mode) and to the emulator (replay mode); only the base
// URL, project and Authorization header differ.
type Scenario struct {
	// Op is a short, stable operation id used in golden filenames and reports.
	Op          string
	Service     string
	Method      string
	Path        string
	Body        string
	ContentType string
	// Save maps a variable name to a dotted path into the response JSON whose
	// scalar value is captured for use by later scenarios (e.g. the ciphertext
	// returned by KMS encrypt feeding KMS decrypt).
	Save map[string]string
}

// serviceBaseURL maps a service to its real-GCP REST origin. All path prefixes
// (e.g. /storage/v1, /v1/projects/...) are already present in Scenario.Path, so
// the emulator can serve every service from one origin unchanged.
var serviceBaseURL = map[string]string{
	"storage":       "https://storage.googleapis.com",
	"pubsub":        "https://pubsub.googleapis.com",
	"secretmanager": "https://secretmanager.googleapis.com",
	"kms":           "https://cloudkms.googleapis.com",
	"bigquery":      "https://bigquery.googleapis.com",
	// Cloud DNS serves its v1 REST surface under the /dns/v1/ path prefix on
	// the dns.googleapis.com origin.
	"dns": "https://dns.googleapis.com",
	// Cloud Workflows serves /v1/projects/{project}/locations/{location}/...
	// on the workflows.googleapis.com origin.
	"workflows": "https://workflows.googleapis.com",
	// IAM service accounts are served under /v1/projects/{project}/serviceAccounts.
	"iam": "https://iam.googleapis.com",
}

// runSuffix returns a per-run unique, resource-name-safe suffix. Record and
// replay each generate their own; the normalizer folds it back to <suffix> so
// the committed goldens stay stable across runs.
func runSuffix() string {
	return fmt.Sprintf("%06x%06x", os.Getpid()&0xffffff, time.Now().UnixNano()&0xffffff)
}

// ResourceNames are the concrete, run-suffixed identifiers created by the
// scenario set. They are normalized to placeholders so goldens contain no
// project- or run-specific strings.
type ResourceNames struct {
	Bucket string
	Topic  string
	Sub    string
	Secret string
	DS     string
	Table  string
	// Cloud DNS: a managed zone, its DNS name, and one record set in it.
	DNSZone  string
	DNSName  string
	DNSRRSet string
	// Cloud Workflows: one workflow definition.
	Workflow string
	// IAM: the accountId of one service account (the email is derived from it
	// and the project).
	ServiceAccount string
}

// Names derives the run's resource identifiers from suffix.
func Names(suffix string) ResourceNames {
	dnsName := "conf-dns-" + suffix + ".example.com."
	return ResourceNames{
		Bucket:   "conf-bucket-" + suffix,
		Topic:    "conf-topic-" + suffix,
		Sub:      "conf-sub-" + suffix,
		Secret:   "conf-secret-" + suffix,
		DS:       "conf_ds_" + suffix,
		Table:    "conf_tbl_" + suffix,
		DNSZone:  "conf-zone-" + suffix,
		DNSName:  dnsName,
		DNSRRSet: "www." + dnsName,
		Workflow: "conf-workflow-" + suffix,

		ServiceAccount: "conf-sa-" + suffix,
	}
}

// Scenarios returns the curated request list for the given project and run
// suffix. The recorded (golden-backed) services are storage, pubsub,
// secretmanager, kms and bigquery. It additionally carries Cloud DNS and Cloud
// Workflows scenarios that have not been recorded yet; those stay "pending
// recording" (skipped by TestReplay) until a real-GCP capture folds them into
// goldens. datastore, logging and monitoring are gRPC-only in the emulator
// (see internal/gcp/adapter), so they are out of scope for this REST
// differential and documented as such.
//
// The list is ordered so resources exist before they are read and are deleted
// at the end; error (404) responses are included deliberately.
func Scenarios(project, suffix string) []Scenario {
	n := Names(suffix)

	topicName := "projects/" + project + "/topics/" + n.Topic
	subName := "projects/" + project + "/subscriptions/" + n.Sub

	var sc []Scenario

	// ─── Cloud Storage (GCS JSON API) ─────────────────────────────────────────
	sc = append(sc,
		Scenario{Op: "bucket_create", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q}`, n.Bucket)},
		Scenario{Op: "bucket_get", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket},
		Scenario{Op: "buckets_list", Service: "storage", Method: "GET", Path: "/storage/v1/b?project=" + project},
		Scenario{Op: "object_upload", Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + n.Bucket + "/o?uploadType=media&name=hello.txt",
			Body:        "hello jaiscloud",
			ContentType: "text/plain"},
		Scenario{Op: "object_get", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket + "/o/hello.txt"},
		Scenario{Op: "objects_list", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket + "/o"},
		Scenario{Op: "object_get_missing", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket + "/o/missing-" + suffix},
		Scenario{Op: "object_delete", Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + n.Bucket + "/o/hello.txt"},
		Scenario{Op: "bucket_delete", Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + n.Bucket},
		Scenario{Op: "bucket_get_deleted", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket},
	)

	// ─── Pub/Sub ──────────────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Op: "topic_create", Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + project + "/topics/" + n.Topic,
			Body: fmt.Sprintf(`{"name":%q}`, topicName)},
		Scenario{Op: "topic_get", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/topics/" + n.Topic},
		Scenario{Op: "topics_list", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/topics"},
		Scenario{Op: "topic_get_missing", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/topics/missing-" + suffix},
		Scenario{Op: "sub_create", Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub,
			Body: fmt.Sprintf(`{"name":%q,"topic":%q,"ackDeadlineSeconds":10}`, subName, topicName)},
		Scenario{Op: "sub_get", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub},
		Scenario{Op: "subs_list", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/subscriptions"},
		Scenario{Op: "topic_publish", Service: "pubsub", Method: "POST", Path: "/v1/projects/" + project + "/topics/" + n.Topic + ":publish",
			Body: `{"messages":[{"data":"aGVsbG8="}]}`},
		Scenario{Op: "sub_pull", Service: "pubsub", Method: "POST", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub + ":pull",
			Body: `{"maxMessages":10}`},
		Scenario{Op: "sub_delete", Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub},
		Scenario{Op: "topic_delete", Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + project + "/topics/" + n.Topic},
		Scenario{Op: "sub_get_missing", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/subscriptions/missing-" + suffix},
	)

	// ─── Secret Manager ───────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Op: "secret_create", Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + project + "/secrets?secretId=" + n.Secret,
			Body: `{"replication":{"automatic":{}}}`},
		Scenario{Op: "secret_get", Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + project + "/secrets/" + n.Secret},
		Scenario{Op: "secrets_list", Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + project + "/secrets"},
		Scenario{Op: "secret_add_version", Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + project + "/secrets/" + n.Secret + ":addVersion",
			Body: `{"payload":{"data":"c2VjcmV0"}}`},
		Scenario{Op: "secret_access_version", Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + project + "/secrets/" + n.Secret + "/versions/1:access"},
		Scenario{Op: "secret_versions_list", Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + project + "/secrets/" + n.Secret + "/versions"},
		Scenario{Op: "secret_get_missing", Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + project + "/secrets/missing-" + suffix},
		Scenario{Op: "secret_delete", Service: "secretmanager", Method: "DELETE", Path: "/v1/projects/" + project + "/secrets/" + n.Secret},
	)

	// ─── Cloud KMS (read + encrypt/decrypt against fixed, reusable resources) ──
	kmsBase := "/v1/projects/" + project + "/locations/global/keyRings"
	ringPath := kmsBase + "/" + FixedKMSKeyRing
	keyPath := ringPath + "/cryptoKeys/" + FixedKMSCryptoKey
	sc = append(sc,
		Scenario{Op: "keyring_get", Service: "kms", Method: "GET", Path: ringPath},
		Scenario{Op: "keyrings_list", Service: "kms", Method: "GET", Path: kmsBase},
		Scenario{Op: "cryptokey_get", Service: "kms", Method: "GET", Path: keyPath},
		Scenario{Op: "cryptokeys_list", Service: "kms", Method: "GET", Path: ringPath + "/cryptoKeys"},
		Scenario{Op: "cryptokey_encrypt", Service: "kms", Method: "POST", Path: keyPath + ":encrypt",
			Body: `{"plaintext":"aGVsbG8="}`, Save: map[string]string{"ciphertext": "ciphertext"}},
		Scenario{Op: "cryptokey_decrypt", Service: "kms", Method: "POST", Path: keyPath + ":decrypt",
			Body: `{"ciphertext":"${ciphertext}"}`},
		Scenario{Op: "keyring_iam_get", Service: "kms", Method: "GET", Path: ringPath + ":getIamPolicy"},
		Scenario{Op: "cryptokey_get_missing", Service: "kms", Method: "GET", Path: ringPath + "/cryptoKeys/missing-" + suffix},
	)

	// ─── BigQuery ─────────────────────────────────────────────────────────────
	bqBase := "/bigquery/v2/projects/" + project
	sc = append(sc,
		Scenario{Op: "dataset_create", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, project, n.DS)},
		Scenario{Op: "dataset_get", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS},
		Scenario{Op: "datasets_list", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets"},
		Scenario{Op: "table_create", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + n.DS + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"}]}}`, project, n.DS, n.Table)},
		Scenario{Op: "table_get", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table},
		Scenario{Op: "tables_list", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS + "/tables"},
		Scenario{Op: "tabledata_insert_all", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table + "/insertAll",
			Body: `{"rows":[{"insertId":"1","json":{"id":"1"}}]}`},
		Scenario{Op: "tabledata_list", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table + "/data"},
		Scenario{Op: "query", Service: "bigquery", Method: "POST", Path: bqBase + "/queries", Body: `{"query":"SELECT 1"}`},
		Scenario{Op: "dataset_get_missing", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/missing_" + suffix},
		Scenario{Op: "table_delete", Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table},
		Scenario{Op: "dataset_delete", Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + n.DS + "?deleteContents=true"},
	)

	// ─── Cloud DNS (v1 REST, /dns/v1/projects/{project}/managedZones) ─────────
	// A run-suffixed managed zone, its record set (added through a change, the
	// Cloud DNS mutation path), reads, a 404, and no delete scenario: deleting
	// a managed zone answers 204 on real GCP while the emulator answers 200,
	// so the mutation is left to Cleanup() (whose status is not recorded).
	dnsBase := "/dns/v1/projects/" + project + "/managedZones"
	dnsZone := n.DNSZone
	sc = append(sc,
		Scenario{Op: "zone_create", Service: "dns", Method: "POST", Path: dnsBase,
			Body: fmt.Sprintf(`{"name":%q,"dnsName":%q,"description":"jaiscloud differential"}`, dnsZone, n.DNSName)},
		Scenario{Op: "zone_get", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone},
		Scenario{Op: "zones_list", Service: "dns", Method: "GET", Path: dnsBase},
		Scenario{Op: "change_create", Service: "dns", Method: "POST", Path: dnsBase + "/" + dnsZone + "/changes",
			Body: fmt.Sprintf(`{"additions":[{"name":%q,"type":"A","ttl":300,"rrdatas":["192.0.2.1"]}]}`, n.DNSRRSet),
			Save: map[string]string{"changeId": "id"}},
		Scenario{Op: "change_get", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone + "/changes/${changeId}"},
		Scenario{Op: "rrsets_list", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone + "/rrsets"},
		Scenario{Op: "rrset_get", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone + "/rrsets/" + n.DNSRRSet + "/A"},
		Scenario{Op: "zone_get_missing", Service: "dns", Method: "GET", Path: dnsBase + "/missing-" + suffix},
	)

	// ─── Cloud Workflows (/v1/projects/{project}/locations/{location}/workflows) ─
	// Create/update/delete return a done google.longrunning.Operation; the
	// workflow's own get/list reads are plain resources. Delete is the last
	// scenario so the workflow exists for every read above it.
	wfBase := "/v1/projects/" + project + "/locations/us-central1/workflows"
	sc = append(sc,
		Scenario{Op: "workflow_create", Service: "workflows", Method: "POST",
			Path: wfBase + "?workflowId=" + n.Workflow,
			Body: `{"description":"jaiscloud differential","sourceContents":"main:\n  steps:\n    - return: \"ok\"\n"}`},
		Scenario{Op: "workflow_get", Service: "workflows", Method: "GET", Path: wfBase + "/" + n.Workflow},
		Scenario{Op: "workflows_list", Service: "workflows", Method: "GET", Path: wfBase},
		Scenario{Op: "workflow_get_missing", Service: "workflows", Method: "GET", Path: wfBase + "/missing-" + suffix},
		Scenario{Op: "workflow_delete", Service: "workflows", Method: "DELETE", Path: wfBase + "/" + n.Workflow},
	)

	// ─── IAM service accounts (/v1/projects/{project}/serviceAccounts) ───────
	// Create/get/list, the key-ring-style IAM policy read, a 404, then delete.
	// Service-account keys are deliberately excluded: they return private-key
	// material (and signBlob executes a crypto operation), which must not be
	// committed to a golden.
	saBase := "/v1/projects/" + project + "/serviceAccounts"
	saEmail := n.ServiceAccount + "@" + project + ".iam.gserviceaccount.com"
	sc = append(sc,
		Scenario{Op: "sa_create", Service: "iam", Method: "POST",
			Path: saBase + "?accountId=" + n.ServiceAccount,
			Body: `{"serviceAccount":{"displayName":"jaiscloud differential"}}`},
		Scenario{Op: "sa_get", Service: "iam", Method: "GET", Path: saBase + "/" + saEmail},
		Scenario{Op: "sas_list", Service: "iam", Method: "GET", Path: saBase},
		Scenario{Op: "sa_iam_get", Service: "iam", Method: "GET", Path: saBase + "/" + saEmail + ":getIamPolicy"},
		Scenario{Op: "sa_get_missing", Service: "iam", Method: "GET",
			Path: saBase + "/missing-" + suffix + "@" + project + ".iam.gserviceaccount.com"},
		Scenario{Op: "sa_delete", Service: "iam", Method: "DELETE", Path: saBase + "/" + saEmail},
	)

	return sc
}

// expandVars substitutes ${name} references in s using vars.
func expandVars(s string, vars map[string]string) string {
	for name, val := range vars {
		s = strings.ReplaceAll(s, "${"+name+"}", val)
	}
	return s
}

// captureVar walks a dotted path into a decoded JSON value and returns its
// scalar string form.
func captureVar(v any, path string) (string, bool) {
	cur := v
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = m[seg]
		if !ok {
			return "", false
		}
	}
	switch t := cur.(type) {
	case string:
		return t, true
	case bool:
		return fmt.Sprintf("%t", t), true
	case float64:
		return fmt.Sprintf("%v", t), true
	default:
		return "", false
	}
}
