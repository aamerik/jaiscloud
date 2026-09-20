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
}

// Names derives the run's resource identifiers from suffix.
func Names(suffix string) ResourceNames {
	return ResourceNames{
		Bucket: "conf-bucket-" + suffix,
		Topic:  "conf-topic-" + suffix,
		Sub:    "conf-sub-" + suffix,
		Secret: "conf-secret-" + suffix,
		DS:     "conf_ds_" + suffix,
		Table:  "conf_tbl_" + suffix,
	}
}

// Scenarios returns the curated request list for the given project and run
// suffix. It covers storage, pubsub, secretmanager, kms and bigquery — the
// services the emulator and real GCP both expose over REST. datastore, logging
// and monitoring are gRPC-only in the emulator (see internal/gcp/adapter),
// so they are out of scope for this REST differential and documented as such.
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
