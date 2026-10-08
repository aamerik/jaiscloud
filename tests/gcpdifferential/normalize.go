//go:build gcp_differential

package gcpdifferential

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
)

// objectIDGeneration matches a GCS object id of the form
// <bucket>/<object>/<generation> after project/resource substitution, folding
// the embedded generation so it is stable across runs.
var objectIDGeneration = regexp.MustCompile(`^(<bucket>/.*)/[0-9]+$`)

// opaqueHexID matches a scalar id that is entirely hexadecimal/decimal. Such
// ids are server-generated and differ between real GCP and the emulator (e.g.
// Cloud DNS managed-zone ids are decimal, change ids are hex in the emulator
// and decimal on real GCP). They are folded to <id> — but ONLY in responses:
// request bodies are produced by the harness and must never be rewritten.
var opaqueHexID = regexp.MustCompile(`^[0-9a-fA-F]+$`)

// operationName matches a resolved google.longrunning Operation resource name
// ("projects/<project>/{locations|regions}/{scope}/operations/{id}"). The id is
// server-generated and random on both sides, so it is folded to <operation>.
// Dataproc publishes region-scoped names and every other service location-scoped,
// so both segments are recognized (the scope segment is preserved). It is applied
// to every string value; only this exact name shape matches.
var operationName = regexp.MustCompile(`^projects/<project>/(locations|regions)/([^/]+)/operations/[^/]+$`)

// Normalizer rewrites captured JSON so goldens are stable across runs and
// contain no project-specific secrets. It performs two passes:
//
//  1. Textual substitution of concrete identifiers (project id/number, the
//     run suffix, concrete resource names) with placeholders.
//  2. Structural normalization of volatile fields (etags, generations,
//     timestamps, tokens, hashes, ...) keyed by field name, plus deterministic
//     ordering of list responses whose element order is unspecified.
type Normalizer struct {
	repls [][2]string
}

// NewNormalizer builds a Normalizer for one run. resourceNames are the concrete
// run-suffixed identifiers; they are folded to generic placeholders.
func NewNormalizer(project, projectNumber, suffix string, names ResourceNames) *Normalizer {
	repls := [][2]string{
		{project, "<project>"},
		// Real GCP canonicalizes resource names to the project *number* while
		// clients address resources by project *id*; the emulator echoes
		// whichever id it was given. Both are valid aliases for the same
		// project, so they collapse to a single placeholder — otherwise the
		// alias difference manufactures a false divergence.
		{projectNumber, "<project>"},
		// Fixed KMS names are stable but still project resources.
		{FixedKMSKeyRing, "<keyRing>"},
		{FixedKMSCryptoKey, "<cryptoKey>"},
		// Concrete run resources (longest first below).
		{names.Bucket, "<bucket>"},
		{names.Topic, "<topic>"},
		{names.Sub, "<subscription>"},
		{names.Secret, "<secret>"},
		{names.DS, "<dataset>"},
		{names.Table, "<table>"},
		// Cloud DNS run resources. The record-set name embeds the zone's DNS
		// name, so it must be listed first to win the longest-match ordering
		// (the sort below also enforces this).
		{names.DNSRRSet, "<rrset>"},
		{names.DNSName, "<dnsName>"},
		{names.DNSZone, "<dnsZone>"},
		// Cloud Workflows run resource.
		{names.Workflow, "<workflow>"},
		// Firestore run resources. The document name embeds the collection, so
		// the longest-first sort makes the collection win.
		{names.FSCollection, "<fsCollection>"},
		{names.FSDoc, "<fsDoc>"},
		// Metadata-only control-plane probes (G6): always-absent resources whose
		// names carry the run suffix; fold them so a golden is stable.
		{names.ComputeInstance, "<computeInstance>"},
		{names.SQLInstance, "<sqlInstance>"},
		{names.RedisInstance, "<redisInstance>"},
		// AUD6 breadth: the GKE/Dataproc read-only probes, the custom log name
		// and the nonexistent metric type all embed the run suffix.
		{names.ContainerCluster, "<containerCluster>"},
		{names.DataprocCluster, "<dataprocCluster>"},
		{names.LogName, "<logName>"},
		{names.MetricType, "<metricType>"},
		// AUD6-1 gRPC differential: the Datastore kind and the entity written
		// into it are run-suffixed, so both fold to placeholders.
		{names.DSEntity, "<dsEntity>"},
		{names.DSKind, "<dsKind>"},
		// A missing service-account probe is an email-shaped 404 path; fold it
		// too so a golden never carries an "@" or the gserviceaccount domain.
		{"missing-" + suffix + "@" + project + ".iam.gserviceaccount.com", "<serviceAccount>"},
		{"missing-" + suffix, "<missing>"},
		{"missing_" + suffix, "<missing>"},
		// Fallback: any residual occurrence of the run suffix.
		{suffix, "<suffix>"},
	}
	// IAM service account. Fold the full email before the project so a committed
	// golden never contains an "@" or the .iam.gserviceaccount.com domain
	// (TestGoldensAreClean forbids both).
	if names.ServiceAccount != "" {
		repls = append(repls,
			[2]string{names.ServiceAccount + "@" + project + ".iam.gserviceaccount.com", "<serviceAccount>"},
			[2]string{names.ServiceAccount, "<serviceAccountId>"},
		)
	}
	// Longest values first so a longer resource name is replaced before a
	// shorter substring of it.
	sort.SliceStable(repls, func(i, j int) bool { return len(repls[i][0]) > len(repls[j][0]) })
	return &Normalizer{repls: repls}
}

// operationIDSuffix matches a trailing long-running-operation id in a request
// path (e.g. ".../operations/operation-1789941434240-..."). Real GCP mints a
// fresh id per operation, so an LRO poll path would otherwise churn the golden
// on every recording.
var operationIDSuffix = regexp.MustCompile(`/operations/[^/]+$`)

// Path normalizes a request path: textual resource substitution plus folding a
// trailing long-running-operation id. Exchange.Path is a report label (matching
// is by Service/Op), so folding only removes per-run churn from committed
// goldens.
func (n *Normalizer) Path(p string) string {
	return operationIDSuffix.ReplaceAllString(n.substitute(p), "/operations/<operation>")
}

// substitute applies textual replacements.
func (n *Normalizer) substitute(s string) string {
	for _, r := range n.repls {
		if r[0] == "" {
			continue
		}
		s = strings.ReplaceAll(s, r[0], r[1])
	}
	return s
}

// Bytes normalizes a raw response body. Invalid JSON is treated as a plain
// string (e.g. XML/HTML error pages) and only textually substituted. Response
// semantics are used, which permits folding server-generated opaque ids.
func (n *Normalizer) Bytes(b []byte) json.RawMessage { return n.normalize(b, false) }

// RequestBytes normalizes a raw request body. Request bodies are produced by
// this harness, so server-generated-id folding is disabled: an id-shaped
// client value (e.g. BigQuery's "id":"1" row key) must survive verbatim.
func (n *Normalizer) RequestBytes(b []byte) json.RawMessage { return n.normalize(b, true) }

func (n *Normalizer) normalize(b []byte, request bool) json.RawMessage {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 {
		return nil
	}
	if !json.Valid(trimmed) {
		// Normalize as a scalar string so callers always get valid JSON.
		out, _ := json.Marshal(n.substitute(string(trimmed)))
		return out
	}
	var v any
	if err := json.Unmarshal(trimmed, &v); err != nil {
		out, _ := json.Marshal(n.substitute(string(trimmed)))
		return out
	}
	norm := n.value("", v, request)
	out, err := marshalCompact(norm)
	if err != nil {
		return nil
	}
	return out
}

// marshalCompact encodes JSON without HTML-escaping (<, >, &), keeping the
// placeholders like <project> readable in committed goldens.
func marshalCompact(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// marshalIndent is marshalCompact with two-space indentation.
func marshalIndent(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// volatileStringKeys maps a JSON field name to the placeholder that replaces
// its value. Values may be strings or numbers; both sides collapse to the same
// placeholder, so type drift introduced here cancels in the diff.
var volatileStringKeys = map[string]string{
	"etag":             "<etag>",
	"generation":       "<generation>",
	"metageneration":   "<metageneration>",
	"selfLink":         "<selfLink>",
	"mediaLink":        "<mediaLink>",
	"timeCreated":      "<time>",
	"updated":          "<time>",
	"createTime":       "<time>",
	"updateTime":       "<time>",
	"deleteTime":       "<time>",
	"expireTime":       "<time>",
	"timestampTime":    "<time>",
	"publishTime":      "<time>",
	"destroyTime":      "<time>",
	"validAfterTime":   "<time>",
	"validBeforeTime":  "<time>",
	"creationTime":     "<time>",
	"lastModifiedTime": "<time>",
	"lastModified":     "<time>",
	// Cloud Workflows assigns a per-revision identifier (e.g. "000001-a4d")
	// that changes on every source update and differs between real GCP and
	// the emulator.
	"revisionId":    "<revisionId>",
	"nextPageToken": "<nextPageToken>",
	"requestId":     "<requestId>",
	"md5Hash":       "<md5Hash>",
	"crc32c":        "<crc32c>",
	"sha256Hash":    "<sha256Hash>",
	"ciphertext":    "<ciphertext>",
	"ackId":         "<ackId>",
	"messageId":     "<messageId>",
	"jobId":         "<jobId>",
	"temporaryHold": "<temporaryHold>",
	// IAM service accounts generate these server-side (the emulator leaves
	// oauth2ClientId empty), so fold both sides to a placeholder.
	"uniqueId":       "<uniqueId>",
	"oauth2ClientId": "<oauth2ClientId>",
	// Cloud Workflows reports a defaulted serviceAccount (a project SA email);
	// fold it so no golden carries an email address.
	"serviceAccount": "<serviceAccount>",
	// BigQuery job/query scheduling is wall-clock dependent.
	"startTime":   "<time>",
	"endTime":     "<time>",
	"queryId":     "<queryId>",
	"totalSlotMs": "<totalSlotMs>",
	// CRC32C of random/opaque payloads is itself random.
	"dataCrc32c":       "<crc32c>",
	"ciphertextCrc32c": "<crc32c>",
	// BigQuery dataset access carries the creating user's email.
	"userByEmail": "<userByEmail>",
	// GCS bucket.projectNumber is the project number; the emulator does not
	// track one and emits "0". Collapse the field to the same <project>
	// placeholder as the project id/number so the informational field cannot
	// manufacture a divergence.
	"projectNumber": "<project>",
	// List counts are polluted by unrelated real-project resources; the element
	// arrays are scoped to this harness's resources, so the raw count is noise.
	"totalSize": "<totalSize>",
	// Cloud Datastore entity versions are opaque, monotonically increasing
	// server-side values that differ between real GCP and the emulator.
	"version": "<version>",
}

// volatileResponseKeys are fields that are server-generated in a response and
// must be folded there, but that a request body may legitimately carry as a
// client-authored value which must survive verbatim.
var volatileResponseKeys = map[string]string{
	// BigQuery's tabledata.insertAll request carries a client-supplied "insertId"
	// dedup key ("1") while Cloud Logging's ListLogEntries response carries a
	// server-generated "insertId", so the key cannot be folded unconditionally.
	"insertId": "<insertId>",
	// AUD6-1 gRPC differential. protojson carries these server-generated or
	// wall-clock response fields: Datastore/Firestore read/commit/snapshot times,
	// query cursors and transaction ids, Datastore's index-update count, and
	// Cloud Logging's timestamp/receive timestamp. They are folded in responses
	// only, so a committed gRPC golden is stable without ever rewriting a
	// harness-authored request body.
	"readTime":         "<time>",
	"snapshotVersion":  "<snapshotVersion>",
	"commitTime":       "<time>",
	"endCursor":        "<cursor>",
	"skippedCursor":    "<cursor>",
	"cursor":           "<cursor>",
	"transaction":      "<transaction>",
	"indexUpdates":     "<indexUpdates>",
	"timestamp":        "<time>",
	"receiveTimestamp": "<time>",
}

// volatileObjectKeys are fields whose entire value is opaque/volatile and is
// replaced by a fixed object so shape differences still surface but content
// churn does not.
var volatileObjectKeys = map[string]bool{
	"jobReference": true,
}

// volatileArrayKeys maps a field to a fixed placeholder value that replaces the
// whole array (used for arrays of opaque, per-run identifiers).
var volatileArrayKeys = map[string]any{
	"messageIds": []any{"<messageId>"},
	// Cloud DNS synthesizes a delegation set per managed zone whose names
	// differ between real GCP and the emulator; the count is not part of
	// this harness's contract, so the whole array folds to one placeholder.
	"nameServers": []any{"<nameServer>"},
}

// sortArrayKeys are collection fields whose element order is unspecified. Their
// normalized elements are sorted by canonical JSON so record and replay agree.
var sortArrayKeys = map[string]bool{
	"items":            true,
	"buckets":          true,
	"topics":           true,
	"subscriptions":    true,
	"secrets":          true,
	"keyRings":         true,
	"cryptoKeys":       true,
	"keys":             true,
	"tables":           true,
	"datasets":         true,
	"versions":         true,
	"receivedMessages": true,
	"rrsets":           true,
	// Cloud DNS and Cloud Workflows list responses.
	"managedZones": true,
	"workflows":    true,
	// IAM service-account list.
	"accounts": true,
	// Firestore document list.
	"documents": true,
	// AUD6 breadth: GKE/Dataproc cluster lists, workflow-execution lists and
	// the project log-name list have unspecified element order.
	"clusters":   true,
	"executions": true,
	"logNames":   true,
	// AUD6-2 breadth: Eventarc trigger, Cloud Tasks queue, Cloud Run service,
	// Cloud Functions function and Metastore service lists.
	"triggers":  true,
	"queues":    true,
	"services":  true,
	"functions": true,
	// AUD6-1 gRPC differential: Datastore lookup/query result arrays, Cloud
	// Monitoring descriptor lists and Cloud Logging entry lists have no
	// guaranteed element order across the two backends.
	"found":             true,
	"missing":           true,
	"entityResults":     true,
	"metricDescriptors": true,
	"entries":           true,
}

// scopedListPlaceholders maps a collection field to the placeholder that
// identifies the resources this harness creates. List responses are filtered to
// those elements so a committed golden is not polluted by unrelated resources
// that happen to exist in the real project (e.g. another keyring).
var scopedListPlaceholders = map[string]string{
	"items":         "<bucket>",
	"buckets":       "<bucket>",
	"topics":        "<topic>",
	"subscriptions": "<subscription>",
	"secrets":       "<secret>",
	"keyRings":      "<keyRing>",
	"cryptoKeys":    "<cryptoKey>",
	"datasets":      "<dataset>",
	"tables":        "<table>",
	"versions":      "<secret>",
	// Cloud DNS: a real managed zone carries the zone's auto-created NS/SOA
	// records plus any user records, so the rrset list is scoped to the record
	// this harness creates; the zone list is scoped to this run's zone.
	"rrsets":       "<rrset>",
	"managedZones": "<dnsZone>",
	// Cloud Workflows: scoped to this run's workflow.
	"workflows": "<workflow>",
	// IAM: scoped to this run's service account.
	"accounts": "<serviceAccount>",
	// Firestore: scoped to this run's collection.
	"documents": "<fsCollection>",
	// Memorystore: scoped to this run's (always-absent) instance, so unrelated
	// real-project instances cannot pollute the empty-list golden.
	"instances": "<redisInstance>",
	// AUD6 breadth. GKE/Dataproc clusters are a read-only smoke that creates no
	// resource, so the list is forced empty: the placeholder matches no
	// normalized element name (the probes fold to <containerCluster> /
	// <dataprocCluster>), which keeps unrelated real-project clusters out of the
	// routing/empty-shape golden — the same intent as compute's `items` →
	// `<bucket>`. The logging log-name list is scoped to this run's custom log,
	// which drops the real project's always-present cloudaudit logs.
	"clusters": "<cluster>",
	"logNames": "<logName>",
	// AUD6-2 breadth. Each is a read-only smoke that creates no resource, so
	// the list is forced empty: the placeholder matches no normalized element
	// name, which keeps unrelated real-project resources out of the
	// routing/empty-shape golden — the same intent as the GKE/Dataproc
	// "clusters" rule above.
	"triggers":  "<trigger>",
	"queues":    "<queue>",
	"services":  "<service>",
	"functions": "<function>",
}

// value normalizes a decoded JSON value, rewriting volatile fields and sorting
// unspecified-order collections. request is true when the value came from a
// harness-authored request body; it suppresses server-generated-id folding.
func (n *Normalizer) value(key string, v any, request bool) any {
	if ph, ok := volatileStringKeys[key]; ok {
		return ph
	}
	// Response-only volatile fields: server-generated in a response, but a
	// client-authored request value (e.g. BigQuery's insertId dedup key) must
	// survive verbatim.
	if !request {
		if ph, ok := volatileResponseKeys[key]; ok {
			return ph
		}
	}
	if volatileObjectKeys[key] {
		return map[string]any{}
	}
	if ph, ok := volatileArrayKeys[key]; ok {
		return ph
	}
	switch t := v.(type) {
	case string:
		s := n.substitute(t)
		if key == "id" {
			s = objectIDGeneration.ReplaceAllString(s, "$1/<generation>")
			// Server-generated opaque ids (Cloud DNS zone/change ids) are
			// folded, but only in responses: request bodies are harness-authored.
			if !request && opaqueHexID.MatchString(s) {
				return "<id>"
			}
		}
		// Cloud Workflows / Dataproc long-running-operation names carry a random
		// id; fold it while preserving the location/region scope segment.
		s = operationName.ReplaceAllString(s, "projects/<project>/$1/$2/operations/<operation>")
		if looksLikeTimestamp(s) {
			return "<time>"
		}
		return s
	case []any:
		out := make([]any, 0, len(t))
		for _, e := range t {
			out = append(out, n.value(key, e, request))
		}
		if ph := scopedListPlaceholders[key]; ph != "" {
			kept := out[:0]
			for _, e := range out {
				if strings.Contains(canonical(e), ph) {
					kept = append(kept, e)
				}
			}
			out = kept
		}
		if sortArrayKeys[key] {
			sort.SliceStable(out, func(i, j int) bool {
				return canonical(out[i]) < canonical(out[j])
			})
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = n.value(k, e, request)
		}
		return out
	default:
		return v
	}
}

func looksLikeTimestamp(s string) bool {
	// RFC3339 with fractional seconds and trailing Z or offset, e.g.
	// 2026-09-20T10:36:52.284003581Z. Cheap shape check; avoids pulling in time
	// parsing for every string.
	if len(s) < 20 || s[4] != '-' || s[7] != '-' || s[10] != 'T' {
		return false
	}
	if s[len(s)-1] != 'Z' && s[len(s)-1] != 'z' && !strings.ContainsRune(s, '+') {
		if !strings.Contains(s, "-") || strings.LastIndexByte(s, '-') <= 10 {
			return false
		}
	}
	for i := 0; i < 19; i++ {
		c := s[i]
		switch i {
		case 4, 7:
			if c != '-' {
				return false
			}
		case 10:
			if c != 'T' {
				return false
			}
		case 13, 16:
			if c != ':' {
				return false
			}
		default:
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

func canonical(v any) string {
	b, err := marshalCompact(v)
	if err != nil {
		return ""
	}
	return string(b)
}
