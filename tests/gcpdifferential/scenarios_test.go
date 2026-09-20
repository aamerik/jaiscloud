//go:build gcp_differential

package gcpdifferential

import (
	"strings"
	"testing"
)

// TestScenariosValid exercises the curated list contract: every scenario is
// addressable (non-empty Service/Op/Method/Path), every Service has a real-GCP
// origin in serviceBaseURL, and (Service, Op) is unique so goldens can be
// matched to scenarios by key instead of by position.
func TestScenariosValid(t *testing.T) {
	const project = "proj"
	const suffix = "abc123"
	scenarios := Scenarios(project, suffix)
	if len(scenarios) == 0 {
		t.Fatal("Scenarios returned no scenarios")
	}

	seen := map[string]bool{}
	var haveDNS, haveWorkflows, haveIAM bool
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Path == "" {
			t.Errorf("scenario %+v has an empty required field", sc)
		}
		if _, ok := serviceBaseURL[sc.Service]; !ok {
			t.Errorf("scenario %s/%s: service %q has no serviceBaseURL entry", sc.Service, sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate (Service, Op): %s/%s", sc.Service, sc.Op)
		}
		seen[k] = true
		switch sc.Service {
		case "dns":
			haveDNS = true
		case "workflows":
			haveWorkflows = true
		case "iam":
			haveIAM = true
		}
	}
	if !haveDNS {
		t.Error("expected at least one dns scenario")
	}
	if !haveWorkflows {
		t.Error("expected at least one workflows scenario")
	}
	if !haveIAM {
		t.Error("expected at least one iam scenario")
	}
}

// TestNormalizerCoversResources guards that every run-suffixed resource name is
// folded to its placeholder, so committed goldens never carry run-specific
// identifiers.
func TestNormalizerCoversResources(t *testing.T) {
	const project = "proj"
	const suffix = "abc123"
	names := Names(suffix)
	norm := NewNormalizer(project, "123456789", suffix, names)

	cases := []struct{ in, want string }{
		{names.Bucket, "<bucket>"},
		{names.Topic, "<topic>"},
		{names.Sub, "<subscription>"},
		{names.Secret, "<secret>"},
		{names.DS, "<dataset>"},
		{names.Table, "<table>"},
		{names.DNSRRSet, "<rrset>"},
		{names.DNSName, "<dnsName>"},
		{names.DNSZone, "<dnsZone>"},
		{names.Workflow, "<workflow>"},
		{names.ServiceAccount + "@" + project + ".iam.gserviceaccount.com", "<serviceAccount>"},
		{names.ServiceAccount, "<serviceAccountId>"},
		{"missing-" + suffix + "@" + project + ".iam.gserviceaccount.com", "<serviceAccount>"},
		{"missing-" + suffix, "<missing>"},
		{project, "<project>"},
	}
	for _, tc := range cases {
		if got := norm.substitute(tc.in); got != tc.want {
			t.Errorf("substitute(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestNormalizerFoldsOpaqueIDs verifies the directional id folding: server
// responses fold generated decimal/hex ids, while harness-authored request
// bodies keep client id values verbatim (e.g. BigQuery's row "id":"1").
func TestNormalizerFoldsOpaqueIDs(t *testing.T) {
	// Use a project id that is not a substring of "projects" (real project ids
	// and the emulator default are both longer/distinct), so the operation-name
	// shape survives textual substitution.
	names := Names("abc123")
	norm := NewNormalizer("differential-proj", "998877665544", "abc123", names)

	resp := string(norm.Bytes([]byte(`{"id":"1234567890"}`)))
	if !strings.Contains(resp, `"<id>"`) {
		t.Errorf("response id was not folded: %s", resp)
	}
	req := string(norm.RequestBytes([]byte(`{"id":"1234567890"}`)))
	if strings.Contains(req, "<id>") {
		t.Errorf("request id must be preserved verbatim: %s", req)
	}

	op := string(norm.Bytes([]byte(`{"name":"projects/differential-proj/locations/us-central1/operations/abcdef123456"}`)))
	if !strings.Contains(op, "/operations/<operation>") {
		t.Errorf("operation name was not folded: %s", op)
	}
}

// TestMatchScenariosToGoldens pins the (Service, Op) matcher contract that lets
// TestReplay run with unrecorded scenarios in the tree: a scenario without a
// golden is pending (not fatal), while a golden without a scenario is an orphan
// (fatal), and duplicate keys are reported.
func TestMatchScenariosToGoldens(t *testing.T) {
	scenarios := []Scenario{
		{Service: "dns", Op: "zone_get", Method: "GET", Path: "/a"},
		{Service: "workflows", Op: "workflow_get", Method: "GET", Path: "/b"},
	}
	goldens := []Exchange{{Service: "dns", Op: "zone_get"}}

	matched, pending, orphans, duplicates := matchScenariosToGoldens(scenarios, goldens)
	if len(matched) != 1 || matched[0].Scenario.Op != "zone_get" {
		t.Fatalf("matched = %+v, want one dns/zone_get pair", matched)
	}
	if len(pending) != 1 || pending[0] != "workflows/workflow_get" {
		t.Fatalf("pending = %v, want [workflows/workflow_get]", pending)
	}
	if len(orphans) != 0 || len(duplicates) != 0 {
		t.Fatalf("orphans = %v, duplicates = %v, want none", orphans, duplicates)
	}

	// A golden with no scenario is an orphan.
	_, _, orphans, _ = matchScenariosToGoldens(scenarios, append(goldens, Exchange{Service: "x", Op: "y"}))
	if len(orphans) != 1 || orphans[0] != "x/y" {
		t.Fatalf("orphans = %v, want [x/y]", orphans)
	}

	// A duplicated scenario key is reported.
	_, _, _, duplicates = matchScenariosToGoldens(
		append(scenarios, Scenario{Service: "dns", Op: "zone_get"}),
		goldens,
	)
	if len(duplicates) != 1 || duplicates[0] != "dns/zone_get" {
		t.Fatalf("duplicates = %v, want [dns/zone_get]", duplicates)
	}
}
