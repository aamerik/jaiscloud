//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Flags are parsed by the test binary: `go test -tags gcp_differential ./... -record`.
var (
	recordFlag = flag.Bool("record", false, "record goldens from REAL GCP (requires ADC)")
	strictFlag = flag.Bool("strict", false, "fail the replay when any divergence is found")
)

const (
	defaultGoldenDir = "testdata/golden"
	defaultReportDir = "testdata/report"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func goldenDir() string { return envOr("GCP_DIFFERENTIAL_GOLDEN_DIR", defaultGoldenDir) }
func reportDir() string { return envOr("GCP_DIFFERENTIAL_REPORT_DIR", defaultReportDir) }

// TestRecord captures goldens from real GCP. It is skipped unless -record (or
// GCP_DIFFERENTIAL_RECORD=1) is set, and requires ADC. It never prints the
// token, and it cleans up every created resource (KMS excepted: GCP cannot
// delete keyrings/keys, so fixed reusable names are used).
func TestRecord(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture goldens from real GCP")
	}
	project := envOr("GCP_DIFFERENTIAL_PROJECT", RealProjectDefault)
	projectNumber := envOr("GCP_DIFFERENTIAL_PROJECT_NUMBER", RealProjectNumberDefault)

	token, err := ADCToken()
	if err != nil {
		t.Fatalf("ADC: %v", err)
	}
	suffix := runSuffix()
	names := Names(suffix)
	target := RealTarget(project, projectNumber, suffix, names, token)
	scenarios := Scenarios(project, suffix)

	if err := target.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS resources: %v", err)
	}

	// Cleanup always runs, even if the capture aborts partway.
	defer func() {
		for _, line := range target.Cleanup() {
			t.Log(line)
		}
		for _, line := range target.VerifyAbsent() {
			t.Log(line)
		}
	}()

	exs, err := target.Run(scenarios)
	if err != nil {
		t.Fatalf("record against real GCP: %v", err)
	}

	dir := goldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write goldens: %v", err)
	}

	byService := map[string]int{}
	for _, ex := range exs {
		byService[ex.Service]++
	}
	t.Logf("recorded %d ops from real GCP project %q", len(exs), project)
	for _, svc := range sortedKeys(byService, nil) {
		t.Logf("  %-16s %d ops", svc, byService[svc])
	}
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d golden files under %s", len(files), dir)
}

// TestReplay is the offline CI entry point: it replays the committed goldens
// against the local emulator, diffs, and writes report.{json,md}. Divergences
// are reported, not fatal, unless -strict is passed.
func TestReplay(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping replay")
	}
	exs, err := ReadGoldens(goldenDir())
	if err != nil {
		t.Skipf("no goldens at %s (run `make record-gcp-differential`): %v", goldenDir(), err)
	}

	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_ENDPOINT", "http://localhost:8080"), "/")
	emulatorProject := envOr("GCP_DIFFERENTIAL_EMULATOR_PROJECT", EmulatorProjectDefault)
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorTarget(endpoint, emulatorProject, suffix, names)

	if err := target.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS on emulator (is it running at %s?): %v", endpoint, err)
	}

	scenarios := Scenarios(emulatorProject, suffix)
	if len(scenarios) != len(exs) {
		t.Fatalf("scenario/golden mismatch: %d scenarios vs %d goldens (regenerate goldens?)", len(scenarios), len(exs))
	}

	actual, err := target.Run(scenarios)
	if err != nil {
		t.Fatalf("replay against emulator: %v", err)
	}

	var divs []Divergence
	for i := range exs {
		divs = append(divs, DiffExchanges(exs[i], actual[i])...)
	}

	rep := BuildReport(target.Name, emulatorProject, len(actual), divs)
	if err := WriteReport(reportDir(), rep); err != nil {
		t.Errorf("write report: %v", err)
	}

	t.Logf("replayed %d ops against emulator -> %d divergences", len(actual), len(divs))
	for _, sev := range sortedKeys(rep.BySeverity, severityLess) {
		t.Logf("by severity: %-8s %d", sev, rep.BySeverity[sev])
	}
	for _, k := range sortedKeys(rep.ByKind, nil) {
		t.Logf("by kind:     %-24s %d", k, rep.ByKind[k])
	}
	t.Logf("report written to %s/report.{json,md}", reportDir())

	if *strictFlag && len(divs) > 0 {
		t.Fatalf("strict mode: %d divergence(s)", len(divs))
	}
}

// TestGoldensAreClean guards the hard requirement that committed goldens never
// contain credentials or project-specific identifiers.
func TestGoldensAreClean(t *testing.T) {
	files, err := GoldenFiles(goldenDir())
	if err != nil {
		t.Skipf("no goldens at %s: %v", goldenDir(), err)
	}
	forbidden := []struct{ name, needle string }{
		{"real project id", RealProjectDefault},
		{"real project number", RealProjectNumberDefault},
		{"emulator project id", EmulatorProjectDefault},
		{"bearer token", "Bearer "},
		{"oauth access token", "ya29."},
		{"private key marker", "PRIVATE KEY"},
		{"service account email", ".iam.gserviceaccount.com"},
		{"email address", "@"},
	}
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(goldenDir(), f))
		if err != nil {
			t.Fatalf("read golden %s: %v", f, err)
		}
		for _, fb := range forbidden {
			if strings.Contains(string(data), fb.needle) {
				t.Errorf("golden %s contains forbidden %s (%q)", f, fb.name, fb.needle)
			}
		}
		var ex Exchange
		if err := json.Unmarshal(data, &ex); err != nil {
			t.Errorf("golden %s is not valid JSON: %v", f, err)
			continue
		}
		if ex.Op == "" || ex.Service == "" || ex.Method == "" || ex.Path == "" {
			t.Errorf("golden %s missing required fields: %+v", f, ex)
		}
	}
}

func TestGoldenManifest(t *testing.T) {
	files, err := GoldenFiles(goldenDir())
	if err != nil {
		t.Skipf("no goldens at %s: %v", goldenDir(), err)
	}
	data, err := os.ReadFile(filepath.Join(goldenDir(), "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if m.Ops != len(files) {
		t.Errorf("manifest ops=%d but %d golden files present", m.Ops, len(files))
	}
	if m.Project != "<project>" {
		t.Errorf("manifest project %q must be the normalized placeholder <project>", m.Project)
	}
}
