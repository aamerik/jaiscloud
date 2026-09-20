//go:build gcp_differential

package gcpdifferential

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Report is the machine-readable report.json payload, matching the shape used
// by tests/gcpconformance (total + by_severity + by_kind + divergences).
type Report struct {
	Target      string         `json:"target"`
	Project     string         `json:"project"`
	Ops         int            `json:"ops"`
	Total       int            `json:"total"`
	BySeverity  map[string]int `json:"by_severity"`
	ByKind      map[string]int `json:"by_kind"`
	ByService   map[string]int `json:"by_service"`
	Divergences []Divergence   `json:"divergences"`
}

// SeverityRank orders severities from most to least severe.
func SeverityRank(sev string) int {
	switch sev {
	case "high":
		return 0
	case "medium":
		return 1
	case "low":
		return 2
	case "info":
		return 3
	default:
		return 4
	}
}

// BuildReport aggregates divergences.
func BuildReport(target, project string, ops int, divs []Divergence) Report {
	if divs == nil {
		divs = []Divergence{}
	}
	rep := Report{
		Target:      target,
		Project:     project,
		Ops:         ops,
		Total:       len(divs),
		BySeverity:  map[string]int{},
		ByKind:      map[string]int{},
		ByService:   map[string]int{},
		Divergences: divs,
	}
	for _, d := range divs {
		rep.BySeverity[d.Severity]++
		rep.ByKind[d.Kind]++
		rep.ByService[d.Service]++
	}
	return rep
}

// WriteReport writes report.json (machine-readable) and report.md (grouped by
// severity then service, with expected vs actual) into dir.
func WriteReport(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}
	if rep.Divergences == nil {
		rep.Divergences = []Divergence{}
	}
	data, err := marshalIndent(rep)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(dir, "report.json"), data, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.md"), []byte(renderMarkdown(rep)), 0o644)
}

func renderMarkdown(rep Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# GCP differential conformance report\n\n")
	fmt.Fprintf(&b, "Target: `%s`  \nProject: `%s`  \nOperations: %d  \nTotal divergences: **%d**\n\n",
		mdEscape(rep.Target), mdEscape(rep.Project), rep.Ops, rep.Total)

	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "| Severity | Count |\n| --- | --- |\n")
	for _, sev := range sortedKeys(rep.BySeverity, severityLess) {
		fmt.Fprintf(&b, "| %s | %d |\n", sev, rep.BySeverity[sev])
	}
	fmt.Fprintf(&b, "\n| Kind | Count |\n| --- | --- |\n")
	for _, k := range sortedKeys(rep.ByKind, nil) {
		fmt.Fprintf(&b, "| %s | %d |\n", k, rep.ByKind[k])
	}
	fmt.Fprintf(&b, "\n| Service | Count |\n| --- | --- |\n")
	for _, s := range sortedKeys(rep.ByService, nil) {
		fmt.Fprintf(&b, "| %s | %d |\n", s, rep.ByService[s])
	}

	grouped := map[string][]Divergence{}
	for _, d := range rep.Divergences {
		grouped[d.Severity] = append(grouped[d.Severity], d)
	}
	for _, sev := range sortedKeys(grouped, severityLess) {
		fmt.Fprintf(&b, "\n## Severity: %s (%d)\n\n", sev, len(grouped[sev]))
		byService := map[string][]Divergence{}
		for _, d := range grouped[sev] {
			svc := d.Service
			if svc == "" {
				svc = "(unknown)"
			}
			byService[svc] = append(byService[svc], d)
		}
		for _, svc := range sortedKeys(byService, nil) {
			fmt.Fprintf(&b, "### Service: %s (%d)\n\n", svc, len(byService[svc]))
			fmt.Fprintf(&b, "| Op | Kind | Location | Expected (real GCP) | Actual (emulator) |\n| --- | --- | --- | --- | --- |\n")
			for _, d := range byService[svc] {
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
					mdEscape(d.Op), mdEscape(d.Kind), mdEscape(d.Location), mdEscape(d.Expected), mdEscape(d.Actual))
			}
			fmt.Fprintf(&b, "\n")
		}
	}
	return b.String()
}

func severityLess(a, b string) bool {
	ra, rb := SeverityRank(a), SeverityRank(b)
	if ra != rb {
		return ra < rb
	}
	return a < b
}

func sortedKeys[V any](m map[string]V, less func(a, b string) bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	if less != nil {
		sort.Slice(keys, func(i, j int) bool { return less(keys[i], keys[j]) })
	} else {
		sort.Strings(keys)
	}
	return keys
}

func mdEscape(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
