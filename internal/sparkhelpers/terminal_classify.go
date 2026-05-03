package sparkhelpers

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"strings"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
)

// WaitTerminal wraps k8shelpers.WaitTerminal with Spark-specific classification.
//
// Classification rules (in priority order):
//  1. Pod Succeeded (exit 0)                                              → SparkSucceeded=true
//  2. Pod exit 143/129/130 AND logs contain "SparkContext stopped successfully" → SparkSucceeded=true
//  3. Pod exit non-zero AND logs contain "Shutdown hook called" AND no ERROR in last 200 lines → SparkSucceeded=true
//  4. Otherwise SparkSucceeded=false, SparkReason from last ERROR line or pod Reason
func WaitTerminal(ctx context.Context, k8s kubernetes.Interface, handle k8shelpers.JobHandle) (Final, error) {
	base, err := k8shelpers.WaitTerminal(ctx, k8s, handle)
	if err != nil {
		return Final{}, err
	}

	f := Final{Final: base}

	// Rule 1: exit 0.
	if base.Succeeded && base.ExitCode == 0 {
		f.SparkSucceeded = true
		f.SparkReason = "exit 0"
		return f, nil
	}

	// Collect logs for further classification (best-effort, ignore errors).
	logLines := collectLogs(ctx, k8s, handle)

	// Rule 2: signal exit codes + SparkContext stopped marker.
	if (base.ExitCode == 143 || base.ExitCode == 129 || base.ExitCode == 130) &&
		containsLine(logLines, "SparkContext stopped successfully") {
		f.SparkSucceeded = true
		f.SparkReason = "SparkContext stopped successfully"
		return f, nil
	}

	// Rule 3: Shutdown hook + no ERROR in last 200 lines.
	if base.ExitCode != 0 && containsLine(logLines, "Shutdown hook called") && !hasErrorInTail(logLines, 200) {
		f.SparkSucceeded = true
		f.SparkReason = "Shutdown hook called (no errors in tail)"
		return f, nil
	}

	// Rule 4: failure.
	f.SparkSucceeded = false
	f.SparkReason = lastErrorLine(logLines)
	if f.SparkReason == "" {
		f.SparkReason = base.Reason
	}
	return f, nil
}

// collectLogs fetches logs from the main container of the job's pod.
// Returns individual log lines. Log fetch errors are logged at WARN and
// treated as best-effort — classification falls back to pod-level reason.
func collectLogs(ctx context.Context, k8s kubernetes.Interface, handle k8shelpers.JobHandle) []string {
	var buf bytes.Buffer
	if err := k8shelpers.TailLogs(ctx, k8s, handle, k8shelpers.LogKindMain, &buf); err != nil {
		slog.Warn("sparkhelpers: could not fetch driver logs for Spark classification", "job", handle.JobName, "err", err)
	}
	return splitLines(buf.String())
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

// containsLine reports whether any line contains the given substring.
func containsLine(lines []string, substr string) bool {
	for _, l := range lines {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

// hasErrorInTail reports whether any of the last n lines contains "ERROR".
func hasErrorInTail(lines []string, n int) bool {
	start := len(lines) - n
	if start < 0 {
		start = 0
	}
	for _, l := range lines[start:] {
		if strings.Contains(l, "ERROR") {
			return true
		}
	}
	return false
}

// lastErrorLine returns the last line containing "ERROR", or empty string.
func lastErrorLine(lines []string) string {
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.Contains(lines[i], "ERROR") {
			return lines[i]
		}
	}
	return ""
}

