package emroneks

import (
	"strings"

	"jaiscloud/internal/sparkhelpers"
)

// isTerminalJobRunState reports whether a job-run state string is terminal.
func isTerminalJobRunState(s string) bool {
	switch s {
	case "COMPLETED", "FAILED", "CANCELLED":
		return true
	}
	return false
}

// extractJobRunEntryPoint parses StartJobRun jobDriver.sparkSubmitJobDriver into
// a sparkhelpers.EntryPoint, sparkArgs []string, and jarArgs []string.
//
// Returns (nil, nil, nil) when no sparkSubmitJobDriver is present.
func extractJobRunEntryPoint(params map[string]any) (sparkhelpers.EntryPoint, []string, []string) {
	jd, ok := params["jobDriver"].(map[string]any)
	if !ok {
		return nil, nil, nil
	}
	sc, ok := jd["sparkSubmitJobDriver"].(map[string]any)
	if !ok {
		return nil, nil, nil
	}

	entryPointStr, _ := sc["entryPoint"].(string)
	sparkParamsStr, _ := sc["sparkSubmitParameters"].(string)

	var jarArgs []string
	if raw, ok := sc["entryPointArguments"].([]any); ok {
		for _, a := range raw {
			if s, ok := a.(string); ok {
				jarArgs = append(jarArgs, s)
			}
		}
	}

	// Merge configurationOverrides as --conf flags (template keys excluded).
	overrideConfs := flattenAppConfiguration(params)
	const driverTemplateKey = "spark.kubernetes.driver.podTemplateFile"
	const execTemplateKey = "spark.kubernetes.executor.podTemplateFile"
	var confArgs []string
	for k, v := range overrideConfs {
		if k == driverTemplateKey || k == execTemplateKey {
			continue
		}
		confArgs = append(confArgs, "--conf", k+"="+v)
	}

	// Parse sparkSubmitParameters into individual flags.
	sparkArgs := tokeniseSparkParams(sparkParamsStr)
	sparkArgs = append(confArgs, sparkArgs...)

	// Build entry point.
	var ep sparkhelpers.EntryPoint
	lower := strings.ToLower(entryPointStr)
	switch {
	case strings.HasSuffix(lower, ".py"):
		ep = sparkhelpers.PythonEntryPoint{MainPythonFile: entryPointStr}
	case strings.HasSuffix(lower, ".r"):
		ep = sparkhelpers.REntryPoint{MainRFile: entryPointStr}
	default:
		// Extract --class from sparkArgs if present.
		mainClass := ""
		for i, a := range sparkArgs {
			if (a == "--class" || a == "--main-class") && i+1 < len(sparkArgs) {
				mainClass = sparkArgs[i+1]
				break
			}
		}
		ep = sparkhelpers.JarEntryPoint{JarURI: entryPointStr, MainClass: mainClass}
	}

	return ep, sparkArgs, jarArgs
}

// tokeniseSparkParams splits a space-delimited spark-submit parameter string
// into individual tokens (handles quoted strings naively).
func tokeniseSparkParams(s string) []string {
	return strings.Fields(s)
}

// flattenAppConfiguration extracts Spark properties from the nested
// configurationOverrides.applicationConfiguration structure and returns a flat
// key→value map.
func flattenAppConfiguration(params map[string]any) map[string]string {
	out := make(map[string]string)
	co, ok := params["configurationOverrides"].(map[string]any)
	if !ok {
		return out
	}
	appConfs, ok := co["applicationConfiguration"].([]any)
	if !ok {
		return out
	}
	for _, ac := range appConfs {
		acMap, ok := ac.(map[string]any)
		if !ok {
			continue
		}
		props, ok := acMap["properties"].(map[string]any)
		if !ok {
			continue
		}
		for k, v := range props {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
	}
	return out
}
