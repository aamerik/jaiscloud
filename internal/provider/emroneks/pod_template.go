package emroneks

import (
	"strings"

	"jaiscloud/internal/executor/spark"
)

// flattenAppConfiguration extracts Spark properties from the nested
// configurationOverrides.applicationConfiguration structure and returns a flat
// key→value map. Keys not recognised are included verbatim — callers filter
// for spark.kubernetes.* properties.
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

// extractTemplateURIs reads pod-template URIs from configuration overrides and
// sparkSubmitParameters. configurationOverrides take precedence when both
// specify the same key. Returns ("", "") if neither source provides a URI.
func extractTemplateURIs(confOverrides map[string]string, sparkParams string) (driverURI, executorURI string) {
	const driverKey = "spark.kubernetes.driver.podTemplateFile"
	const execKey = "spark.kubernetes.executor.podTemplateFile"

	// Parse sparkSubmitParameters first (lower precedence).
	parsed := parseSparkParams(sparkParams)
	driverURI = parsed[driverKey]
	executorURI = parsed[execKey]

	// configurationOverrides win when set.
	if v, ok := confOverrides[driverKey]; ok && v != "" {
		driverURI = v
	}
	if v, ok := confOverrides[execKey]; ok && v != "" {
		executorURI = v
	}
	return driverURI, executorURI
}

// parseSparkParams splits a space-delimited spark-submit parameter string into
// a map of key→value for --conf k=v tokens. Silently ignores non-conf tokens.
func parseSparkParams(sparkParams string) map[string]string {
	out := make(map[string]string)
	tokens := strings.Fields(sparkParams)
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		if t == "--conf" && i+1 < len(tokens) {
			kv := tokens[i+1]
			if idx := strings.IndexByte(kv, '='); idx > 0 {
				out[kv[:idx]] = kv[idx+1:]
			}
			i++
		} else if strings.HasPrefix(t, "--conf=") {
			kv := strings.TrimPrefix(t, "--conf=")
			if idx := strings.IndexByte(kv, '='); idx > 0 {
				out[kv[:idx]] = kv[idx+1:]
			}
		}
	}
	return out
}

// shouldEngageClusterMode returns true when cluster mode should be activated
// for the job, based on cfg.ClusterMode policy and the presence of template URIs.
//
//   "auto"   (default) — engage when at least one template URI is present
//   "always" — engage even without templates
//   "never"  — never engage, regardless of URIs
func shouldEngageClusterMode(cfg spark.SparkConfig, driverURI, executorURI string) bool {
	switch cfg.ClusterMode {
	case "always":
		return true
	case "never":
		return false
	default: // "auto"
		return driverURI != "" || executorURI != ""
	}
}
