// Package transform implements EventBridge Input / InputPath / InputTransformer.
package transform

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Target carries the relevant transformation fields from an EventBridge target.
type Target struct {
	Input            string
	InputPath        string
	InputTransformer *InputTransformer
}

// InputTransformer holds the InputPathsMap and template.
type InputTransformer struct {
	InputPathsMap map[string]string
	InputTemplate string
}

// Apply transforms the event envelope for the given target.
// Returns the transformed payload bytes.
// When no transformation is configured, the full envelope is JSON-encoded.
func Apply(t Target, env map[string]any) ([]byte, error) {
	if t.Input != "" {
		return []byte(t.Input), nil
	}
	if t.InputPath != "" {
		val, err := extractJSONPath(t.InputPath, env)
		if err != nil {
			return nil, fmt.Errorf("transform: InputPath %q: %w", t.InputPath, err)
		}
		return json.Marshal(val)
	}
	if t.InputTransformer != nil {
		return applyTransformer(t.InputTransformer, env)
	}
	return json.Marshal(env)
}

// extractJSONPath evaluates a simple dot-path JSONPath expression (e.g. "$.detail.state")
// against the envelope. Only dot-notation paths without filters are supported.
func extractJSONPath(path string, env map[string]any) (any, error) {
	if !strings.HasPrefix(path, "$.") && path != "$" {
		return nil, fmt.Errorf("path must start with $.")
	}
	if path == "$" {
		return env, nil
	}
	parts := strings.Split(path[2:], ".")
	var cur any = env
	for _, part := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cannot traverse path at %q: not an object", part)
		}
		cur, ok = m[part]
		if !ok {
			return nil, fmt.Errorf("key %q not found", part)
		}
	}
	return cur, nil
}

func applyTransformer(it *InputTransformer, env map[string]any) ([]byte, error) {
	// Evaluate each path in InputPathsMap.
	vars := make(map[string]string, len(it.InputPathsMap))
	for name, path := range it.InputPathsMap {
		val, err := extractJSONPath(path, env)
		if err != nil {
			vars[name] = ""
			continue
		}
		switch v := val.(type) {
		case string:
			vars[name] = v
		default:
			b, _ := json.Marshal(v)
			vars[name] = string(b)
		}
	}

	// Inject predefined keys.
	if arn, ok := env["resources"]; ok {
		if arns, ok := arn.([]any); ok && len(arns) > 0 {
			vars["aws.events.rule-arn"] = fmt.Sprint(arns[0])
		}
	}
	if id, ok := env["id"].(string); ok {
		vars["aws.events.event.ingestion-time"] = id
	}
	if b, err := json.Marshal(env); err == nil {
		vars["aws.events.event"] = string(b)
		vars["aws.events.event.json"] = string(b)
	}

	// Substitute <name> placeholders in template.
	result := it.InputTemplate
	for name, val := range vars {
		result = strings.ReplaceAll(result, "<"+name+">", val)
	}
	return []byte(result), nil
}
