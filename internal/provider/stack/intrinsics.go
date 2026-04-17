package stack

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// resolveCtx carries all state needed to evaluate CloudFormation intrinsic functions.
type resolveCtx struct {
	params       map[string]string      // Parameters: name → resolved value (string after substitution)
	resources    map[string]*cfResource // logicalID → resource (PhysicalResourceId + Attributes set after creation)
	conditions   map[string]bool        // Conditions: name → bool
	mappings     map[string]any         // Mappings section
	pseudoParams map[string]string      // AWS::Region, AWS::AccountId, etc.
}

// newResolveCtx builds a resolveCtx from a parsed template and runtime values.
func newResolveCtx(region, accountID string, port int) *resolveCtx {
	return &resolveCtx{
		params:     make(map[string]string),
		resources:  make(map[string]*cfResource),
		conditions: make(map[string]bool),
		mappings:   make(map[string]any),
		pseudoParams: map[string]string{
			"AWS::Region":        region,
			"AWS::AccountId":     accountID,
			"AWS::Partition":     "aws",
			"AWS::URLSuffix":     "amazonaws.com",
			"AWS::StackName":     "", // set later
			"AWS::StackId":       "", // set later
			"AWS::NoValue":       "",
			"AWS::NotificationARNs": "",
		},
	}
}

// Resolve recursively evaluates a value, expanding all intrinsic functions.
func (rc *resolveCtx) Resolve(val any) any {
	switch v := val.(type) {
	case map[string]any:
		if ref, ok := v["Ref"]; ok && len(v) == 1 {
			return rc.resolveRef(fmt.Sprintf("%v", ref))
		}
		for key, fnVal := range v {
			switch key {
			case "Fn::GetAtt":
				return rc.resolveGetAtt(fnVal)
			case "Fn::Sub":
				return rc.resolveSub(fnVal)
			case "Fn::Join":
				return rc.resolveJoin(fnVal)
			case "Fn::If":
				return rc.resolveIf(fnVal)
			case "Fn::Select":
				return rc.resolveSelect(fnVal)
			case "Fn::Split":
				return rc.resolveSplit(fnVal)
			case "Fn::FindInMap":
				return rc.resolveFindInMap(fnVal)
			case "Fn::Base64":
				return rc.resolveBase64(fnVal)
			case "Fn::Not":
				return rc.resolveNot(fnVal)
			case "Fn::And":
				return rc.resolveAnd(fnVal)
			case "Fn::Or":
				return rc.resolveOr(fnVal)
			case "Fn::Equals":
				return rc.resolveEquals(fnVal)
			case "Fn::Length":
				return rc.resolveLength(fnVal)
			case "Condition":
				name := fmt.Sprintf("%v", fnVal)
				if b, ok := rc.conditions[name]; ok {
					return b
				}
				return false
			}
		}
		// Plain object: resolve each value recursively.
		out := make(map[string]any, len(v))
		for k, vv := range v {
			out[k] = rc.Resolve(vv)
		}
		return out

	case []any:
		out := make([]any, len(v))
		for i, vv := range v {
			out[i] = rc.Resolve(vv)
		}
		return out

	default:
		return val
	}
}

func (rc *resolveCtx) resolveRef(name string) string {
	if v, ok := rc.pseudoParams[name]; ok {
		return v
	}
	if v, ok := rc.params[name]; ok {
		return v
	}
	if r, ok := rc.resources[name]; ok {
		return r.PhysicalResourceId
	}
	return "${" + name + "}"
}

func (rc *resolveCtx) resolveGetAtt(val any) string {
	parts, ok := val.([]any)
	if !ok || len(parts) < 2 {
		return ""
	}
	logicalID := fmt.Sprintf("%v", parts[0])
	attr := fmt.Sprintf("%v", parts[1])
	if r, ok := rc.resources[logicalID]; ok {
		if v, found := r.Attributes[attr]; found {
			return fmt.Sprintf("%v", v)
		}
		// Common fallback: Arn → physical ID when it looks like an ARN.
		if attr == "Arn" && strings.HasPrefix(r.PhysicalResourceId, "arn:") {
			return r.PhysicalResourceId
		}
	}
	return fmt.Sprintf("${%s.%s}", logicalID, attr)
}

func (rc *resolveCtx) resolveSub(val any) string {
	var tmpl string
	var localVars map[string]any
	switch v := val.(type) {
	case string:
		tmpl = v
	case []any:
		if len(v) >= 1 {
			tmpl = fmt.Sprintf("%v", v[0])
		}
		if len(v) >= 2 {
			if m, ok := v[1].(map[string]any); ok {
				localVars = m
			}
		}
	default:
		return fmt.Sprintf("%v", val)
	}
	return subReplace(tmpl, func(key string) string {
		if localVars != nil {
			if vv, ok := localVars[key]; ok {
				return fmt.Sprintf("%v", rc.Resolve(vv))
			}
		}
		return rc.resolveRef(key)
	})
}

// subReplace replaces all ${Key} occurrences in tmpl using lookup.
func subReplace(tmpl string, lookup func(string) string) string {
	var b strings.Builder
	i := 0
	for i < len(tmpl) {
		if tmpl[i] == '$' && i+1 < len(tmpl) && tmpl[i+1] == '{' {
			end := strings.Index(tmpl[i+2:], "}")
			if end >= 0 {
				key := tmpl[i+2 : i+2+end]
				b.WriteString(lookup(key))
				i += 2 + end + 1
				continue
			}
		}
		b.WriteByte(tmpl[i])
		i++
	}
	return b.String()
}

func (rc *resolveCtx) resolveJoin(val any) string {
	parts, ok := val.([]any)
	if !ok || len(parts) < 2 {
		return ""
	}
	delim := fmt.Sprintf("%v", parts[0])
	raw := rc.Resolve(parts[1])
	items, ok := raw.([]any)
	if !ok {
		return ""
	}
	strs := make([]string, len(items))
	for i, item := range items {
		strs[i] = fmt.Sprintf("%v", rc.Resolve(item))
	}
	return strings.Join(strs, delim)
}

func (rc *resolveCtx) resolveIf(val any) any {
	parts, ok := val.([]any)
	if !ok || len(parts) < 3 {
		return nil
	}
	condName := fmt.Sprintf("%v", parts[0])
	if rc.conditions[condName] {
		return rc.Resolve(parts[1])
	}
	return rc.Resolve(parts[2])
}

func (rc *resolveCtx) resolveSelect(val any) any {
	parts, ok := val.([]any)
	if !ok || len(parts) < 2 {
		return nil
	}
	idx := 0
	switch n := parts[0].(type) {
	case float64:
		idx = int(n)
	case int:
		idx = n
	case string:
		fmt.Sscanf(n, "%d", &idx)
	}
	raw := rc.Resolve(parts[1])
	items, ok := raw.([]any)
	if !ok || idx < 0 || idx >= len(items) {
		return nil
	}
	return rc.Resolve(items[idx])
}

func (rc *resolveCtx) resolveSplit(val any) []any {
	parts, ok := val.([]any)
	if !ok || len(parts) < 2 {
		return nil
	}
	delim := fmt.Sprintf("%v", parts[0])
	s := fmt.Sprintf("%v", rc.Resolve(parts[1]))
	strs := strings.Split(s, delim)
	out := make([]any, len(strs))
	for i, ss := range strs {
		out[i] = ss
	}
	return out
}

func (rc *resolveCtx) resolveFindInMap(val any) any {
	parts, ok := val.([]any)
	if !ok || len(parts) < 3 {
		return nil
	}
	mapName := fmt.Sprintf("%v", rc.Resolve(parts[0]))
	key1 := fmt.Sprintf("%v", rc.Resolve(parts[1]))
	key2 := fmt.Sprintf("%v", rc.Resolve(parts[2]))

	m1, ok := rc.mappings[mapName]
	if !ok {
		return nil
	}
	m1map, ok := m1.(map[string]any)
	if !ok {
		return nil
	}
	m2, ok := m1map[key1]
	if !ok {
		return nil
	}
	m2map, ok := m2.(map[string]any)
	if !ok {
		return nil
	}
	return m2map[key2]
}

func (rc *resolveCtx) resolveBase64(val any) string {
	s := fmt.Sprintf("%v", rc.Resolve(val))
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func (rc *resolveCtx) resolveNot(val any) bool {
	parts, ok := val.([]any)
	if !ok || len(parts) < 1 {
		return false
	}
	v := rc.Resolve(parts[0])
	if b, ok := v.(bool); ok {
		return !b
	}
	return false
}

func (rc *resolveCtx) resolveAnd(val any) bool {
	parts, ok := val.([]any)
	if !ok {
		return false
	}
	for _, p := range parts {
		v := rc.Resolve(p)
		if b, ok := v.(bool); ok && !b {
			return false
		}
	}
	return true
}

func (rc *resolveCtx) resolveOr(val any) bool {
	parts, ok := val.([]any)
	if !ok {
		return false
	}
	for _, p := range parts {
		v := rc.Resolve(p)
		if b, ok := v.(bool); ok && b {
			return true
		}
	}
	return false
}

func (rc *resolveCtx) resolveEquals(val any) bool {
	parts, ok := val.([]any)
	if !ok || len(parts) < 2 {
		return false
	}
	a := fmt.Sprintf("%v", rc.Resolve(parts[0]))
	b := fmt.Sprintf("%v", rc.Resolve(parts[1]))
	return a == b
}

func (rc *resolveCtx) resolveLength(val any) int {
	v := rc.Resolve(val)
	if items, ok := v.([]any); ok {
		return len(items)
	}
	return 0
}

// ─── Condition evaluation ─────────────────────────────────────────────────────

// evaluateConditions resolves all Conditions in the template.
func (rc *resolveCtx) evaluateConditions(conditions map[string]any) {
	for name, expr := range conditions {
		v := rc.Resolve(expr)
		if b, ok := v.(bool); ok {
			rc.conditions[name] = b
		}
	}
}

// ─── Parameter resolution ─────────────────────────────────────────────────────

// resolveParameters fills rc.params from template Parameters + caller overrides.
func (rc *resolveCtx) resolveParameters(tplParams map[string]any, callerValues map[string]string) {
	for name, def := range tplParams {
		defMap, ok := def.(map[string]any)
		if !ok {
			continue
		}
		if cv, ok := callerValues[name]; ok {
			rc.params[name] = cv
		} else if dv, ok := defMap["Default"]; ok {
			rc.params[name] = fmt.Sprintf("%v", dv)
		} else {
			rc.params[name] = ""
		}
	}
	// Also add any caller values not in the template (pass-through).
	for k, v := range callerValues {
		if _, exists := rc.params[k]; !exists {
			rc.params[k] = v
		}
	}
}

// ─── Ref scanner ──────────────────────────────────────────────────────────────

// findRefs scans a value for Ref / Fn::GetAtt references to resource logical IDs.
func findRefs(val any, knownResources map[string]struct{}) []string {
	var out []string
	switch v := val.(type) {
	case map[string]any:
		if ref, ok := v["Ref"]; ok {
			name := fmt.Sprintf("%v", ref)
			if _, ok := knownResources[name]; ok {
				out = append(out, name)
			}
		}
		if ga, ok := v["Fn::GetAtt"]; ok {
			if parts, ok := ga.([]any); ok && len(parts) >= 1 {
				name := fmt.Sprintf("%v", parts[0])
				if _, ok := knownResources[name]; ok {
					out = append(out, name)
				}
			}
		}
		for _, vv := range v {
			out = append(out, findRefs(vv, knownResources)...)
		}
	case []any:
		for _, vv := range v {
			out = append(out, findRefs(vv, knownResources)...)
		}
	}
	return out
}
