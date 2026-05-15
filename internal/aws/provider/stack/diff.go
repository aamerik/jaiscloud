package stack

import "encoding/json"

// ResourceChange describes a single resource change in a ChangeSet.
type ResourceChange struct {
	Action            string   // "Add" | "Modify" | "Remove"
	LogicalResourceID string
	ResourceType      string
	Replacement       string   // "True" | "False" | "Conditional"
	Scope             []string // changed property names (for Modify)
}

// BuildChangeSet computes the diff between oldTemplate and newTemplate.
// handlers is used to look up ReplacementRules per resource type.
func BuildChangeSet(
	oldTemplate, newTemplate map[string]any,
	handlers map[string]ResourceHandler,
) []ResourceChange {
	oldResources, _ := oldTemplate["Resources"].(map[string]any)
	newResources, _ := newTemplate["Resources"].(map[string]any)

	var changes []ResourceChange

	// Removed resources
	for logicalID, oldRaw := range oldResources {
		if _, exists := newResources[logicalID]; !exists {
			old, _ := oldRaw.(map[string]any)
			resType, _ := old["Type"].(string)
			changes = append(changes, ResourceChange{
				Action: "Remove", LogicalResourceID: logicalID, ResourceType: resType,
			})
		}
	}

	// Added resources
	for logicalID, newRaw := range newResources {
		if _, exists := oldResources[logicalID]; !exists {
			newRes, _ := newRaw.(map[string]any)
			resType, _ := newRes["Type"].(string)
			changes = append(changes, ResourceChange{
				Action: "Add", LogicalResourceID: logicalID, ResourceType: resType,
			})
		}
	}

	// Modified resources
	for logicalID, newRaw := range newResources {
		oldRaw, exists := oldResources[logicalID]
		if !exists {
			continue
		}
		newRes, _ := newRaw.(map[string]any)
		oldRes, _ := oldRaw.(map[string]any)
		newProps, _ := newRes["Properties"].(map[string]any)
		oldProps, _ := oldRes["Properties"].(map[string]any)
		resType, _ := newRes["Type"].(string)

		changed := diffProps(oldProps, newProps)
		if len(changed) == 0 {
			continue
		}

		replacement := "False"
		handler, ok := handlers[resType]
		if ok {
			for _, prop := range changed {
				for _, rr := range handler.ReplacementRules.RequireReplacement {
					if rr == prop {
						replacement = "True"
					}
				}
			}
		}

		changes = append(changes, ResourceChange{
			Action: "Modify", LogicalResourceID: logicalID,
			ResourceType: resType, Replacement: replacement, Scope: changed,
		})
	}

	return changes
}

// diffProps returns property names that differ between old and new.
func diffProps(old, new map[string]any) []string {
	seen := map[string]bool{}
	var changed []string
	for k, nv := range new {
		ov := old[k]
		if !deepEqual(ov, nv) {
			changed = append(changed, k)
		}
		seen[k] = true
	}
	for k := range old {
		if !seen[k] {
			changed = append(changed, k)
		}
	}
	return changed
}

// deepEqual compares two values for equality (JSON-compatible comparison).
func deepEqual(a, b any) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}
