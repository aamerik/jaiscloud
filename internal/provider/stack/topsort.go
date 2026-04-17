package stack

import "fmt"

// topoSort returns resource logical IDs in dependency order (dependencies first).
// Explicit DependsOn and implicit Ref/Fn::GetAtt references are both honoured.
// Returns an error if a cycle is detected.
func topoSort(resources map[string]any) ([]string, error) {
	knownResources := make(map[string]struct{}, len(resources))
	for id := range resources {
		knownResources[id] = struct{}{}
	}

	// deps[id] = list of IDs that 'id' directly depends on.
	deps := make(map[string][]string, len(resources))
	inDeg := make(map[string]int, len(resources))
	for id := range resources {
		deps[id] = nil
		inDeg[id] = 0
	}

	addDep := func(from, to string) {
		if _, ok := knownResources[to]; !ok {
			return // reference to parameter or external — ignore
		}
		if from == to {
			return
		}
		for _, existing := range deps[from] {
			if existing == to {
				return // deduplicate
			}
		}
		deps[from] = append(deps[from], to)
		inDeg[from]++
	}

	for id, res := range resources {
		m, ok := res.(map[string]any)
		if !ok {
			continue
		}

		// Explicit DependsOn (string or list).
		switch d := m["DependsOn"].(type) {
		case string:
			addDep(id, d)
		case []any:
			for _, dep := range d {
				addDep(id, fmt.Sprintf("%v", dep))
			}
		}

		// Implicit dependencies from Ref / Fn::GetAtt in Properties.
		if props, ok := m["Properties"]; ok {
			for _, ref := range findRefs(props, knownResources) {
				addDep(id, ref)
			}
		}
		// Also scan Condition references in the resource body.
		if cond, ok := m["Condition"]; ok {
			_ = cond // conditions don't create resource dependencies
		}
	}

	// Kahn's algorithm: start with all nodes that have no dependencies.
	queue := make([]string, 0, len(resources))
	for id := range resources {
		if inDeg[id] == 0 {
			queue = append(queue, id)
		}
	}
	sortStrings(queue)

	order := make([]string, 0, len(resources))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		order = append(order, cur)

		// For every node that lists 'cur' as a dependency, decrement in-degree.
		for node := range resources {
			for _, dep := range deps[node] {
				if dep == cur {
					inDeg[node]--
					if inDeg[node] == 0 {
						queue = append(queue, node)
						sortStrings(queue)
					}
					break
				}
			}
		}
	}

	if len(order) != len(resources) {
		return nil, fmt.Errorf("cloudformation: circular dependency detected in resource graph")
	}
	return order, nil
}

// sortStrings insertion-sorts a string slice in place (small slices, n ≤ ~50).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
