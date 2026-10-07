//go:build gcp_conformance

package gcpconformance

import (
	"strings"
	"testing"
)

// TestSynthProbesMatchResolvedMethods is the offline invariant behind the
// coverage gate: every registry operation that resolves to a Discovery method
// must have at least one synthesized probe whose method+path matches that
// Discovery method. Whether the emulator then returns 2xx is the recorder's
// job; this proves the synthesizer emits a probe for every mapped op.
func TestSynthProbesMatchResolvedMethods(t *testing.T) {
	docs := loadDocs(t)
	ix := BuildMethodIndex(docs)
	byService := resolveOps(docs)

	scenarios := SynthScenarios(docs, "000000000000")
	bySvc := map[string][]Scenario{}
	for _, sc := range scenarios {
		bySvc[sc.Service] = append(bySvc[sc.Service], sc)
	}

	var missing []string
	for svc, ops := range byService {
		for _, ro := range ops {
			matched := false
			for _, sc := range bySvc[svc] {
				if !strings.EqualFold(sc.Method, ro.Method.HTTPMethod) {
					continue
				}
				_, m, ok := ix.MatchMethodInService(svc, sc.Method, sc.Path)
				if ok && m.ID == ro.ID {
					matched = true
					break
				}
			}
			if !matched {
				missing = append(missing, ro.Op.Key()+" ["+ro.ID+"]")
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("synthScenarios emitted no probe matching %d mapped method(s):\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// TestSynthCreateAndSingletonShareID proves a synthesized create and the
// singleton probe that reads the resource use the same id, so the recorded
// create actually stages the resource the later probe needs. It checks the
// eventarc channels pair: at least one created channel id must equal the id of
// a channel get probe.
func TestSynthCreateAndSingletonShareID(t *testing.T) {
	docs := loadDocs(t)
	scenarios := SynthScenarios(docs, "000000000000")

	created := map[string]bool{}
	read := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service != "eventarc" {
			continue
		}
		base := strings.SplitN(sc.Path, "?", 2)[0]
		if sc.Method == "POST" && strings.HasSuffix(base, "/channels") {
			q := strings.SplitN(sc.Path, "?", 2)
			if len(q) == 2 {
				for _, kv := range strings.Split(q[1], "&") {
					if v, ok := strings.CutPrefix(kv, "channelId="); ok {
						created[v] = true
					}
				}
			}
		}
		if sc.Method == "GET" && strings.Contains(base, "/channels/") && !strings.Contains(base, ":") {
			parts := strings.Split(base, "/channels/")
			read[parts[len(parts)-1]] = true
		}
	}
	if len(created) == 0 || len(read) == 0 {
		t.Fatalf("expected eventarc channel create+get probes, got %d created, %d read", len(created), len(read))
	}
	for id := range read {
		if created[id] {
			return
		}
	}
	t.Fatalf("no channel get probe reads a created channel id; created=%v read=%v", keys(created), keys(read))
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestPlaceholderOverrides checks collection ids that must be real catalogue
// names rather than generated ones.
func TestPlaceholderOverrides(t *testing.T) {
	cfg := synthConfigFor("compute")
	cases := map[string]string{
		"machineTypes": "e2-medium",
		"providers":    "pubsub.googleapis.com",
		"versions":     "1",
		"acls":         "allTopics",
	}
	for coll, want := range cases {
		if got := placeholderValue(strings.TrimSuffix(coll, "s")+"Id", coll, cfg, "s", nil); got != want {
			t.Errorf("placeholderValue(%s) = %q, want %q", coll, got, want)
		}
	}
}

// TestActionOverridesResolve guards the resolver's override table: a key that
// is not an enumerated operation, or a value that is not a method id in the
// operation's Discovery snapshot, silently removes the op from the coverage
// gate (Resolve returns false, and the gate treats unmapped ops as out of
// scope). Both are regressions this test catches.
func TestActionOverridesResolve(t *testing.T) {
	docs := loadDocs(t)
	enumerated := map[string]bool{}
	for _, op := range Enumerate() {
		enumerated[op.Key()] = true
	}
	idsByService := map[string]map[string]bool{}
	for svc, doc := range docs {
		m := map[string]bool{}
		doc.WalkMethods(func(mm *Method) {
			if mm.ID != "" {
				m[mm.ID] = true
			}
		})
		idsByService[svc] = m
	}
	for key, want := range actionOverrides {
		if !enumerated[key] {
			t.Errorf("actionOverrides key %q is not an enumerated operation", key)
			continue
		}
		i := strings.IndexByte(key, '.')
		svc := providerPrefixes[key[:i]]
		if svc == "" {
			t.Errorf("actionOverrides key %q has no providerPrefixes entry", key)
			continue
		}
		if !idsByService[svc][want] {
			t.Errorf("actionOverrides[%q] = %q is not a method in the %q snapshot", key, want, svc)
		}
	}
}

// TestSynthPrelude verifies the fixed-name prelude resources the body overrides
// reference exist.
func TestSynthPrelude(t *testing.T) {
	pre := SynthPrelude()
	if len(pre) == 0 {
		t.Fatal("empty prelude")
	}
	var haveTopic, haveBus, haveChannel bool
	for _, sc := range pre {
		switch {
		case strings.Contains(sc.Path, "/topics/conf-probe-topic"):
			haveTopic = true
		case strings.Contains(sc.Path, "messageBuses"):
			haveBus = true
		case strings.Contains(sc.Path, "/channels"):
			haveChannel = true
		}
	}
	if !haveTopic || !haveBus || !haveChannel {
		t.Fatalf("prelude missing resources: topic=%v bus=%v channel=%v", haveTopic, haveBus, haveChannel)
	}
}
