//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"encoding/json"
	"testing"
)

// failFindings returns the findings that would fail the gate.
func failFindings(fs []Finding) []Finding {
	var out []Finding
	for _, f := range fs {
		if f.failing() {
			out = append(out, f)
		}
	}
	return out
}

func kinds(fs []Finding) map[string]int {
	m := map[string]int{}
	for _, f := range fs {
		m[f.Kind]++
	}
	return m
}

// TestMutationNormalizeFoldsTwinSide proves the gRPC (...-grpc-…) and REST
// (...-rest-…) twins normalize to one canonical mutation-response body, so the
// twin token itself never decides a mutation-parity comparison (AUD3-12).
func TestMutationNormalizeFoldsTwinSide(t *testing.T) {
	grpc := json.RawMessage(`{"name":"projects/p/topics/topic-grpc-abc123","topic":"projects/p/topics/topic-grpc-abc123"}`)
	rest := json.RawMessage(`{"name":"projects/p/topics/topic-rest-abc123","topic":"projects/p/topics/topic-rest-abc123"}`)

	gn, err := mutationNormalize(grpc, nil)
	if err != nil {
		t.Fatal(err)
	}
	rn, err := mutationNormalize(rest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(gn) != string(rn) {
		t.Fatalf("twins must fold to one body:\n grpc=%s\n rest=%s", gn, rn)
	}
	if fs := compareNormalized("pubsub", "CreateTopic", rn, gn, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("folded twins must not diverge, got %+v", failFindings(fs))
	}
}

// TestMutationParityFailsOnDivergentResponse is the seeded proof that a field
// one transport drops from a Create/Update response is a gate failure — the bug
// class AUD3-12 closes.
func TestMutationParityFailsOnDivergentResponse(t *testing.T) {
	rest := json.RawMessage(`{"name":"projects/p/topics/topic-abc","labels":{"env":"prod"}}`)
	grpc := json.RawMessage(`{"name":"projects/p/topics/topic-abc"}`)

	rn, _ := mutationNormalize(rest, nil)
	gn, _ := mutationNormalize(grpc, nil)
	fs := compareNormalized("pubsub", "CreateTopic", rn, gn, nil)
	if k := kinds(fs); k["missing_field"] != 1 {
		t.Fatalf("want 1 missing_field for the dropped labels, got kinds=%v", k)
	}
	if len(failFindings(fs)) == 0 {
		t.Fatal("a dropped mutation-response field must fail the gate")
	}
}

// TestDifferFailsOnDroppedField is the seeded proof that a field the REST body
// exposes and the gRPC transcode drops is a gate failure — the bug class this
// harness exists to catch.
func TestDifferFailsOnDroppedField(t *testing.T) {
	rest := json.RawMessage(`{"name":"projects/p/secrets/s","labels":{"env":"prod"},"etag":"abc"}`)
	grpc := json.RawMessage(`{"name":"projects/p/secrets/s","etag":"different"}`)

	rn, err := normalizeJSON(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := normalizeJSON(grpc)
	if err != nil {
		t.Fatal(err)
	}
	fs := compareNormalized("secretmanager", "GetSecret", rn, gn, nil)

	k := kinds(fs)
	if k["missing_field"] != 1 {
		t.Fatalf("want 1 missing_field for the dropped labels, got kinds=%v", k)
	}
	var loc string
	for _, f := range fs {
		if f.Kind == "missing_field" {
			loc = f.Location
		}
	}
	if loc != "response.labels" {
		t.Errorf("missing_field location = %q, want response.labels", loc)
	}
	if len(failFindings(fs)) == 0 {
		t.Fatal("a dropped logical field must fail the gate")
	}
}

// TestDifferFailsOnRenamedField proves a renamed field surfaces as both a
// missing (REST) and an extra (gRPC) finding.
func TestDifferFailsOnRenamedField(t *testing.T) {
	rest := json.RawMessage(`{"displayName":"prod"}`)
	grpc := json.RawMessage(`{"name":"prod"}`)

	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	fs := compareNormalized("resourcemanager", "GetProject", rn, gn, nil)

	k := kinds(fs)
	if k["missing_field"] != 1 || k["extra_field"] != 1 {
		t.Fatalf("want missing+extra for a renamed field, got kinds=%v", k)
	}
}

// TestDifferPassesOnEquivalentBodies proves the normalizer equalizes the two
// legitimate representation differences without manufacturing a failure:
// protojson renders a 100ms duration as "0.100s" vs REST "0.1s", and int64 as a
// quoted string vs an unquoted number.
func TestDifferPassesOnEquivalentBodies(t *testing.T) {
	rest := json.RawMessage(`{"retryConfig":{"minBackoff":"0.1s","maxBackoff":"3600s"},"memoryBytes":3221225472,"etag":"x"}`)
	grpc := json.RawMessage(`{"retryConfig":{"minBackoff":"0.100s","maxBackoff":"3600s"},"memoryBytes":"3221225472","etag":"y"}`)

	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	fs := compareNormalized("tasks", "GetQueue", rn, gn, nil)
	if len(failFindings(fs)) != 0 {
		t.Fatalf("equivalent bodies must not fail, got %+v", failFindings(fs))
	}
}

// TestDatastoreValueProjectionCanonicalizesNullValue proves the Datastore
// projection equalizes the one Value-union member whose REST and protojson
// encodings differ — NullValue: the Discovery enum name "NULL_VALUE" vs JSON
// null — while leaving every other member, and any real logical difference,
// intact.
func TestDatastoreValueProjectionCanonicalizesNullValue(t *testing.T) {
	// The same logical entity: REST renders nullValue as the Discovery enum
	// name, protojson renders it as JSON null — at the top level and nested
	// inside an entityValue and an arrayValue, so the recursion is exercised.
	rest := json.RawMessage(`{"found":[{"entity":{"properties":{
		"nul":{"nullValue":"NULL_VALUE"},
		"nest":{"entityValue":{"properties":{"nul":{"nullValue":"NULL_VALUE"}}}},
		"arr":{"arrayValue":{"values":[{"nullValue":"NULL_VALUE"},{"stringValue":"x"}]}},
		"str":{"stringValue":"x"}}}}]}`)
	grpc := json.RawMessage(`{"found":[{"entity":{"properties":{
		"nul":{"nullValue":null},
		"nest":{"entityValue":{"properties":{"nul":{"nullValue":null}}}},
		"arr":{"arrayValue":{"values":[{"nullValue":null},{"stringValue":"x"}]}},
		"str":{"stringValue":"x"}}}}]}`)

	rn, err := datastoreValueProjection(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := datastoreValueProjection(grpc)
	if err != nil {
		t.Fatal(err)
	}
	if string(rn) != string(gn) {
		t.Fatalf("projection must fold the nested nullValue encodings:\n rest=%s\n grpc=%s", rn, gn)
	}
	nr, _ := normalizeJSON(rn)
	ng, _ := normalizeJSON(gn)
	if fs := compareNormalized("datastore", "Lookup", nr, ng, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("projected bodies must not diverge, got %+v", failFindings(fs))
	}

	// A real dropped property must still fail after projection.
	dropped, err := datastoreValueProjection(json.RawMessage(`{"found":[{"entity":{"properties":{"str":{"stringValue":"x"}}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	nd, _ := normalizeJSON(dropped)
	if len(failFindings(compareNormalized("datastore", "Lookup", nr, nd, nil))) == 0 {
		t.Fatal("a property one transport drops must still gate after projection")
	}
}

// TestMutationProjectionAppliesSymmetric proves the mutation path applies a
// MutationParity projection to both sides after the twin token is folded out
// (the datastore CommitResponse carries no Value, so the scenario projection is
// a no-op there; this pins the plumbing directly).
func TestMutationProjectionAppliesSymmetric(t *testing.T) {
	// The two sides differ only in the nullValue encoding; a projection that
	// runs after the twin token is folded must equalize them.
	grpc := json.RawMessage(`{"name":"x-grpc-abc","nul":"null"}`)
	rest := json.RawMessage(`{"name":"x-rest-abc","nul":"NULL_VALUE"}`)
	proj := func(raw json.RawMessage) (json.RawMessage, error) {
		return bytes.ReplaceAll(raw, []byte(`"null"`), []byte(`"NULL_VALUE"`)), nil
	}

	// Without the projection the encoding difference is a real divergence.
	wn, _ := mutationNormalize(grpc, nil)
	wr, _ := mutationNormalize(rest, nil)
	if len(failFindings(compareNormalized("datastore", "Commit", wr, wn, nil))) == 0 {
		t.Fatal("without the projection the two encodings must diverge")
	}

	gn, err := mutationNormalize(grpc, proj)
	if err != nil {
		t.Fatal(err)
	}
	rn, err := mutationNormalize(rest, proj)
	if err != nil {
		t.Fatal(err)
	}
	if string(gn) != string(rn) {
		t.Fatalf("projection must apply after twin folding on both sides:\n grpc=%s\n rest=%s", gn, rn)
	}
	if fs := compareNormalized("datastore", "Commit", rn, gn, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("projected twins must not diverge, got %+v", failFindings(fs))
	}
}

// TestAllowanceAcceptsDocumentedDifference proves an allowance with a reason
// turns a divergence into an accepted, non-failing finding.
func TestAllowanceAcceptsDocumentedDifference(t *testing.T) {
	rest := json.RawMessage(`{"name":"c","bootstrapAddress":"broker:9092"}`)
	grpc := json.RawMessage(`{"name":"c"}`)
	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)

	allow := []Allowance{{Service: "managedkafka", Op: "GetCluster", Path: "response.bootstrapAddress", Reason: "upstream proto has no bootstrap_address"}}
	fs := compareNormalized("managedkafka", "GetCluster", rn, gn, allow)
	if len(failFindings(fs)) != 0 {
		t.Fatalf("an allowed divergence must not fail, got %+v", failFindings(fs))
	}
	var allowed int
	for _, f := range fs {
		if f.Allowed {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("want 1 allowed finding, got %d", allowed)
	}
}

// TestNormalizeDropsZeroAndEmptyMembers proves a transport that omits a
// defaulted field does not diverge from one that emits it.
func TestNormalizeDropsZeroAndEmptyMembers(t *testing.T) {
	rest := json.RawMessage(`{"name":"n","labels":{},"count":0,"enabled":false,"desc":""}`)
	grpc := json.RawMessage(`{"name":"n"}`)
	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	fs := compareNormalized("svc", "Op", rn, gn, nil)
	if len(failFindings(fs)) != 0 {
		t.Fatalf("defaulted members must cancel, got %+v", failFindings(fs))
	}
}
