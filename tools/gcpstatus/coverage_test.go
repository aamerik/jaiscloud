package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// ─── name normalization / attribution ─────────────────────────────────────────

func TestCanonicalService(t *testing.T) {
	cases := map[string]string{
		"storage":              "storage",
		"redis":                "memorystore",
		"sqladmin":             "cloudsql",
		"dns":                  "clouddns",
		"managed-kafka":        "managedkafka",
		"cloudkms":             "kms",
		"cloudfunctions":       "functions",
		"cloudresourcemanager": "resourcemanager",
		"CloudDNS":             "clouddns",
		"":                     "",
	}
	for in, want := range cases {
		if got := canonicalService(in); got != want {
			t.Errorf("canonicalService(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestImportService(t *testing.T) {
	cases := map[string]string{
		// Plain official clients (fallback token + alias).
		"cloud.google.com/go/storage":                   "storage",
		"cloud.google.com/go/logging/apiv2":             "logging",
		"cloud.google.com/go/monitoring/apiv3/v2":       "monitoring",
		"cloud.google.com/go/firestore":                 "firestore",
		"cloud.google.com/go/workflows":                 "workflows",
		"cloud.google.com/go/iam/apiv1":                 "iam",
		"google.golang.org/api/sqladmin/v1beta4":        "cloudsql",
		"google.golang.org/api/redis/v1":                "memorystore",
		"google.golang.org/api/cloudkms/v1":             "kms",
		"google.golang.org/api/cloudresourcemanager/v1": "resourcemanager",
		"google.golang.org/api/cloudfunctions/v2":       "functions",
		"google.golang.org/api/iam/v1":                  "iam",
		"google.golang.org/api/dns/v1":                  "clouddns",
		// Nested / renamed clients: explicit prefix mappings beat the token.
		"cloud.google.com/go/cloudtasks/apiv2":                  "tasks",
		"cloud.google.com/go/iam/credentials/apiv1":             "iamcredentials",
		"cloud.google.com/go/firestore/apiv1/admin":             "firestoreadmin",
		"cloud.google.com/go/workflows/executions/apiv1":        "workflowexecutions",
		"cloud.google.com/go/longrunning/autogen/longrunningpb": "operations",
		// Non-service packages must not become phantom keys.
		"cloud.google.com/go/iam":             "",
		"google.golang.org/api/option":        "",
		"google.golang.org/api/googleapi":     "",
		"google.golang.org/api/iterator":      "",
		"github.com/stretchr/testify/require": "",
		"google.golang.org/grpc":              "",
	}
	for in, want := range cases {
		if got := importService(in); got != want {
			t.Errorf("importService(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCoverageServiceNames(t *testing.T) {
	facts := map[string]matrixServiceFacts{
		// A matrix-only service with no registry descriptor.
		"operations":  {transports: map[string]bool{"grpc": true}, ga: 5, total: 5},
		"memorystore": {transports: map[string]bool{"rest": true}, ga: 0, total: 8},
		"storage":     {transports: map[string]bool{"rest": true, "grpc": true}, ga: 57, total: 57},
	}
	// `redis` is the registry name that aliases to matrix `memorystore`.
	got := coverageServiceNames([]string{"redis", "storage"}, facts)
	want := map[string]bool{"redis": true, "storage": true, "operations": true}
	for _, name := range got {
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("coverageServiceNames missing %v (got %v)", want, got)
	}
	// No duplicate: memorystore must not reappear beside its registry alias.
	for _, name := range got {
		if name == "memorystore" {
			t.Fatalf("coverageServiceNames duplicated the aliased matrix service: %v", got)
		}
	}
	if len(got) != 3 {
		t.Fatalf("coverageServiceNames = %v, want 3 names", got)
	}
}

func TestDedicatedSuiteService(t *testing.T) {
	cases := map[string]string{
		"sdk-clouddns":      "clouddns",
		"sdk-managed-kafka": "managedkafka",
		"sdk-memorystore":   "memorystore",
		"sdk-gcs-grpc":      "", // generic gRPC suite
		"sdk-rest":          "", // generic REST suite
		"sdk":               "", // generic suite
		"terraform":         "",
		".":                 "",
	}
	for in, want := range cases {
		if got := dedicatedSuiteService(in); got != want {
			t.Errorf("dedicatedSuiteService(%q) = %q, want %q", in, got, want)
		}
	}
}

// ─── matrix rollup + ga predicate ─────────────────────────────────────────────

func TestMatrixServiceFactsByService(t *testing.T) {
	mf := matrixFile{Cells: []matrixCell{
		{Service: "storage", Transport: "rest", State: "ga"},
		{Service: "storage", Transport: "grpc", State: "ga"},
		{Service: "container", Transport: "rest", State: "ga"},
		{Service: "container", Transport: "rest", State: "unsupported"},
		{Service: "iceberg", Transport: "rest", State: "preview"},
	}}
	f := matrixServiceFactsByService(mf)
	if got := f["storage"]; got.ga != 2 || got.total != 2 || !got.transports["grpc"] || !serviceIsGA(got) {
		t.Fatalf("storage facts = %+v, want ga 2/2 grpc", got)
	}
	// A ga service may carry explicit unsupported stubs and stay ga.
	if got := f["container"]; !serviceIsGA(got) || got.total != 2 {
		t.Fatalf("container facts = %+v, want ga with an unsupported stub", got)
	}
	// A preview cell makes the service non-ga.
	if got := f["iceberg"]; serviceIsGA(got) {
		t.Fatalf("iceberg must not be ga: %+v", got)
	}
}

// ─── the inventory join (the DoD) ─────────────────────────────────────────────

func TestBuildCoverage(t *testing.T) {
	facts := map[string]matrixServiceFacts{
		"scheduler": {transports: map[string]bool{"rest": true, "grpc": true}, ga: 16, total: 16},
		"storage":   {transports: map[string]bool{"rest": true, "grpc": true}, ga: 57, total: 57},
		"container": {transports: map[string]bool{"rest": true, "grpc": true}, ga: 58, total: 66},
		"bigquery":  {transports: map[string]bool{"rest": true}, ga: 0, total: 25},
		"iceberg":   {transports: map[string]bool{"rest": true}, ga: 0, total: 14, preview: true},
	}
	files := []coverageTestFile{
		{Suite: "sdk", Services: map[string]bool{"storage": true}, Funcs: 3},
		{Suite: "sdk-rest", Services: map[string]bool{"storage": true, "kms": true}, Funcs: 2},
	}
	rows := buildCoverage([]string{"scheduler", "storage", "container", "bigquery", "iceberg", "redis"}, facts, files)
	bySvc := map[string]coverageRow{}
	for _, r := range rows {
		bySvc[r.Service] = r
	}

	// ga + suite -> covered, funcs/suites aggregated.
	if r := bySvc["storage"]; !r.GA || !r.Covered || r.Funcs != 5 ||
		!reflect.DeepEqual(r.Suites, []string{"sdk", "sdk-rest"}) {
		t.Fatalf("storage = %+v, want covered by sdk+sdk-rest (5 funcs)", r)
	}
	// ga + no suite -> uncovered.
	if r := bySvc["scheduler"]; !r.GA || r.Covered || len(r.Suites) != 0 {
		t.Fatalf("scheduler = %+v, want uncovered ga", r)
	}
	// ga with explicit unsupported stubs still requires a suite.
	if r := bySvc["container"]; !r.GA || r.Covered {
		t.Fatalf("container = %+v, want uncovered ga", r)
	}
	// Non-ga services (no ga cells) are not required.
	if r := bySvc["bigquery"]; r.GA {
		t.Fatalf("bigquery must not be ga: %+v", r)
	}
	// A preview cell makes a service non-ga.
	if r := bySvc["iceberg"]; r.GA {
		t.Fatalf("iceberg must not be ga: %+v", r)
	}
	// The registry/matrix name alias is applied (redis -> memorystore).
	if r := bySvc["redis"]; r.Canonical != "memorystore" || r.GA {
		t.Fatalf("redis = %+v, want canonical memorystore and not ga", r)
	}

	if got, want := uncoveredGAServices(rows), []string{"container", "scheduler"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("uncoveredGAServices = %v, want %v", got, want)
	}
}

// ─── test-tree scan ───────────────────────────────────────────────────────────

func TestScanGCPTestFiles(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A dedicated suite: the dir names the service; an aliased import does too.
	write("sdk-clouddns/zone_test.go", `package sdk_clouddns_test

import (
	"testing"

	dns "google.golang.org/api/dns/v1"
)

func TestZone(t *testing.T) {}
func helper(t *testing.T) {}
`)
	// A generic suite: services come from the imports only.
	write("sdk-rest/extra_test.go", `package sdkrest_test

import (
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/cloudkms/v1"
)

func TestKmsAndStorage(t *testing.T) {}
`)

	files, err := scanGCPTestFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]coverageTestFile{}
	for _, f := range files {
		got[f.Suite] = f
	}
	if f, ok := got["sdk-clouddns"]; !ok || f.Funcs != 1 || !f.Services["clouddns"] {
		t.Fatalf("sdk-clouddns = %+v, want 1 func on clouddns", f)
	}
	if f, ok := got["sdk-rest"]; !ok || f.Funcs != 1 || !f.Services["storage"] || !f.Services["kms"] {
		t.Fatalf("sdk-rest = %+v, want 1 func on storage+kms", f)
	}
}
