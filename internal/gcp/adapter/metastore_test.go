package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestMetastoreCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"POST", "/v1/projects/p/locations/us/services?serviceId=s", "CreateService"},
		{"GET", "/v1/projects/p/locations/us/services", "ListServices"},
		{"GET", "/v1/projects/p/locations/us/services/s", "GetService"},
		{"PATCH", "/v1/projects/p/locations/us/services/s", "UpdateService"},
		{"DELETE", "/v1/projects/p/locations/us/services/s", "DeleteService"},
		{"POST", "/v1/projects/p/locations/us/services/s/backups?backupId=b", "CreateBackup"},
		{"GET", "/v1/projects/p/locations/us/services/s/backups", "ListBackups"},
		{"GET", "/v1/projects/p/locations/us/services/s/backups/b", "GetBackup"},
		{"DELETE", "/v1/projects/p/locations/us/services/s/backups/b", "DeleteBackup"},
		{"POST", "/v1/projects/p/locations/us/services/s/metadataImports?metadataImportId=m", "CreateMetadataImport"},
		{"GET", "/v1/projects/p/locations/us/services/s/metadataImports", "ListMetadataImports"},
		{"GET", "/v1/projects/p/locations/us/services/s/metadataImports/m", "GetMetadataImport"},
		{"PATCH", "/v1/projects/p/locations/us/services/s/metadataImports/m", "UpdateMetadataImport"},
		{"GET", "/v1/projects/p/locations/us/operations/op", "GetOperation"},
		{"POST", "/v1/projects/p/locations/us/services/s:exportMetadata", "ExportMetadata"},
		{"POST", "/v1/projects/p/locations/us/services/s:restore", "RestoreService"},
		{"POST", "/v1/projects/p/locations/us/services/s:queryMetadata", "QueryMetadata"},
		{"POST", "/v1/projects/p/locations/us/services/s:moveTableToDatabase", "MoveTableToDatabase"},
		{"POST", "/v1/projects/p/locations/us/services/s:alterLocation", "AlterMetadataResourceLocation"},
	}
	for _, tc := range cases {
		codec := &MetastoreCodec{Service: "metastore"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}

func TestDetectMetastoreService(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us/services":                     "metastore",
		"/v1/projects/p/locations/us/services/s":                   "metastore",
		"/v1/projects/p/locations/us/services/s/backups":           "metastore",
		"/v1/projects/p/locations/us/services/s/backups/b":         "metastore",
		"/v1/projects/p/locations/us/services/s/metadataImports/m": "metastore",
		// Shared operations path stays on workflows (path-ambiguous on one host).
		"/v1/projects/p/locations/us/operations/op": "workflows",
		// Unrelated locations/{l}/... services are unaffected.
		"/v1/projects/p/locations/us/clusters/c":           "managedkafka",
		"/v1/projects/p/locations/us-central1/functions/f": "functions",
		"/v1/projects/p/locations/us/workflows/w":          "workflows",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}
}

// TestDetectMetastoreResourceTypeDoesNotClaimOperations locks the
// operations-routing decision: the shared locations/{l}/operations/{id} LRO
// path is path-ambiguous with Workflows on a single host, so
// detectMetastoreResourceType must NOT claim it (it stays routed to workflows).
// Only the "services" segment is claimed.
func TestDetectMetastoreResourceTypeDoesNotClaimOperations(t *testing.T) {
	if got := detectMetastoreResourceType([]string{"locations", "us", "operations", "op"}); got != "" {
		t.Errorf("detectMetastoreResourceType claimed the shared operations path: %q", got)
	}
	if got := detectMetastoreResourceType([]string{"locations", "us", "services"}); got != "services" {
		t.Errorf("detectMetastoreResourceType should claim services, got %q", got)
	}
}
