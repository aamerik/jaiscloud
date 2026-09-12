package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestBigQueryCodecDecode_DeferredResources(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		// routines: collection + item, read + write.
		{"GET", "/bigquery/v2/projects/p/datasets/d/routines", "Routines"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/routines", "Routines"},
		{"GET", "/bigquery/v2/projects/p/datasets/d/routines/r", "Routines"},
		{"DELETE", "/bigquery/v2/projects/p/datasets/d/routines/r", "Routines"},
		// models: collection + item, read + write.
		{"GET", "/bigquery/v2/projects/p/datasets/d/models", "Models"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/models", "Models"},
		{"GET", "/bigquery/v2/projects/p/datasets/d/models/m", "Models"},
		{"DELETE", "/bigquery/v2/projects/p/datasets/d/models/m", "Models"},
		// rowAccessPolicies: table-scoped collection/item + custom method.
		{"GET", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies", "RowAccessPolicies"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies", "RowAccessPolicies"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies:batchDelete", "RowAccessPolicies"},
		{"GET", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies/rp", "RowAccessPolicies"},
		{"DELETE", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies/rp", "RowAccessPolicies"},
		// WithEndpoint-stripped form (no bigquery/v2 servicePath).
		{"GET", "/projects/p/datasets/d/routines", "Routines"},
		{"GET", "/projects/p/datasets/d/models", "Models"},
		{"GET", "/projects/p/datasets/d/tables/tbl/rowAccessPolicies", "RowAccessPolicies"},
	}
	for _, tc := range cases {
		codec := &BigQueryCodec{Service: "bigquery"}
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

func TestDetectBigQueryDeferredResources(t *testing.T) {
	cases := map[string]string{
		"/bigquery/v2/projects/p/datasets/d/routines":                   "bigquery",
		"/bigquery/v2/projects/p/datasets/d/tables/t/rowAccessPolicies": "bigquery",
		"/projects/p/datasets/d/models":                                 "bigquery",
	}
	for path, want := range cases {
		r := httptest.NewRequest("GET", path, nil)
		if got, _ := DetectService(r); got != want {
			t.Errorf("DetectService(%s) = %q, want %q", path, got, want)
		}
	}
}
