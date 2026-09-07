package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestIcebergCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action, namespace, table string
	}{
		{"GET", "/iceberg/v1/config", "GetConfig", "", ""},
		{"GET", "/iceberg/v1/wh/config", "GetConfig", "", ""},
		{"GET", "/iceberg/v1/namespaces", "ListNamespaces", "", ""},
		{"POST", "/iceberg/v1/namespaces", "CreateNamespace", "", ""},
		{"GET", "/iceberg/v1/wh/namespaces", "ListNamespaces", "", ""},
		{"GET", "/iceberg/v1/namespaces/a", "GetNamespace", "a", ""},
		{"HEAD", "/iceberg/v1/namespaces/a", "NamespaceExists", "a", ""},
		{"DELETE", "/iceberg/v1/namespaces/a", "DropNamespace", "a", ""},
		{"GET", "/iceberg/v1/namespaces/a/b", "GetNamespace", "a/b", ""},
		{"POST", "/iceberg/v1/namespaces/a/properties", "UpdateNamespaceProperties", "a", ""},
		{"GET", "/iceberg/v1/namespaces/a/tables", "ListTables", "a", ""},
		{"POST", "/iceberg/v1/namespaces/a/tables", "CreateTable", "a", ""},
		{"GET", "/iceberg/v1/namespaces/a/tables/t1", "LoadTable", "a", "t1"},
		{"POST", "/iceberg/v1/namespaces/a/tables/t1", "CommitTable", "a", "t1"},
		{"DELETE", "/iceberg/v1/namespaces/a/tables/t1", "DropTable", "a", "t1"},
		{"GET", "/iceberg/v1/namespaces/a/tables/t1/metrics", "TableMetrics", "a", "t1"},
		{"POST", "/iceberg/v1/tables/rename", "RenameTable", "", ""},
		{"POST", "/iceberg/v1/wh/tables/rename", "RenameTable", "", ""},
		// Multi-level namespace.
		{"GET", "/iceberg/v1/namespaces/a/b/c/tables/t1", "LoadTable", "a/b/c", "t1"},
	}
	for _, tc := range cases {
		codec := &IcebergCodec{}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if tc.namespace != "" && nr.Params["namespace"] != tc.namespace {
			t.Errorf("%s %s: namespace = %q, want %q", tc.method, tc.path, nr.Params["namespace"], tc.namespace)
		}
		if tc.table != "" && nr.Params["table"] != tc.table {
			t.Errorf("%s %s: table = %q, want %q", tc.method, tc.path, nr.Params["table"], tc.table)
		}
	}
}

func TestDetectIcebergService(t *testing.T) {
	cases := map[string]string{
		"/iceberg/v1/config":                "iceberg",
		"/iceberg/v1/namespaces/a/tables/b": "iceberg",
		"/iceberg/v1/wh/namespaces":         "iceberg",
		"/iceberg/v1/tables/rename":         "iceberg",
		// Non-iceberg paths are unaffected.
		"/v1/projects/p/topics/t":              "pubsub",
		"/v1/projects/p/locations/us/services": "metastore",
	}
	for path, want := range cases {
		req := httptest.NewRequest("GET", path, nil)
		if got, _ := DetectService(req); got != want {
			t.Errorf("DetectService(%q) = %q, want %q", path, got, want)
		}
	}
}
