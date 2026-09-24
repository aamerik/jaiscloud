package dataproc

import (
	"testing"

	"jaiscloud/internal/sparkhelpers"
)

func TestJobToEntryPoint_SparkJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkJob", map[string]any{
		"mainJarFileUri": "gs://b/a.jar",
		"mainClass":      "Main",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	jar, ok := ep.(sparkhelpers.JarEntryPoint)
	if !ok || jar.JarURI != "gs://b/a.jar" || jar.MainClass != "Main" {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_PySparkJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("pysparkJob", map[string]any{
		"mainPythonFileUri": "gs://b/main.py",
		"pythonFileUris":    []any{"gs://b/dep.py"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	py, ok := ep.(sparkhelpers.PythonEntryPoint)
	if !ok || py.MainPythonFile != "gs://b/main.py" || len(py.PyFiles) != 1 {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_SparkRJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkRJob", map[string]any{"mainRFileUri": "gs://b/main.R"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r, ok := ep.(sparkhelpers.REntryPoint)
	if !ok || r.MainRFile != "gs://b/main.R" {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_SparkSqlJob(t *testing.T) {
	if _, _, err := jobToEntryPoint("sparkSqlJob", map[string]any{"queryFileUri": "gs://b/q.sql"}); err == nil {
		t.Fatal("expected error for sparkSqlJob (unsupported)")
	}
}

func TestJobToEntryPoint_Unsupported(t *testing.T) {
	for _, jobType := range []string{"hadoopJob", "hiveJob", "pigJob", "sparkSqlJob", "prestoJob", "trinoJob", "flinkJob"} {
		if _, _, err := jobToEntryPoint(jobType, map[string]any{}); err == nil {
			t.Fatalf("expected error for %s", jobType)
		}
	}
}

func TestExtractJobType(t *testing.T) {
	jobType, typeJob := extractJobType(map[string]any{
		"labels": map[string]any{"k": "v"},
		"pysparkJob": map[string]any{
			"mainPythonFileUri": "gs://b/main.py",
		},
	})
	if jobType != "pysparkJob" || typeJob == nil {
		t.Fatalf("extractJobType: %q %v", jobType, typeJob)
	}
}
