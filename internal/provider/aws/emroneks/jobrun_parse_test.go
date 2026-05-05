package emroneks

import (
	"testing"

	"jaiscloud/internal/sparkhelpers"
)

// ─── extractJobRunEntryPoint ──────────────────────────────────────────────────

func TestExtractJobRunEntryPoint_NoJobDriver(t *testing.T) {
	ep, sparkArgs, jarArgs := extractJobRunEntryPoint(map[string]any{})
	if ep != nil || sparkArgs != nil || jarArgs != nil {
		t.Fatal("missing jobDriver should return all nil")
	}
}

func TestExtractJobRunEntryPoint_NoSparkSubmitDriver(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"hadoopJobDriver": map[string]any{},
		},
	}
	ep, _, _ := extractJobRunEntryPoint(params)
	if ep != nil {
		t.Fatal("missing sparkSubmitJobDriver should return nil entry point")
	}
}

func TestExtractJobRunEntryPoint_JarEntryPoint(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint":            "s3://bucket/app.jar",
				"sparkSubmitParameters": "--class com.example.Main --num-executors 4",
			},
		},
	}
	ep, sparkArgs, jarArgs := extractJobRunEntryPoint(params)
	jar, ok := ep.(sparkhelpers.JarEntryPoint)
	if !ok {
		t.Fatalf("expected JarEntryPoint, got %T", ep)
	}
	if jar.JarURI != "s3://bucket/app.jar" {
		t.Errorf("JarURI=%q", jar.JarURI)
	}
	if jar.MainClass != "com.example.Main" {
		t.Errorf("MainClass=%q", jar.MainClass)
	}
	if len(jarArgs) != 0 {
		t.Errorf("jarArgs=%v want empty", jarArgs)
	}
	// --class and --num-executors must be in sparkArgs
	found := map[string]bool{}
	for _, a := range sparkArgs {
		found[a] = true
	}
	if !found["--num-executors"] {
		t.Errorf("--num-executors missing from sparkArgs: %v", sparkArgs)
	}
}

func TestExtractJobRunEntryPoint_PythonEntryPoint(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint": "s3://bucket/etl.py",
			},
		},
	}
	ep, _, _ := extractJobRunEntryPoint(params)
	if _, ok := ep.(sparkhelpers.PythonEntryPoint); !ok {
		t.Fatalf("expected PythonEntryPoint, got %T", ep)
	}
}

func TestExtractJobRunEntryPoint_REntryPoint(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint": "analysis.R",
			},
		},
	}
	ep, _, _ := extractJobRunEntryPoint(params)
	if _, ok := ep.(sparkhelpers.REntryPoint); !ok {
		t.Fatalf("expected REntryPoint, got %T", ep)
	}
}

func TestExtractJobRunEntryPoint_EntryPointArguments(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint":            "app.jar",
				"entryPointArguments":   []any{"--input", "/data", "--output", "/out"},
			},
		},
	}
	_, _, jarArgs := extractJobRunEntryPoint(params)
	if len(jarArgs) != 4 {
		t.Fatalf("jarArgs=%v want 4 entries", jarArgs)
	}
	if jarArgs[0] != "--input" || jarArgs[2] != "--output" {
		t.Errorf("unexpected jarArgs: %v", jarArgs)
	}
}

func TestExtractJobRunEntryPoint_ConfigurationOverrides(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint": "app.jar",
			},
		},
		"configurationOverrides": map[string]any{
			"applicationConfiguration": []any{
				map[string]any{
					"properties": map[string]any{
						"spark.executor.memory": "4g",
						"spark.executor.cores":  "2",
					},
				},
			},
		},
	}
	_, sparkArgs, _ := extractJobRunEntryPoint(params)
	confs := map[string]string{}
	for i := 0; i+1 < len(sparkArgs); i += 2 {
		if sparkArgs[i] == "--conf" {
			parts := splitOnFirst(sparkArgs[i+1], "=")
			if len(parts) == 2 {
				confs[parts[0]] = parts[1]
			}
		}
	}
	if confs["spark.executor.memory"] != "4g" {
		t.Errorf("spark.executor.memory=%q want 4g", confs["spark.executor.memory"])
	}
	if confs["spark.executor.cores"] != "2" {
		t.Errorf("spark.executor.cores=%q want 2", confs["spark.executor.cores"])
	}
}

func TestExtractJobRunEntryPoint_PodTemplateKeysExcluded(t *testing.T) {
	params := map[string]any{
		"jobDriver": map[string]any{
			"sparkSubmitJobDriver": map[string]any{
				"entryPoint": "app.jar",
			},
		},
		"configurationOverrides": map[string]any{
			"applicationConfiguration": []any{
				map[string]any{
					"properties": map[string]any{
						"spark.kubernetes.driver.podTemplateFile":   "s3://bucket/driver.yaml",
						"spark.kubernetes.executor.podTemplateFile": "s3://bucket/exec.yaml",
						"spark.executor.memory":                     "2g",
					},
				},
			},
		},
	}
	_, sparkArgs, _ := extractJobRunEntryPoint(params)
	for _, a := range sparkArgs {
		if a == "spark.kubernetes.driver.podTemplateFile" ||
			a == "spark.kubernetes.executor.podTemplateFile" {
			t.Errorf("pod template key must be excluded from sparkArgs: %v", sparkArgs)
		}
	}
}

// ─── finalToJobRunState ───────────────────────────────────────────────────────

func TestFinalToJobRunState(t *testing.T) {
	cases := []struct {
		sparkSucceeded bool
		cancelled      bool
		want           string
	}{
		{true, false, "COMPLETED"},
		{false, true, "CANCELLED"},
		{false, false, "FAILED"},
		{true, true, "COMPLETED"}, // SparkSucceeded takes precedence
	}
	for _, tc := range cases {
		f := sparkhelpers.Final{SparkSucceeded: tc.sparkSucceeded}
		f.Cancelled = tc.cancelled
		if got := finalToJobRunState(f); got != tc.want {
			t.Errorf("finalToJobRunState(succeeded=%v, cancelled=%v) = %q, want %q",
				tc.sparkSucceeded, tc.cancelled, got, tc.want)
		}
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func splitOnFirst(s, sep string) []string {
	idx := len(s)
	for i := range s {
		if s[i] == sep[0] {
			idx = i
			break
		}
	}
	if idx == len(s) {
		return []string{s}
	}
	return []string{s[:idx], s[idx+1:]}
}
