package emr

import (
	"testing"

	"jaiscloud/internal/sparkhelpers"
)

// ─── extractStepArgv ──────────────────────────────────────────────────────────

func TestExtractStepArgv_HappyPath(t *testing.T) {
	cfg := map[string]any{
		"HadoopJarStep": map[string]any{
			"Args": []any{"spark-submit", "--class", "com.example.Main", "s3://bucket/app.jar", "arg1"},
		},
	}
	got := extractStepArgv(cfg)
	want := []string{"spark-submit", "--class", "com.example.Main", "s3://bucket/app.jar", "arg1"}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %v", len(got), len(want), got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("[%d] got=%q want=%q", i, got[i], v)
		}
	}
}

func TestExtractStepArgv_NoHadoopJarStep(t *testing.T) {
	if got := extractStepArgv(map[string]any{}); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestExtractStepArgv_NoArgs(t *testing.T) {
	cfg := map[string]any{"HadoopJarStep": map[string]any{}}
	if got := extractStepArgv(cfg); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestExtractStepArgv_SkipsNonStringArgs(t *testing.T) {
	cfg := map[string]any{
		"HadoopJarStep": map[string]any{
			"Args": []any{"spark-submit", 42, "app.jar"},
		},
	}
	got := extractStepArgv(cfg)
	if len(got) != 2 || got[0] != "spark-submit" || got[1] != "app.jar" {
		t.Fatalf("unexpected: %v", got)
	}
}

// ─── parseSparkSubmitArgv ─────────────────────────────────────────────────────

func TestParseSparkSubmitArgv_JarEntryPoint(t *testing.T) {
	argv := []string{
		"--master", "k8s://https://localhost",
		"--deploy-mode", "client",
		"--class", "com.example.Main",
		"s3://bucket/app.jar",
		"arg1", "arg2",
	}
	ep, sparkArgs, userArgs, err := parseSparkSubmitArgv(argv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	if len(userArgs) != 2 || userArgs[0] != "arg1" || userArgs[1] != "arg2" {
		t.Errorf("userArgs=%v", userArgs)
	}
	// sparkArgs must contain the --master and --class entries
	found := false
	for _, a := range sparkArgs {
		if a == "--master" {
			found = true
		}
	}
	if !found {
		t.Errorf("--master not in sparkArgs: %v", sparkArgs)
	}
}

func TestParseSparkSubmitArgv_PythonEntryPoint(t *testing.T) {
	argv := []string{
		"--conf", "spark.executor.memory=2g",
		"s3://bucket/script.py",
		"--input", "/data",
	}
	ep, _, userArgs, err := parseSparkSubmitArgv(argv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	py, ok := ep.(sparkhelpers.PythonEntryPoint)
	if !ok {
		t.Fatalf("expected PythonEntryPoint, got %T", ep)
	}
	if py.MainPythonFile != "s3://bucket/script.py" {
		t.Errorf("MainPythonFile=%q", py.MainPythonFile)
	}
	if len(userArgs) != 2 {
		t.Errorf("userArgs=%v", userArgs)
	}
}

func TestParseSparkSubmitArgv_REntryPoint(t *testing.T) {
	argv := []string{"analysis.R", "param1"}
	ep, _, _, err := parseSparkSubmitArgv(argv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := ep.(sparkhelpers.REntryPoint); !ok {
		t.Fatalf("expected REntryPoint, got %T", ep)
	}
}

func TestParseSparkSubmitArgv_NoEntryPointReturnsError(t *testing.T) {
	argv := []string{"--master", "local", "--class", "com.example.Main"}
	_, _, _, err := parseSparkSubmitArgv(argv)
	if err == nil {
		t.Fatal("expected error when no entry-point is present")
	}
}

func TestParseSparkSubmitArgv_PyFilesExtracted(t *testing.T) {
	argv := []string{
		"--py-files", "dep1.zip,dep2.zip",
		"main.py",
	}
	ep, sparkArgs, _, err := parseSparkSubmitArgv(argv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	py, ok := ep.(sparkhelpers.PythonEntryPoint)
	if !ok {
		t.Fatalf("expected PythonEntryPoint, got %T", ep)
	}
	if len(py.PyFiles) != 2 {
		t.Errorf("PyFiles=%v", py.PyFiles)
	}
	// --py-files must be preserved in sparkArgs
	found := false
	for _, a := range sparkArgs {
		if a == "--py-files" {
			found = true
		}
	}
	if !found {
		t.Errorf("--py-files not in sparkArgs: %v", sparkArgs)
	}
}

// ─── isEntryPointArg ──────────────────────────────────────────────────────────

func TestIsEntryPointArg(t *testing.T) {
	cases := []struct {
		arg  string
		want bool
	}{
		{"app.jar", true},
		{"APP.JAR", true},
		{"script.py", true},
		{"analysis.r", true},
		{"analysis.R", true},
		{"s3://bucket/app.jar", true},
		{"s3a://bucket/script.py", true},
		{"--class", false},
		{"--conf", false},
		{"local[*]", false},
		{"k8s://https://localhost", false},
	}
	for _, tc := range cases {
		if got := isEntryPointArg(tc.arg); got != tc.want {
			t.Errorf("isEntryPointArg(%q) = %v, want %v", tc.arg, got, tc.want)
		}
	}
}

// ─── finalToStepState ─────────────────────────────────────────────────────────

func finalForState(sparkSucceeded, cancelled bool) sparkhelpers.Final {
	f := sparkhelpers.Final{SparkSucceeded: sparkSucceeded}
	f.Cancelled = cancelled // promoted from embedded k8shelpers.Final
	return f
}

func TestFinalToStepState(t *testing.T) {
	cases := []struct {
		sparkSucceeded bool
		cancelled      bool
		want           string
	}{
		{true, false, "COMPLETED"},
		{false, true, "CANCELLED"},
		{false, false, "FAILED"},
		// SparkSucceeded takes precedence over Cancelled
		{true, true, "COMPLETED"},
	}
	for _, tc := range cases {
		f := finalForState(tc.sparkSucceeded, tc.cancelled)
		if got := finalToStepState(f); got != tc.want {
			t.Errorf("finalToStepState(succeeded=%v, cancelled=%v) = %q, want %q",
				tc.sparkSucceeded, tc.cancelled, got, tc.want)
		}
	}
}
