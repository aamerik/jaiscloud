package spark

import (
	"errors"
	"testing"
)

// ── isClusterCompatibleMaster ─────────────────────────────────────────────────

func TestIsClusterCompatibleMaster(t *testing.T) {
	cases := []struct {
		master string
		want   bool
	}{
		// K8s native — always compatible.
		{"k8s://https://127.0.0.1:6443", true},
		{"k8s://kubernetes.default.svc", true},
		// local variants — compatible (same-JVM fallback).
		{"local", true},
		{"local[*]", true},
		{"local[4]", true},
		{"local[16]", true},
		// Incompatible masters.
		{"spark://spark-master:7077", false},
		{"yarn", false},
		{"mesos://zk://localhost:2181", false},
		{"", false},
		// Edge: local prefix without brackets is NOT local[N].
		{"local-cluster[2,1,512]", false},
	}
	for _, c := range cases {
		got := isClusterCompatibleMaster(c.master)
		if got != c.want {
			t.Errorf("isClusterCompatibleMaster(%q) = %v, want %v", c.master, got, c.want)
		}
	}
}

// ── findMasterArg ─────────────────────────────────────────────────────────────

func TestFindMasterArg_TwoToken(t *testing.T) {
	args := []string{"--deploy-mode", "cluster", "--master", "k8s://host:6443", "--class", "App"}
	got, ok := findMasterArg(args)
	if !ok {
		t.Fatal("findMasterArg: expected ok=true")
	}
	if got != "k8s://host:6443" {
		t.Errorf("got %q, want k8s://host:6443", got)
	}
}

func TestFindMasterArg_SingleToken(t *testing.T) {
	args := []string{"--master=local[*]", "--class", "App"}
	got, ok := findMasterArg(args)
	if !ok {
		t.Fatal("findMasterArg: expected ok=true")
	}
	if got != "local[*]" {
		t.Errorf("got %q, want local[*]", got)
	}
}

func TestFindMasterArg_Absent(t *testing.T) {
	args := []string{"--class", "App", "--conf", "k=v"}
	_, ok := findMasterArg(args)
	if ok {
		t.Error("findMasterArg: expected ok=false when --master is absent")
	}
}

func TestFindMasterArg_MasterAtEnd_NoValue(t *testing.T) {
	// --master at the last position with no following value — must not panic.
	args := []string{"--conf", "k=v", "--master"}
	_, ok := findMasterArg(args)
	if ok {
		t.Error("findMasterArg: --master at end with no value should return ok=false")
	}
}

// ── resolveMasterArgs ─────────────────────────────────────────────────────────

func TestResolveMasterArgs_ClusterModeOff_RewritesToLocal(t *testing.T) {
	job := SparkJob{
		AllowClusterMode: false,
		Config:           SparkConfig{Mode: "k8s"},
	}
	args := []string{"--master", "k8s://host:6443", "--class", "App"}
	got, err := resolveMasterArgs(job, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	master, ok := findMasterArg(got)
	if !ok || master != "local[*]" {
		t.Errorf("expected --master local[*] when cluster mode off, got %q (args: %v)", master, got)
	}
}

func TestResolveMasterArgs_NonK8sMode_RewritesToLocal(t *testing.T) {
	job := SparkJob{
		AllowClusterMode: true,
		Config:           SparkConfig{Mode: "docker"}, // not k8s
	}
	args := []string{"--master", "k8s://host:6443"}
	got, err := resolveMasterArgs(job, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	master, ok := findMasterArg(got)
	if !ok || master != "local[*]" {
		t.Errorf("expected --master rewritten to local[*] for docker mode, got %q", master)
	}
}

func TestResolveMasterArgs_ClusterMode_K8sMaster_Preserved(t *testing.T) {
	job := SparkJob{
		AllowClusterMode: true,
		Config:           SparkConfig{Mode: "k8s"},
	}
	args := []string{"--master", "k8s://https://127.0.0.1:6443", "--class", "App"}
	got, err := resolveMasterArgs(job, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	master, ok := findMasterArg(got)
	if !ok || master != "k8s://https://127.0.0.1:6443" {
		t.Errorf("expected k8s:// master preserved, got %q (args: %v)", master, got)
	}
}

func TestResolveMasterArgs_ClusterMode_LocalMaster_Preserved(t *testing.T) {
	job := SparkJob{
		AllowClusterMode: true,
		Config:           SparkConfig{Mode: "k8s"},
	}
	args := []string{"--master", "local[*]"}
	got, err := resolveMasterArgs(job, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	master, _ := findMasterArg(got)
	if master != "local[*]" {
		t.Errorf("expected local[*] preserved, got %q", master)
	}
}

func TestResolveMasterArgs_ClusterMode_IncompatibleMaster_Error(t *testing.T) {
	job := SparkJob{
		AllowClusterMode: true,
		Config:           SparkConfig{Mode: "k8s"},
	}
	args := []string{"--master", "yarn"}
	_, err := resolveMasterArgs(job, args)
	if err == nil {
		t.Fatal("expected error for incompatible master, got nil")
	}
	if !errors.Is(err, ErrIncompatibleMasterInClusterMode) {
		t.Errorf("expected ErrIncompatibleMasterInClusterMode, got %v", err)
	}
}

func TestResolveMasterArgs_ClusterMode_NoMaster_ReturnsArgsUnchanged(t *testing.T) {
	job := SparkJob{
		AllowClusterMode: true,
		Config:           SparkConfig{Mode: "k8s"},
	}
	args := []string{"--class", "com.example.App", "s3://bucket/app.jar"}
	got, err := resolveMasterArgs(job, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Args returned unchanged (WARN logged; no error).
	if len(got) != len(args) {
		t.Errorf("expected args unchanged when no --master, got %v", got)
	}
}

// ── stripTemplateFileConfs ────────────────────────────────────────────────────

func TestStripTemplateFileConfs_TwoToken_Driver(t *testing.T) {
	args := []string{
		"--conf", "spark.kubernetes.driver.podTemplateFile=s3://b/driver.yaml",
		"--class", "App",
	}
	got := stripTemplateFileConfs(args)
	for _, a := range got {
		if a == "spark.kubernetes.driver.podTemplateFile=s3://b/driver.yaml" {
			t.Error("driver podTemplateFile conf must be stripped")
		}
	}
	found := false
	for _, a := range got {
		if a == "App" {
			found = true
		}
	}
	if !found {
		t.Errorf("non-template args must be preserved, got %v", got)
	}
}

func TestStripTemplateFileConfs_TwoToken_Executor(t *testing.T) {
	args := []string{
		"--class", "App",
		"--conf", "spark.kubernetes.executor.podTemplateFile=s3://b/exec.yaml",
	}
	got := stripTemplateFileConfs(args)
	for _, a := range got {
		if a == "spark.kubernetes.executor.podTemplateFile=s3://b/exec.yaml" {
			t.Error("executor podTemplateFile conf must be stripped")
		}
	}
}

func TestStripTemplateFileConfs_SingleToken(t *testing.T) {
	args := []string{
		"--conf=spark.kubernetes.driver.podTemplateFile=s3://b/driver.yaml",
		"--conf=spark.kubernetes.executor.podTemplateFile=s3://b/exec.yaml",
		"--conf=spark.sql.shuffle.partitions=200",
	}
	got := stripTemplateFileConfs(args)
	if len(got) != 1 || got[0] != "--conf=spark.sql.shuffle.partitions=200" {
		t.Errorf("only non-template confs should remain, got %v", got)
	}
}

func TestStripTemplateFileConfs_BothForms_Mixed(t *testing.T) {
	args := []string{
		"--conf", "spark.kubernetes.driver.podTemplateFile=s3://b/d.yaml",
		"--conf=spark.kubernetes.executor.podTemplateFile=s3://b/e.yaml",
		"--conf", "spark.app.name=myapp",
		"--conf=spark.executor.instances=2",
	}
	got := stripTemplateFileConfs(args)
	for _, a := range got {
		if a == "spark.kubernetes.driver.podTemplateFile=s3://b/d.yaml" ||
			a == "--conf=spark.kubernetes.executor.podTemplateFile=s3://b/e.yaml" {
			t.Errorf("template conf must be stripped, still present: %q", a)
		}
	}
	// Non-template confs must be preserved.
	hasAppName, hasInstances := false, false
	for _, a := range got {
		if a == "spark.app.name=myapp" {
			hasAppName = true
		}
		if a == "--conf=spark.executor.instances=2" {
			hasInstances = true
		}
	}
	if !hasAppName || !hasInstances {
		t.Errorf("non-template confs missing from %v", got)
	}
}

func TestStripTemplateFileConfs_NoTemplateConfs_Unchanged(t *testing.T) {
	args := []string{"--class", "App", "--conf", "k=v", "s3://b/a.jar"}
	got := stripTemplateFileConfs(args)
	if len(got) != len(args) {
		t.Errorf("args without template confs should be unchanged: got %v", got)
	}
}

func TestStripTemplateFileConfs_Empty(t *testing.T) {
	got := stripTemplateFileConfs(nil)
	if len(got) != 0 {
		t.Errorf("expected empty result for nil input, got %v", got)
	}
}
