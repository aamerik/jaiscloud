package spark

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeK8s runs an httptest.TLSServer that fakes the K8s batch/v1 Jobs API.
type fakeK8s struct {
	server  *httptest.Server
	client  *k8sClient
	created []batchJob
	deleted []string
	status  batchJobStatus // returned by GET /jobs/{name}
}

func newFakeK8s(t *testing.T) *fakeK8s {
	t.Helper()
	fk := &fakeK8s{}

	fk.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/jobs"):
			var job batchJob
			if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			fk.created = append(fk.created, job)
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(job)

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/jobs/"):
			resp := batchJob{Status: fk.status}
			json.NewEncoder(w).Encode(resp)

		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/jobs/"):
			parts := strings.Split(strings.TrimSuffix(r.URL.Path, "/"), "/")
			fk.deleted = append(fk.deleted, parts[len(parts)-1])
			w.WriteHeader(http.StatusOK)

		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))

	// Wire a k8sClient that trusts the test server's self-signed TLS cert.
	fk.client = &k8sClient{
		apiURL:       fk.server.URL,
		namespace:    "default",
		tokenLiteral: "test-token",
	}
	fk.client.httpClient = &http.Client{
		Transport: &bearingTransport{
			base:   fk.server.Client().Transport,
			client: fk.client,
		},
	}

	t.Cleanup(fk.server.Close)
	return fk
}

func newTestK8sExecutor(t *testing.T, fk *fakeK8s) *K8sExecutor {
	t.Helper()
	cfg := SparkConfigFrom("k8s", SizeSmall)
	cfg.Image = "apache/spark:3.5.0"
	cfg.Namespace = "default"
	return &K8sExecutor{cfg: cfg, client: fk.client}
}

// ── Submit ───────────────────────────────────────────────────────────────────

func TestK8sExecutor_Submit(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)

	job := SparkJob{
		JobID:     "j-ABC123",
		JarURI:    "s3://my-bucket/app.jar",
		MainClass: "com.example.Main",
		Config:    exec.cfg,
	}

	if err := exec.Submit(context.Background(), job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(fk.created) != 1 {
		t.Fatalf("expected 1 job created, got %d", len(fk.created))
	}

	created := fk.created[0]
	if created.Kind != "Job" || created.APIVersion != "batch/v1" {
		t.Errorf("unexpected manifest header: kind=%q apiVersion=%q", created.Kind, created.APIVersion)
	}
	if !strings.HasPrefix(created.Metadata.Name, "spark-") {
		t.Errorf("job name should start with spark-, got %q", created.Metadata.Name)
	}
	containers := created.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}
	if containers[0].Command[0] != "/opt/spark/bin/spark-submit" {
		t.Errorf("unexpected container command: %v", containers[0].Command)
	}
	hasMaster := false
	for i, a := range containers[0].Args {
		if a == "--master" && i+1 < len(containers[0].Args) && containers[0].Args[i+1] == "local[*]" {
			hasMaster = true
		}
	}
	if !hasMaster {
		t.Errorf("expected --master local[*] in args, got %v", containers[0].Args)
	}
	if _, ok := exec.jobs.Load(job.JobID); !ok {
		t.Error("expected job to be tracked in executor map after submit")
	}
}

func TestK8sExecutor_Submit_ServiceAccount(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)
	exec.cfg.ServiceAccount = "spark-sa"

	job := SparkJob{JobID: "j-sa", JarURI: "s3://b/app.jar", Config: exec.cfg}
	if err := exec.Submit(context.Background(), job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if fk.created[0].Spec.Template.Spec.ServiceAccountName != "spark-sa" {
		t.Errorf("service account not set in job spec")
	}
}

// ── Status ───────────────────────────────────────────────────────────────────

func TestK8sExecutor_Status_Completed(t *testing.T) {
	fk := newFakeK8s(t)
	fk.status = batchJobStatus{
		Conditions: []jobCondition{{Type: "Complete", Status: "True"}},
	}
	exec := newTestK8sExecutor(t, fk)
	exec.jobs.Store("job-1", "spark-job-1")

	s, err := exec.Status(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if s.State != StateCompleted {
		t.Errorf("expected COMPLETED, got %s", s.State)
	}
}

func TestK8sExecutor_Status_Failed(t *testing.T) {
	fk := newFakeK8s(t)
	fk.status = batchJobStatus{
		Conditions: []jobCondition{{Type: "Failed", Status: "True"}},
	}
	exec := newTestK8sExecutor(t, fk)
	exec.jobs.Store("job-2", "spark-job-2")

	s, err := exec.Status(context.Background(), "job-2")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if s.State != StateFailed {
		t.Errorf("expected FAILED, got %s", s.State)
	}
}

func TestK8sExecutor_Status_Running(t *testing.T) {
	fk := newFakeK8s(t)
	fk.status = batchJobStatus{Active: 1, StartTime: "2026-01-01T00:00:00Z"}
	exec := newTestK8sExecutor(t, fk)
	exec.jobs.Store("job-3", "spark-job-3")

	s, err := exec.Status(context.Background(), "job-3")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if s.State != StateRunning {
		t.Errorf("expected RUNNING, got %s", s.State)
	}
}

func TestK8sExecutor_Status_UnknownJob_ReturnsPending(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)

	s, err := exec.Status(context.Background(), "no-such-job")
	if err != nil {
		t.Fatalf("Status of unknown job should not error: %v", err)
	}
	if s.State != StatePending {
		t.Errorf("expected PENDING for unknown job, got %s", s.State)
	}
}

// ── Cancel ───────────────────────────────────────────────────────────────────

func TestK8sExecutor_Cancel(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)
	exec.jobs.Store("job-4", "spark-job-4")

	if err := exec.Cancel(context.Background(), "job-4"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if len(fk.deleted) != 1 || fk.deleted[0] != "spark-job-4" {
		t.Errorf("expected spark-job-4 deleted, got %v", fk.deleted)
	}
	if _, ok := exec.jobs.Load("job-4"); ok {
		t.Error("job should be removed from map after cancel")
	}
}

func TestK8sExecutor_Cancel_UnknownJob_NoOp(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)

	if err := exec.Cancel(context.Background(), "not-tracked"); err != nil {
		t.Fatalf("Cancel of unknown job should be no-op: %v", err)
	}
	if len(fk.deleted) != 0 {
		t.Errorf("expected no DELETE call, got %v", fk.deleted)
	}
}

// ── Reset ────────────────────────────────────────────────────────────────────

func TestK8sExecutor_Reset_ClearsMap(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)
	exec.jobs.Store("job-a", "spark-job-a")
	exec.jobs.Store("job-b", "spark-job-b")

	exec.Reset()

	count := 0
	exec.jobs.Range(func(_, _ any) bool { count++; return true })
	if count != 0 {
		t.Errorf("expected empty map after Reset, got %d entries", count)
	}
}

// ── k8sJobName ───────────────────────────────────────────────────────────────

func TestK8sJobName_Sanitisation(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"j-ABC123", "spark-j-abc123"},
		{"J_ABCDEF", "spark-j-abcdef"},
		{"EMR_STEP_001", "spark-emr-step-001"},
	}
	for _, c := range cases {
		got := k8sJobName(c.input)
		if got != c.want {
			t.Errorf("k8sJobName(%q) = %q, want %q", c.input, got, c.want)
		}
		if len(got) > 63 {
			t.Errorf("k8sJobName(%q) len=%d > 63", c.input, len(got))
		}
	}
}

func TestK8sJobName_Truncation(t *testing.T) {
	long := strings.Repeat("x", 100)
	got := k8sJobName(long)
	if len(got) > 63 {
		t.Errorf("k8sJobName with long input: len=%d > 63", len(got))
	}
	if strings.HasSuffix(got, "-") {
		t.Errorf("k8sJobName should not end with hyphen after truncation: %q", got)
	}
}

// ── rewriteSparkMaster ───────────────────────────────────────────────────────

func TestRewriteSparkMaster(t *testing.T) {
	cases := []struct {
		name           string
		in             []string
		wantMaster     string
		wantDeployMode string
	}{
		{
			name:       "yarn_rewritten_to_local",
			in:         []string{"--master", "yarn", "--class", "Main", "app.jar"},
			wantMaster: "local[*]",
		},
		{
			name:       "k8s_rewritten_to_local",
			in:         []string{"--master", "k8s://https://kubernetes.default.svc"},
			wantMaster: "local[*]",
		},
		{
			name:       "local_star_preserved",
			in:         []string{"--master", "local[*]", "--class", "Main"},
			wantMaster: "local[*]",
		},
		{
			name:       "local_n_preserved",
			in:         []string{"--master", "local[2]", "--class", "Main"},
			wantMaster: "local[2]",
		},
		{
			name:           "deploy_mode_cluster_rewritten",
			in:             []string{"--master", "local[*]", "--deploy-mode", "cluster"},
			wantMaster:     "local[*]",
			wantDeployMode: "client",
		},
		{
			name:           "no_master_prepended",
			in:             []string{"--class", "Main", "app.jar"},
			wantMaster:     "local[*]",
			wantDeployMode: "client",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := rewriteSparkMaster(c.in)
			master, deployMode := "", ""
			for i, a := range out {
				if a == "--master" && i+1 < len(out) {
					master = out[i+1]
				}
				if a == "--deploy-mode" && i+1 < len(out) {
					deployMode = out[i+1]
				}
			}
			if master != c.wantMaster {
				t.Errorf("master: got %q, want %q (args: %v)", master, c.wantMaster, out)
			}
			if c.wantDeployMode != "" && deployMode != c.wantDeployMode {
				t.Errorf("deploy-mode: got %q, want %q (args: %v)", deployMode, c.wantDeployMode, out)
			}
		})
	}
}

// ── EMR Containers pattern ───────────────────────────────────────────────────

func TestK8sExecutor_Submit_EMRContainersPattern(t *testing.T) {
	fk := newFakeK8s(t)
	exec := newTestK8sExecutor(t, fk)

	// Pattern 1: EMR Containers style — JarURI is the spark-submit binary path.
	job := SparkJob{
		JobID:  "jr-ABC123",
		JarURI: "/opt/spark/bin/spark-submit",
		Args: []string{
			"--master", "yarn",
			"--class", "org.apache.spark.examples.SparkPi",
			"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
			"10",
		},
		Config: exec.cfg,
	}

	if err := exec.Submit(context.Background(), job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(fk.created) != 1 {
		t.Fatalf("expected 1 job created, got %d", len(fk.created))
	}

	containers := fk.created[0].Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}
	if containers[0].Command[0] != "/opt/spark/bin/spark-submit" {
		t.Errorf("unexpected command: %v", containers[0].Command)
	}
	hasMaster := false
	for i, a := range containers[0].Args {
		if a == "--master" && i+1 < len(containers[0].Args) {
			hasMaster = true
			if containers[0].Args[i+1] != "local[*]" {
				t.Errorf("expected --master local[*], got %q", containers[0].Args[i+1])
			}
		}
	}
	if !hasMaster {
		t.Errorf("expected --master in args, got %v", containers[0].Args)
	}
}
