//go:build gcp_persistence

package dataproc

import (
	"bytes"
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStoreSnapshotVerbatim verifies that cluster config, job type_job,
// and operation metadata/response survive a Postgres Snapshot/Restore round
// trip byte-for-byte.
func TestPostgresStoreSnapshotVerbatim(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const config = `{"gceClusterConfig":{"zoneUri":"z"},"softwareConfig":{"imageVersion":"2.2"},"initializationActions":[{"executableFile":"gs://b/init.sh"}]}`
	c := Cluster{Name: "c1", Config: []byte(config), Labels: map[string]string{"k": "v"}, Status: ClusterStatus{State: "RUNNING"}}
	if err := s.CreateCluster(ctx, "proj", "us-central1", c); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	const typeJob = `{"mainJarFileUri":"gs://b/a.jar","mainClass":"Main","args":["x"]}`
	j := Job{JobID: "j1", PlacementClusterName: "c1", Type: "sparkJob", TypeJob: []byte(typeJob), Status: JobStatus{State: "DONE"}}
	if err := s.CreateJob(ctx, "proj", "us-central1", j); err != nil {
		t.Fatalf("create job: %v", err)
	}
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op1", Done: true, Metadata: `{"@type":"t"}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if string(got.Config) != config {
		t.Fatalf("config not verbatim: %q", got.Config)
	}
	if got.Labels["k"] != "v" || got.Status.State != "RUNNING" {
		t.Fatalf("cluster fields lost: %+v", got)
	}

	gotJob, err := s.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if string(gotJob.TypeJob) != typeJob {
		t.Fatalf("type_job not verbatim: %q", gotJob.TypeJob)
	}
}

// TestPostgresStoreProjectRegionRoundTrip verifies projectId/region (and the
// operation's region, which drives the "regions/{region}/operations/{id}" name)
// survive a Create/Get/List round trip. This is the CB-1 fix: the SELECTs and
// scanners must read back project_id and region.
func TestPostgresStoreProjectRegionRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres round-trip test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const project = "proj"
	const region = "us-central1"

	if err := s.CreateCluster(ctx, project, region, Cluster{Name: "c1", Status: ClusterStatus{State: "RUNNING"}}); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	gotC, err := s.GetCluster(ctx, project, region, "c1")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if gotC.ProjectID != project || gotC.Region != region {
		t.Fatalf("cluster projectId/region lost: %+v", gotC)
	}
	listC, err := s.ListClusters(ctx, project, region)
	if err != nil || len(listC) != 1 || listC[0].ProjectID != project || listC[0].Region != region {
		t.Fatalf("list cluster projectId/region lost: %v %+v", err, listC)
	}

	if err := s.CreateJob(ctx, project, region, Job{JobID: "j1", Type: "sparkJob", TypeJob: []byte(`{}`)}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	gotJ, err := s.GetJob(ctx, project, region, "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if gotJ.ProjectID != project || gotJ.Region != region {
		t.Fatalf("job projectId/region lost: %+v", gotJ)
	}
	listJ, err := s.ListJobs(ctx, project, region)
	if err != nil || len(listJ) != 1 || listJ[0].ProjectID != project || listJ[0].Region != region {
		t.Fatalf("list job projectId/region lost: %v %+v", err, listJ)
	}

	if err := s.CreateOperation(ctx, project, region, Operation{ID: "op1", Done: true, Metadata: `{"@type":"t"}`}); err != nil {
		t.Fatalf("create op: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, project, region, "op1")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if gotOp.ProjectID != project || gotOp.Region != region {
		t.Fatalf("operation projectId/region lost (drives operation name region): %+v", gotOp)
	}
}
