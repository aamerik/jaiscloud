// Package sdk_dataproc_test exercises the jaiscloud-gcp emulator's Cloud
// Dataproc surface (dataproc.googleapis.com/v1) through the official Google
// REST apiary client. This validates wire-level parity with the real SDK.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_dataproc_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/dataproc/v1"
	"google.golang.org/api/option"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestSDKDataproc(t *testing.T) {
	ctx := context.Background()
	svc, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)

	const project = "proj"
	const region = "us-central1"
	clusterName := unique("c")

	// CreateCluster returns a done LRO in the emulator.
	op, err := svc.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:   project,
		ClusterName: clusterName,
		Config: &dataproc.ClusterConfig{
			GceClusterConfig: &dataproc.GceClusterConfig{ZoneUri: "us-central1-a"},
			SoftwareConfig:   &dataproc.SoftwareConfig{ImageVersion: "2.2"},
		},
	}).Do()
	require.NoError(t, err)
	require.True(t, op.Done)
	require.NotEmpty(t, op.Name)

	// GetCluster returns the logical cluster (created synchronously).
	cluster, err := svc.Projects.Regions.Clusters.Get(project, region, clusterName).Do()
	require.NoError(t, err)
	require.Equal(t, clusterName, cluster.ClusterName)
	require.Equal(t, project, cluster.ProjectId)
	require.Equal(t, "RUNNING", cluster.Status.State)

	// ListClusters includes it.
	list, err := svc.Projects.Regions.Clusters.List(project, region).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Clusters)

	// Submit a pyspark job (mock mode → DONE).
	jobID := unique("job")
	submitted, err := svc.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: jobID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			PysparkJob: &dataproc.PySparkJob{
				MainPythonFileUri: "gs://jaiscloud-bucket/main.py",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", submitted.Status.State)
	require.True(t, submitted.Done)

	// GetJob returns the completed job.
	got, err := svc.Projects.Regions.Jobs.Get(project, region, jobID).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", got.Status.State)
	require.Equal(t, clusterName, got.Placement.ClusterName)

	// Submit an unsupported hadoop job → ERROR (fail-loud, never silent).
	hadoopID := unique("hadoop")
	hadoop, err := svc.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: hadoopID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			HadoopJob: &dataproc.HadoopJob{
				MainJarFileUri: "gs://jaiscloud-bucket/mr.jar",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "ERROR", hadoop.Status.State)
	require.Contains(t, hadoop.Status.Details, "not supported")

	// DeleteCluster returns a done LRO.
	del, err := svc.Projects.Regions.Clusters.Delete(project, region, clusterName).Do()
	require.NoError(t, err)
	require.True(t, del.Done)

	// The cluster is gone.
	_, err = svc.Projects.Regions.Clusters.Get(project, region, clusterName).Do()
	require.Error(t, err)
}
