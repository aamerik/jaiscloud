package sdk_dataproc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	container "google.golang.org/api/container/v1"
	"google.golang.org/api/dataproc/v1"
	"google.golang.org/api/option"
)

// gkeEndpoint is the emulator's REST root with the "/container/" path prefix,
// so GKE's shared /v1/projects/{p}/locations/{l}/clusters path is routed to the
// GKE service rather than Managed Kafka (see the sdk-container suite).
func gkeEndpoint() string { return endpoint() + "container/" }

// gkeOpts are the client options for the GKE apiary client.
func gkeOpts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(gkeEndpoint()), option.WithoutAuthentication()}
}

// TestSDKDataprocOnGKE reconciles a Dataproc-on-GKE virtual cluster's node pools
// against the emulated GKE API and reads them back through the GKE client — the
// cross-API case users actually exercise. Every assertion is a call to one of
// the two documented APIs; nothing here inspects pods, nodes or manifests.
func TestSDKDataprocOnGKE(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	const project = "proj"
	const region = "us-central1"
	gkeName := unique("gke")
	gkeParent := "projects/" + project + "/locations/" + region + "/clusters/" + gkeName

	// A standard, Workload-Identity-enabled GKE cluster is the prerequisite:
	// Dataproc on GKE rejects an Autopilot or non-WI cluster.
	_, err = gke.Projects.Locations.Clusters.Create(
		"projects/"+project+"/locations/"+region,
		&container.CreateClusterRequest{Cluster: &container.Cluster{
			Name:                   gkeName,
			InitialClusterVersion:  "1.30",
			WorkloadIdentityConfig: &container.WorkloadIdentityConfig{WorkloadPool: project + ".svc.id.goog"},
		}},
	).Do()
	require.NoError(t, err)

	// One caller-shaped pool and one named-only pool (nodePoolConfig omitted →
	// "any node pool with the specified name is used").
	clusterName := unique("dpgke")
	vcc := &dataproc.VirtualClusterConfig{
		StagingBucket: "dataproc-staging-" + project,
		KubernetesClusterConfig: &dataproc.KubernetesClusterConfig{
			KubernetesNamespace: "dataproc",
			GkeClusterConfig: &dataproc.GkeClusterConfig{
				GkeClusterTarget: gkeParent,
				NodePoolTarget: []*dataproc.GkeNodePoolTarget{
					{
						NodePool: gkeParent + "/nodePools/shaped-pool",
						Roles:    []string{"DEFAULT", "CONTROLLER"},
						NodePoolConfig: &dataproc.GkeNodePoolConfig{
							Config: &dataproc.GkeNodeConfig{
								MachineType:   "n1-standard-4",
								LocalSsdCount: 1,
							},
							Autoscaling: &dataproc.GkeNodePoolAutoscalingConfig{MinNodeCount: 1, MaxNodeCount: 4},
							Locations:   []string{region + "-a"},
						},
					},
					{
						NodePool: "spark-pool",
						Roles:    []string{"SPARK_DRIVER", "SPARK_EXECUTOR"},
					},
				},
			},
			KubernetesSoftwareConfig: &dataproc.KubernetesSoftwareConfig{
				ComponentVersion: map[string]string{"SPARK": "3.5"},
			},
		},
	}
	op, err := dp.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:            project,
		ClusterName:          clusterName,
		VirtualClusterConfig: vcc,
	}).Do()
	require.NoError(t, err)
	created := pollOperation(t, dp, op.Name)
	require.True(t, created.Done)
	var cluster dataproc.Cluster
	require.NoError(t, json.Unmarshal(created.Response, &cluster))
	require.Equal(t, "RUNNING", cluster.Status.State)

	// nodePoolConfig is input-only: the read never echoes it.
	require.NotNil(t, cluster.VirtualClusterConfig)
	require.NotNil(t, cluster.VirtualClusterConfig.KubernetesClusterConfig)
	readBack := cluster.VirtualClusterConfig.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget
	require.Len(t, readBack, 2)
	for _, target := range readBack {
		require.Nil(t, target.NodePoolConfig, "nodePoolConfig must not be returned by the API")
	}

	// Both pools exist in the GKE API with the applied shape and the Dataproc
	// auto labels.
	pools, err := gke.Projects.Locations.Clusters.NodePools.List(gkeParent).Do()
	require.NoError(t, err)
	byName := map[string]*container.NodePool{}
	for _, p := range pools.NodePools {
		byName[p.Name] = p
	}
	shaped, ok := byName["shaped-pool"]
	require.True(t, ok, "shaped pool not created: %v", byName)
	require.Equal(t, "n1-standard-4", shaped.Config.MachineType)
	require.EqualValues(t, 1, shaped.Config.LocalSsdCount)
	require.EqualValues(t, 4, shaped.Autoscaling.MaxNodeCount)
	require.Equal(t, []string{region + "-a"}, shaped.Locations)
	require.Equal(t, clusterName, shaped.Config.Labels["goog-dataproc-cluster-name"])
	require.Equal(t, region, shaped.Config.Labels["goog-dataproc-location"])
	require.NotEmpty(t, shaped.Config.Labels["goog-dataproc-cluster-uuid"])

	adopted, ok := byName["spark-pool"]
	require.True(t, ok, "named-only pool not created: %v", byName)
	require.NotNil(t, adopted.Autoscaling)
	require.EqualValues(t, 1, adopted.Autoscaling.MinNodeCount)
	require.EqualValues(t, 10, adopted.Autoscaling.MaxNodeCount)

	// A job submitted to a reconciled virtual cluster still runs to DONE.
	jobID := unique("gkejob")
	job, err := dp.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: jobID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			PysparkJob: &dataproc.PySparkJob{
				MainPythonFileUri: "gs://jaiscloud-bucket/jobs/pi.py",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", pollJob(t, dp, project, region, jobID).Status.State)
	_ = job

	// Deleting the virtual cluster leaves its node pools in place (real Dataproc
	// does not delete them).
	del, err := dp.Projects.Regions.Clusters.Delete(project, region, clusterName).Do()
	require.NoError(t, err)
	require.True(t, pollOperation(t, dp, del.Name).Done)
	pools, err = gke.Projects.Locations.Clusters.NodePools.List(gkeParent).Do()
	require.NoError(t, err)
	post := map[string]bool{}
	for _, p := range pools.NodePools {
		post[p.Name] = true
	}
	require.True(t, post["shaped-pool"])
	require.True(t, post["spark-pool"])
}

// TestSDKDataprocOnGKEConflictingPoolShape verifies a nodePoolConfig that
// disagrees with an existing pool fails the create operation: the cluster is
// persisted in ERROR with a status.detail naming the pool and field.
func TestSDKDataprocOnGKEConflictingPoolShape(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	const project = "proj"
	const region = "us-central1"
	gkeName := unique("gke-c")
	gkeParent := "projects/" + project + "/locations/" + region + "/clusters/" + gkeName
	_, err = gke.Projects.Locations.Clusters.Create(
		"projects/"+project+"/locations/"+region,
		&container.CreateClusterRequest{Cluster: &container.Cluster{
			Name:                   gkeName,
			WorkloadIdentityConfig: &container.WorkloadIdentityConfig{WorkloadPool: project + ".svc.id.goog"},
		}},
	).Do()
	require.NoError(t, err)
	// An existing pool with a shape the virtual cluster disagrees with.
	_, err = gke.Projects.Locations.Clusters.NodePools.Create(gkeParent, &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{
			Name:   "taken-pool",
			Config: &container.NodeConfig{MachineType: "e2-medium"},
		},
	}).Do()
	require.NoError(t, err)

	clusterName := unique("dpgke-c")
	op, err := dp.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:   project,
		ClusterName: clusterName,
		VirtualClusterConfig: &dataproc.VirtualClusterConfig{
			StagingBucket: "dataproc-staging-" + project,
			KubernetesClusterConfig: &dataproc.KubernetesClusterConfig{
				GkeClusterConfig: &dataproc.GkeClusterConfig{
					GkeClusterTarget: gkeParent,
					NodePoolTarget: []*dataproc.GkeNodePoolTarget{{
						NodePool: "taken-pool",
						Roles:    []string{"DEFAULT"},
						NodePoolConfig: &dataproc.GkeNodePoolConfig{
							Config: &dataproc.GkeNodeConfig{MachineType: "n1-standard-4"},
						},
					}},
				},
				KubernetesSoftwareConfig: &dataproc.KubernetesSoftwareConfig{
					ComponentVersion: map[string]string{"SPARK": "3.5"},
				},
			},
		},
	}).Do()
	require.NoError(t, err)
	created := pollOperation(t, dp, op.Name)
	require.True(t, created.Done)
	var cluster dataproc.Cluster
	require.NoError(t, json.Unmarshal(created.Response, &cluster))
	require.Equal(t, "ERROR", cluster.Status.State)
	require.Contains(t, cluster.Status.Detail, "taken-pool")
	require.Contains(t, cluster.Status.Detail, "machineType")
}
