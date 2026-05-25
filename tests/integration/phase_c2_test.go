package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// pollTaskStatus polls DescribeTasks until LastStatus matches wantStatus or the
// deadline is exceeded. It returns the final observed status.
func pollTaskStatus(t *testing.T, client *awsecs.Client, clusterName, taskARN, wantStatus string, timeout time.Duration) string {
	t.Helper()
	deadline := clock.RealNow().Add(timeout)
	for clock.RealNow().Before(deadline) {
		out, err := client.DescribeTasks(context.Background(), &awsecs.DescribeTasksInput{
			Cluster: aws.String(clusterName),
			Tasks:   []string{taskARN},
		})
		require.NoError(t, err)
		require.Len(t, out.Tasks, 1)
		if aws.ToString(out.Tasks[0].LastStatus) == wantStatus {
			return wantStatus
		}
		time.Sleep(500 * time.Millisecond)
	}
	// Return final status regardless.
	out, _ := client.DescribeTasks(context.Background(), &awsecs.DescribeTasksInput{
		Cluster: aws.String(clusterName),
		Tasks:   []string{taskARN},
	})
	if len(out.Tasks) > 0 {
		return aws.ToString(out.Tasks[0].LastStatus)
	}
	return ""
}

func TestECSRunTaskMock(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("test-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("mock-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{
				Name:  aws.String("app"),
				Image: aws.String("alpine:latest"),
			},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("test-cluster"),
		TaskDefinition: aws.String("mock-task:1"),
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)

	taskARN := aws.ToString(runOut.Tasks[0].TaskArn)
	assert.NotEmpty(t, taskARN)
	assert.NotEmpty(t, aws.ToString(runOut.Tasks[0].ClusterArn))
	assert.NotEmpty(t, aws.ToString(runOut.Tasks[0].TaskDefinitionArn))

	// With the mock executor the task transitions RUNNING → STOPPED within a few seconds.
	finalStatus := pollTaskStatus(t, client, "test-cluster", taskARN, "STOPPED", 10*time.Second)
	assert.Equal(t, "STOPPED", finalStatus)

	// Verify task record fields via DescribeTasks.
	descOut, err := client.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
		Cluster: aws.String("test-cluster"),
		Tasks:   []string{taskARN},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, "STOPPED", aws.ToString(descOut.Tasks[0].LastStatus))
	assert.NotEmpty(t, aws.ToString(descOut.Tasks[0].TaskArn))
	assert.NotEmpty(t, aws.ToString(descOut.Tasks[0].ClusterArn))
}

func TestECSStopTask(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("stop-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("stop-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("alpine:latest")},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("stop-cluster"),
		TaskDefinition: aws.String("stop-task:1"),
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)
	taskARN := aws.ToString(runOut.Tasks[0].TaskArn)

	stopOut, err := client.StopTask(ctx, &awsecs.StopTaskInput{
		Cluster: aws.String("stop-cluster"),
		Task:    aws.String(taskARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", aws.ToString(stopOut.Task.LastStatus))

	// DescribeTasks confirms STOPPED.
	descOut, err := client.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
		Cluster: aws.String("stop-cluster"),
		Tasks:   []string{taskARN},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, "STOPPED", aws.ToString(descOut.Tasks[0].LastStatus))
}

func TestECSTaskWithMultipleContainers(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("multi-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("multi-container-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{Name: aws.String("frontend"), Image: aws.String("nginx:latest")},
			{Name: aws.String("backend"), Image: aws.String("alpine:latest")},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("multi-cluster"),
		TaskDefinition: aws.String("multi-container-task:1"),
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)
	taskARN := aws.ToString(runOut.Tasks[0].TaskArn)
	assert.NotEmpty(t, taskARN)

	// Poll until STOPPED so containers are populated.
	pollTaskStatus(t, client, "multi-cluster", taskARN, "STOPPED", 10*time.Second)

	descOut, err := client.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
		Cluster: aws.String("multi-cluster"),
		Tasks:   []string{taskARN},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	// Both containers should appear in the task response.
	assert.Len(t, descOut.Tasks[0].Containers, 2)

	containerNames := make([]string, 0, 2)
	for _, c := range descOut.Tasks[0].Containers {
		containerNames = append(containerNames, aws.ToString(c.Name))
	}
	assert.ElementsMatch(t, []string{"frontend", "backend"}, containerNames)
}
