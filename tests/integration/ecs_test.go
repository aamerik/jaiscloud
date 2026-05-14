package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECS_CreateListDeleteCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	out, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("my-cluster"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-cluster", aws.ToString(out.Cluster.ClusterName))
	assert.Equal(t, "ACTIVE", aws.ToString(out.Cluster.Status))

	listOut, err := client.ListClusters(ctx, &awsecs.ListClustersInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.ClusterArns, 1)

	_, err = client.DeleteCluster(ctx, &awsecs.DeleteClusterInput{
		Cluster: aws.String("my-cluster"),
	})
	require.NoError(t, err)

	listOut, err = client.ListClusters(ctx, &awsecs.ListClustersInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.ClusterArns, 0)
}

func TestECS_RegisterDescribeDeregisterTaskDefinition(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	out, err := client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("my-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{
				Name:  aws.String("app"),
				Image: aws.String("nginx:latest"),
			},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-task", aws.ToString(out.TaskDefinition.Family))
	assert.Equal(t, int32(1), out.TaskDefinition.Revision)
	assert.Equal(t, types.TaskDefinitionStatusActive, out.TaskDefinition.Status)

	// Register again → revision 2
	out2, err := client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("my-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:1.25")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), out2.TaskDefinition.Revision)

	descOut, err := client.DescribeTaskDefinition(ctx, &awsecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("my-task:1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-task", aws.ToString(descOut.TaskDefinition.Family))

	listOut, err := client.ListTaskDefinitions(ctx, &awsecs.ListTaskDefinitionsInput{
		FamilyPrefix: aws.String("my-task"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.TaskDefinitionArns, 2)

	_, err = client.DeregisterTaskDefinition(ctx, &awsecs.DeregisterTaskDefinitionInput{
		TaskDefinition: aws.String("my-task:1"),
	})
	require.NoError(t, err)
}

func TestECS_CreateDescribeDeleteService(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("my-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("my-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	svcOut, err := client.CreateService(ctx, &awsecs.CreateServiceInput{
		Cluster:        aws.String("my-cluster"),
		ServiceName:    aws.String("my-service"),
		TaskDefinition: aws.String("my-task:1"),
		DesiredCount:   aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-service", aws.ToString(svcOut.Service.ServiceName))
	assert.Equal(t, int32(2), svcOut.Service.DesiredCount)

	descOut, err := client.DescribeServices(ctx, &awsecs.DescribeServicesInput{
		Cluster:  aws.String("my-cluster"),
		Services: []string{"my-service"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Services, 1)
	assert.Equal(t, "my-service", aws.ToString(descOut.Services[0].ServiceName))

	_, err = client.UpdateService(ctx, &awsecs.UpdateServiceInput{
		Cluster:      aws.String("my-cluster"),
		Service:      aws.String("my-service"),
		DesiredCount: aws.Int32(3),
	})
	require.NoError(t, err)

	_, err = client.DeleteService(ctx, &awsecs.DeleteServiceInput{
		Cluster: aws.String("my-cluster"),
		Service: aws.String("my-service"),
		Force:   aws.Bool(true),
	})
	require.NoError(t, err)

	listOut, err := client.ListServices(ctx, &awsecs.ListServicesInput{
		Cluster: aws.String("my-cluster"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.ServiceArns, 0)
}

func TestECS_RunDescribeStopTask(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("my-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("my-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("my-cluster"),
		TaskDefinition: aws.String("my-task:1"),
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)
	taskArn := aws.ToString(runOut.Tasks[0].TaskArn)
	assert.NotEmpty(t, taskArn)

	listOut, err := client.ListTasks(ctx, &awsecs.ListTasksInput{
		Cluster: aws.String("my-cluster"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.TaskArns, 1)

	descOut, err := client.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
		Cluster: aws.String("my-cluster"),
		Tasks:   []string{taskArn},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, "RUNNING", aws.ToString(descOut.Tasks[0].LastStatus))

	_, err = client.StopTask(ctx, &awsecs.StopTaskInput{
		Cluster: aws.String("my-cluster"),
		Task:    aws.String(taskArn),
	})
	require.NoError(t, err)
}

func TestECSARNFormat(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)
	out, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("arn-cluster"),
	})
	require.NoError(t, err)
	arn := aws.ToString(out.Cluster.ClusterArn)
	assert.Regexp(t, `^arn:aws:ecs:us-east-1:000000000000:cluster/arn-cluster$`, arn)
}

// ensure types import is used
var _ types.Cluster
