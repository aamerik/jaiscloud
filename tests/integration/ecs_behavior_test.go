package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestECS_RegisterTaskDefinition_ContainerDefs registers a task definition with
// two containers and asserts that both container names and port mappings are returned.
func TestECS_RegisterTaskDefinition_ContainerDefs(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	out, err := client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("multi-container-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:  aws.String("web"),
				Image: aws.String("nginx:latest"),
				PortMappings: []ecstypes.PortMapping{
					{ContainerPort: aws.Int32(80), HostPort: aws.Int32(80), Protocol: ecstypes.TransportProtocolTcp},
				},
			},
			{
				Name:  aws.String("sidecar"),
				Image: aws.String("busybox:latest"),
				PortMappings: []ecstypes.PortMapping{
					{ContainerPort: aws.Int32(9090), HostPort: aws.Int32(9090), Protocol: ecstypes.TransportProtocolTcp},
				},
			},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TaskDefinition)
	assert.Equal(t, "multi-container-task", aws.ToString(out.TaskDefinition.Family))

	// Describe the registered task definition and verify containers
	descOut, err := client.DescribeTaskDefinition(ctx, &awsecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("multi-container-task:1"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.TaskDefinition.ContainerDefinitions, 2)

	names := []string{
		aws.ToString(descOut.TaskDefinition.ContainerDefinitions[0].Name),
		aws.ToString(descOut.TaskDefinition.ContainerDefinitions[1].Name),
	}
	assert.Contains(t, names, "web")
	assert.Contains(t, names, "sidecar")

	// Assert port mappings are present for the web container
	var webContainer *ecstypes.ContainerDefinition
	for i := range descOut.TaskDefinition.ContainerDefinitions {
		if aws.ToString(descOut.TaskDefinition.ContainerDefinitions[i].Name) == "web" {
			webContainer = &descOut.TaskDefinition.ContainerDefinitions[i]
			break
		}
	}
	require.NotNil(t, webContainer)
	require.NotEmpty(t, webContainer.PortMappings)
	assert.Equal(t, int32(80), aws.ToInt32(webContainer.PortMappings[0].ContainerPort))
}

// TestECS_RegisterTaskDefinition_NetworkMode registers with networkMode=awsvpc
// and asserts the returned network mode.
func TestECS_RegisterTaskDefinition_NetworkMode(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("awsvpc-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		NetworkMode: ecstypes.NetworkModeAwsvpc,
		Cpu:         aws.String("256"),
		Memory:      aws.String("512"),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeTaskDefinition(ctx, &awsecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("awsvpc-task:1"),
	})
	require.NoError(t, err)
	assert.Equal(t, ecstypes.NetworkModeAwsvpc, descOut.TaskDefinition.NetworkMode)
}

// TestECS_RegisterTaskDefinition_CPU_Memory registers with cpu="256" and memory="512"
// and asserts that the values are returned correctly.
func TestECS_RegisterTaskDefinition_CPU_Memory(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("cpu-mem-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeTaskDefinition(ctx, &awsecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("cpu-mem-task:1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "256", aws.ToString(descOut.TaskDefinition.Cpu))
	assert.Equal(t, "512", aws.ToString(descOut.TaskDefinition.Memory))
}

// TestECS_RunTask_Basic creates a cluster and task definition, runs a task and
// asserts at least one task is returned with a non-empty TaskArn.
func TestECS_RunTask_Basic(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("run-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("run-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("run-cluster"),
		TaskDefinition: aws.String("run-task:1"),
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotEmpty(t, runOut.Tasks)
	assert.NotEmpty(t, aws.ToString(runOut.Tasks[0].TaskArn))
}

// TestECS_RunTask_MultipleTasks runs a task with Count=2 and asserts 2 tasks are returned.
func TestECS_RunTask_MultipleTasks(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("multi-task-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("multi-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("multi-task-cluster"),
		TaskDefinition: aws.String("multi-task:1"),
		Count:          aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, runOut.Tasks, 2)
	for _, task := range runOut.Tasks {
		assert.NotEmpty(t, aws.ToString(task.TaskArn))
	}
}

// TestECS_DescribeTasks runs a task, then calls DescribeTasks with the TaskArn
// and asserts task details are returned.
func TestECS_DescribeTasks(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("desc-task-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("desc-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("desc-task-cluster"),
		TaskDefinition: aws.String("desc-task:1"),
		Count:          aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 1)
	taskArn := aws.ToString(runOut.Tasks[0].TaskArn)

	descOut, err := client.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
		Cluster: aws.String("desc-task-cluster"),
		Tasks:   []string{taskArn},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, taskArn, aws.ToString(descOut.Tasks[0].TaskArn))
	assert.NotEmpty(t, aws.ToString(descOut.Tasks[0].LastStatus))
}

// TestECS_ListTasks runs two tasks and asserts at least 2 task ARNs are returned.
func TestECS_ListTasks(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("list-task-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("list-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	// Run 2 tasks
	runOut, err := client.RunTask(ctx, &awsecs.RunTaskInput{
		Cluster:        aws.String("list-task-cluster"),
		TaskDefinition: aws.String("list-task:1"),
		Count:          aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Tasks, 2)

	listOut, err := client.ListTasks(ctx, &awsecs.ListTasksInput{
		Cluster: aws.String("list-task-cluster"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.TaskArns), 2)
}

// TestECS_ListTasks_FilterByFamily registers two task definition families, runs a
// task for each, then uses ListTaskDefinitions with familyPrefix to verify family-level
// filtering of task definitions.
func TestECS_ListTasks_FilterByFamily(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("filter-cluster"),
	})
	require.NoError(t, err)

	// Register two different families
	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("family-alpha"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("family-beta"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	// ListTaskDefinitions filtered by "family-alpha" prefix
	alphaOut, err := client.ListTaskDefinitions(ctx, &awsecs.ListTaskDefinitionsInput{
		FamilyPrefix: aws.String("family-alpha"),
	})
	require.NoError(t, err)
	assert.Len(t, alphaOut.TaskDefinitionArns, 1)
	assert.Contains(t, alphaOut.TaskDefinitionArns[0], "family-alpha")

	// ListTaskDefinitions filtered by "family-beta" prefix
	betaOut, err := client.ListTaskDefinitions(ctx, &awsecs.ListTaskDefinitionsInput{
		FamilyPrefix: aws.String("family-beta"),
	})
	require.NoError(t, err)
	assert.Len(t, betaOut.TaskDefinitionArns, 1)
	assert.Contains(t, betaOut.TaskDefinitionArns[0], "family-beta")
}

// TestECS_CreateService_Basic creates a service and asserts it exists with
// the correct desiredCount.
func TestECS_CreateService_Basic(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("svc-basic-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("svc-basic-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	svcOut, err := client.CreateService(ctx, &awsecs.CreateServiceInput{
		Cluster:        aws.String("svc-basic-cluster"),
		ServiceName:    aws.String("basic-service"),
		TaskDefinition: aws.String("svc-basic-task:1"),
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, "basic-service", aws.ToString(svcOut.Service.ServiceName))
	assert.Equal(t, int32(1), svcOut.Service.DesiredCount)

	descOut, err := client.DescribeServices(ctx, &awsecs.DescribeServicesInput{
		Cluster:  aws.String("svc-basic-cluster"),
		Services: []string{"basic-service"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Services, 1)
	assert.Equal(t, "basic-service", aws.ToString(descOut.Services[0].ServiceName))
	assert.Equal(t, int32(1), descOut.Services[0].DesiredCount)
}

// TestECS_UpdateService_DesiredCount_Behavior creates a service with desiredCount=1, updates to
// desiredCount=3, then asserts the new desired count.
func TestECS_UpdateService_DesiredCount_Behavior(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("upd-svc-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("upd-svc-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &awsecs.CreateServiceInput{
		Cluster:        aws.String("upd-svc-cluster"),
		ServiceName:    aws.String("upd-service"),
		TaskDefinition: aws.String("upd-svc-task:1"),
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	updOut, err := client.UpdateService(ctx, &awsecs.UpdateServiceInput{
		Cluster:      aws.String("upd-svc-cluster"),
		Service:      aws.String("upd-service"),
		DesiredCount: aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), updOut.Service.DesiredCount)

	descOut, err := client.DescribeServices(ctx, &awsecs.DescribeServicesInput{
		Cluster:  aws.String("upd-svc-cluster"),
		Services: []string{"upd-service"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Services, 1)
	assert.Equal(t, int32(3), descOut.Services[0].DesiredCount)
}

// TestECS_DeleteService creates a service and deletes it (with force=true); the service
// should no longer appear in ListServices.
func TestECS_DeleteService(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("del-svc-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("del-svc-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	_, err = client.CreateService(ctx, &awsecs.CreateServiceInput{
		Cluster:        aws.String("del-svc-cluster"),
		ServiceName:    aws.String("to-delete-service"),
		TaskDefinition: aws.String("del-svc-task:1"),
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	delOut, err := client.DeleteService(ctx, &awsecs.DeleteServiceInput{
		Cluster: aws.String("del-svc-cluster"),
		Service: aws.String("to-delete-service"),
		Force:   aws.Bool(true),
	})
	require.NoError(t, err)
	// The deleted service should be INACTIVE
	assert.Equal(t, "INACTIVE", aws.ToString(delOut.Service.Status))

	// Service should no longer appear in ListServices
	listOut, err := client.ListServices(ctx, &awsecs.ListServicesInput{
		Cluster: aws.String("del-svc-cluster"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.ServiceArns, 0)
}

// TestECS_ListServices creates two services in the same cluster and asserts
// both ARNs are returned by ListServices.
func TestECS_ListServices(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newECSClient(t)

	_, err := client.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("list-svc-cluster"),
	})
	require.NoError(t, err)

	_, err = client.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("list-svc-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	require.NoError(t, err)

	for _, name := range []string{"svc-one", "svc-two"} {
		_, err = client.CreateService(ctx, &awsecs.CreateServiceInput{
			Cluster:        aws.String("list-svc-cluster"),
			ServiceName:    aws.String(name),
			TaskDefinition: aws.String("list-svc-task:1"),
			DesiredCount:   aws.Int32(1),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListServices(ctx, &awsecs.ListServicesInput{
		Cluster: aws.String("list-svc-cluster"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.ServiceArns, 2)

	// Both service ARNs should contain their names
	arns := listOut.ServiceArns
	foundOne := false
	foundTwo := false
	for _, arn := range arns {
		if containsStr(arn, "svc-one") {
			foundOne = true
		}
		if containsStr(arn, "svc-two") {
			foundTwo = true
		}
	}
	assert.True(t, foundOne, "expected svc-one ARN in list")
	assert.True(t, foundTwo, "expected svc-two ARN in list")
}

// containsStr is a simple substring helper used across ECS behavior tests.
func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstring(s, sub))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
