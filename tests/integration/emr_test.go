package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEMR_RunJobFlowDescribeTerminate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEMRClient(t)

	out, err := client.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("my-cluster"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType: aws.String("m5.xlarge"),
			SlaveInstanceType:  aws.String("m5.xlarge"),
			InstanceCount:      aws.Int32(3),
		},
		Applications: []types.Application{
			{Name: aws.String("Spark")},
			{Name: aws.String("Hadoop")},
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)
	assert.NotEmpty(t, clusterID)

	descOut, err := client.DescribeCluster(ctx, &awsemr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	assert.Equal(t, clusterID, aws.ToString(descOut.Cluster.Id))
	assert.Equal(t, "my-cluster", aws.ToString(descOut.Cluster.Name))
	assert.Equal(t, types.ClusterStateWaiting, descOut.Cluster.Status.State)

	_, err = client.TerminateJobFlows(ctx, &awsemr.TerminateJobFlowsInput{
		JobFlowIds: []string{clusterID},
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeCluster(ctx, &awsemr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterStateTerminated, descOut2.Cluster.Status.State)
}

func TestEMR_ListClusters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEMRClient(t)

	for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
		_, err := client.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
			Name:         aws.String(name),
			ReleaseLabel: aws.String("emr-6.10.0"),
			Instances: &types.JobFlowInstancesConfig{
				MasterInstanceType: aws.String("m5.xlarge"),
				SlaveInstanceType:  aws.String("m5.xlarge"),
				InstanceCount:      aws.Int32(1),
			},
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListClusters(ctx, &awsemr.ListClustersInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Clusters, 3)
}

func TestEMR_AddAndListSteps(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEMRClient(t)

	out, err := client.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("step-cluster"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType: aws.String("m5.xlarge"),
			SlaveInstanceType:  aws.String("m5.xlarge"),
			InstanceCount:      aws.Int32(2),
		},
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)

	addOut, err := client.AddJobFlowSteps(ctx, &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("my-step"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep: &types.HadoopJarStepConfig{
					Jar:  aws.String("s3://my-bucket/my-job.jar"),
					Args: []string{"arg1", "arg2"},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, addOut.StepIds, 1)

	listOut, err := client.ListSteps(ctx, &awsemr.ListStepsInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Steps, 1)
	assert.Equal(t, "my-step", aws.ToString(listOut.Steps[0].Name))

	descOut, err := client.DescribeStep(ctx, &awsemr.DescribeStepInput{
		ClusterId: aws.String(clusterID),
		StepId:    aws.String(addOut.StepIds[0]),
	})
	require.NoError(t, err)
	assert.Equal(t, addOut.StepIds[0], aws.ToString(descOut.Step.Id))
}

func TestEMR_ListInstanceGroups(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEMRClient(t)

	out, err := client.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("ig-cluster"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType: aws.String("m5.xlarge"),
			SlaveInstanceType:  aws.String("m5.xlarge"),
			InstanceCount:      aws.Int32(3),
		},
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)

	igOut, err := client.ListInstanceGroups(ctx, &awsemr.ListInstanceGroupsInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(igOut.InstanceGroups), 2)

	roles := map[string]bool{}
	for _, g := range igOut.InstanceGroups {
		roles[string(g.InstanceGroupType)] = true
	}
	assert.True(t, roles["MASTER"])
	assert.True(t, roles["CORE"])
}
