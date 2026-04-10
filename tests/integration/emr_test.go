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

// ─── helpers ──────────────────────────────────────────────────────────────────

func createWaitingCluster(t *testing.T, client *awsemr.Client, name string) string {
	t.Helper()
	out, err := client.RunJobFlow(context.Background(), &awsemr.RunJobFlowInput{
		Name:         aws.String(name),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType:         aws.String("m5.xlarge"),
			SlaveInstanceType:          aws.String("m5.xlarge"),
			InstanceCount:              aws.Int32(3),
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)
	id := aws.ToString(out.JobFlowId)
	assert.True(t, len(id) > 0 && id[:2] == "j-", "cluster ID should start with j-")
	assert.NotEmpty(t, aws.ToString(out.ClusterArn))
	return id
}

// ─── Cluster lifecycle ────────────────────────────────────────────────────────

func TestEMR_RunJobFlowDescribeTerminate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterID := createWaitingCluster(t, c, "my-cluster")

	desc, err := c.DescribeCluster(ctx, &awsemr.DescribeClusterInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	assert.Equal(t, clusterID, aws.ToString(desc.Cluster.Id))
	assert.Equal(t, "my-cluster", aws.ToString(desc.Cluster.Name))
	assert.Equal(t, types.ClusterStateWaiting, desc.Cluster.Status.State)
	assert.Equal(t, "emr-6.10.0", aws.ToString(desc.Cluster.ReleaseLabel))
	assert.False(t, aws.ToBool(desc.Cluster.AutoTerminate))

	_, err = c.TerminateJobFlows(ctx, &awsemr.TerminateJobFlowsInput{JobFlowIds: []string{clusterID}})
	require.NoError(t, err)

	desc2, err := c.DescribeCluster(ctx, &awsemr.DescribeClusterInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterStateTerminated, desc2.Cluster.Status.State)
}

func TestEMR_AutoTerminateState(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	out, err := c.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("auto-terminate"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType:         aws.String("m5.xlarge"),
			InstanceCount:              aws.Int32(1),
			KeepJobFlowAliveWhenNoSteps: aws.Bool(false),
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)

	desc, err := c.DescribeCluster(ctx, &awsemr.DescribeClusterInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterStateTerminated, desc.Cluster.Status.State)
	assert.True(t, aws.ToBool(desc.Cluster.AutoTerminate))
}

func TestEMR_TerminationProtection(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	out, err := c.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("protected"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType:         aws.String("m5.xlarge"),
			InstanceCount:              aws.Int32(1),
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			TerminationProtected:        aws.Bool(true),
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)

	_, err = c.TerminateJobFlows(ctx, &awsemr.TerminateJobFlowsInput{JobFlowIds: []string{clusterID}})
	require.Error(t, err, "terminating a protected cluster should fail")

	// Disable protection, then terminate
	_, err = c.SetTerminationProtection(ctx, &awsemr.SetTerminationProtectionInput{
		JobFlowIds:           []string{clusterID},
		TerminationProtected: aws.Bool(false),
	})
	require.NoError(t, err)

	_, err = c.TerminateJobFlows(ctx, &awsemr.TerminateJobFlowsInput{JobFlowIds: []string{clusterID}})
	require.NoError(t, err)
}

func TestEMR_ListClusters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
		createWaitingCluster(t, c, name)
	}

	listOut, err := c.ListClusters(ctx, &awsemr.ListClustersInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Clusters, 3)
	for _, cl := range listOut.Clusters {
		assert.NotEmpty(t, aws.ToString(cl.ClusterArn))
	}
}

func TestEMR_ModifyCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterID := createWaitingCluster(t, c, "modify-test")
	resp, err := c.ModifyCluster(ctx, &awsemr.ModifyClusterInput{
		ClusterId:            aws.String(clusterID),
		StepConcurrencyLevel: aws.Int32(5),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(5), aws.ToInt32(resp.StepConcurrencyLevel))
}

// ─── Steps ────────────────────────────────────────────────────────────────────

func TestEMR_AddAndListSteps(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterID := createWaitingCluster(t, c, "step-cluster")

	addOut, err := c.AddJobFlowSteps(ctx, &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("my-step"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep: &types.HadoopJarStepConfig{
					Jar:  aws.String("command-runner.jar"),
					Args: []string{"spark-submit", "--class", "com.example.Main"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, addOut.StepIds, 1)
	stepID := addOut.StepIds[0]
	assert.True(t, len(stepID) > 0 && stepID[:2] == "s-", "step ID should start with s-")

	listOut, err := c.ListSteps(ctx, &awsemr.ListStepsInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	require.Len(t, listOut.Steps, 1)
	assert.Equal(t, "my-step", aws.ToString(listOut.Steps[0].Name))

	descOut, err := c.DescribeStep(ctx, &awsemr.DescribeStepInput{
		ClusterId: aws.String(clusterID),
		StepId:    aws.String(stepID),
	})
	require.NoError(t, err)
	assert.Equal(t, stepID, aws.ToString(descOut.Step.Id))
	assert.Equal(t, types.StepStateCompleted, descOut.Step.Status.State)
}

func TestEMR_CancelSteps(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterID := createWaitingCluster(t, c, "cancel-cluster")
	addOut, err := c.AddJobFlowSteps(ctx, &awsemr.AddJobFlowStepsInput{
		JobFlowId: aws.String(clusterID),
		Steps: []types.StepConfig{
			{
				Name:            aws.String("step-to-cancel"),
				ActionOnFailure: types.ActionOnFailureContinue,
				HadoopJarStep:   &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar")},
			},
		},
	})
	require.NoError(t, err)
	stepID := addOut.StepIds[0]

	// Steps start as COMPLETED in our emulator — cancel returns FAILED_TO_CANCEL
	cancelOut, err := c.CancelSteps(ctx, &awsemr.CancelStepsInput{
		ClusterId: aws.String(clusterID),
		StepIds:   []string{stepID},
	})
	require.NoError(t, err)
	require.Len(t, cancelOut.CancelStepsInfoList, 1)
	assert.Equal(t, stepID, aws.ToString(cancelOut.CancelStepsInfoList[0].StepId))
}

// ─── Instance groups ──────────────────────────────────────────────────────────

func TestEMR_ListInstanceGroups(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterID := createWaitingCluster(t, c, "ig-cluster")

	igOut, err := c.ListInstanceGroups(ctx, &awsemr.ListInstanceGroupsInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(igOut.InstanceGroups), 2)

	roles := map[string]bool{}
	for _, g := range igOut.InstanceGroups {
		roles[string(g.InstanceGroupType)] = true
		assert.True(t, len(aws.ToString(g.Id)) > 0)
	}
	assert.True(t, roles["MASTER"])
	assert.True(t, roles["CORE"])

	// Add a TASK group
	addOut, err := c.AddInstanceGroups(ctx, &awsemr.AddInstanceGroupsInput{
		JobFlowId: aws.String(clusterID),
		InstanceGroups: []types.InstanceGroupConfig{
			{
				Name:          aws.String("Task"),
				InstanceRole:  types.InstanceRoleTypeTask,
				InstanceType:  aws.String("m5.xlarge"),
				InstanceCount: aws.Int32(2),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, addOut.InstanceGroupIds, 1)

	igOut2, err := c.ListInstanceGroups(ctx, &awsemr.ListInstanceGroupsInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	assert.Equal(t, 3, len(igOut2.InstanceGroups))
}

// ─── Instance fleets ──────────────────────────────────────────────────────────

func TestEMR_InstanceFleets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	out, err := c.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("fleet-cluster"),
		ReleaseLabel: aws.String("emr-6.15.0"),
		Instances: &types.JobFlowInstancesConfig{
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			InstanceFleets: []types.InstanceFleetConfig{
				{
					InstanceFleetType:        types.InstanceFleetTypeMaster,
					Name:                     aws.String("master-fleet"),
					TargetOnDemandCapacity:   aws.Int32(1),
					InstanceTypeConfigs: []types.InstanceTypeConfig{
						{InstanceType: aws.String("m5.xlarge")},
					},
				},
			},
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)

	// Add CORE fleet
	addFleet, err := c.AddInstanceFleet(ctx, &awsemr.AddInstanceFleetInput{
		ClusterId: aws.String(clusterID),
		InstanceFleet: &types.InstanceFleetConfig{
			InstanceFleetType:      types.InstanceFleetTypeCore,
			Name:                   aws.String("core-fleet"),
			TargetOnDemandCapacity: aws.Int32(2),
			InstanceTypeConfigs: []types.InstanceTypeConfig{
				{InstanceType: aws.String("m5.xlarge")},
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(addFleet.InstanceFleetId))

	listFleets, err := c.ListInstanceFleets(ctx, &awsemr.ListInstanceFleetsInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	fleetTypes := map[string]bool{}
	for _, f := range listFleets.InstanceFleets {
		fleetTypes[string(f.InstanceFleetType)] = true
	}
	assert.True(t, fleetTypes["MASTER"])
	assert.True(t, fleetTypes["CORE"])
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestEMR_Tags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	out, err := c.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("tagged-cluster"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &types.JobFlowInstancesConfig{
			MasterInstanceType:         aws.String("m5.xlarge"),
			InstanceCount:              aws.Int32(1),
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
		},
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)
	clusterID := aws.ToString(out.JobFlowId)

	_, err = c.AddTags(ctx, &awsemr.AddTagsInput{
		ResourceId: aws.String(clusterID),
		Tags:       []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}},
	})
	require.NoError(t, err)

	desc, err := c.DescribeCluster(ctx, &awsemr.DescribeClusterInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	tagMap := map[string]string{}
	for _, tag := range desc.Cluster.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "data", tagMap["team"])

	_, err = c.RemoveTags(ctx, &awsemr.RemoveTagsInput{
		ResourceId: aws.String(clusterID),
		TagKeys:    []string{"env"},
	})
	require.NoError(t, err)

	desc2, err := c.DescribeCluster(ctx, &awsemr.DescribeClusterInput{ClusterId: aws.String(clusterID)})
	require.NoError(t, err)
	tagKeys := map[string]bool{}
	for _, tag := range desc2.Cluster.Tags {
		tagKeys[aws.ToString(tag.Key)] = true
	}
	assert.False(t, tagKeys["env"])
	assert.True(t, tagKeys["team"])
}

// ─── Block public access ──────────────────────────────────────────────────────

func TestEMR_BlockPublicAccess(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	getResp, err := c.GetBlockPublicAccessConfiguration(ctx, &awsemr.GetBlockPublicAccessConfigurationInput{})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(getResp.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules))

	_, err = c.PutBlockPublicAccessConfiguration(ctx, &awsemr.PutBlockPublicAccessConfigurationInput{
		BlockPublicAccessConfiguration: &types.BlockPublicAccessConfiguration{
			BlockPublicSecurityGroupRules: aws.Bool(true),
			PermittedPublicSecurityGroupRuleRanges: []types.PortRange{
				{MinRange: aws.Int32(22), MaxRange: aws.Int32(22)},
			},
		},
	})
	require.NoError(t, err)

	getResp2, err := c.GetBlockPublicAccessConfiguration(ctx, &awsemr.GetBlockPublicAccessConfigurationInput{})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(getResp2.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules))
}

// ─── Managed scaling ──────────────────────────────────────────────────────────

func TestEMR_ManagedScalingPolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterID := createWaitingCluster(t, c, "scaling-cluster")

	_, err := c.PutManagedScalingPolicy(ctx, &awsemr.PutManagedScalingPolicyInput{
		ClusterId: aws.String(clusterID),
		ManagedScalingPolicy: &types.ManagedScalingPolicy{
			ComputeLimits: &types.ComputeLimits{
				UnitType:                     types.ComputeLimitsUnitTypeInstanceFleetUnits,
				MinimumCapacityUnits:         aws.Int32(2),
				MaximumCapacityUnits:         aws.Int32(10),
				MaximumOnDemandCapacityUnits: aws.Int32(5),
			},
		},
	})
	require.NoError(t, err)

	getResp, err := c.GetManagedScalingPolicy(ctx, &awsemr.GetManagedScalingPolicyInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	assert.NotNil(t, getResp.ManagedScalingPolicy)

	_, err = c.RemoveManagedScalingPolicy(ctx, &awsemr.RemoveManagedScalingPolicyInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
}
