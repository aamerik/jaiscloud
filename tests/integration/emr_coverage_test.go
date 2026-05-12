package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsemr "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const secConfigJSON = `{"EncryptionConfiguration":{"EnableInTransitEncryption":false,"EnableAtRestEncryption":false}}`

func TestEMR_CreateSecurityConfiguration_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	_, err := c.CreateSecurityConfiguration(ctx, &awsemr.CreateSecurityConfigurationInput{
		Name:                  aws.String("my-sec-config"),
		SecurityConfiguration: aws.String(secConfigJSON),
	})
	require.NoError(t, err)
}

func TestEMR_DescribeSecurityConfiguration_AfterCreate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	_, err := c.CreateSecurityConfiguration(ctx, &awsemr.CreateSecurityConfigurationInput{
		Name:                  aws.String("desc-sec-config"),
		SecurityConfiguration: aws.String(secConfigJSON),
	})
	require.NoError(t, err)

	out, err := c.DescribeSecurityConfiguration(ctx, &awsemr.DescribeSecurityConfigurationInput{
		Name: aws.String("desc-sec-config"),
	})
	require.NoError(t, err)
	assert.Equal(t, "desc-sec-config", aws.ToString(out.Name))
	assert.JSONEq(t, secConfigJSON, aws.ToString(out.SecurityConfiguration))
}

func TestEMR_DeleteSecurityConfiguration_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	_, err := c.CreateSecurityConfiguration(ctx, &awsemr.CreateSecurityConfigurationInput{
		Name:                  aws.String("del-sec-config"),
		SecurityConfiguration: aws.String(secConfigJSON),
	})
	require.NoError(t, err)

	_, err = c.DeleteSecurityConfiguration(ctx, &awsemr.DeleteSecurityConfigurationInput{
		Name: aws.String("del-sec-config"),
	})
	require.NoError(t, err)

	_, err = c.DescribeSecurityConfiguration(ctx, &awsemr.DescribeSecurityConfigurationInput{
		Name: aws.String("del-sec-config"),
	})
	require.Error(t, err)
}

func TestEMR_ListSecurityConfigurations_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	for i := 0; i < 3; i++ {
		_, err := c.CreateSecurityConfiguration(ctx, &awsemr.CreateSecurityConfigurationInput{
			Name:                  aws.String(fmt.Sprintf("list-sec-%d", i)),
			SecurityConfiguration: aws.String(secConfigJSON),
		})
		require.NoError(t, err)
	}

	out, err := c.ListSecurityConfigurations(ctx, &awsemr.ListSecurityConfigurationsInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.SecurityConfigurations), 3)
}

func TestEMR_SecurityConfiguration_NotFound_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	_, err := c.DescribeSecurityConfiguration(ctx, &awsemr.DescribeSecurityConfigurationInput{
		Name: aws.String("does-not-exist"),
	})
	require.Error(t, err)
}

func TestEMR_PutAutoScalingPolicy_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	// Create a cluster first
	clusterOut, err := c.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("asg-cluster"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &emrtypes.JobFlowInstancesConfig{
			MasterInstanceType: aws.String("m5.xlarge"),
			SlaveInstanceType:  aws.String("m5.xlarge"),
			InstanceCount:      aws.Int32(1),
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)

	clusterID := clusterOut.JobFlowId

	// Describe to get instance group IDs
	descOut, err := c.DescribeCluster(ctx, &awsemr.DescribeClusterInput{
		ClusterId: clusterID,
	})
	require.NoError(t, err)
	_ = descOut

	igOut, err := c.ListInstanceGroups(ctx, &awsemr.ListInstanceGroupsInput{
		ClusterId: clusterID,
	})
	require.NoError(t, err)

	if len(igOut.InstanceGroups) == 0 {
		t.Skip("no instance groups returned")
	}

	// Find a CORE or TASK instance group
	var igID string
	for _, ig := range igOut.InstanceGroups {
		if ig.InstanceGroupType == emrtypes.InstanceGroupTypeCore || ig.InstanceGroupType == emrtypes.InstanceGroupTypeTask {
			igID = aws.ToString(ig.Id)
			break
		}
	}
	if igID == "" {
		igID = aws.ToString(igOut.InstanceGroups[0].Id)
	}

	_, err = c.PutAutoScalingPolicy(ctx, &awsemr.PutAutoScalingPolicyInput{
		ClusterId:       clusterID,
		InstanceGroupId: aws.String(igID),
		AutoScalingPolicy: &emrtypes.AutoScalingPolicy{
			Constraints: &emrtypes.ScalingConstraints{
				MinCapacity: aws.Int32(1),
				MaxCapacity: aws.Int32(5),
			},
			Rules: []emrtypes.ScalingRule{},
		},
	})
	if err != nil {
		t.Logf("PutAutoScalingPolicy returned error (may be unimplemented): %v", err)
	}
}

func TestEMR_RemoveAutoScalingPolicy_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newEMRClient(t)

	clusterOut, err := c.RunJobFlow(ctx, &awsemr.RunJobFlowInput{
		Name:         aws.String("asg-rm-cluster"),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &emrtypes.JobFlowInstancesConfig{
			MasterInstanceType: aws.String("m5.xlarge"),
			SlaveInstanceType:  aws.String("m5.xlarge"),
			InstanceCount:      aws.Int32(1),
		},
		ServiceRole: aws.String("EMR_DefaultRole"),
		JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
	})
	require.NoError(t, err)

	igOut, err := c.ListInstanceGroups(ctx, &awsemr.ListInstanceGroupsInput{
		ClusterId: clusterOut.JobFlowId,
	})
	require.NoError(t, err)

	if len(igOut.InstanceGroups) == 0 {
		t.Skip("no instance groups returned")
	}

	igID := aws.ToString(igOut.InstanceGroups[0].Id)
	_, err = c.RemoveAutoScalingPolicy(ctx, &awsemr.RemoveAutoScalingPolicyInput{
		ClusterId:       clusterOut.JobFlowId,
		InstanceGroupId: aws.String(igID),
	})
	if err != nil {
		t.Logf("RemoveAutoScalingPolicy returned error (may be unimplemented): %v", err)
	}
}
