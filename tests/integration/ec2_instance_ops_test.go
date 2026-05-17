package integration_test

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEC2RunInstancesWithParams verifies RunInstances with SecurityGroupIds,
// UserData, TagSpecifications, and MaxCount=2.
func TestEC2RunInstancesWithParams(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	userData := base64.StdEncoding.EncodeToString([]byte("#!/bin/bash\necho hello"))

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(2),
		MaxCount:     aws.Int32(2),
		SecurityGroupIds: []string{"sg-aabbccdd", "sg-11223344"},
		UserData:     aws.String(userData),
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeInstance,
				Tags: []types.Tag{
					{Key: aws.String("Name"), Value: aws.String("test-instance")},
					{Key: aws.String("Env"), Value: aws.String("integration")},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 2, "MaxCount=2 must return 2 instances")

	for i, inst := range runOut.Instances {
		assert.NotEmpty(t, aws.ToString(inst.InstanceId), "instance %d must have an ID", i)

		// Tags must be present on both instances.
		tagMap := make(map[string]string)
		for _, tag := range inst.Tags {
			tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
		assert.Equal(t, "test-instance", tagMap["Name"], "instance %d: Name tag must be set", i)
		assert.Equal(t, "integration", tagMap["Env"], "instance %d: Env tag must be set", i)

		// SecurityGroups must be stored.
		assert.NotEmpty(t, inst.SecurityGroups, "instance %d: SecurityGroups must be populated", i)
		sgIds := make([]string, 0, len(inst.SecurityGroups))
		for _, sg := range inst.SecurityGroups {
			sgIds = append(sgIds, aws.ToString(sg.GroupId))
		}
		assert.Contains(t, sgIds, "sg-aabbccdd", "instance %d: sg-aabbccdd must be in SecurityGroups", i)
		assert.Contains(t, sgIds, "sg-11223344", "instance %d: sg-11223344 must be in SecurityGroups", i)
	}

	// Verify both instances are distinct.
	assert.NotEqual(t,
		aws.ToString(runOut.Instances[0].InstanceId),
		aws.ToString(runOut.Instances[1].InstanceId),
		"the two instances must have different IDs",
	)
}

// TestEC2RebootInstances verifies that RebootInstances does not error and the
// instance remains in a running state afterwards.
func TestEC2RebootInstances(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceId := aws.ToString(runOut.Instances[0].InstanceId)

	// Wait for the instance to reach running state.
	time.Sleep(3 * time.Second)

	// Reboot the instance.
	_, err = client.RebootInstances(ctx, &awsec2.RebootInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)

	// Verify the instance is still present and running.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)
	assert.Equal(t, types.InstanceStateNameRunning, descOut.Reservations[0].Instances[0].State.Name,
		"instance should still be running after reboot")
}

// TestEC2ModifyInstanceAttribute verifies that ModifyInstanceAttribute updates
// InstanceType and that DescribeInstanceAttribute reflects the change.
func TestEC2ModifyInstanceAttribute(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceId := aws.ToString(runOut.Instances[0].InstanceId)

	// Modify InstanceType to t3.small.
	_, err = client.ModifyInstanceAttribute(ctx, &awsec2.ModifyInstanceAttributeInput{
		InstanceId: aws.String(instanceId),
		InstanceType: &types.AttributeValue{
			Value: aws.String("t3.small"),
		},
	})
	require.NoError(t, err)

	// DescribeInstanceAttribute should return the updated instance type.
	attrOut, err := client.DescribeInstanceAttribute(ctx, &awsec2.DescribeInstanceAttributeInput{
		InstanceId: aws.String(instanceId),
		Attribute:  types.InstanceAttributeNameInstanceType,
	})
	require.NoError(t, err)
	require.NotNil(t, attrOut.InstanceType)
	assert.Equal(t, "t3.small", aws.ToString(attrOut.InstanceType.Value),
		"InstanceType attribute should reflect the modified value")
}

// TestEC2DescribeInstanceStatus verifies that DescribeInstanceStatus returns
// ok/ok health status for running instances.
func TestEC2DescribeInstanceStatus(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceId := aws.ToString(runOut.Instances[0].InstanceId)

	// DescribeInstanceStatus must return status entries.
	statusOut, err := client.DescribeInstanceStatus(ctx, &awsec2.DescribeInstanceStatusInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)
	require.Len(t, statusOut.InstanceStatuses, 1, "one status entry expected for the launched instance")

	status := statusOut.InstanceStatuses[0]
	assert.Equal(t, instanceId, aws.ToString(status.InstanceId))
	assert.Equal(t, types.SummaryStatusOk, status.InstanceStatus.Status,
		"instance health status should be ok")
	assert.Equal(t, types.SummaryStatusOk, status.SystemStatus.Status,
		"system health status should be ok")
}
