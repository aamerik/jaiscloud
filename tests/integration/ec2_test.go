package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsec2 "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Security Groups ──────────────────────────────────────────────────────────

func TestEC2_CreateDescribeDeleteSecurityGroup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	// Create VPC first
	vpcOut, err := client.CreateVpc(ctx, &awsec2.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
	})
	require.NoError(t, err)
	vpcId := aws.ToString(vpcOut.Vpc.VpcId)

	// Create security group
	sgOut, err := client.CreateSecurityGroup(ctx, &awsec2.CreateSecurityGroupInput{
		GroupName:   aws.String("my-sg"),
		Description: aws.String("test security group"),
		VpcId:       aws.String(vpcId),
	})
	require.NoError(t, err)
	sgId := aws.ToString(sgOut.GroupId)
	assert.NotEmpty(t, sgId)

	// Describe
	descOut, err := client.DescribeSecurityGroups(ctx, &awsec2.DescribeSecurityGroupsInput{
		GroupIds: []string{sgId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.SecurityGroups, 1)
	assert.Equal(t, "my-sg", aws.ToString(descOut.SecurityGroups[0].GroupName))

	// Delete
	_, err = client.DeleteSecurityGroup(ctx, &awsec2.DeleteSecurityGroupInput{
		GroupId: aws.String(sgId),
	})
	require.NoError(t, err)

	// Confirm gone
	descOut, err = client.DescribeSecurityGroups(ctx, &awsec2.DescribeSecurityGroupsInput{
		GroupIds: []string{sgId},
	})
	require.NoError(t, err)
	assert.Len(t, descOut.SecurityGroups, 0)
}

// ─── Key Pairs ────────────────────────────────────────────────────────────────

func TestEC2_CreateDescribeDeleteKeyPair(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	out, err := client.CreateKeyPair(ctx, &awsec2.CreateKeyPairInput{
		KeyName: aws.String("my-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-key", aws.ToString(out.KeyName))
	assert.NotEmpty(t, aws.ToString(out.KeyMaterial))

	descOut, err := client.DescribeKeyPairs(ctx, &awsec2.DescribeKeyPairsInput{
		KeyNames: []string{"my-key"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.KeyPairs, 1)
	assert.Equal(t, "my-key", aws.ToString(descOut.KeyPairs[0].KeyName))

	_, err = client.DeleteKeyPair(ctx, &awsec2.DeleteKeyPairInput{
		KeyName: aws.String("my-key"),
	})
	require.NoError(t, err)

	descOut, err = client.DescribeKeyPairs(ctx, &awsec2.DescribeKeyPairsInput{
		KeyNames: []string{"my-key"},
	})
	require.NoError(t, err)
	assert.Len(t, descOut.KeyPairs, 0)
}

// ─── VPC ──────────────────────────────────────────────────────────────────────

func TestEC2_CreateDescribeDeleteVpc(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	out, err := client.CreateVpc(ctx, &awsec2.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
	})
	require.NoError(t, err)
	vpcId := aws.ToString(out.Vpc.VpcId)
	assert.NotEmpty(t, vpcId)
	assert.Equal(t, "10.0.0.0/16", aws.ToString(out.Vpc.CidrBlock))
	assert.Equal(t, types.VpcState("available"), out.Vpc.State)

	descOut, err := client.DescribeVpcs(ctx, &awsec2.DescribeVpcsInput{
		VpcIds: []string{vpcId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Vpcs, 1)
	assert.Equal(t, vpcId, aws.ToString(descOut.Vpcs[0].VpcId))

	_, err = client.DeleteVpc(ctx, &awsec2.DeleteVpcInput{VpcId: aws.String(vpcId)})
	require.NoError(t, err)

	descOut, err = client.DescribeVpcs(ctx, &awsec2.DescribeVpcsInput{VpcIds: []string{vpcId}})
	require.NoError(t, err)
	assert.Len(t, descOut.Vpcs, 0)
}

// ─── Subnets ──────────────────────────────────────────────────────────────────

func TestEC2_CreateDescribeDeleteSubnet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	vpcOut, err := client.CreateVpc(ctx, &awsec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcId := aws.ToString(vpcOut.Vpc.VpcId)

	snOut, err := client.CreateSubnet(ctx, &awsec2.CreateSubnetInput{
		VpcId:            aws.String(vpcId),
		CidrBlock:        aws.String("10.0.1.0/24"),
		AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)
	snId := aws.ToString(snOut.Subnet.SubnetId)
	assert.Equal(t, vpcId, aws.ToString(snOut.Subnet.VpcId))
	assert.Equal(t, "10.0.1.0/24", aws.ToString(snOut.Subnet.CidrBlock))

	descOut, err := client.DescribeSubnets(ctx, &awsec2.DescribeSubnetsInput{
		SubnetIds: []string{snId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Subnets, 1)

	_, err = client.DeleteSubnet(ctx, &awsec2.DeleteSubnetInput{SubnetId: aws.String(snId)})
	require.NoError(t, err)
}

// ─── Internet Gateway ─────────────────────────────────────────────────────────

func TestEC2_InternetGateway(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	vpcOut, err := client.CreateVpc(ctx, &awsec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcId := aws.ToString(vpcOut.Vpc.VpcId)

	igwOut, err := client.CreateInternetGateway(ctx, &awsec2.CreateInternetGatewayInput{})
	require.NoError(t, err)
	igwId := aws.ToString(igwOut.InternetGateway.InternetGatewayId)
	assert.NotEmpty(t, igwId)

	_, err = client.AttachInternetGateway(ctx, &awsec2.AttachInternetGatewayInput{
		InternetGatewayId: aws.String(igwId),
		VpcId:             aws.String(vpcId),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeInternetGateways(ctx, &awsec2.DescribeInternetGatewaysInput{
		InternetGatewayIds: []string{igwId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.InternetGateways, 1)
	assert.Len(t, descOut.InternetGateways[0].Attachments, 1)

	_, err = client.DetachInternetGateway(ctx, &awsec2.DetachInternetGatewayInput{
		InternetGatewayId: aws.String(igwId),
		VpcId:             aws.String(vpcId),
	})
	require.NoError(t, err)

	_, err = client.DeleteInternetGateway(ctx, &awsec2.DeleteInternetGatewayInput{
		InternetGatewayId: aws.String(igwId),
	})
	require.NoError(t, err)
}

// ─── Route Tables ─────────────────────────────────────────────────────────────

func TestEC2_RouteTable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	vpcOut, err := client.CreateVpc(ctx, &awsec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcId := aws.ToString(vpcOut.Vpc.VpcId)

	igwOut, err := client.CreateInternetGateway(ctx, &awsec2.CreateInternetGatewayInput{})
	require.NoError(t, err)
	igwId := aws.ToString(igwOut.InternetGateway.InternetGatewayId)

	rtOut, err := client.CreateRouteTable(ctx, &awsec2.CreateRouteTableInput{
		VpcId: aws.String(vpcId),
	})
	require.NoError(t, err)
	rtId := aws.ToString(rtOut.RouteTable.RouteTableId)
	assert.NotEmpty(t, rtId)

	_, err = client.CreateRoute(ctx, &awsec2.CreateRouteInput{
		RouteTableId:         aws.String(rtId),
		DestinationCidrBlock: aws.String("0.0.0.0/0"),
		GatewayId:            aws.String(igwId),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeRouteTables(ctx, &awsec2.DescribeRouteTablesInput{
		RouteTableIds: []string{rtId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.RouteTables, 1)

	_, err = client.DeleteRouteTable(ctx, &awsec2.DeleteRouteTableInput{RouteTableId: aws.String(rtId)})
	require.NoError(t, err)
}

// ─── Instances ────────────────────────────────────────────────────────────────

func TestEC2_RunDescribeTerminateInstances(t *testing.T) {
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
	assert.NotEmpty(t, instanceId)
	// Immediately after RunInstances the instance starts in pending state.
	assert.Equal(t, types.InstanceStateNamePending, runOut.Instances[0].State.Name)

	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)

	// Wait for the instance to transition to running before stopping it.
	time.Sleep(3 * time.Second)

	_, err = client.StopInstances(ctx, &awsec2.StopInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)

	// Wait for stopping → stopped before terminating.
	time.Sleep(3 * time.Second)

	_, err = client.TerminateInstances(ctx, &awsec2.TerminateInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)

	// Terminated/shutting-down instances are filtered from DescribeInstances after the transition.
	time.Sleep(3 * time.Second)
	descOut, err = client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{})
	require.NoError(t, err)
	assert.Len(t, descOut.Reservations, 0)
}

// TestEC2SGAuthDescribeRoundtrip tests multi-rule SG authorize, describe, and revoke (fix 1.1.4).
func TestEC2SGAuthDescribeRoundtrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	// Create a VPC and SG.
	vpcOut, err := client.CreateVpc(ctx, &awsec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	sgOut, err := client.CreateSecurityGroup(ctx, &awsec2.CreateSecurityGroupInput{
		GroupName:   aws.String("test-sg"),
		Description: aws.String("test"),
		VpcId:       aws.String(vpcID),
	})
	require.NoError(t, err)
	sgID := aws.ToString(sgOut.GroupId)

	// Authorize 2 ingress rules each with 1 CIDR.
	_, err = client.AuthorizeSecurityGroupIngress(ctx, &awsec2.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(sgID),
		IpPermissions: []types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(80),
				ToPort:     aws.Int32(80),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
			},
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(443),
				ToPort:     aws.Int32(443),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
			},
		},
	})
	require.NoError(t, err)

	// Describe — both rules must surface with IpPermissions.
	descOut, err := client.DescribeSecurityGroups(ctx, &awsec2.DescribeSecurityGroupsInput{
		GroupIds: []string{sgID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.SecurityGroups, 1)
	assert.Len(t, descOut.SecurityGroups[0].IpPermissions, 2, "both ingress rules must be present")

	// Revoke port 80 rule and verify only port 443 remains.
	_, err = client.RevokeSecurityGroupIngress(ctx, &awsec2.RevokeSecurityGroupIngressInput{
		GroupId: aws.String(sgID),
		IpPermissions: []types.IpPermission{
			{IpProtocol: aws.String("tcp"), FromPort: aws.Int32(80), ToPort: aws.Int32(80)},
		},
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeSecurityGroups(ctx, &awsec2.DescribeSecurityGroupsInput{
		GroupIds: []string{sgID},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.SecurityGroups, 1)
	assert.Len(t, descOut2.SecurityGroups[0].IpPermissions, 1, "only port-443 rule should remain after revoke")
}

// ─── Lifecycle Transitions ────────────────────────────────────────────────────

func TestEC2InstanceLifecycleTransitions(t *testing.T) {
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

	// Immediately after RunInstances: state must be "pending".
	assert.Equal(t, types.InstanceStateNamePending, runOut.Instances[0].State.Name,
		"instance should start in pending state")

	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	assert.Equal(t, types.InstanceStateNamePending, descOut.Reservations[0].Instances[0].State.Name,
		"DescribeInstances should also show pending immediately after launch")

	// Wait 3s for pending → running transition.
	time.Sleep(3 * time.Second)

	descOut2, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.Reservations, 1)
	assert.Equal(t, types.InstanceStateNameRunning, descOut2.Reservations[0].Instances[0].State.Name,
		"instance should be running after 3s")
}

// ─── H-PENDING-3: Default VPC at startup ─────────────────────────────────────

// TestEC2DefaultVPCAtStartup verifies that a default VPC and subnets are seeded
// automatically so that RunInstances without a SubnetId succeeds.
func TestEC2DefaultVPCAtStartup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	// There must be at least one default VPC.
	vpcsOut, err := client.DescribeVpcs(ctx, &awsec2.DescribeVpcsInput{})
	require.NoError(t, err)
	foundDefault := false
	for _, v := range vpcsOut.Vpcs {
		if v.IsDefault != nil && *v.IsDefault {
			foundDefault = true
			break
		}
	}
	assert.True(t, foundDefault, "at least one default VPC must exist after startup")

	// There must be at least one default subnet.
	subnetsOut, err := client.DescribeSubnets(ctx, &awsec2.DescribeSubnetsInput{})
	require.NoError(t, err)
	foundDefaultSubnet := false
	for _, s := range subnetsOut.Subnets {
		if s.DefaultForAz != nil && *s.DefaultForAz {
			foundDefaultSubnet = true
			break
		}
	}
	assert.True(t, foundDefaultSubnet, "at least one default subnet must exist after startup")

	// RunInstances without SubnetId must succeed and be placed in the default subnet.
	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	assert.NotEmpty(t, aws.ToString(runOut.Instances[0].SubnetId),
		"instance launched without explicit SubnetId must be placed in the default subnet")
}

// ─── H-PENDING-2: DescribeInstances filter support ───────────────────────────

// TestEC2DescribeInstancesFilters verifies that the instance-type filter returns
// only instances matching the requested type.
func TestEC2DescribeInstancesFilters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	// Launch a t2.micro instance.
	run1, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, run1.Instances, 1)

	// Launch a t3.nano instance.
	run2, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Nano,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, run2.Instances, 1)

	// Filter by instance-type=t2.micro — must return exactly 1 instance.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("instance-type"), Values: []string{"t2.micro"}},
		},
	})
	require.NoError(t, err)
	total := 0
	for _, r := range descOut.Reservations {
		total += len(r.Instances)
	}
	assert.Equal(t, 1, total, "only the t2.micro instance should be returned")
	if total == 1 {
		assert.Equal(t, types.InstanceTypeT2Micro, descOut.Reservations[0].Instances[0].InstanceType)
	}
}

// TestEC2DescribeInstancesStateFilter verifies that filtering by
// instance-state-name=terminated returns terminated instances and that
// filtering by instance-state-name=running excludes them.
func TestEC2DescribeInstancesStateFilter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEC2Client(t)

	// Launch and immediately terminate one instance.
	runOut, err := client.RunInstances(ctx, &awsec2.RunInstancesInput{
		ImageId:      aws.String("ami-0abcdef1234567890"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceId := aws.ToString(runOut.Instances[0].InstanceId)

	// Wait for pending → running before terminating.
	time.Sleep(3 * time.Second)

	_, err = client.TerminateInstances(ctx, &awsec2.TerminateInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)

	// Wait for shutting-down → terminated transition.
	time.Sleep(3 * time.Second)

	// Filter running — the terminated instance must not appear.
	runningOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("instance-state-name"), Values: []string{"running"}},
		},
	})
	require.NoError(t, err)
	runningCount := 0
	for _, r := range runningOut.Reservations {
		runningCount += len(r.Instances)
	}
	assert.Equal(t, 0, runningCount, "no running instances expected after termination")

	// Filter terminated — the terminated instance must appear.
	termOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("instance-state-name"), Values: []string{"terminated"}},
		},
	})
	require.NoError(t, err)
	termCount := 0
	for _, r := range termOut.Reservations {
		termCount += len(r.Instances)
	}
	assert.Equal(t, 1, termCount, "terminated instance must be visible when filtering by terminated state")
}

// ─── IAM Instance Profile ─────────────────────────────────────────────────────

func TestEC2AssociateIamInstanceProfile(t *testing.T) {
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
	instanceId := aws.ToString(runOut.Instances[0].InstanceId)

	// Wait for the instance to reach running state.
	time.Sleep(3 * time.Second)

	// Associate an IAM instance profile.
	assocOut, err := client.AssociateIamInstanceProfile(ctx, &awsec2.AssociateIamInstanceProfileInput{
		InstanceId: aws.String(instanceId),
		IamInstanceProfile: &types.IamInstanceProfileSpecification{
			Arn:  aws.String("arn:aws:iam::000000000000:instance-profile/MyProfile"),
			Name: aws.String("MyProfile"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.IamInstanceProfileAssociation)
	assert.Equal(t, "associated", string(assocOut.IamInstanceProfileAssociation.State))
	assert.NotEmpty(t, aws.ToString(assocOut.IamInstanceProfileAssociation.AssociationId))

	// DescribeInstances should show the IAM profile.
	descOut, err := client.DescribeInstances(ctx, &awsec2.DescribeInstancesInput{
		InstanceIds: []string{instanceId},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	inst := descOut.Reservations[0].Instances[0]
	require.NotNil(t, inst.IamInstanceProfile, "IAM instance profile should be present")
	assert.Equal(t, "arn:aws:iam::000000000000:instance-profile/MyProfile",
		aws.ToString(inst.IamInstanceProfile.Arn))
}
