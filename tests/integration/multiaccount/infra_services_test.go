package multiaccount

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetype "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── EventBridge ──────────────────────────────────────────────────────────────

func TestEventBridge_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ebA := newEventsFor(t, AcctA)
	ebB := newEventsFor(t, AcctB)

	_, err := ebA.PutRule(ctx, &eventbridge.PutRuleInput{
		Name:               aws.String("shared-rule"),
		ScheduleExpression: aws.String("rate(5 minutes)"),
		State:              ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	_, err = ebB.PutRule(ctx, &eventbridge.PutRuleInput{
		Name:               aws.String("shared-rule"),
		ScheduleExpression: aws.String("rate(5 minutes)"),
		State:              ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)

	listA, err := ebA.ListRules(ctx, &eventbridge.ListRulesInput{})
	require.NoError(t, err)
	for _, r := range listA.Rules {
		assert.Contains(t, aws.ToString(r.Arn), AcctA, "A's rules must embed A's account in ARN")
		assert.NotContains(t, aws.ToString(r.Arn), AcctB)
	}

	listB, err := ebB.ListRules(ctx, &eventbridge.ListRulesInput{})
	require.NoError(t, err)
	for _, r := range listB.Rules {
		assert.Contains(t, aws.ToString(r.Arn), AcctB, "B's rules must embed B's account in ARN")
		assert.NotContains(t, aws.ToString(r.Arn), AcctA)
	}

	// Delete A's rule — B's must survive.
	_, err = ebA.DeleteRule(ctx, &eventbridge.DeleteRuleInput{Name: aws.String("shared-rule")})
	require.NoError(t, err)
	listBAfter, err := ebB.ListRules(ctx, &eventbridge.ListRulesInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Rules, 1, "B's rule must survive A's delete")
}

// ─── CloudWatch ───────────────────────────────────────────────────────────────

func TestCloudWatch_MetricIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cwA := newCWFor(t, AcctA)
	cwB := newCWFor(t, AcctB)

	// A puts metric data under its account.
	_, err := cwA.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Acct/Test"),
		MetricData: []cwtypes.MetricDatum{
			{MetricName: aws.String("Requests"), Value: aws.Float64(42), Unit: cwtypes.StandardUnitCount},
		},
	})
	require.NoError(t, err)

	// B lists metrics — must not see A's namespace.
	listB, err := cwB.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("Acct/Test")})
	require.NoError(t, err)
	assert.Empty(t, listB.Metrics, "B must not see A's metric namespace")

	// A can list its own metric.
	listA, err := cwA.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("Acct/Test")})
	require.NoError(t, err)
	assert.NotEmpty(t, listA.Metrics, "A must see its own metric")
}

func TestCloudWatch_AlarmIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cwA := newCWFor(t, AcctA)
	cwB := newCWFor(t, AcctB)

	_, err := cwA.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
		AlarmName:          aws.String("shared-alarm"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("Errors"),
		Namespace:          aws.String("Acct/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticSum,
		Threshold:          aws.Float64(10),
	})
	require.NoError(t, err)
	_, err = cwB.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
		AlarmName:          aws.String("shared-alarm"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		MetricName:         aws.String("Errors"),
		Namespace:          aws.String("Acct/Test"),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticSum,
		Threshold:          aws.Float64(10),
	})
	require.NoError(t, err)

	descA, err := cwA.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"shared-alarm"}})
	require.NoError(t, err)
	require.Len(t, descA.MetricAlarms, 1)
	assert.Contains(t, aws.ToString(descA.MetricAlarms[0].AlarmArn), AcctA)

	descB, err := cwB.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"shared-alarm"}})
	require.NoError(t, err)
	require.Len(t, descB.MetricAlarms, 1)
	assert.Contains(t, aws.ToString(descB.MetricAlarms[0].AlarmArn), AcctB)

	// Delete A's alarm — B's must survive.
	_, err = cwA.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{AlarmNames: []string{"shared-alarm"}})
	require.NoError(t, err)
	descBAfter, err := cwB.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"shared-alarm"}})
	require.NoError(t, err)
	assert.Len(t, descBAfter.MetricAlarms, 1, "B's alarm must survive A's delete")
}

// ─── Kinesis ──────────────────────────────────────────────────────────────────

func TestKinesis_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	kA := newKinesisFor(t, AcctA)
	kB := newKinesisFor(t, AcctB)

	_, err := kA.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String("shared-stream"), ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	_, err = kB.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String("shared-stream"), ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	listA, err := kA.ListStreams(ctx, &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listA.StreamNames, "shared-stream")

	listB, err := kB.ListStreams(ctx, &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listB.StreamNames, "shared-stream")

	// A's stream ARN must embed A's account.
	descA, err := kA.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String("shared-stream")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descA.StreamDescription.StreamARN), AcctA)

	// B's stream ARN must embed B's account.
	descB, err := kB.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String("shared-stream")})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descB.StreamDescription.StreamARN), AcctB)

	// Delete A's stream — B's must survive.
	_, err = kA.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String("shared-stream")})
	require.NoError(t, err)
	listBAfter, err := kB.ListStreams(ctx, &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listBAfter.StreamNames, "shared-stream", "B's stream must survive A's delete")
}

// ─── Firehose ─────────────────────────────────────────────────────────────────

func TestFirehose_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	fhA := newFirehoseFor(t, AcctA)
	fhB := newFirehoseFor(t, AcctB)

	s3Dest := &firehosetype.ExtendedS3DestinationConfiguration{
		BucketARN: aws.String("arn:aws:s3:::dummy-bucket"),
		RoleARN:   aws.String("arn:aws:iam::" + AcctA + ":role/firehose-role"),
	}
	s3DestB := &firehosetype.ExtendedS3DestinationConfiguration{
		BucketARN: aws.String("arn:aws:s3:::dummy-bucket"),
		RoleARN:   aws.String("arn:aws:iam::" + AcctB + ":role/firehose-role"),
	}

	_, err := fhA.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName:                 aws.String("shared-stream"),
		ExtendedS3DestinationConfiguration: s3Dest,
	})
	require.NoError(t, err)
	_, err = fhB.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName:                 aws.String("shared-stream"),
		ExtendedS3DestinationConfiguration: s3DestB,
	})
	require.NoError(t, err)

	listA, err := fhA.ListDeliveryStreams(ctx, &firehose.ListDeliveryStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listA.DeliveryStreamNames, "shared-stream")

	listB, err := fhB.ListDeliveryStreams(ctx, &firehose.ListDeliveryStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listB.DeliveryStreamNames, "shared-stream")

	// A's delivery stream ARN must embed A's account.
	descA, err := fhA.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("shared-stream"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descA.DeliveryStreamDescription.DeliveryStreamARN), AcctA)

	// B's delivery stream ARN must embed B's account.
	descB, err := fhB.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("shared-stream"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descB.DeliveryStreamDescription.DeliveryStreamARN), AcctB)

	// Delete A's stream — B's must survive.
	_, err = fhA.DeleteDeliveryStream(ctx, &firehose.DeleteDeliveryStreamInput{
		DeliveryStreamName: aws.String("shared-stream"),
	})
	require.NoError(t, err)
	listBAfter, err := fhB.ListDeliveryStreams(ctx, &firehose.ListDeliveryStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listBAfter.DeliveryStreamNames, "shared-stream", "B's stream must survive A's delete")
}

// ─── ECS ──────────────────────────────────────────────────────────────────────

func TestECS_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ecsA := newECSFor(t, AcctA)
	ecsB := newECSFor(t, AcctB)

	_, err := ecsA.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("shared-cluster")})
	require.NoError(t, err)
	_, err = ecsB.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("shared-cluster")})
	require.NoError(t, err)

	listA, err := ecsA.ListClusters(ctx, &ecs.ListClustersInput{})
	require.NoError(t, err)
	for _, arn := range listA.ClusterArns {
		assert.Contains(t, arn, AcctA, "A's cluster ARNs must embed A's account")
		assert.NotContains(t, arn, AcctB)
	}

	listB, err := ecsB.ListClusters(ctx, &ecs.ListClustersInput{})
	require.NoError(t, err)
	for _, arn := range listB.ClusterArns {
		assert.Contains(t, arn, AcctB, "B's cluster ARNs must embed B's account")
		assert.NotContains(t, arn, AcctA)
	}

	// Delete A's cluster — B's must survive.
	_, err = ecsA.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String("shared-cluster")})
	require.NoError(t, err)
	listBAfter, err := ecsB.ListClusters(ctx, &ecs.ListClustersInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.ClusterArns, 1, "B's cluster must survive A's delete")
}

// ─── EC2 ──────────────────────────────────────────────────────────────────────

func TestEC2_VPCIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ec2A := newEC2For(t, AcctA)
	ec2B := newEC2For(t, AcctB)

	// Each account has a default VPC seeded at startup; verify they are separate.
	vpcsA, err := ec2A.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	require.NoError(t, err)

	vpcsB, err := ec2B.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	require.NoError(t, err)

	// Collect all VPC IDs seen by each account.
	idsA := make(map[string]bool)
	for _, v := range vpcsA.Vpcs {
		idsA[aws.ToString(v.VpcId)] = true
	}
	idsB := make(map[string]bool)
	for _, v := range vpcsB.Vpcs {
		idsB[aws.ToString(v.VpcId)] = true
	}

	// No VPC ID should appear in both accounts.
	for id := range idsA {
		assert.False(t, idsB[id], "VPC %s visible in both A and B — must be isolated", id)
	}
}

func TestEC2_SecurityGroupIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ec2A := newEC2For(t, AcctA)
	ec2B := newEC2For(t, AcctB)

	// Get A's default VPC.
	vpcsA, err := ec2A.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, vpcsA.Vpcs)
	vpcA := aws.ToString(vpcsA.Vpcs[0].VpcId)

	vpcsB, err := ec2B.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, vpcsB.Vpcs)
	vpcB := aws.ToString(vpcsB.Vpcs[0].VpcId)

	_, err = ec2A.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String("shared-sg"),
		Description: aws.String("test sg"),
		VpcId:       aws.String(vpcA),
	})
	require.NoError(t, err)
	_, err = ec2B.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String("shared-sg"),
		Description: aws.String("test sg"),
		VpcId:       aws.String(vpcB),
	})
	require.NoError(t, err)

	sgA, err := ec2A.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{})
	require.NoError(t, err)
	sgB, err := ec2B.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{})
	require.NoError(t, err)

	groupIdsA := make(map[string]bool)
	for _, g := range sgA.SecurityGroups {
		groupIdsA[aws.ToString(g.GroupId)] = true
	}
	for _, g := range sgB.SecurityGroups {
		assert.False(t, groupIdsA[aws.ToString(g.GroupId)],
			"security group %s should not appear in both accounts", aws.ToString(g.GroupId))
	}
}

// ─── Route53 ──────────────────────────────────────────────────────────────────

func TestRoute53_HostedZoneIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	r53A := newRoute53For(t, AcctA)
	r53B := newRoute53For(t, AcctB)

	_, err := r53A.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
		Name:             aws.String("shared.example.com."),
		CallerReference:  aws.String("ref-A-1"),
		HostedZoneConfig: &r53types.HostedZoneConfig{PrivateZone: false},
	})
	require.NoError(t, err)
	_, err = r53B.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
		Name:             aws.String("shared.example.com."),
		CallerReference:  aws.String("ref-B-1"),
		HostedZoneConfig: &r53types.HostedZoneConfig{PrivateZone: false},
	})
	require.NoError(t, err)

	listA, err := r53A.ListHostedZones(ctx, &route53.ListHostedZonesInput{})
	require.NoError(t, err)
	assert.Len(t, listA.HostedZones, 1, "A should see exactly its own hosted zone")

	listB, err := r53B.ListHostedZones(ctx, &route53.ListHostedZonesInput{})
	require.NoError(t, err)
	assert.Len(t, listB.HostedZones, 1, "B should see exactly its own hosted zone")

	// Zone IDs must differ.
	assert.NotEqual(t,
		aws.ToString(listA.HostedZones[0].Id),
		aws.ToString(listB.HostedZones[0].Id),
		"zone IDs must be distinct across accounts",
	)

	// Delete A's zone — B's must survive.
	zoneAID := aws.ToString(listA.HostedZones[0].Id)
	_, err = r53A.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{Id: aws.String(zoneAID)})
	require.NoError(t, err)
	listBAfter, err := r53B.ListHostedZones(ctx, &route53.ListHostedZonesInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.HostedZones, 1, "B's hosted zone must survive A's delete")
}
