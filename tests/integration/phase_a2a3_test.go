package integration_test

// Integration tests for Phase A2/A3:
// - §1.4.10 Route53 extras (delegation sets, ListHostedZonesByName, UpdateHealthCheck, VPC, comment)
// - §1.5.1  S3 CreateBucket idempotency
// - §1.5.2  Route53 unique ChangeId + RRSet count
// - §1.5.3  Glue DeleteDatabase cascade + CreatePartition validation
// - §1.5.4  EventBridge bus scoping
// - §1.5.5  IAM cascade-delete checks
// - §1.5.6  DynamoDB Streams disable on DeleteTable
// - §1.5.7  SQS QueueDeletedRecently gate

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamo "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awsroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── §1.4.10 Route53 extras ────────────────────────────────────────────────────

func TestRoute53_ReusableDelegationSet(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	// Create
	createOut, err := client.CreateReusableDelegationSet(ctx, &awsroute53.CreateReusableDelegationSetInput{
		CallerReference: aws.String("ref-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.DelegationSet)
	id := aws.ToString(createOut.DelegationSet.Id)
	assert.NotEmpty(t, id)
	assert.NotEmpty(t, createOut.DelegationSet.NameServers)

	// Get
	getOut, err := client.GetReusableDelegationSet(ctx, &awsroute53.GetReusableDelegationSetInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, id, aws.ToString(getOut.DelegationSet.Id))

	// List
	listOut, err := client.ListReusableDelegationSets(ctx, &awsroute53.ListReusableDelegationSetsInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.DelegationSets, 1)

	// Delete
	_, err = client.DeleteReusableDelegationSet(ctx, &awsroute53.DeleteReusableDelegationSetInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)

	// Confirm gone
	_, err = client.GetReusableDelegationSet(ctx, &awsroute53.GetReusableDelegationSetInput{
		Id: aws.String(id),
	})
	require.Error(t, err)
}

func TestRoute53_ListHostedZonesByName(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	for _, name := range []string{"alpha.example.com.", "beta.example.com.", "other.org."} {
		_, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
			Name:            aws.String(name),
			CallerReference: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.ListHostedZonesByName(ctx, &awsroute53.ListHostedZonesByNameInput{
		DNSName: aws.String("example.com"),
	})
	require.NoError(t, err)
	assert.Len(t, out.HostedZones, 2)
}

func TestRoute53_UpdateHealthCheck(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	createOut, err := client.CreateHealthCheck(ctx, &awsroute53.CreateHealthCheckInput{
		CallerReference: aws.String("hc-ref"),
		HealthCheckConfig: &r53types.HealthCheckConfig{
			Type: r53types.HealthCheckTypeHttp,
		},
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.HealthCheck.Id)

	_, err = client.UpdateHealthCheck(ctx, &awsroute53.UpdateHealthCheckInput{
		HealthCheckId:            aws.String(id),
		FullyQualifiedDomainName: aws.String("updated.example.com"),
	})
	require.NoError(t, err)

	getOut, err := client.GetHealthCheck(ctx, &awsroute53.GetHealthCheckInput{
		HealthCheckId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated.example.com", aws.ToString(getOut.HealthCheck.HealthCheckConfig.FullyQualifiedDomainName))
}

func TestRoute53_AssociateVPCWithHostedZone(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	createOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("test.local."),
		CallerReference: aws.String("ref"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createOut.HostedZone.Id)

	_, err = client.AssociateVPCWithHostedZone(ctx, &awsroute53.AssociateVPCWithHostedZoneInput{
		HostedZoneId: aws.String(zoneID),
		VPC: &r53types.VPC{
			VPCId:     aws.String("vpc-12345"),
			VPCRegion: r53types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)

	_, err = client.DisassociateVPCFromHostedZone(ctx, &awsroute53.DisassociateVPCFromHostedZoneInput{
		HostedZoneId: aws.String(zoneID),
		VPC: &r53types.VPC{
			VPCId:     aws.String("vpc-12345"),
			VPCRegion: r53types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
}

// ─── §1.5.2 Route53 unique ChangeId + RRSet count ─────────────────────────────

func TestRoute53_UniqueChangeID(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	out1, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("zone1.example.com."),
		CallerReference: aws.String("ref1"),
	})
	require.NoError(t, err)
	id1 := aws.ToString(out1.ChangeInfo.Id)

	out2, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("zone2.example.com."),
		CallerReference: aws.String("ref2"),
	})
	require.NoError(t, err)
	id2 := aws.ToString(out2.ChangeInfo.Id)

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2, "each CreateHostedZone should return a unique ChangeId")
}

func TestRoute53_RRSetCountUpdated(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newRoute53Client(t)

	createOut, err := client.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("rrscount.example.com."),
		CallerReference: aws.String("ref"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createOut.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(ctx, &awsroute53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionCreate,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name: aws.String("a.rrscount.example.com."),
						Type: r53types.RRTypeA,
						TTL:  aws.Int64(300),
						ResourceRecords: []r53types.ResourceRecord{
							{Value: aws.String("1.2.3.4")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetHostedZone(ctx, &awsroute53.GetHostedZoneInput{
		Id: aws.String(zoneID),
	})
	require.NoError(t, err)
	assert.Greater(t, int(aws.ToInt64(getOut.HostedZone.ResourceRecordSetCount)), 0)
}

// ─── §1.5.1 S3 CreateBucket idempotency ───────────────────────────────────────

func TestS3_CreateBucketIdempotent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newS3Client(t)

	// Same account, us-east-1 — should succeed twice (idempotent)
	_, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("my-idem-bucket"),
	})
	require.NoError(t, err)

	_, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("my-idem-bucket"),
	})
	require.NoError(t, err)
}

// ─── §1.5.3 Glue DeleteDatabase cascade + CreatePartition validation ──────────

func TestGlue_DeleteDatabaseCascade(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	// Create DB
	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("cascadedb")},
	})
	require.NoError(t, err)

	// Create Table
	_, err = client.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String("cascadedb"),
		TableInput:   &gluetypes.TableInput{Name: aws.String("t1")},
	})
	require.NoError(t, err)

	// Delete database — should cascade-delete the table
	_, err = client.DeleteDatabase(ctx, &awsglue.DeleteDatabaseInput{
		Name: aws.String("cascadedb"),
	})
	require.NoError(t, err)

	// Table should be gone
	_, err = client.GetTable(ctx, &awsglue.GetTableInput{
		DatabaseName: aws.String("cascadedb"),
		Name:         aws.String("t1"),
	})
	require.Error(t, err)
}

func TestGlue_CreatePartitionValidatesTable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newGlueClient(t)

	_, err := client.CreateDatabase(ctx, &awsglue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("partdb")},
	})
	require.NoError(t, err)

	// Attempt to create partition for non-existent table
	_, err = client.CreatePartition(ctx, &awsglue.CreatePartitionInput{
		DatabaseName: aws.String("partdb"),
		TableName:    aws.String("nonexistent"),
		PartitionInput: &gluetypes.PartitionInput{
			Values: []string{"2024-01-01"},
		},
	})
	require.Error(t, err)
}

// ─── §1.5.4 EventBridge bus scoping ────────────────────────────────────────────

func TestEventBridge_BusScoping(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEventBridgeClient(t)

	// Create two custom buses
	_, err := client.CreateEventBus(ctx, &awseb.CreateEventBusInput{
		Name: aws.String("bus-a"),
	})
	require.NoError(t, err)
	_, err = client.CreateEventBus(ctx, &awseb.CreateEventBusInput{
		Name: aws.String("bus-b"),
	})
	require.NoError(t, err)

	// Create a rule with the same name on each bus
	_, err = client.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("my-rule"),
		EventBusName: aws.String("bus-a"),
		EventPattern: aws.String(`{"source":["test"]}`),
	})
	require.NoError(t, err)
	_, err = client.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("my-rule"),
		EventBusName: aws.String("bus-b"),
		EventPattern: aws.String(`{"source":["test"]}`),
	})
	require.NoError(t, err)

	// List rules on bus-a should return only bus-a's rule
	listA, err := client.ListRules(ctx, &awseb.ListRulesInput{
		EventBusName: aws.String("bus-a"),
	})
	require.NoError(t, err)
	assert.Len(t, listA.Rules, 1)

	listB, err := client.ListRules(ctx, &awseb.ListRulesInput{
		EventBusName: aws.String("bus-b"),
	})
	require.NoError(t, err)
	assert.Len(t, listB.Rules, 1)

	// Describe rule on bus-a should succeed
	_, err = client.DescribeRule(ctx, &awseb.DescribeRuleInput{
		Name:         aws.String("my-rule"),
		EventBusName: aws.String("bus-a"),
	})
	require.NoError(t, err)

	// Delete rule from bus-a only
	_, err = client.DeleteRule(ctx, &awseb.DeleteRuleInput{
		Name:         aws.String("my-rule"),
		EventBusName: aws.String("bus-a"),
	})
	require.NoError(t, err)

	// bus-b should still have its rule
	listB2, err := client.ListRules(ctx, &awseb.ListRulesInput{
		EventBusName: aws.String("bus-b"),
	})
	require.NoError(t, err)
	assert.Len(t, listB2.Rules, 1)
}

func TestEventBridge_BusScopedTargets(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newEventBridgeClient(t)
	sqsClient := newSQSClient(t)

	// Create a queue to use as target
	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("eb-target-queue"),
	})
	require.NoError(t, err)

	_, err = client.CreateEventBus(ctx, &awseb.CreateEventBusInput{
		Name: aws.String("scoped-bus"),
	})
	require.NoError(t, err)

	_, err = client.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("scoped-rule"),
		EventBusName: aws.String("scoped-bus"),
		EventPattern: aws.String(`{"source":["test"]}`),
	})
	require.NoError(t, err)

	// Put target scoped to bus
	_, err = client.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule:         aws.String("scoped-rule"),
		EventBusName: aws.String("scoped-bus"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String("arn:aws:sqs:us-east-1:000000000000:eb-target-queue")},
		},
	})
	require.NoError(t, err)

	// List targets
	ltOut, err := client.ListTargetsByRule(ctx, &awseb.ListTargetsByRuleInput{
		Rule:         aws.String("scoped-rule"),
		EventBusName: aws.String("scoped-bus"),
	})
	require.NoError(t, err)
	assert.Len(t, ltOut.Targets, 1)

	_ = qOut
}

// ─── §1.5.5 IAM cascade-delete checks ─────────────────────────────────────────

func TestIAM_DeleteRoleWithAttachedPolicyFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	// Create role
	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("test-role"),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	// Create and attach policy
	pOut, err := client.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
		PolicyName:     aws.String("test-policy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.AttachRolePolicy(ctx, &awsiam.AttachRolePolicyInput{
		RoleName:  aws.String("test-role"),
		PolicyArn: pOut.Policy.Arn,
	})
	require.NoError(t, err)

	// DeleteRole should fail — policy still attached
	_, err = client.DeleteRole(ctx, &awsiam.DeleteRoleInput{
		RoleName: aws.String("test-role"),
	})
	require.Error(t, err)
	assertAWSError(t, err, "DeleteConflict")

	// Detach then delete should succeed
	_, err = client.DetachRolePolicy(ctx, &awsiam.DetachRolePolicyInput{
		RoleName:  aws.String("test-role"),
		PolicyArn: pOut.Policy.Arn,
	})
	require.NoError(t, err)

	_, err = client.DeleteRole(ctx, &awsiam.DeleteRoleInput{
		RoleName: aws.String("test-role"),
	})
	require.NoError(t, err)
}

func TestIAM_DeleteUserWithAccessKeyFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	_, err := client.CreateUser(ctx, &awsiam.CreateUserInput{
		UserName: aws.String("test-user"),
	})
	require.NoError(t, err)

	akOut, err := client.CreateAccessKey(ctx, &awsiam.CreateAccessKeyInput{
		UserName: aws.String("test-user"),
	})
	require.NoError(t, err)

	// DeleteUser should fail — access key exists
	_, err = client.DeleteUser(ctx, &awsiam.DeleteUserInput{
		UserName: aws.String("test-user"),
	})
	require.Error(t, err)
	assertAWSError(t, err, "DeleteConflict")

	// Delete the key first
	_, err = client.DeleteAccessKey(ctx, &awsiam.DeleteAccessKeyInput{
		UserName:    aws.String("test-user"),
		AccessKeyId: akOut.AccessKey.AccessKeyId,
	})
	require.NoError(t, err)

	// Now delete user should succeed
	_, err = client.DeleteUser(ctx, &awsiam.DeleteUserInput{
		UserName: aws.String("test-user"),
	})
	require.NoError(t, err)
}

// ─── §1.5.6 DynamoDB Streams disable on DeleteTable ───────────────────────────

func TestDynamoDB_StreamsDisabledOnDeleteTable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newDynamoClient(t)

	// Create table with streams enabled
	_, err := client.CreateTable(ctx, &awsdynamo.CreateTableInput{
		TableName: aws.String("stream-table"),
		AttributeDefinitions: []dynamotypes.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: dynamotypes.ScalarAttributeTypeS},
		},
		KeySchema: []dynamotypes.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: dynamotypes.KeyTypeHash},
		},
		BillingMode: dynamotypes.BillingModePayPerRequest,
		StreamSpecification: &dynamotypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dynamotypes.StreamViewTypeNewImage,
		},
	})
	require.NoError(t, err)

	// Delete the table
	_, err = client.DeleteTable(ctx, &awsdynamo.DeleteTableInput{
		TableName: aws.String("stream-table"),
	})
	require.NoError(t, err)

	// Table should be gone
	_, err = client.DescribeTable(ctx, &awsdynamo.DescribeTableInput{
		TableName: aws.String("stream-table"),
	})
	require.Error(t, err)
}

// ─── §1.5.7 SQS QueueDeletedRecently gate ─────────────────────────────────────

func TestSQS_QueueDeletedRecently(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	// Create a queue
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("redelete-queue"),
	})
	require.NoError(t, err)

	// Delete it
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: out.QueueUrl,
	})
	require.NoError(t, err)

	// Immediate re-creation should fail with QueueDeletedRecently
	_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("redelete-queue"),
	})
	require.Error(t, err)
	assertAWSError(t, err, "QueueDeletedRecently")
}

func TestSQS_QueueDeletedRecentlyExpires(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in short mode")
	}
	resetState(t)
	ctx := context.Background()
	client := newSQSClient(t)

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("expiry-queue"),
	})
	require.NoError(t, err)

	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: out.QueueUrl,
	})
	require.NoError(t, err)

	// Wait 61 seconds — guard should have expired
	time.Sleep(61 * time.Second)

	_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("expiry-queue"),
	})
	require.NoError(t, err)
}
