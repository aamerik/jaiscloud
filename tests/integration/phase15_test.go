package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	awsathenatype "github.com/aws/aws-sdk-go-v2/service/athena/types"
	awscloudfront "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	awscftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	awscognitoidp "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	awscognitoidptypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	awscognitoidentity "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	awsacm "github.com/aws/aws-sdk-go-v2/service/acm"
	awsacmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	awsfirehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	awsredshift "github.com/aws/aws-sdk-go-v2/service/redshift"
	awsredshifttypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
	awssestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	awss3control "github.com/aws/aws-sdk-go-v2/service/s3control"
)

// ─── Cognito User Pools ───────────────────────────────────────────────────────

func TestCognito_UserPool_CRUD(t *testing.T) {
	t.Skip("Cognito not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newCognitoIDPClient(t)

	// Create
	createOut, err := c.CreateUserPool(ctx, &awscognitoidp.CreateUserPoolInput{
		PoolName: aws.String("test-pool"),
	})
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	poolID := aws.ToString(createOut.UserPool.Id)
	if poolID == "" {
		t.Fatal("expected non-empty pool ID")
	}

	// Describe
	descOut, err := c.DescribeUserPool(ctx, &awscognitoidp.DescribeUserPoolInput{
		UserPoolId: aws.String(poolID),
	})
	if err != nil {
		t.Fatalf("DescribeUserPool: %v", err)
	}
	if aws.ToString(descOut.UserPool.Name) != "test-pool" {
		t.Errorf("expected pool name test-pool, got %s", aws.ToString(descOut.UserPool.Name))
	}

	// List
	listOut, err := c.ListUserPools(ctx, &awscognitoidp.ListUserPoolsInput{
		MaxResults: aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("ListUserPools: %v", err)
	}
	if len(listOut.UserPools) == 0 {
		t.Fatal("expected at least one user pool")
	}

	// Delete
	_, err = c.DeleteUserPool(ctx, &awscognitoidp.DeleteUserPoolInput{
		UserPoolId: aws.String(poolID),
	})
	if err != nil {
		t.Fatalf("DeleteUserPool: %v", err)
	}
}

func TestCognito_UserPool_ClientAndUser(t *testing.T) {
	t.Skip("Cognito not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newCognitoIDPClient(t)

	createOut, err := c.CreateUserPool(ctx, &awscognitoidp.CreateUserPoolInput{
		PoolName: aws.String("auth-pool"),
	})
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	poolID := aws.ToString(createOut.UserPool.Id)

	// Create client
	clientOut, err := c.CreateUserPoolClient(ctx, &awscognitoidp.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("my-app"),
	})
	if err != nil {
		t.Fatalf("CreateUserPoolClient: %v", err)
	}
	clientID := aws.ToString(clientOut.UserPoolClient.ClientId)
	if clientID == "" {
		t.Fatal("expected non-empty client ID")
	}

	// List clients
	listOut, err := c.ListUserPoolClients(ctx, &awscognitoidp.ListUserPoolClientsInput{
		UserPoolId: aws.String(poolID),
	})
	if err != nil {
		t.Fatalf("ListUserPoolClients: %v", err)
	}
	if len(listOut.UserPoolClients) == 0 {
		t.Fatal("expected at least one client")
	}

	// Admin create user
	_, err = c.AdminCreateUser(ctx, &awscognitoidp.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("alice"),
		TemporaryPassword: aws.String("Temp1234!"),
	})
	if err != nil {
		t.Fatalf("AdminCreateUser: %v", err)
	}

	// Admin get user
	getUserOut, err := c.AdminGetUser(ctx, &awscognitoidp.AdminGetUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("alice"),
	})
	if err != nil {
		t.Fatalf("AdminGetUser: %v", err)
	}
	if aws.ToString(getUserOut.Username) != "alice" {
		t.Errorf("expected username alice, got %s", aws.ToString(getUserOut.Username))
	}

	// Admin update user attributes
	_, err = c.AdminUpdateUserAttributes(ctx, &awscognitoidp.AdminUpdateUserAttributesInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("alice"),
		UserAttributes: []awscognitoidptypes.AttributeType{
			{Name: aws.String("email"), Value: aws.String("alice@example.com")},
		},
	})
	if err != nil {
		t.Fatalf("AdminUpdateUserAttributes: %v", err)
	}

	// Admin confirm sign up
	_, err = c.AdminConfirmSignUp(ctx, &awscognitoidp.AdminConfirmSignUpInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("alice"),
	})
	if err != nil {
		t.Fatalf("AdminConfirmSignUp: %v", err)
	}

	// Admin delete user
	_, err = c.AdminDeleteUser(ctx, &awscognitoidp.AdminDeleteUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("alice"),
	})
	if err != nil {
		t.Fatalf("AdminDeleteUser: %v", err)
	}
}

// ─── Cognito Identity ─────────────────────────────────────────────────────────

func TestCognitoIdentity_PoolCRUD(t *testing.T) {
	t.Skip("CognitoIdentity not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newCognitoIdentityClient(t)

	createOut, err := c.CreateIdentityPool(ctx, &awscognitoidentity.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("my-pool"),
		AllowUnauthenticatedIdentities: false,
	})
	if err != nil {
		t.Fatalf("CreateIdentityPool: %v", err)
	}
	poolID := aws.ToString(createOut.IdentityPoolId)
	if poolID == "" {
		t.Fatal("expected non-empty pool ID")
	}

	descOut, err := c.DescribeIdentityPool(ctx, &awscognitoidentity.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	if err != nil {
		t.Fatalf("DescribeIdentityPool: %v", err)
	}
	if aws.ToString(descOut.IdentityPoolName) != "my-pool" {
		t.Errorf("expected my-pool, got %s", aws.ToString(descOut.IdentityPoolName))
	}

	listOut, err := c.ListIdentityPools(ctx, &awscognitoidentity.ListIdentityPoolsInput{
		MaxResults: aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("ListIdentityPools: %v", err)
	}
	if len(listOut.IdentityPools) == 0 {
		t.Fatal("expected at least one identity pool")
	}

	_, err = c.DeleteIdentityPool(ctx, &awscognitoidentity.DeleteIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	if err != nil {
		t.Fatalf("DeleteIdentityPool: %v", err)
	}
}

// ─── ACM ─────────────────────────────────────────────────────────────────────

func TestACM_CertificateCRUD(t *testing.T) {
	t.Skip("ACM not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newACMClient(t)

	reqOut, err := c.RequestCertificate(ctx, &awsacm.RequestCertificateInput{
		DomainName: aws.String("example.com"),
		SubjectAlternativeNames: []string{"www.example.com"},
	})
	if err != nil {
		t.Fatalf("RequestCertificate: %v", err)
	}
	certARN := aws.ToString(reqOut.CertificateArn)
	if certARN == "" {
		t.Fatal("expected non-empty cert ARN")
	}

	descOut, err := c.DescribeCertificate(ctx, &awsacm.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	if err != nil {
		t.Fatalf("DescribeCertificate: %v", err)
	}
	if aws.ToString(descOut.Certificate.DomainName) != "example.com" {
		t.Errorf("expected example.com, got %s", aws.ToString(descOut.Certificate.DomainName))
	}

	listOut, err := c.ListCertificates(ctx, &awsacm.ListCertificatesInput{})
	if err != nil {
		t.Fatalf("ListCertificates: %v", err)
	}
	if len(listOut.CertificateSummaryList) == 0 {
		t.Fatal("expected at least one certificate")
	}

	_, err = c.AddTagsToCertificate(ctx, &awsacm.AddTagsToCertificateInput{
		CertificateArn: aws.String(certARN),
		Tags: []awsacmtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	if err != nil {
		t.Fatalf("AddTagsToCertificate: %v", err)
	}

	tagsOut, err := c.ListTagsForCertificate(ctx, &awsacm.ListTagsForCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	if err != nil {
		t.Fatalf("ListTagsForCertificate: %v", err)
	}
	if len(tagsOut.Tags) == 0 {
		t.Fatal("expected tags")
	}

	_, err = c.DeleteCertificate(ctx, &awsacm.DeleteCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	if err != nil {
		t.Fatalf("DeleteCertificate: %v", err)
	}
}

// ─── SES ─────────────────────────────────────────────────────────────────────

func TestSES_VerifyAndSend(t *testing.T) {
	t.Skip("SES not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newSESClient(t)

	// Verify email
	_, err := c.VerifyEmailIdentity(ctx, &awsses.VerifyEmailIdentityInput{
		EmailAddress: aws.String("sender@example.com"),
	})
	if err != nil {
		t.Fatalf("VerifyEmailIdentity: %v", err)
	}

	// Verify domain
	domainOut, err := c.VerifyDomainIdentity(ctx, &awsses.VerifyDomainIdentityInput{
		Domain: aws.String("example.com"),
	})
	if err != nil {
		t.Fatalf("VerifyDomainIdentity: %v", err)
	}
	if aws.ToString(domainOut.VerificationToken) == "" {
		t.Fatal("expected non-empty verification token")
	}

	// List identities
	listOut, err := c.ListIdentities(ctx, &awsses.ListIdentitiesInput{})
	if err != nil {
		t.Fatalf("ListIdentities: %v", err)
	}
	if len(listOut.Identities) < 2 {
		t.Errorf("expected at least 2 identities, got %d", len(listOut.Identities))
	}

	// Send email
	sendOut, err := c.SendEmail(ctx, &awsses.SendEmailInput{
		Source: aws.String("sender@example.com"),
		Destination: &awssestypes.Destination{
			ToAddresses: []string{"recipient@example.com"},
		},
		Message: &awssestypes.Message{
			Subject: &awssestypes.Content{Data: aws.String("Hello")},
			Body: &awssestypes.Body{
				Text: &awssestypes.Content{Data: aws.String("World")},
			},
		},
	})
	if err != nil {
		t.Fatalf("SendEmail: %v", err)
	}
	if aws.ToString(sendOut.MessageId) == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Get send quota
	quotaOut, err := c.GetSendQuota(ctx, &awsses.GetSendQuotaInput{})
	if err != nil {
		t.Fatalf("GetSendQuota: %v", err)
	}
	if quotaOut.Max24HourSend == 0 {
		t.Fatal("expected non-zero max send quota")
	}

	// Delete identity
	_, err = c.DeleteIdentity(ctx, &awsses.DeleteIdentityInput{
		Identity: aws.String("sender@example.com"),
	})
	if err != nil {
		t.Fatalf("DeleteIdentity: %v", err)
	}
}

// ─── Firehose ─────────────────────────────────────────────────────────────────

func TestFirehose_StreamCRUD(t *testing.T) {
	t.Skip("Firehose not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newFirehoseClient(t)

	createOut, err := c.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("test-stream"),
		DeliveryStreamType: awsfirehosetypes.DeliveryStreamTypeDirectPut,
		S3DestinationConfiguration: &awsfirehosetypes.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::my-bucket"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
		},
	})
	if err != nil {
		t.Fatalf("CreateDeliveryStream: %v", err)
	}
	streamARN := aws.ToString(createOut.DeliveryStreamARN)
	if streamARN == "" {
		t.Fatal("expected non-empty stream ARN")
	}

	descOut, err := c.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("test-stream"),
	})
	if err != nil {
		t.Fatalf("DescribeDeliveryStream: %v", err)
	}
	if aws.ToString(descOut.DeliveryStreamDescription.DeliveryStreamName) != "test-stream" {
		t.Errorf("wrong stream name")
	}

	listOut, err := c.ListDeliveryStreams(ctx, &awsfirehose.ListDeliveryStreamsInput{})
	if err != nil {
		t.Fatalf("ListDeliveryStreams: %v", err)
	}
	if len(listOut.DeliveryStreamNames) == 0 {
		t.Fatal("expected at least one stream")
	}

	// PutRecord
	putOut, err := c.PutRecord(ctx, &awsfirehose.PutRecordInput{
		DeliveryStreamName: aws.String("test-stream"),
		Record:             &awsfirehosetypes.Record{Data: []byte("hello world")},
	})
	if err != nil {
		t.Fatalf("PutRecord: %v", err)
	}
	if aws.ToString(putOut.RecordId) == "" {
		t.Fatal("expected non-empty record ID")
	}

	// PutRecordBatch
	batchOut, err := c.PutRecordBatch(ctx, &awsfirehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("test-stream"),
		Records: []awsfirehosetypes.Record{
			{Data: []byte("record1")},
			{Data: []byte("record2")},
		},
	})
	if err != nil {
		t.Fatalf("PutRecordBatch: %v", err)
	}
	if batchOut.FailedPutCount == nil || *batchOut.FailedPutCount != 0 {
		t.Fatal("expected 0 failed records")
	}

	_, err = c.DeleteDeliveryStream(ctx, &awsfirehose.DeleteDeliveryStreamInput{
		DeliveryStreamName: aws.String("test-stream"),
	})
	if err != nil {
		t.Fatalf("DeleteDeliveryStream: %v", err)
	}
}

// ─── CloudFront ───────────────────────────────────────────────────────────────

func TestCloudFront_DistributionCRUD(t *testing.T) {
	t.Skip("CloudFront not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newCloudfrontClient(t)

	createOut, err := c.CreateDistribution(ctx, &awscloudfront.CreateDistributionInput{
		DistributionConfig: &awscftypes.DistributionConfig{
			CallerReference: aws.String("ref-1"),
			Comment:         aws.String("test dist"),
			Enabled:         aws.Bool(true),
			DefaultCacheBehavior: &awscftypes.DefaultCacheBehavior{
				ViewerProtocolPolicy: awscftypes.ViewerProtocolPolicyAllowAll,
				TargetOriginId:       aws.String("origin-1"),
				ForwardedValues: &awscftypes.ForwardedValues{
					QueryString: aws.Bool(false),
					Cookies:     &awscftypes.CookiePreference{Forward: awscftypes.ItemSelectionNone},
				},
				MinTTL: aws.Int64(0),
			},
			Origins: &awscftypes.Origins{
				Quantity: aws.Int32(1),
				Items: []awscftypes.Origin{{
					Id:         aws.String("origin-1"),
					DomainName: aws.String("example.com"),
					S3OriginConfig: &awscftypes.S3OriginConfig{
						OriginAccessIdentity: aws.String(""),
					},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDistribution: %v", err)
	}
	distID := aws.ToString(createOut.Distribution.Id)
	if distID == "" {
		t.Fatal("expected non-empty distribution ID")
	}

	getOut, err := c.GetDistribution(ctx, &awscloudfront.GetDistributionInput{
		Id: aws.String(distID),
	})
	if err != nil {
		t.Fatalf("GetDistribution: %v", err)
	}
	if aws.ToString(getOut.Distribution.Id) != distID {
		t.Errorf("distribution ID mismatch")
	}

	listOut, err := c.ListDistributions(ctx, &awscloudfront.ListDistributionsInput{})
	if err != nil {
		t.Fatalf("ListDistributions: %v", err)
	}
	if aws.ToInt32(listOut.DistributionList.Quantity) == 0 {
		t.Fatal("expected at least one distribution")
	}

	// Disable before delete
	etag := aws.ToString(getOut.ETag)
	config := getOut.Distribution.DistributionConfig
	config.Enabled = aws.Bool(false)
	updateOut, err := c.UpdateDistribution(ctx, &awscloudfront.UpdateDistributionInput{
		Id:                 aws.String(distID),
		IfMatch:            aws.String(etag),
		DistributionConfig: config,
	})
	if err != nil {
		t.Fatalf("UpdateDistribution: %v", err)
	}
	newETag := aws.ToString(updateOut.ETag)

	_, err = c.DeleteDistribution(ctx, &awscloudfront.DeleteDistributionInput{
		Id:      aws.String(distID),
		IfMatch: aws.String(newETag),
	})
	if err != nil {
		t.Fatalf("DeleteDistribution: %v", err)
	}
}

// ─── Athena ───────────────────────────────────────────────────────────────────

func TestAthena_QueryExecution(t *testing.T) {
	t.Skip("Athena not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newAthenaClient(t)

	startOut, err := c.StartQueryExecution(ctx, &awsathena.StartQueryExecutionInput{
		QueryString: aws.String("SELECT 1"),
		WorkGroup:   aws.String("primary"),
	})
	if err != nil {
		t.Fatalf("StartQueryExecution: %v", err)
	}
	qid := aws.ToString(startOut.QueryExecutionId)
	if qid == "" {
		t.Fatal("expected non-empty query execution ID")
	}

	getOut, err := c.GetQueryExecution(ctx, &awsathena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(qid),
	})
	if err != nil {
		t.Fatalf("GetQueryExecution: %v", err)
	}
	if getOut.QueryExecution.Status.State != awsathenatype.QueryExecutionStateSucceeded {
		t.Errorf("expected SUCCEEDED state, got %s", getOut.QueryExecution.Status.State)
	}

	resultsOut, err := c.GetQueryResults(ctx, &awsathena.GetQueryResultsInput{
		QueryExecutionId: aws.String(qid),
	})
	if err != nil {
		t.Fatalf("GetQueryResults: %v", err)
	}
	_ = resultsOut

	listOut, err := c.ListQueryExecutions(ctx, &awsathena.ListQueryExecutionsInput{})
	if err != nil {
		t.Fatalf("ListQueryExecutions: %v", err)
	}
	if len(listOut.QueryExecutionIds) == 0 {
		t.Fatal("expected at least one execution ID")
	}

	batchOut, err := c.BatchGetQueryExecution(ctx, &awsathena.BatchGetQueryExecutionInput{
		QueryExecutionIds: []string{qid},
	})
	if err != nil {
		t.Fatalf("BatchGetQueryExecution: %v", err)
	}
	if len(batchOut.QueryExecutions) == 0 {
		t.Fatal("expected at least one execution")
	}
}

func TestAthena_WorkGroups(t *testing.T) {
	t.Skip("Athena not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newAthenaClient(t)

	_, err := c.CreateWorkGroup(ctx, &awsathena.CreateWorkGroupInput{
		Name:        aws.String("my-wg"),
		Description: aws.String("test workgroup"),
	})
	if err != nil {
		t.Fatalf("CreateWorkGroup: %v", err)
	}

	getOut, err := c.GetWorkGroup(ctx, &awsathena.GetWorkGroupInput{
		WorkGroup: aws.String("my-wg"),
	})
	if err != nil {
		t.Fatalf("GetWorkGroup: %v", err)
	}
	if aws.ToString(getOut.WorkGroup.Name) != "my-wg" {
		t.Errorf("wrong workgroup name")
	}

	listOut, err := c.ListWorkGroups(ctx, &awsathena.ListWorkGroupsInput{})
	if err != nil {
		t.Fatalf("ListWorkGroups: %v", err)
	}
	found := false
	for _, wg := range listOut.WorkGroups {
		if aws.ToString(wg.Name) == "primary" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected primary workgroup to always be in list")
	}

	_, err = c.DeleteWorkGroup(ctx, &awsathena.DeleteWorkGroupInput{
		WorkGroup: aws.String("my-wg"),
	})
	if err != nil {
		t.Fatalf("DeleteWorkGroup: %v", err)
	}
}

// ─── Redshift ─────────────────────────────────────────────────────────────────

func TestRedshift_ClusterCRUD(t *testing.T) {
	t.Skip("Redshift not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newRedshiftClient(t)

	_, err := c.CreateCluster(ctx, &awsredshift.CreateClusterInput{
		ClusterIdentifier:  aws.String("my-cluster"),
		NodeType:           aws.String("dc2.large"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Admin1234!"),
		DBName:             aws.String("mydb"),
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	descOut, err := c.DescribeClusters(ctx, &awsredshift.DescribeClustersInput{
		ClusterIdentifier: aws.String("my-cluster"),
	})
	if err != nil {
		t.Fatalf("DescribeClusters: %v", err)
	}
	if len(descOut.Clusters) == 0 {
		t.Fatal("expected at least one cluster")
	}
	if aws.ToString(descOut.Clusters[0].ClusterIdentifier) != "my-cluster" {
		t.Errorf("wrong cluster identifier")
	}

	_, err = c.ModifyCluster(ctx, &awsredshift.ModifyClusterInput{
		ClusterIdentifier: aws.String("my-cluster"),
		MasterUserPassword: aws.String("NewPass1234!"),
	})
	if err != nil {
		t.Fatalf("ModifyCluster: %v", err)
	}

	// Create subnet group
	_, err = c.CreateClusterSubnetGroup(ctx, &awsredshift.CreateClusterSubnetGroupInput{
		ClusterSubnetGroupName: aws.String("my-sg"),
		Description:            aws.String("test subnet group"),
		SubnetIds:              []string{"subnet-12345"},
	})
	if err != nil {
		t.Fatalf("CreateClusterSubnetGroup: %v", err)
	}

	sgOut, err := c.DescribeClusterSubnetGroups(ctx, &awsredshift.DescribeClusterSubnetGroupsInput{
		ClusterSubnetGroupName: aws.String("my-sg"),
	})
	if err != nil {
		t.Fatalf("DescribeClusterSubnetGroups: %v", err)
	}
	if len(sgOut.ClusterSubnetGroups) == 0 {
		t.Fatal("expected at least one subnet group")
	}

	// Tags
	_, err = c.CreateTags(ctx, &awsredshift.CreateTagsInput{
		ResourceName: aws.String("arn:aws:redshift:us-east-1:000000000000:cluster:my-cluster"),
		Tags: []awsredshifttypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	if err != nil {
		t.Fatalf("CreateTags: %v", err)
	}

	tagsOut, err := c.DescribeTags(ctx, &awsredshift.DescribeTagsInput{
		ResourceName: aws.String("arn:aws:redshift:us-east-1:000000000000:cluster:my-cluster"),
	})
	if err != nil {
		t.Fatalf("DescribeTags: %v", err)
	}
	if len(tagsOut.TaggedResources) == 0 {
		t.Fatal("expected tags")
	}

	_, err = c.DeleteCluster(ctx, &awsredshift.DeleteClusterInput{
		ClusterIdentifier:        aws.String("my-cluster"),
		SkipFinalClusterSnapshot: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
}

// ─── S3 Select ────────────────────────────────────────────────────────────────

func TestS3Select_SelectObjectContent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	s3c := newS3Client(t)

	// Create bucket and put object
	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("select-bucket"),
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	csvData := "name,age\nalice,30\nbob,25\n"
	_, err = s3c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String("select-bucket"),
		Key:         aws.String("data.csv"),
		Body:        bytes.NewReader([]byte(csvData)),
		ContentType: aws.String("text/csv"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// SelectObjectContent
	selectOut, err := s3c.SelectObjectContent(ctx, &awss3.SelectObjectContentInput{
		Bucket:         aws.String("select-bucket"),
		Key:            aws.String("data.csv"),
		ExpressionType: awss3types.ExpressionTypeSql,
		Expression:     aws.String("SELECT * FROM S3Object"),
		InputSerialization: &awss3types.InputSerialization{
			CSV: &awss3types.CSVInput{
				FileHeaderInfo: awss3types.FileHeaderInfoUse,
			},
		},
		OutputSerialization: &awss3types.OutputSerialization{
			CSV: &awss3types.CSVOutput{},
		},
	})
	if err != nil {
		t.Fatalf("SelectObjectContent: %v", err)
	}
	defer selectOut.GetStream().Close()

	// Drain the event stream
	stream := selectOut.GetStream()
	for event := range stream.Events() {
		_ = event
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("event stream error: %v", err)
	}
}

// ─── S3 Access Points ─────────────────────────────────────────────────────────

func TestS3AccessPoints_CRUD(t *testing.T) {
	t.Skip("S3 Control adds account-ID host prefix which DNS cannot resolve against localhost")
	resetState(t)
	ctx := context.Background()
	s3c := newS3Client(t)
	s3ctrl := newS3ControlClient(t)

	// Create bucket first
	_, err := s3c.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String("ap-bucket"),
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	accountID := "000000000000"

	// Create access point
	createOut, err := s3ctrl.CreateAccessPoint(ctx, &awss3control.CreateAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
		Bucket:    aws.String("ap-bucket"),
	})
	if err != nil {
		t.Fatalf("CreateAccessPoint: %v", err)
	}
	if aws.ToString(createOut.AccessPointArn) == "" {
		t.Fatal("expected non-empty access point ARN")
	}

	// Get access point
	getOut, err := s3ctrl.GetAccessPoint(ctx, &awss3control.GetAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
	})
	if err != nil {
		t.Fatalf("GetAccessPoint: %v", err)
	}
	if aws.ToString(getOut.Name) != "my-ap" {
		t.Errorf("expected my-ap, got %s", aws.ToString(getOut.Name))
	}
	if aws.ToString(getOut.Bucket) != "ap-bucket" {
		t.Errorf("expected ap-bucket, got %s", aws.ToString(getOut.Bucket))
	}

	// List access points
	listOut, err := s3ctrl.ListAccessPoints(ctx, &awss3control.ListAccessPointsInput{
		AccountId: aws.String(accountID),
	})
	if err != nil {
		t.Fatalf("ListAccessPoints: %v", err)
	}
	if len(listOut.AccessPointList) == 0 {
		t.Fatal("expected at least one access point")
	}

	// Put access point policy
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`
	_, err = s3ctrl.PutAccessPointPolicy(ctx, &awss3control.PutAccessPointPolicyInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
		Policy:    aws.String(policy),
	})
	if err != nil {
		t.Fatalf("PutAccessPointPolicy: %v", err)
	}

	// Get access point policy
	getPolicyOut, err := s3ctrl.GetAccessPointPolicy(ctx, &awss3control.GetAccessPointPolicyInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
	})
	if err != nil {
		t.Fatalf("GetAccessPointPolicy: %v", err)
	}
	if !strings.Contains(aws.ToString(getPolicyOut.Policy), "s3:GetObject") {
		t.Errorf("policy mismatch: %s", aws.ToString(getPolicyOut.Policy))
	}

	// Get access point policy status
	statusOut, err := s3ctrl.GetAccessPointPolicyStatus(ctx, &awss3control.GetAccessPointPolicyStatusInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
	})
	if err != nil {
		t.Fatalf("GetAccessPointPolicyStatus: %v", err)
	}
	if statusOut.PolicyStatus != nil && statusOut.PolicyStatus.IsPublic {
		t.Error("expected IsPublic=false")
	}

	// Delete access point policy
	_, err = s3ctrl.DeleteAccessPointPolicy(ctx, &awss3control.DeleteAccessPointPolicyInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
	})
	if err != nil {
		t.Fatalf("DeleteAccessPointPolicy: %v", err)
	}

	// Delete access point
	_, err = s3ctrl.DeleteAccessPoint(ctx, &awss3control.DeleteAccessPointInput{
		AccountId: aws.String(accountID),
		Name:      aws.String("my-ap"),
	})
	if err != nil {
		t.Fatalf("DeleteAccessPoint: %v", err)
	}
}
