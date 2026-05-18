package multiaccount

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmparam "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── SNS ──────────────────────────────────────────────────────────────────────

func TestSNS_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	snsA := newSNSFor(t, AcctA)
	snsB := newSNSFor(t, AcctB)

	outA, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("shared-topic")})
	require.NoError(t, err)
	_, err = snsB.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("shared-topic")})
	require.NoError(t, err)

	listA, err := snsA.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	for _, top := range listA.Topics {
		assert.Contains(t, aws.ToString(top.TopicArn), AcctA, "A's ListTopics must only include A's topics")
		assert.NotContains(t, aws.ToString(top.TopicArn), AcctB)
	}

	listB, err := snsB.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	for _, top := range listB.Topics {
		assert.Contains(t, aws.ToString(top.TopicArn), AcctB, "B's ListTopics must only include B's topics")
		assert.NotContains(t, aws.ToString(top.TopicArn), AcctA)
	}

	// Delete A's topic — B's must survive.
	_, err = snsA.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: outA.TopicArn})
	require.NoError(t, err)
	listBAfter, err := snsB.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Topics, 1, "B's topic must survive A's delete")
}

func TestSNS_SubscriptionIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	snsA := newSNSFor(t, AcctA)
	snsB := newSNSFor(t, AcctB)
	sqsA := newSQSFor(t, AcctA)
	sqsB := newSQSFor(t, AcctB)

	topicA, err := snsA.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("sub-topic")})
	require.NoError(t, err)
	topicB, err := snsB.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("sub-topic")})
	require.NoError(t, err)

	qA, err := sqsA.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sub-q")})
	require.NoError(t, err)
	qB, err := sqsB.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sub-q")})
	require.NoError(t, err)

	_, err = snsA.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicA.TopicArn, Protocol: aws.String("sqs"), Endpoint: qA.QueueUrl,
	})
	require.NoError(t, err)
	_, err = snsB.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicB.TopicArn, Protocol: aws.String("sqs"), Endpoint: qB.QueueUrl,
	})
	require.NoError(t, err)

	subA, err := snsA.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	require.NoError(t, err)
	for _, sub := range subA.Subscriptions {
		assert.Contains(t, aws.ToString(sub.TopicArn), AcctA, "A's subscriptions must reference A's topics")
	}

	subB, err := snsB.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	require.NoError(t, err)
	for _, sub := range subB.Subscriptions {
		assert.Contains(t, aws.ToString(sub.TopicArn), AcctB, "B's subscriptions must reference B's topics")
	}
}

// ─── S3 ───────────────────────────────────────────────────────────────────────

func TestS3_BucketIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	s3A := newS3For(t, AcctA)
	s3B := newS3For(t, AcctB)

	// S3 bucket names are globally unique; use account-unique names.
	bucketA := "bucket-" + AcctA
	bucketB := "bucket-" + AcctB

	_, err := s3A.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketA)})
	require.NoError(t, err)
	_, err = s3B.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketB)})
	require.NoError(t, err)

	listA, err := s3A.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, listA.Buckets, 1, "A should see exactly its own bucket")

	listB, err := s3B.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, listB.Buckets, 1, "B should see exactly its own bucket")

	// Delete A's bucket — B's must survive.
	_, err = s3A.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketA)})
	require.NoError(t, err)
	listBAfter, err := s3B.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Buckets, 1, "B's bucket must survive A's delete")
}

func TestS3_ObjectIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	s3A := newS3For(t, AcctA)
	s3B := newS3For(t, AcctB)

	// S3 bucket names are globally unique; use account-unique names.
	bucketA := "obj-bucket-" + AcctA
	bucketB := "obj-bucket-" + AcctB

	_, err := s3A.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketA)})
	require.NoError(t, err)
	_, err = s3B.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketB)})
	require.NoError(t, err)

	_, err = s3A.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketA),
		Key:    aws.String("secret.txt"),
		Body:   bytes.NewReader([]byte("account A data")),
	})
	require.NoError(t, err)

	// B's bucket must not expose A's object.
	listB, err := s3B.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketB)})
	require.NoError(t, err)
	assert.Empty(t, listB.Contents, "B's bucket must not contain A's objects")

	listA, err := s3A.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketA)})
	require.NoError(t, err)
	assert.Len(t, listA.Contents, 1, "A must see its own object")
}

// ─── Lambda ───────────────────────────────────────────────────────────────────

func minimalLambdaZip() []byte {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, _ := w.Create("index.py")
	f.Write([]byte("def handler(e,c): return {}")) //nolint:errcheck
	w.Close()
	return buf.Bytes()
}

func TestLambda_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	lambdaA := newLambdaFor(t, AcctA)
	lambdaB := newLambdaFor(t, AcctB)
	iamA := newIAMFor(t, AcctA)
	iamB := newIAMFor(t, AcctB)

	assumeDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	roleA, err := iamA.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("lambda-exec"),
		AssumeRolePolicyDocument: aws.String(assumeDoc),
	})
	require.NoError(t, err)
	roleB, err := iamB.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("lambda-exec"),
		AssumeRolePolicyDocument: aws.String(assumeDoc),
	})
	require.NoError(t, err)

	code := minimalLambdaZip()
	_, err = lambdaA.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("shared-fn"),
		Role:         roleA.Role.Arn,
		Runtime:      lambdatypes.RuntimePython312,
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: code},
	})
	require.NoError(t, err)
	_, err = lambdaB.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("shared-fn"),
		Role:         roleB.Role.Arn,
		Runtime:      lambdatypes.RuntimePython312,
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: code},
	})
	require.NoError(t, err)

	listA, err := lambdaA.ListFunctions(ctx, &awslambda.ListFunctionsInput{})
	require.NoError(t, err)
	for _, fn := range listA.Functions {
		assert.Contains(t, aws.ToString(fn.FunctionArn), AcctA, "A's ListFunctions must include A's account in ARN")
		assert.NotContains(t, aws.ToString(fn.FunctionArn), AcctB)
	}

	listB, err := lambdaB.ListFunctions(ctx, &awslambda.ListFunctionsInput{})
	require.NoError(t, err)
	for _, fn := range listB.Functions {
		assert.Contains(t, aws.ToString(fn.FunctionArn), AcctB, "B's ListFunctions must include B's account in ARN")
		assert.NotContains(t, aws.ToString(fn.FunctionArn), AcctA)
	}

	// Delete A's function — B's must survive.
	_, err = lambdaA.DeleteFunction(ctx, &awslambda.DeleteFunctionInput{FunctionName: aws.String("shared-fn")})
	require.NoError(t, err)
	listBAfter, err := lambdaB.ListFunctions(ctx, &awslambda.ListFunctionsInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Functions, 1, "B's function must survive A's delete")
}

// ─── SecretsManager ───────────────────────────────────────────────────────────

func TestSecretsManager_CRUDIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	smA := newSMFor(t, AcctA)
	smB := newSMFor(t, AcctB)

	_, err := smA.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("shared-secret"),
		SecretString: aws.String("value-A"),
	})
	require.NoError(t, err)
	_, err = smB.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("shared-secret"),
		SecretString: aws.String("value-B"),
	})
	require.NoError(t, err)

	listA, err := smA.ListSecrets(ctx, &awssm.ListSecretsInput{})
	require.NoError(t, err)
	for _, s := range listA.SecretList {
		assert.Contains(t, aws.ToString(s.ARN), AcctA, "A's ListSecrets must include A's account in ARN")
		assert.NotContains(t, aws.ToString(s.ARN), AcctB)
	}

	listB, err := smB.ListSecrets(ctx, &awssm.ListSecretsInput{})
	require.NoError(t, err)
	for _, s := range listB.SecretList {
		assert.Contains(t, aws.ToString(s.ARN), AcctB, "B's ListSecrets must include B's account in ARN")
		assert.NotContains(t, aws.ToString(s.ARN), AcctA)
	}

	// Each account reads its own value independently.
	valA, err := smA.GetSecretValue(ctx, &awssm.GetSecretValueInput{SecretId: aws.String("shared-secret")})
	require.NoError(t, err)
	assert.Equal(t, "value-A", aws.ToString(valA.SecretString))

	valB, err := smB.GetSecretValue(ctx, &awssm.GetSecretValueInput{SecretId: aws.String("shared-secret")})
	require.NoError(t, err)
	assert.Equal(t, "value-B", aws.ToString(valB.SecretString))

	// Delete A's secret — B's must survive.
	_, err = smA.DeleteSecret(ctx, &awssm.DeleteSecretInput{
		SecretId:                   aws.String("shared-secret"),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	require.NoError(t, err)
	_, err = smB.GetSecretValue(ctx, &awssm.GetSecretValueInput{SecretId: aws.String("shared-secret")})
	require.NoError(t, err, "B's secret must survive A's delete")
}

// ─── SSM ──────────────────────────────────────────────────────────────────────

func TestSSM_AccountIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	ssmA := newSSMFor(t, AcctA)
	ssmB := newSSMFor(t, AcctB)

	_, err := ssmA.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/shared/param"),
		Value: aws.String("value-A"),
		Type:  ssmparam.ParameterTypeString,
	})
	require.NoError(t, err)
	_, err = ssmB.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/shared/param"),
		Value: aws.String("value-B"),
		Type:  ssmparam.ParameterTypeString,
	})
	require.NoError(t, err)

	getA, err := ssmA.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/shared/param")})
	require.NoError(t, err)
	assert.Equal(t, "value-A", aws.ToString(getA.Parameter.Value))

	getB, err := ssmB.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/shared/param")})
	require.NoError(t, err)
	assert.Equal(t, "value-B", aws.ToString(getB.Parameter.Value))

	// Verify GetParametersByPath scopes to account.
	pathA, err := ssmA.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{Path: aws.String("/shared")})
	require.NoError(t, err)
	assert.Len(t, pathA.Parameters, 1)
	assert.Equal(t, "value-A", aws.ToString(pathA.Parameters[0].Value))

	// Delete A's parameter — B's must survive.
	_, err = ssmA.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: aws.String("/shared/param")})
	require.NoError(t, err)
	_, err = ssmB.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/shared/param")})
	require.NoError(t, err, "B's parameter must survive A's delete")
}

// ─── IAM ──────────────────────────────────────────────────────────────────────

func TestIAM_RoleIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	iamA := newIAMFor(t, AcctA)
	iamB := newIAMFor(t, AcctB)

	assumeDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err := iamA.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("shared-role"),
		AssumeRolePolicyDocument: aws.String(assumeDoc),
	})
	require.NoError(t, err)
	_, err = iamB.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("shared-role"),
		AssumeRolePolicyDocument: aws.String(assumeDoc),
	})
	require.NoError(t, err)

	listA, err := iamA.ListRoles(ctx, &awsiam.ListRolesInput{})
	require.NoError(t, err)
	for _, r := range listA.Roles {
		assert.Contains(t, aws.ToString(r.Arn), AcctA, "A's roles must embed A's account in ARN")
		assert.NotContains(t, aws.ToString(r.Arn), AcctB)
	}

	listB, err := iamB.ListRoles(ctx, &awsiam.ListRolesInput{})
	require.NoError(t, err)
	for _, r := range listB.Roles {
		assert.Contains(t, aws.ToString(r.Arn), AcctB, "B's roles must embed B's account in ARN")
		assert.NotContains(t, aws.ToString(r.Arn), AcctA)
	}

	// Delete A's role — B's must survive.
	_, err = iamA.DeleteRole(ctx, &awsiam.DeleteRoleInput{RoleName: aws.String("shared-role")})
	require.NoError(t, err)
	listBAfter, err := iamB.ListRoles(ctx, &awsiam.ListRolesInput{})
	require.NoError(t, err)
	found := false
	for _, r := range listBAfter.Roles {
		if aws.ToString(r.RoleName) == "shared-role" {
			found = true
		}
	}
	assert.True(t, found, "B's role must survive A's delete")
}

func TestIAM_PolicyIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	iamA := newIAMFor(t, AcctA)
	iamB := newIAMFor(t, AcctB)

	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	outA, err := iamA.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
		PolicyName:     aws.String("shared-policy"),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)
	_, err = iamB.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
		PolicyName:     aws.String("shared-policy"),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)

	getA, err := iamA.GetPolicy(ctx, &awsiam.GetPolicyInput{PolicyArn: outA.Policy.Arn})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(getA.Policy.Arn), AcctA)

	listA, err := iamA.ListPolicies(ctx, &awsiam.ListPoliciesInput{Scope: "Local"})
	require.NoError(t, err)
	for _, p := range listA.Policies {
		assert.Contains(t, aws.ToString(p.Arn), AcctA, "A's local policies must embed A's account")
	}
}

func TestIAM_UserIsolation(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	iamA := newIAMFor(t, AcctA)
	iamB := newIAMFor(t, AcctB)

	_, err := iamA.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String("shared-user")})
	require.NoError(t, err)
	_, err = iamB.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String("shared-user")})
	require.NoError(t, err)

	listA, err := iamA.ListUsers(ctx, &awsiam.ListUsersInput{})
	require.NoError(t, err)
	for _, u := range listA.Users {
		assert.Contains(t, aws.ToString(u.Arn), AcctA)
		assert.NotContains(t, aws.ToString(u.Arn), AcctB)
	}

	listB, err := iamB.ListUsers(ctx, &awsiam.ListUsersInput{})
	require.NoError(t, err)
	for _, u := range listB.Users {
		assert.Contains(t, aws.ToString(u.Arn), AcctB)
		assert.NotContains(t, aws.ToString(u.Arn), AcctA)
	}

	// Delete A's user — B's must survive.
	_, err = iamA.DeleteUser(ctx, &awsiam.DeleteUserInput{UserName: aws.String("shared-user")})
	require.NoError(t, err)
	listBAfter, err := iamB.ListUsers(ctx, &awsiam.ListUsersInput{})
	require.NoError(t, err)
	assert.Len(t, listBAfter.Users, 1, "B's user must survive A's delete")
}
