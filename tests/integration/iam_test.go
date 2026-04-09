package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/require"
)

const trustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

func TestIAM_CreateGetDeleteRole(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	out, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("my-role"),
		AssumeRolePolicyDocument: aws.String(trustPolicy),
	})
	require.NoError(t, err)
	require.Equal(t, "my-role", *out.Role.RoleName)
	require.True(t, strings.HasPrefix(*out.Role.Arn, "arn:aws:iam::"))

	get, err := client.GetRole(ctx, &awsiam.GetRoleInput{RoleName: aws.String("my-role")})
	require.NoError(t, err)
	require.Equal(t, *out.Role.Arn, *get.Role.Arn)

	_, err = client.DeleteRole(ctx, &awsiam.DeleteRoleInput{RoleName: aws.String("my-role")})
	require.NoError(t, err)

	_, err = client.GetRole(ctx, &awsiam.GetRoleInput{RoleName: aws.String("my-role")})
	require.Error(t, err)
}

func TestIAM_ListRoles(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	for _, name := range []string{"role-a", "role-b", "role-c"} {
		_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
			RoleName:                 aws.String(name),
			AssumeRolePolicyDocument: aws.String(trustPolicy),
		})
		require.NoError(t, err)
	}

	out, err := client.ListRoles(ctx, &awsiam.ListRolesInput{})
	require.NoError(t, err)
	require.Len(t, out.Roles, 3)
}

func TestIAM_CreateGetDeletePolicy(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	out, err := client.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
		PolicyName:     aws.String("my-policy"),
		PolicyDocument: aws.String(doc),
	})
	require.NoError(t, err)
	require.Equal(t, "my-policy", *out.Policy.PolicyName)

	get, err := client.GetPolicy(ctx, &awsiam.GetPolicyInput{PolicyArn: out.Policy.Arn})
	require.NoError(t, err)
	require.Equal(t, *out.Policy.Arn, *get.Policy.Arn)

	_, err = client.DeletePolicy(ctx, &awsiam.DeletePolicyInput{PolicyArn: out.Policy.Arn})
	require.NoError(t, err)
}

func TestIAM_AttachDetachRolePolicy(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("test-role"),
		AssumeRolePolicyDocument: aws.String(trustPolicy),
	})
	require.NoError(t, err)

	policyOut, err := client.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
		PolicyName:     aws.String("test-policy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.AttachRolePolicy(ctx, &awsiam.AttachRolePolicyInput{
		RoleName:  aws.String("test-role"),
		PolicyArn: policyOut.Policy.Arn,
	})
	require.NoError(t, err)

	listOut, err := client.ListAttachedRolePolicies(ctx, &awsiam.ListAttachedRolePoliciesInput{
		RoleName: aws.String("test-role"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.AttachedPolicies, 1)
	require.Equal(t, *policyOut.Policy.Arn, *listOut.AttachedPolicies[0].PolicyArn)

	_, err = client.DetachRolePolicy(ctx, &awsiam.DetachRolePolicyInput{
		RoleName:  aws.String("test-role"),
		PolicyArn: policyOut.Policy.Arn,
	})
	require.NoError(t, err)

	listOut2, err := client.ListAttachedRolePolicies(ctx, &awsiam.ListAttachedRolePoliciesInput{
		RoleName: aws.String("test-role"),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.AttachedPolicies, 0)
}

func TestIAM_PutGetDeleteRolePolicy(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("inline-role"),
		AssumeRolePolicyDocument: aws.String(trustPolicy),
	})
	require.NoError(t, err)

	_, err = client.PutRolePolicy(ctx, &awsiam.PutRolePolicyInput{
		RoleName:       aws.String("inline-role"),
		PolicyName:     aws.String("inline-policy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	getOut, err := client.GetRolePolicy(ctx, &awsiam.GetRolePolicyInput{
		RoleName:   aws.String("inline-role"),
		PolicyName: aws.String("inline-policy"),
	})
	require.NoError(t, err)
	require.Equal(t, "inline-policy", *getOut.PolicyName)

	_, err = client.DeleteRolePolicy(ctx, &awsiam.DeleteRolePolicyInput{
		RoleName:   aws.String("inline-role"),
		PolicyName: aws.String("inline-policy"),
	})
	require.NoError(t, err)
}

func TestIAM_CreateGetDeleteUser(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	out, err := client.CreateUser(ctx, &awsiam.CreateUserInput{
		UserName: aws.String("alice"),
	})
	require.NoError(t, err)
	require.Equal(t, "alice", *out.User.UserName)

	get, err := client.GetUser(ctx, &awsiam.GetUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)
	require.Equal(t, *out.User.Arn, *get.User.Arn)

	_, err = client.DeleteUser(ctx, &awsiam.DeleteUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)
}

func TestIAM_CreateDeleteAccessKey(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	_, err := client.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String("bob")})
	require.NoError(t, err)

	keyOut, err := client.CreateAccessKey(ctx, &awsiam.CreateAccessKeyInput{
		UserName: aws.String("bob"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *keyOut.AccessKey.AccessKeyId)
	require.NotEmpty(t, *keyOut.AccessKey.SecretAccessKey)

	listOut, err := client.ListAccessKeys(ctx, &awsiam.ListAccessKeysInput{
		UserName: aws.String("bob"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.AccessKeyMetadata, 1)

	_, err = client.DeleteAccessKey(ctx, &awsiam.DeleteAccessKeyInput{
		AccessKeyId: keyOut.AccessKey.AccessKeyId,
	})
	require.NoError(t, err)
}

func TestSTS_GetCallerIdentity(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	require.NoError(t, err)
	require.NotEmpty(t, *out.Account)
	require.NotEmpty(t, *out.Arn)
}

func TestSTS_AssumeRole(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleOut, err := iamClient.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("assume-me"),
		AssumeRolePolicyDocument: aws.String(trustPolicy),
	})
	require.NoError(t, err)

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         roleOut.Role.Arn,
		RoleSessionName: aws.String("test-session"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *out.Credentials.AccessKeyId)
	require.NotEmpty(t, *out.Credentials.SecretAccessKey)
	require.NotEmpty(t, *out.Credentials.SessionToken)
}
