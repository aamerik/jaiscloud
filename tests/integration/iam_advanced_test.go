package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const advTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

func TestIAMAdvanced_GetFederationToken(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	out, err := client.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
		Name: aws.String("my-federated-user"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.Credentials.AccessKeyId))
	assert.NotEmpty(t, aws.ToString(out.Credentials.SecretAccessKey))
	assert.NotEmpty(t, aws.ToString(out.Credentials.SessionToken))
	assert.NotNil(t, out.FederatedUser)
	assert.Contains(t, aws.ToString(out.FederatedUser.Arn), "federated-user/my-federated-user")
}

func TestIAMAdvanced_Groups(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	// Create group
	createOut, err := client.CreateGroup(ctx, &awsiam.CreateGroupInput{
		GroupName: aws.String("Developers"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Developers", aws.ToString(createOut.Group.GroupName))
	assert.Contains(t, aws.ToString(createOut.Group.Arn), "group/Developers")

	// Create user and add to group
	_, err = client.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	_, err = client.AddUserToGroup(ctx, &awsiam.AddUserToGroupInput{
		GroupName: aws.String("Developers"),
		UserName:  aws.String("alice"),
	})
	require.NoError(t, err)

	// GetGroup should list members
	getOut, err := client.GetGroup(ctx, &awsiam.GetGroupInput{
		GroupName: aws.String("Developers"),
	})
	require.NoError(t, err)
	assert.Len(t, getOut.Users, 1)
	assert.Equal(t, "alice", aws.ToString(getOut.Users[0].UserName))

	// ListGroupsForUser
	lgfuOut, err := client.ListGroupsForUser(ctx, &awsiam.ListGroupsForUserInput{
		UserName: aws.String("alice"),
	})
	require.NoError(t, err)
	assert.Len(t, lgfuOut.Groups, 1)
	assert.Equal(t, "Developers", aws.ToString(lgfuOut.Groups[0].GroupName))

	// RemoveUserFromGroup
	_, err = client.RemoveUserFromGroup(ctx, &awsiam.RemoveUserFromGroupInput{
		GroupName: aws.String("Developers"),
		UserName:  aws.String("alice"),
	})
	require.NoError(t, err)

	getOut2, err := client.GetGroup(ctx, &awsiam.GetGroupInput{
		GroupName: aws.String("Developers"),
	})
	require.NoError(t, err)
	assert.Len(t, getOut2.Users, 0)

	// ListGroups
	listOut, err := client.ListGroups(ctx, &awsiam.ListGroupsInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Groups, 1)

	// DeleteGroup
	_, err = client.DeleteGroup(ctx, &awsiam.DeleteGroupInput{
		GroupName: aws.String("Developers"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListGroups(ctx, &awsiam.ListGroupsInput{})
	require.NoError(t, err)
	assert.Len(t, listOut2.Groups, 0)
}

func TestIAMAdvanced_UserPolicies(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	_, err := client.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String("bob")})
	require.NoError(t, err)

	// Attach managed policy
	_, err = client.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
		PolicyName:     aws.String("ReadOnlyPolicy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`),
	})
	require.NoError(t, err)

	_, err = client.AttachUserPolicy(ctx, &awsiam.AttachUserPolicyInput{
		UserName:  aws.String("bob"),
		PolicyArn: aws.String("arn:aws:iam::000000000000:policy/ReadOnlyPolicy"),
	})
	require.NoError(t, err)

	attachedOut, err := client.ListAttachedUserPolicies(ctx, &awsiam.ListAttachedUserPoliciesInput{
		UserName: aws.String("bob"),
	})
	require.NoError(t, err)
	assert.Len(t, attachedOut.AttachedPolicies, 1)
	assert.Equal(t, "ReadOnlyPolicy", aws.ToString(attachedOut.AttachedPolicies[0].PolicyName))

	// Inline policy
	_, err = client.PutUserPolicy(ctx, &awsiam.PutUserPolicyInput{
		UserName:       aws.String("bob"),
		PolicyName:     aws.String("InlinePolicy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	lpOut, err := client.ListUserPolicies(ctx, &awsiam.ListUserPoliciesInput{
		UserName: aws.String("bob"),
	})
	require.NoError(t, err)
	assert.Len(t, lpOut.PolicyNames, 1)
	assert.Equal(t, "InlinePolicy", lpOut.PolicyNames[0])

	gpOut, err := client.GetUserPolicy(ctx, &awsiam.GetUserPolicyInput{
		UserName:   aws.String("bob"),
		PolicyName: aws.String("InlinePolicy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "InlinePolicy", aws.ToString(gpOut.PolicyName))

	// Detach managed, delete inline
	_, err = client.DetachUserPolicy(ctx, &awsiam.DetachUserPolicyInput{
		UserName:  aws.String("bob"),
		PolicyArn: aws.String("arn:aws:iam::000000000000:policy/ReadOnlyPolicy"),
	})
	require.NoError(t, err)

	_, err = client.DeleteUserPolicy(ctx, &awsiam.DeleteUserPolicyInput{
		UserName:   aws.String("bob"),
		PolicyName: aws.String("InlinePolicy"),
	})
	require.NoError(t, err)

	attachedOut2, err := client.ListAttachedUserPolicies(ctx, &awsiam.ListAttachedUserPoliciesInput{
		UserName: aws.String("bob"),
	})
	require.NoError(t, err)
	assert.Len(t, attachedOut2.AttachedPolicies, 0)
}

func TestIAMAdvanced_UserTagsAndAccessKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	_, err := client.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String("charlie")})
	require.NoError(t, err)

	// Tag user
	_, err = client.TagUser(ctx, &awsiam.TagUserInput{
		UserName: aws.String("charlie"),
		Tags: []iamtypes.Tag{
			{Key: aws.String("team"), Value: aws.String("platform")},
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListUserTags(ctx, &awsiam.ListUserTagsInput{
		UserName: aws.String("charlie"),
	})
	require.NoError(t, err)
	assert.Len(t, tagsOut.Tags, 2)

	// Untag one
	_, err = client.UntagUser(ctx, &awsiam.UntagUserInput{
		UserName: aws.String("charlie"),
		TagKeys:  []string{"env"},
	})
	require.NoError(t, err)

	tagsOut2, err := client.ListUserTags(ctx, &awsiam.ListUserTagsInput{
		UserName: aws.String("charlie"),
	})
	require.NoError(t, err)
	assert.Len(t, tagsOut2.Tags, 1)

	// Access key lifecycle
	akOut, err := client.CreateAccessKey(ctx, &awsiam.CreateAccessKeyInput{
		UserName: aws.String("charlie"),
	})
	require.NoError(t, err)
	keyID := aws.ToString(akOut.AccessKey.AccessKeyId)

	_, err = client.UpdateAccessKey(ctx, &awsiam.UpdateAccessKeyInput{
		AccessKeyId: aws.String(keyID),
		UserName:    aws.String("charlie"),
		Status:      iamtypes.StatusTypeInactive,
	})
	require.NoError(t, err)

	listKeys, err := client.ListAccessKeys(ctx, &awsiam.ListAccessKeysInput{
		UserName: aws.String("charlie"),
	})
	require.NoError(t, err)
	require.Len(t, listKeys.AccessKeyMetadata, 1)
	assert.Equal(t, iamtypes.StatusTypeInactive, listKeys.AccessKeyMetadata[0].Status)
}

func TestIAMAdvanced_InstanceProfiles(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	// Create role
	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("EC2Role"),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	// Create instance profile
	createOut, err := client.CreateInstanceProfile(ctx, &awsiam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("EC2Profile"),
	})
	require.NoError(t, err)
	assert.Equal(t, "EC2Profile", aws.ToString(createOut.InstanceProfile.InstanceProfileName))
	assert.Len(t, createOut.InstanceProfile.Roles, 0)

	// Add role
	_, err = client.AddRoleToInstanceProfile(ctx, &awsiam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String("EC2Profile"),
		RoleName:            aws.String("EC2Role"),
	})
	require.NoError(t, err)

	// Get profile — should include role
	getOut, err := client.GetInstanceProfile(ctx, &awsiam.GetInstanceProfileInput{
		InstanceProfileName: aws.String("EC2Profile"),
	})
	require.NoError(t, err)
	assert.Len(t, getOut.InstanceProfile.Roles, 1)
	assert.Equal(t, "EC2Role", aws.ToString(getOut.InstanceProfile.Roles[0].RoleName))

	// ListInstanceProfiles
	listOut, err := client.ListInstanceProfiles(ctx, &awsiam.ListInstanceProfilesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.InstanceProfiles, 1)

	// Remove role
	_, err = client.RemoveRoleFromInstanceProfile(ctx, &awsiam.RemoveRoleFromInstanceProfileInput{
		InstanceProfileName: aws.String("EC2Profile"),
		RoleName:            aws.String("EC2Role"),
	})
	require.NoError(t, err)

	getOut2, err := client.GetInstanceProfile(ctx, &awsiam.GetInstanceProfileInput{
		InstanceProfileName: aws.String("EC2Profile"),
	})
	require.NoError(t, err)
	assert.Len(t, getOut2.InstanceProfile.Roles, 0)

	// Delete profile
	_, err = client.DeleteInstanceProfile(ctx, &awsiam.DeleteInstanceProfileInput{
		InstanceProfileName: aws.String("EC2Profile"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListInstanceProfiles(ctx, &awsiam.ListInstanceProfilesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut2.InstanceProfiles, 0)
}

func TestIAMAdvanced_GetSessionToken(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, aws.ToString(out.Credentials.AccessKeyId))
	assert.NotEmpty(t, aws.ToString(out.Credentials.SecretAccessKey))
	assert.NotEmpty(t, aws.ToString(out.Credentials.SessionToken))
}

func TestIAMAdvanced_RoleTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("tagged-role"),
		AssumeRolePolicyDocument: aws.String(advTrustPolicy),
	})
	require.NoError(t, err)

	_, err = client.TagRole(ctx, &awsiam.TagRoleInput{
		RoleName: aws.String("tagged-role"),
		Tags: []iamtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("staging")},
			{Key: aws.String("cost-center"), Value: aws.String("eng")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListRoleTags(ctx, &awsiam.ListRoleTagsInput{
		RoleName: aws.String("tagged-role"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	_, err = client.UntagRole(ctx, &awsiam.UntagRoleInput{
		RoleName: aws.String("tagged-role"),
		TagKeys:  []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListRoleTags(ctx, &awsiam.ListRoleTagsInput{
		RoleName: aws.String("tagged-role"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "cost-center", aws.ToString(listOut2.Tags[0].Key))
}

func TestIAMAdvanced_RolePolicies(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("policy-role"),
		AssumeRolePolicyDocument: aws.String(advTrustPolicy),
	})
	require.NoError(t, err)

	// Put two inline policies.
	for _, name := range []string{"inline-one", "inline-two"} {
		_, err = client.PutRolePolicy(ctx, &awsiam.PutRolePolicyInput{
			RoleName:       aws.String("policy-role"),
			PolicyName:     aws.String(name),
			PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListRolePolicies(ctx, &awsiam.ListRolePoliciesInput{
		RoleName: aws.String("policy-role"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.PolicyNames, 2)
	assert.Contains(t, listOut.PolicyNames, "inline-one")

	// GetRolePolicy.
	getOut, err := client.GetRolePolicy(ctx, &awsiam.GetRolePolicyInput{
		RoleName:   aws.String("policy-role"),
		PolicyName: aws.String("inline-one"),
	})
	require.NoError(t, err)
	assert.Equal(t, "inline-one", aws.ToString(getOut.PolicyName))

	// UpdateAssumeRolePolicy.
	newTrust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err = client.UpdateAssumeRolePolicy(ctx, &awsiam.UpdateAssumeRolePolicyInput{
		RoleName:       aws.String("policy-role"),
		PolicyDocument: aws.String(newTrust),
	})
	require.NoError(t, err)

	// Delete one inline policy.
	_, err = client.DeleteRolePolicy(ctx, &awsiam.DeleteRolePolicyInput{
		RoleName:   aws.String("policy-role"),
		PolicyName: aws.String("inline-one"),
	})
	require.NoError(t, err)

	listOut2, err := client.ListRolePolicies(ctx, &awsiam.ListRolePoliciesInput{
		RoleName: aws.String("policy-role"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.PolicyNames, 1)
}

func TestIAMAdvanced_ListUsers(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newIAMClient(t)

	for _, name := range []string{"user-a", "user-b", "user-c"} {
		_, err := client.CreateUser(ctx, &awsiam.CreateUserInput{UserName: aws.String(name)})
		require.NoError(t, err)
	}

	listOut, err := client.ListUsers(ctx, &awsiam.ListUsersInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Users, 3)

	// DeleteAccessKey is exercised here: create then delete.
	akOut, err := client.CreateAccessKey(ctx, &awsiam.CreateAccessKeyInput{
		UserName: aws.String("user-a"),
	})
	require.NoError(t, err)
	keyID := aws.ToString(akOut.AccessKey.AccessKeyId)

	_, err = client.DeleteAccessKey(ctx, &awsiam.DeleteAccessKeyInput{
		UserName:    aws.String("user-a"),
		AccessKeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	keysOut, err := client.ListAccessKeys(ctx, &awsiam.ListAccessKeysInput{
		UserName: aws.String("user-a"),
	})
	require.NoError(t, err)
	assert.Len(t, keysOut.AccessKeyMetadata, 0)
}
