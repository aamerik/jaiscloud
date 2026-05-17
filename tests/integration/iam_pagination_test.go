package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/require"
)

const paginationTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
const paginationPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

func TestIAMPaged_ListRolesThreePages(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create 5 roles
	roleNames := []string{"role-a", "role-b", "role-c", "role-d", "role-e"}
	for _, name := range roleNames {
		_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
			RoleName:                 aws.String(name),
			AssumeRolePolicyDocument: aws.String(paginationTrustPolicy),
		})
		require.NoError(t, err)
	}

	// Page 1: MaxItems=2 → 2 roles + IsTruncated=true + Marker
	out1, err := client.ListRoles(ctx, &awsiam.ListRolesInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out1.Roles, 2)
	require.True(t, out1.IsTruncated)
	require.NotNil(t, out1.Marker)

	// Page 2: MaxItems=2 Marker=<prev> → next 2 roles + IsTruncated=true + Marker
	out2, err := client.ListRoles(ctx, &awsiam.ListRolesInput{
		MaxItems: aws.Int32(2),
		Marker:   out1.Marker,
	})
	require.NoError(t, err)
	require.Len(t, out2.Roles, 2)
	require.True(t, out2.IsTruncated)
	require.NotNil(t, out2.Marker)

	// Page 3: MaxItems=2 Marker=<prev> → last 1 role + IsTruncated=false
	out3, err := client.ListRoles(ctx, &awsiam.ListRolesInput{
		MaxItems: aws.Int32(2),
		Marker:   out2.Marker,
	})
	require.NoError(t, err)
	require.Len(t, out3.Roles, 1)
	require.False(t, out3.IsTruncated)
	require.Nil(t, out3.Marker)

	// Verify all 5 unique role names were returned across pages
	seen := map[string]bool{}
	for _, r := range append(append(out1.Roles, out2.Roles...), out3.Roles...) {
		seen[aws.ToString(r.RoleName)] = true
	}
	require.Len(t, seen, 5)
}

func TestIAMPaged_ListUsersTwoPages(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create 3 users
	for i := 0; i < 3; i++ {
		_, err := client.CreateUser(ctx, &awsiam.CreateUserInput{
			UserName: aws.String(fmt.Sprintf("user-%d", i)),
		})
		require.NoError(t, err)
	}

	// Page 1: MaxItems=2 → 2 users + truncated
	out1, err := client.ListUsers(ctx, &awsiam.ListUsersInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out1.Users, 2)
	require.True(t, out1.IsTruncated)
	require.NotNil(t, out1.Marker)

	// Page 2: MaxItems=2 Marker=<prev> → 1 user + not truncated
	out2, err := client.ListUsers(ctx, &awsiam.ListUsersInput{
		MaxItems: aws.Int32(2),
		Marker:   out1.Marker,
	})
	require.NoError(t, err)
	require.Len(t, out2.Users, 1)
	require.False(t, out2.IsTruncated)
	require.Nil(t, out2.Marker)

	// Verify all 3 unique user names returned
	seen := map[string]bool{}
	for _, u := range append(out1.Users, out2.Users...) {
		seen[aws.ToString(u.UserName)] = true
	}
	require.Len(t, seen, 3)
}

func TestIAMPaged_ListPoliciesTwoPages(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create 3 policies
	for i := 0; i < 3; i++ {
		_, err := client.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
			PolicyName:     aws.String(fmt.Sprintf("policy-%d", i)),
			PolicyDocument: aws.String(paginationPolicyDoc),
		})
		require.NoError(t, err)
	}

	// Page 1: MaxItems=2 → 2 policies + truncated
	out1, err := client.ListPolicies(ctx, &awsiam.ListPoliciesInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(out1.Policies), 2)
	require.True(t, out1.IsTruncated)
	require.NotNil(t, out1.Marker)

	// Page 2: follow the marker → not truncated (remaining policies)
	out2, err := client.ListPolicies(ctx, &awsiam.ListPoliciesInput{
		MaxItems: aws.Int32(2),
		Marker:   out1.Marker,
	})
	require.NoError(t, err)
	require.False(t, out2.IsTruncated)
	require.Nil(t, out2.Marker)
}

func TestIAMPaged_ListRolesPathPrefix(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create roles under different paths
	for _, name := range []string{"svc-a", "svc-b"} {
		_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
			RoleName:                 aws.String(name),
			AssumeRolePolicyDocument: aws.String(paginationTrustPolicy),
			Path:                     aws.String("/service/"),
		})
		require.NoError(t, err)
	}
	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("other-role"),
		AssumeRolePolicyDocument: aws.String(paginationTrustPolicy),
		Path:                     aws.String("/other/"),
	})
	require.NoError(t, err)

	// ListRoles with PathPrefix=/service/ → only 2 roles
	out, err := client.ListRoles(ctx, &awsiam.ListRolesInput{
		PathPrefix: aws.String("/service/"),
	})
	require.NoError(t, err)
	require.Len(t, out.Roles, 2)
	for _, r := range out.Roles {
		require.Equal(t, "/service/", aws.ToString(r.Path))
	}
}

func TestIAMPaged_ListAttachedRolePoliciesTwoPages(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create a role and 3 policies, attach all to the role
	_, err := client.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("paged-role"),
		AssumeRolePolicyDocument: aws.String(paginationTrustPolicy),
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		out, err := client.CreatePolicy(ctx, &awsiam.CreatePolicyInput{
			PolicyName:     aws.String(fmt.Sprintf("attach-pol-%d", i)),
			PolicyDocument: aws.String(paginationPolicyDoc),
		})
		require.NoError(t, err)
		_, err = client.AttachRolePolicy(ctx, &awsiam.AttachRolePolicyInput{
			RoleName:  aws.String("paged-role"),
			PolicyArn: out.Policy.Arn,
		})
		require.NoError(t, err)
	}

	// Page 1: MaxItems=2 → 2 attached policies + truncated
	out1, err := client.ListAttachedRolePolicies(ctx, &awsiam.ListAttachedRolePoliciesInput{
		RoleName: aws.String("paged-role"),
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out1.AttachedPolicies, 2)
	require.True(t, out1.IsTruncated)
	require.NotNil(t, out1.Marker)

	// Page 2: → last 1 + not truncated
	out2, err := client.ListAttachedRolePolicies(ctx, &awsiam.ListAttachedRolePoliciesInput{
		RoleName: aws.String("paged-role"),
		MaxItems: aws.Int32(2),
		Marker:   out1.Marker,
	})
	require.NoError(t, err)
	require.Len(t, out2.AttachedPolicies, 1)
	require.False(t, out2.IsTruncated)
}

func TestIAMPaged_ListGroupsTwoPages(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create 3 groups
	for i := 0; i < 3; i++ {
		_, err := client.CreateGroup(ctx, &awsiam.CreateGroupInput{
			GroupName: aws.String(fmt.Sprintf("group-%d", i)),
		})
		require.NoError(t, err)
	}

	// Page 1: MaxItems=2 → 2 groups + truncated
	out1, err := client.ListGroups(ctx, &awsiam.ListGroupsInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out1.Groups, 2)
	require.True(t, out1.IsTruncated)
	require.NotNil(t, out1.Marker)

	// Page 2: → 1 group + not truncated
	out2, err := client.ListGroups(ctx, &awsiam.ListGroupsInput{
		MaxItems: aws.Int32(2),
		Marker:   out1.Marker,
	})
	require.NoError(t, err)
	require.Len(t, out2.Groups, 1)
	require.False(t, out2.IsTruncated)
}

func TestIAMPaged_ListInstanceProfilesTwoPages(t *testing.T) {
	resetState(t)
	client := newIAMClient(t)
	ctx := context.Background()

	// Create 3 instance profiles
	for i := 0; i < 3; i++ {
		_, err := client.CreateInstanceProfile(ctx, &awsiam.CreateInstanceProfileInput{
			InstanceProfileName: aws.String(fmt.Sprintf("profile-%d", i)),
		})
		require.NoError(t, err)
	}

	// Page 1: MaxItems=2 → 2 profiles + truncated
	out1, err := client.ListInstanceProfiles(ctx, &awsiam.ListInstanceProfilesInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out1.InstanceProfiles, 2)
	require.True(t, out1.IsTruncated)
	require.NotNil(t, out1.Marker)

	// Page 2: → 1 profile + not truncated
	out2, err := client.ListInstanceProfiles(ctx, &awsiam.ListInstanceProfilesInput{
		MaxItems: aws.Int32(2),
		Marker:   out1.Marker,
	})
	require.NoError(t, err)
	require.Len(t, out2.InstanceProfiles, 1)
	require.False(t, out2.IsTruncated)
}
