package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	stsCovTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	stsCovRoleArn     = "arn:aws:iam::000000000000:role/test-role"
)

// createSTSCovRole is a helper that creates the standard test role and returns its ARN.
func createSTSCovRole(t *testing.T, iamClient *awsiam.Client, roleName string) string {
	t.Helper()
	ctx := context.Background()
	out, err := iamClient.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(stsCovTrustPolicy),
	})
	require.NoError(t, err)
	return *out.Role.Arn
}

// TestSTS_GetCallerIdentity_ReturnsAccountAndArn verifies that GetCallerIdentity
// returns the expected fixed account ID and an ARN containing "arn:aws".
func TestSTS_GetCallerIdentity_ReturnsAccountAndArn(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Account)
	assert.Equal(t, "000000000000", *out.Account)
	require.NotNil(t, out.Arn)
	assert.Contains(t, *out.Arn, "arn:aws")
}

// TestSTS_GetCallerIdentity_UserIdNotEmpty verifies that UserId is non-empty.
func TestSTS_GetCallerIdentity_UserIdNotEmpty(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	require.NoError(t, err)
	require.NotNil(t, out.UserId)
	assert.NotEmpty(t, *out.UserId)
}

// TestSTS_AssumeRole_ReturnsCredentials verifies that AssumeRole returns all
// four credential fields.
func TestSTS_AssumeRole_ReturnsCredentials(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("my-session"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.NotNil(t, out.Credentials.Expiration)
}

// TestSTS_AssumeRole_AccessKeyIdStartsWithASIA verifies that the temporary
// access key ID starts with the ASIA prefix used for assumed-role credentials.
func TestSTS_AssumeRole_AccessKeyIdStartsWithASIA(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("sia-session"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"),
		"expected AccessKeyId to start with ASIA, got %s", *out.Credentials.AccessKeyId)
}

// TestSTS_AssumeRole_ExpirationIsInFuture verifies that the returned expiration
// is strictly after the current time.
func TestSTS_AssumeRole_ExpirationIsInFuture(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")
	now := time.Now()

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("exp-session"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	assert.True(t, out.Credentials.Expiration.After(now),
		"expiration %v should be after now %v", *out.Credentials.Expiration, now)
}

// TestSTS_AssumeRole_SessionNameInResponse verifies that the AssumedRoleId
// contains the supplied session name.
func TestSTS_AssumeRole_SessionNameInResponse(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")
	sessionName := "my-unique-session"

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String(sessionName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.AssumedRoleUser)
	assert.Contains(t, *out.AssumedRoleUser.AssumedRoleId, sessionName)
}

// TestSTS_AssumeRole_DurationSeconds_Custom verifies that specifying
// DurationSeconds=3600 yields an expiration approximately one hour out.
func TestSTS_AssumeRole_DurationSeconds_Custom(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")
	before := time.Now()

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("custom-dur"),
		DurationSeconds: aws.Int32(3600),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	// Expiration should be roughly 1 hour from now — check it is at least 50 minutes out.
	assert.True(t, out.Credentials.Expiration.After(before.Add(50*time.Minute)),
		"expiration %v not ~1 hour from now", *out.Credentials.Expiration)
}

// TestSTS_AssumeRole_DurationSeconds_TooShort_Error verifies that a duration
// of 10 seconds is rejected.
func TestSTS_AssumeRole_DurationSeconds_TooShort_Error(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(stsCovRoleArn),
		RoleSessionName: aws.String("short-dur"),
		DurationSeconds: aws.Int32(10),
	})
	// AWS SDK validates minimum duration client-side or server returns an error.
	require.Error(t, err)
}

// TestSTS_AssumeRole_DurationSeconds_TooLong_Error verifies that an excessively
// large duration is rejected.
func TestSTS_AssumeRole_DurationSeconds_TooLong_Error(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(stsCovRoleArn),
		RoleSessionName: aws.String("long-dur"),
		DurationSeconds: aws.Int32(999999),
	})
	require.Error(t, err)
}

// TestSTS_AssumeRole_InvalidRoleArn_Error verifies that a non-ARN string is
// rejected with a validation error.
func TestSTS_AssumeRole_InvalidRoleArn_Error(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String("not-an-arn"),
		RoleSessionName: aws.String("test-session"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationError")
}

// TestSTS_AssumeRoleWithWebIdentity_ReturnsCredentials verifies that
// AssumeRoleWithWebIdentity with a dummy JWT returns temporary credentials.
func TestSTS_AssumeRoleWithWebIdentity_ReturnsCredentials(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")
	// Minimal three-part JWT: header.payload.sig where payload base64-encodes {"sub":"test-user"}
	token := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.sig"

	out, err := stsClient.AssumeRoleWithWebIdentity(ctx, &awssts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(roleArn),
		RoleSessionName:  aws.String("web-session"),
		WebIdentityToken: aws.String(token),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
}

// TestSTS_GetSessionToken_ReturnsCredentials verifies that GetSessionToken
// returns all four credential fields.
func TestSTS_GetSessionToken_ReturnsCredentials(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{
		DurationSeconds: aws.Int32(900),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.NotNil(t, out.Credentials.Expiration)
}

// TestSTS_GetSessionToken_DefaultDuration verifies that the default session token
// duration is less than 1 day from now (AWS default is 12 hours).
func TestSTS_GetSessionToken_DefaultDuration(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	oneDayFromNow := time.Now().Add(24 * time.Hour)
	assert.True(t, out.Credentials.Expiration.Before(oneDayFromNow),
		"default session token expiration %v should be less than 1 day from now", *out.Credentials.Expiration)
}

// TestSTS_GetFederationToken_ReturnsCredentials verifies that GetFederationToken
// returns temporary credentials and a FederatedUser.
func TestSTS_GetFederationToken_ReturnsCredentials(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
		Name: aws.String("federated-user"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	require.NotNil(t, out.FederatedUser)
	assert.NotEmpty(t, *out.FederatedUser.Arn)
}

// TestSTS_GetFederationToken_NameValidation_Error verifies that a name
// containing spaces is rejected.
func TestSTS_GetFederationToken_NameValidation_Error(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
		Name: aws.String("invalid name with spaces"),
	})
	// The AWS SDK validates the Name pattern client-side or the server rejects it.
	require.Error(t, err)
}

// TestSTS_AssumedRoleArn_Format verifies that the assumed-role ARN follows the
// expected format: arn:aws:sts::000000000000:assumed-role/<role>/<session>.
func TestSTS_AssumedRoleArn_Format(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("arn-check"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.AssumedRoleUser)
	arn := *out.AssumedRoleUser.Arn
	assert.True(t, strings.HasPrefix(arn, "arn:aws:sts::000000000000:assumed-role/"),
		"unexpected assumed-role ARN format: %s", arn)
	assert.Contains(t, arn, "test-role")
	assert.Contains(t, arn, "arn-check")
}

// TestSTS_AssumeRole_WithTags verifies that AssumeRole with session tags
// succeeds without error.
func TestSTS_AssumeRole_WithTags(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("tagged-session"),
		Tags: []ststypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
}

// TestSTS_AssumeRole_WithTransitiveTags verifies that TransitiveTagKeys can be
// specified alongside Tags.
func TestSTS_AssumeRole_WithTransitiveTags(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("transitive-session"),
		Tags: []ststypes.Tag{
			{Key: aws.String("Project"), Value: aws.String("jaiscloud")},
		},
		TransitiveTagKeys: []string{"Project"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
}

// TestSTS_GetAccessKeyInfo_ReturnsAccount verifies that GetAccessKeyInfo
// returns the expected account ID.
func TestSTS_GetAccessKeyInfo_ReturnsAccount(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetAccessKeyInfo(ctx, &awssts.GetAccessKeyInfoInput{
		AccessKeyId: aws.String("AKIAIOSFODNN7EXAMPLE"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Account)
	assert.Equal(t, "000000000000", *out.Account)
}

// TestSTS_MultipleAssumeRole_IndependentSessions verifies that two separate
// AssumeRole calls yield distinct session tokens.
func TestSTS_MultipleAssumeRole_IndependentSessions(t *testing.T) {
	resetState(t)
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)
	ctx := context.Background()

	roleArn := createSTSCovRole(t, iamClient, "test-role")

	out1, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("session-one"),
	})
	require.NoError(t, err)

	out2, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("session-two"),
	})
	require.NoError(t, err)

	assert.NotEqual(t, *out1.Credentials.SessionToken, *out2.Credentials.SessionToken,
		"two independent AssumeRole calls should produce different session tokens")
	assert.NotEqual(t, *out1.Credentials.AccessKeyId, *out2.Credentials.AccessKeyId,
		"two independent AssumeRole calls should produce different access key IDs")
}
