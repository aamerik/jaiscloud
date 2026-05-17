package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── G-PENDING-8: STS Extended Operations ─────────────────────────────────────

const stsExtTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

// TestSTS_GetCallerIdentity_AllFields asserts that GetCallerIdentity returns
// non-empty UserId, Account="000000000000", and an ARN containing "arn:aws".
func TestSTS_GetCallerIdentity_AllFields(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	out, err := client.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	require.NoError(t, err)
	require.NotNil(t, out.UserId)
	assert.NotEmpty(t, *out.UserId, "UserId must be non-empty")
	require.NotNil(t, out.Account)
	assert.Equal(t, "000000000000", *out.Account, "Account must be 000000000000")
	require.NotNil(t, out.Arn)
	assert.Contains(t, *out.Arn, "arn:aws", "Arn must start with arn:aws")
}

// TestSTS_GetSessionToken_CredentialFields asserts that GetSessionToken returns
// all four credential fields: AccessKeyId, SecretAccessKey, SessionToken, Expiration.
func TestSTS_GetSessionToken_CredentialFields(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId, "AccessKeyId must be non-empty")
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey, "SecretAccessKey must be non-empty")
	assert.NotEmpty(t, *out.Credentials.SessionToken, "SessionToken must be non-empty")
	require.NotNil(t, out.Credentials.Expiration, "Expiration must be set")
	assert.True(t, out.Credentials.Expiration.After(time.Now()), "Expiration must be in the future")
}

// TestSTS_GetSessionToken_DurationSeconds verifies that specifying
// DurationSeconds=3600 yields an expiration approximately one hour from now.
func TestSTS_GetSessionToken_DurationSeconds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	before := time.Now()
	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{
		DurationSeconds: aws.Int32(3600),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	// Expiration should be at least 50 minutes from now (allows for test latency).
	assert.True(t, out.Credentials.Expiration.After(before.Add(50*time.Minute)),
		"expiration %v should be ~1 hour from now", *out.Credentials.Expiration)
}

// TestSTS_GetFederationToken_CredentialsAndFederatedUser verifies that
// GetFederationToken returns Credentials and a FederatedUser with a non-empty ARN.
func TestSTS_GetFederationToken_CredentialsAndFederatedUser(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	out, err := client.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
		Name: aws.String("myuser"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials, "Credentials must be returned")
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	require.NotNil(t, out.FederatedUser, "FederatedUser must be returned")
	require.NotNil(t, out.FederatedUser.Arn, "FederatedUser.Arn must be set")
	assert.NotEmpty(t, *out.FederatedUser.Arn, "FederatedUser.Arn must be non-empty")
	assert.Contains(t, *out.FederatedUser.Arn, "myuser", "FederatedUser.Arn must contain the name")
}

// TestSTS_AssumeRole_Basic creates an IAM role and asserts that AssumeRole returns
// valid temporary credentials.
func TestSTS_AssumeRole_Basic(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)

	roleOut, err := iamClient.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("ext-role"),
		AssumeRolePolicyDocument: aws.String(stsExtTrustPolicy),
	})
	require.NoError(t, err)

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         roleOut.Role.Arn,
		RoleSessionName: aws.String("basic-session"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
}

// TestSTS_AssumeRole_ExternalId verifies that AssumeRole accepts an ExternalId
// parameter and still returns credentials.
func TestSTS_AssumeRole_ExternalId(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	iamClient := newIAMClient(t)
	stsClient := newSTSClient(t)

	roleOut, err := iamClient.CreateRole(ctx, &awsiam.CreateRoleInput{
		RoleName:                 aws.String("ext-role-extid"),
		AssumeRolePolicyDocument: aws.String(stsExtTrustPolicy),
	})
	require.NoError(t, err)

	out, err := stsClient.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         roleOut.Role.Arn,
		RoleSessionName: aws.String("extid-session"),
		ExternalId:      aws.String("my-external-id-12345"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
}

// TestSTS_AssumeRoleWithWebIdentity_Stub verifies that AssumeRoleWithWebIdentity
// with a dummy WebIdentityToken returns credentials without performing JWT validation.
func TestSTS_AssumeRoleWithWebIdentity_Stub(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	// Minimal three-part JWT: header.payload.sig
	// payload is base64url({"sub":"test-user"})
	dummyToken := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.dummy-sig"

	out, err := client.AssumeRoleWithWebIdentity(ctx, &awssts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/WebIdentityRole"),
		RoleSessionName:  aws.String("web-id-session"),
		WebIdentityToken: aws.String(dummyToken),
	})
	require.NoError(t, err, "stub should return credentials without JWT validation")
	require.NotNil(t, out.Credentials)
	assert.NotEmpty(t, *out.Credentials.AccessKeyId)
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"),
		"temp access key must start with ASIA")
}

// TestSTS_DecodeAuthorizationMessage_Stub verifies that DecodeAuthorizationMessage
// returns a decoded message (stub — returns the encoded message back) without panicking.
func TestSTS_DecodeAuthorizationMessage_Stub(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)

	// The stub implementation returns the encoded message as the decoded message.
	// Any graceful response (no panic, no 500) is acceptable.
	out, err := client.DecodeAuthorizationMessage(ctx, &awssts.DecodeAuthorizationMessageInput{
		EncodedMessage: aws.String("dummy-encoded-authorization-message"),
	})
	// Either returns a result or a handled error — must not panic or return 500.
	if err != nil {
		// Verify it is a well-formed AWS API error, not a transport/panic error.
		assert.NotContains(t, err.Error(), "panic", "must not panic")
		assert.NotContains(t, err.Error(), "500", "must not return 500 internal server error")
	} else {
		require.NotNil(t, out)
		require.NotNil(t, out.DecodedMessage)
		assert.NotEmpty(t, *out.DecodedMessage)
	}
}

// TestSTS_GetSessionToken_ExpirationInFuture verifies that the default session
// token has an expiration strictly in the future.
func TestSTS_GetSessionToken_ExpirationInFuture(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)
	now := time.Now()

	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	assert.True(t, out.Credentials.Expiration.After(now),
		"Expiration %v must be after now %v", *out.Credentials.Expiration, now)
}

// TestSTS_GetFederationToken_DurationSeconds verifies that specifying
// DurationSeconds=3600 yields an expiration approximately one hour from now.
func TestSTS_GetFederationToken_DurationSeconds(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSTSClient(t)
	before := time.Now()

	out, err := client.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
		Name:            aws.String("feduser"),
		DurationSeconds: aws.Int32(3600),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	require.NotNil(t, out.Credentials.Expiration)
	assert.True(t, out.Credentials.Expiration.After(before.Add(50*time.Minute)),
		"expiration should be ~1 hour from now, got %v", *out.Credentials.Expiration)
}
