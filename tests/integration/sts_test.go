package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSTS_GetCallerIdentity(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	require.NoError(t, err)
	assert.Equal(t, "000000000000", *out.Account)
	assert.Contains(t, *out.Arn, "arn:aws:iam::")
	assert.Contains(t, *out.Arn, "root")
	assert.NotEmpty(t, *out.UserId)
}

func TestSTS_AssumeRole_ValidInput(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/TestRole"),
		RoleSessionName: aws.String("test-session"),
		DurationSeconds: aws.Int32(900),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"))
	assert.NotEmpty(t, *out.Credentials.SecretAccessKey)
	assert.NotEmpty(t, *out.Credentials.SessionToken)
	assert.True(t, out.Credentials.Expiration.After(time.Now()))
	assert.Contains(t, *out.AssumedRoleUser.Arn, "assumed-role/TestRole/test-session")
}

func TestSTS_AssumeRole_InvalidArn(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String("not-an-arn"),
		RoleSessionName: aws.String("test"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationError")
}

func TestSTS_AssumeRole_InvalidSessionName(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/TestRole"),
		RoleSessionName: aws.String("has spaces here"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationError")
}

func TestSTS_AssumeRole_DefaultDuration(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	before := time.Now()
	out, err := client.AssumeRole(ctx, &awssts.AssumeRoleInput{
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/MyRole"),
		RoleSessionName: aws.String("sess"),
	})
	require.NoError(t, err)
	// default 3600s — expiry should be ~1 hour from now
	assert.True(t, out.Credentials.Expiration.After(before.Add(50*time.Minute)))
}

func TestSTS_AssumeRoleWithWebIdentity(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	// minimal JWT-like token (header.payload.sig)
	// payload base64({"sub":"test-user"})
	token := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.sig"

	out, err := client.AssumeRoleWithWebIdentity(ctx, &awssts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String("arn:aws:iam::000000000000:role/WebRole"),
		RoleSessionName:  aws.String("web-session"),
		WebIdentityToken: aws.String(token),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"))
	assert.NotEmpty(t, *out.SubjectFromWebIdentityToken)
}

func TestSTS_AssumeRoleWithSAML(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	import64 := "PHNhbWxwOlJlc3BvbnNlPjwvc2FtbHA6UmVzcG9uc2U+"

	out, err := client.AssumeRoleWithSAML(ctx, &awssts.AssumeRoleWithSAMLInput{
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/SAMLRole"),
		PrincipalArn: aws.String("arn:aws:iam::000000000000:saml-provider/MySAML"),
		SAMLAssertion: aws.String(import64),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"))
}

func TestSTS_GetSessionToken(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{
		DurationSeconds: aws.Int32(900),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"))
	assert.NotEmpty(t, *out.Credentials.SessionToken)
}

func TestSTS_GetSessionToken_InvalidDuration(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	_, err := client.GetSessionToken(ctx, &awssts.GetSessionTokenInput{
		DurationSeconds: aws.Int32(100), // below 900 minimum
	})
	require.Error(t, err)
}

func TestSTS_GetFederationToken(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
		Name: aws.String("federated-user"),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*out.Credentials.AccessKeyId, "ASIA"))
	assert.Contains(t, *out.FederatedUser.Arn, "federated-user/federated-user")
	assert.Contains(t, *out.FederatedUser.FederatedUserId, "000000000000")
}

func TestSTS_GetAccessKeyInfo(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.GetAccessKeyInfo(ctx, &awssts.GetAccessKeyInfoInput{
		AccessKeyId: aws.String("AKIAIOSFODNN7EXAMPLE"),
	})
	require.NoError(t, err)
	assert.Equal(t, "000000000000", *out.Account)
}

func TestSTS_DecodeAuthorizationMessage(t *testing.T) {
	resetState(t)
	client := newSTSClient(t)
	ctx := context.Background()

	out, err := client.DecodeAuthorizationMessage(ctx, &awssts.DecodeAuthorizationMessageInput{
		EncodedMessage: aws.String("some-encoded-message"),
	})
	require.NoError(t, err)
	assert.Equal(t, "some-encoded-message", *out.DecodedMessage)
}
