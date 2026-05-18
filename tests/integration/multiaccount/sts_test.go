package multiaccount

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCallerIdentity_AccountMatchesKey verifies that for each test account,
// GetCallerIdentity returns an Account field matching the LSIA-encoded key.
func TestGetCallerIdentity_AccountMatchesKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	for _, acct := range []string{AcctA, AcctB, AcctC} {
		t.Run(acct, func(t *testing.T) {
			client := newSTSFor(t, acct)
			out, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
			require.NoError(t, err)
			assert.Equal(t, acct, aws.ToString(out.Account),
				"GetCallerIdentity.Account should match the account encoded in the access key")
			assert.Contains(t, aws.ToString(out.Arn), acct,
				"returned ARN should contain the account ID")
		})
	}
}

// TestGetCallerIdentity_DefaultAccount verifies that the "test" key (non-LSIA)
// falls back to the server's default account.
func TestGetCallerIdentity_DefaultAccount(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	client := sts.NewFromConfig(cfg, func(o *sts.Options) { o.BaseEndpoint = aws.String(endpoint) })

	out, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	// Default account must be a 12-digit string (the server default, typically "000000000000").
	assert.Regexp(t, `^\d{12}$`, aws.ToString(out.Account))
}

// TestAssumeRole_CrossAccount verifies that AssumeRole for a role in account B
// returns credentials whose account resolves to B.
func TestAssumeRole_CrossAccount(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	roleArn := "arn:aws:iam::" + AcctB + ":role/cross-account-role"
	clientA := newSTSFor(t, AcctA)

	out, err := clientA.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("my-session"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Credentials)

	// The returned access key must decode to account B.
	ak := aws.ToString(out.Credentials.AccessKeyId)
	assert.True(t, strings.HasPrefix(ak, "ASIA"),
		"assumed-role credentials should be LSIA-encoded (ASIA… prefix)")
	assert.Contains(t, aws.ToString(out.AssumedRoleUser.Arn), AcctB,
		"AssumedRoleUser.Arn should contain the target account")

	// Use the assumed credentials and verify GetCallerIdentity resolves to B.
	assumedCfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			ak, aws.ToString(out.Credentials.SecretAccessKey), aws.ToString(out.Credentials.SessionToken),
		)),
	)
	require.NoError(t, err)
	assumedSTS := sts.NewFromConfig(assumedCfg, func(o *sts.Options) { o.BaseEndpoint = aws.String(endpoint) })

	gci, err := assumedSTS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	assert.Equal(t, AcctB, aws.ToString(gci.Account),
		"GCI after AssumeRole should return the target account B")
}

// TestGetCallerIdentity_LSIA_AssumedRole verifies that after AssumeRole,
// GCI returns an assumed-role ARN (not a user ARN).
func TestGetCallerIdentity_LSIA_AssumedRole(t *testing.T) {
	resetState(t)
	ctx := context.Background()

	roleArn := "arn:aws:iam::" + AcctA + ":role/my-role"
	clientA := newSTSFor(t, AcctA)

	out, err := clientA.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String("sess1"),
	})
	require.NoError(t, err)

	ak := aws.ToString(out.Credentials.AccessKeyId)
	assumedCfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			ak, aws.ToString(out.Credentials.SecretAccessKey), aws.ToString(out.Credentials.SessionToken),
		)),
	)
	require.NoError(t, err)
	assumedSTS := sts.NewFromConfig(assumedCfg, func(o *sts.Options) { o.BaseEndpoint = aws.String(endpoint) })

	gci, err := assumedSTS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err)
	// ARN must be of assumed-role form, not user form.
	assert.Contains(t, aws.ToString(gci.Arn), "assumed-role",
		"ARN after AssumeRole should be an assumed-role ARN")
	assert.Contains(t, aws.ToString(gci.Arn), "my-role",
		"ARN should contain the role name")
}
