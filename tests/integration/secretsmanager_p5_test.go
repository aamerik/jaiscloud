package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P5.8: SecretsManager ValidateResourcePolicy ──────────────────────────────

func TestSM_ValidateResourcePolicy_ValidPolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	policy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS": "arn:aws:iam::000000000000:root"},
			"Action": "secretsmanager:GetSecretValue",
			"Resource": "*"
		}]
	}`

	out, err := c.ValidateResourcePolicy(ctx, &awssm.ValidateResourcePolicyInput{
		ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)
	assert.True(t, out.PolicyValidationPassed)
	assert.Empty(t, out.ValidationErrors)
}

func TestSM_ValidateResourcePolicy_InvalidJSON(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	out, err := c.ValidateResourcePolicy(ctx, &awssm.ValidateResourcePolicyInput{
		ResourcePolicy: aws.String(`{not valid json`),
	})
	require.NoError(t, err, "invalid JSON should return a response, not an error")
	assert.False(t, out.PolicyValidationPassed)
	assert.NotEmpty(t, out.ValidationErrors)
}

func TestSM_ValidateResourcePolicy_MissingStatement(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	policy := `{"Version": "2012-10-17"}`

	out, err := c.ValidateResourcePolicy(ctx, &awssm.ValidateResourcePolicyInput{
		ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)
	assert.False(t, out.PolicyValidationPassed)
	assert.NotEmpty(t, out.ValidationErrors)
}

func TestSM_ValidateResourcePolicy_InvalidVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	policy := `{"Version": "2099-01-01", "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}`

	out, err := c.ValidateResourcePolicy(ctx, &awssm.ValidateResourcePolicyInput{
		ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)
	assert.False(t, out.PolicyValidationPassed)
}
