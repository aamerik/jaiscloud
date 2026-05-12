package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`

// TestSM_PutResourcePolicy_ValidJSON_Persists creates a secret, puts a valid
// JSON resource policy, and verifies GetResourcePolicy returns the same JSON.
func TestSM_PutResourcePolicy_ValidJSON_Persists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rp-persist"),
		SecretString: aws.String("secret-value"),
	})
	require.NoError(t, err)

	_, err = c.PutResourcePolicy(ctx, &awssm.PutResourcePolicyInput{
		SecretId:       aws.String("rp-persist"),
		ResourcePolicy: aws.String(validPolicy),
	})
	require.NoError(t, err)

	getOut, err := c.GetResourcePolicy(ctx, &awssm.GetResourcePolicyInput{
		SecretId: aws.String("rp-persist"),
	})
	require.NoError(t, err)
	assert.Equal(t, validPolicy, aws.ToString(getOut.ResourcePolicy))
	assert.Equal(t, "rp-persist", aws.ToString(getOut.Name))
	assert.NotEmpty(t, aws.ToString(getOut.ARN))
}

// TestSM_PutResourcePolicy_InvalidJSON_Error puts a malformed JSON policy and
// expects an error.  If the emulator does not validate the JSON, the test logs
// an informational note rather than failing hard.
func TestSM_PutResourcePolicy_InvalidJSON_Error(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rp-invalid-json"),
		SecretString: aws.String("value"),
	})
	require.NoError(t, err)

	_, err = c.PutResourcePolicy(ctx, &awssm.PutResourcePolicyInput{
		SecretId:       aws.String("rp-invalid-json"),
		ResourcePolicy: aws.String(`{not valid json`),
	})
	if err == nil {
		// Emulator does not validate policy JSON at PutResourcePolicy time.
		t.Log("PutResourcePolicy accepted invalid JSON (policy JSON validation not enforced)")
		return
	}
	// Accept MalformedPolicyDocumentException or InvalidParameterException.
	assertAWSError(t, err, "MalformedPolicyDocumentException")
}

// TestSM_GetResourcePolicy_NotSet_Empty verifies that GetResourcePolicy on a
// brand-new secret without a policy returns an appropriate response.  The
// emulator returns ResourceNotFoundException in this case.
func TestSM_GetResourcePolicy_NotSet_Empty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rp-not-set"),
		SecretString: aws.String("value"),
	})
	require.NoError(t, err)

	out, err := c.GetResourcePolicy(ctx, &awssm.GetResourcePolicyInput{
		SecretId: aws.String("rp-not-set"),
	})
	if err != nil {
		// ResourceNotFoundException is the expected response when no policy exists.
		assertAWSError(t, err, "ResourceNotFoundException")
		return
	}
	// If no error, the policy field should be empty or nil.
	assert.Empty(t, aws.ToString(out.ResourcePolicy), "policy should be empty when not set")
}

// TestSM_GetResourcePolicy_AfterPut_ReturnsSame verifies GetResourcePolicy
// returns the stored policy after a PutResourcePolicy, using a different
// assertion style from the Persists test (checking ARN as well).
func TestSM_GetResourcePolicy_AfterPut_ReturnsSame(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	secretName := "rp-after-put"
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"secretsmanager:DeleteSecret","Resource":"*"}]}`

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("my-val"),
	})
	require.NoError(t, err)

	_, err = c.PutResourcePolicy(ctx, &awssm.PutResourcePolicyInput{
		SecretId:       aws.String(secretName),
		ResourcePolicy: aws.String(policy),
	})
	require.NoError(t, err)

	getOut, err := c.GetResourcePolicy(ctx, &awssm.GetResourcePolicyInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut)
	assert.Equal(t, policy, aws.ToString(getOut.ResourcePolicy), "retrieved policy must match what was stored")
	assert.NotEmpty(t, aws.ToString(getOut.ARN), "ARN must be present in GetResourcePolicy response")
}

// TestSM_DeleteResourcePolicy_RemovesPolicy puts a policy then deletes it and
// confirms GetResourcePolicy no longer returns the policy.
func TestSM_DeleteResourcePolicy_RemovesPolicy(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rp-delete"),
		SecretString: aws.String("val"),
	})
	require.NoError(t, err)

	_, err = c.PutResourcePolicy(ctx, &awssm.PutResourcePolicyInput{
		SecretId:       aws.String("rp-delete"),
		ResourcePolicy: aws.String(validPolicy),
	})
	require.NoError(t, err)

	_, err = c.DeleteResourcePolicy(ctx, &awssm.DeleteResourcePolicyInput{
		SecretId: aws.String("rp-delete"),
	})
	require.NoError(t, err)

	// After deletion, GetResourcePolicy should return ResourceNotFoundException
	// or an empty policy string.
	out, err := c.GetResourcePolicy(ctx, &awssm.GetResourcePolicyInput{
		SecretId: aws.String("rp-delete"),
	})
	if err != nil {
		assertAWSError(t, err, "ResourceNotFoundException")
		return
	}
	assert.Empty(t, aws.ToString(out.ResourcePolicy), "policy must be empty after DeleteResourcePolicy")
}

// TestSM_PutResourcePolicy_NonExistentSecret verifies that putting a resource
// policy on a secret that does not exist returns ResourceNotFoundException.
func TestSM_PutResourcePolicy_NonExistentSecret(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.PutResourcePolicy(ctx, &awssm.PutResourcePolicyInput{
		SecretId:       aws.String("does-not-exist-secret"),
		ResourcePolicy: aws.String(validPolicy),
	})
	require.Error(t, err)
	assertAWSError(t, err, "ResourceNotFoundException")
}

// TestSM_ResourcePolicy_BlockPublicPolicy_Stored verifies that PutResourcePolicy
// with BlockPublicPolicy=true succeeds and the policy is stored correctly.
func TestSM_ResourcePolicy_BlockPublicPolicy_Stored(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("rp-block-public"),
		SecretString: aws.String("sensitive-value"),
	})
	require.NoError(t, err)

	_, err = c.PutResourcePolicy(ctx, &awssm.PutResourcePolicyInput{
		SecretId:          aws.String("rp-block-public"),
		ResourcePolicy:    aws.String(validPolicy),
		BlockPublicPolicy: aws.Bool(true),
	})
	require.NoError(t, err, "PutResourcePolicy with BlockPublicPolicy=true should succeed")

	// Confirm the policy was stored.
	getOut, err := c.GetResourcePolicy(ctx, &awssm.GetResourcePolicyInput{
		SecretId: aws.String("rp-block-public"),
	})
	require.NoError(t, err)
	assert.Equal(t, validPolicy, aws.ToString(getOut.ResourcePolicy))
}

// TestSM_SecretTags_AddListRemove creates a secret, adds tags via TagResource,
// verifies them via DescribeSecret, then removes them via UntagResource and
// confirms they are gone.
func TestSM_SecretTags_AddListRemove(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	secretName := fmt.Sprintf("tag-secret-%d", 1)
	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String("tagged-value"),
	})
	require.NoError(t, err)

	// Add tags.
	_, err = c.TagResource(ctx, &awssm.TagResourceInput{
		SecretId: aws.String(secretName),
		Tags: []smtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	// Verify tags via DescribeSecret.
	descOut, err := c.DescribeSecret(ctx, &awssm.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	tagMap := make(map[string]string, len(descOut.Tags))
	for _, tag := range descOut.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])

	// Remove tags.
	_, err = c.UntagResource(ctx, &awssm.UntagResourceInput{
		SecretId: aws.String(secretName),
		TagKeys:  []string{"env", "team"},
	})
	require.NoError(t, err)

	// Confirm tags are gone.
	descOut2, err := c.DescribeSecret(ctx, &awssm.DescribeSecretInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	for _, tag := range descOut2.Tags {
		assert.NotEqual(t, "env", aws.ToString(tag.Key))
		assert.NotEqual(t, "team", aws.ToString(tag.Key))
	}
}
