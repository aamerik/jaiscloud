package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── P4.9: BatchGetSecretValue ────────────────────────────────────────────────

func TestSM_BatchGetSecretValue_ReturnsAllValues(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	names := []string{"batch-secret-a", "batch-secret-b", "batch-secret-c"}
	for _, n := range names {
		_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
			Name:         aws.String(n),
			SecretString: aws.String("val-" + n),
		})
		require.NoError(t, err)
	}

	ids := make([]string, len(names))
	copy(ids, names)

	out, err := c.BatchGetSecretValue(ctx, &awssm.BatchGetSecretValueInput{
		SecretIdList: ids,
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 3, "all 3 secret values should be returned")
	assert.Empty(t, out.Errors, "no errors expected for existing secrets")

	got := map[string]string{}
	for _, sv := range out.SecretValues {
		got[aws.ToString(sv.Name)] = aws.ToString(sv.SecretString)
	}
	for _, n := range names {
		assert.Equal(t, "val-"+n, got[n])
	}
}

func TestSM_BatchGetSecretValue_PartialHitReturnsErrors(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	_, err := c.CreateSecret(ctx, &awssm.CreateSecretInput{
		Name:         aws.String("partial-exists"),
		SecretString: aws.String("exists-value"),
	})
	require.NoError(t, err)

	out, err := c.BatchGetSecretValue(ctx, &awssm.BatchGetSecretValueInput{
		SecretIdList: []string{"partial-exists", "partial-missing"},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 1)
	assert.Len(t, out.Errors, 1, "missing secret should appear in Errors")
	assert.Equal(t, "partial-missing", aws.ToString(out.Errors[0].SecretId))
}

func TestSM_BatchGetSecretValue_OverLimitFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSMClient(t)

	// Build a list of 21 secret IDs (max is 20)
	ids := make([]string, 21)
	for i := range ids {
		ids[i] = fmt.Sprintf("over-limit-secret-%d", i)
	}

	_, err := c.BatchGetSecretValue(ctx, &awssm.BatchGetSecretValueInput{
		SecretIdList: ids,
	})
	require.Error(t, err, "BatchGetSecretValue with >20 IDs must return ValidationException")
}
