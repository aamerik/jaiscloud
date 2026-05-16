//go:build ssm_fullmode

package ssm_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSSMLabelPersistsInPostgres verifies that parameter labels survive across
// PostgreSQL persistence — i.e. that label → version mapping is correctly
// stored and retrieved via the full-mode store.
func TestSSMLabelPersistsInPostgres(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSSMClient(t)

	// PutParameter v1
	_, err := client.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/test/labeled-param"),
		Value: aws.String("v1-value"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err, "PutParameter v1")

	// PutParameter v2 (Overwrite=true)
	_, err = client.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:      aws.String("/test/labeled-param"),
		Value:     aws.String("v2-value"),
		Type:      ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err, "PutParameter v2")

	// LabelParameterVersion — label v2 as "prod"
	labelOut, err := client.LabelParameterVersion(ctx, &awsssm.LabelParameterVersionInput{
		Name:             aws.String("/test/labeled-param"),
		ParameterVersion: aws.Int64(2),
		Labels:           []string{"prod"},
	})
	require.NoError(t, err, "LabelParameterVersion")
	assert.Empty(t, labelOut.InvalidLabels, "no invalid labels expected")

	// GetParameter with label selector — should return v2
	out, err := client.GetParameter(ctx, &awsssm.GetParameterInput{
		Name: aws.String("/test/labeled-param:prod"),
	})
	require.NoError(t, err, "GetParameter by label")
	assert.Equal(t, "v2-value", aws.ToString(out.Parameter.Value))
	assert.Equal(t, int64(2), aws.ToInt64(out.Parameter.Version))
}

// TestSSMLabel_MultipleVersions verifies that labels on different versions
// resolve to the correct values independently.
func TestSSMLabel_MultipleVersions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSSMClient(t)

	// Create 3 versions
	versions := []string{"alpha-value", "beta-value", "gamma-value"}
	for i, val := range versions {
		input := &awsssm.PutParameterInput{
			Name:  aws.String("/test/multi-label"),
			Value: aws.String(val),
			Type:  ssmtypes.ParameterTypeString,
		}
		if i > 0 {
			input.Overwrite = aws.Bool(true)
		}
		_, err := client.PutParameter(ctx, input)
		require.NoError(t, err, "PutParameter v%d", i+1)
	}

	// Label v1 as "alpha", v2 as "beta"
	_, err := client.LabelParameterVersion(ctx, &awsssm.LabelParameterVersionInput{
		Name:             aws.String("/test/multi-label"),
		ParameterVersion: aws.Int64(1),
		Labels:           []string{"alpha"},
	})
	require.NoError(t, err, "LabelParameterVersion v1")

	_, err = client.LabelParameterVersion(ctx, &awsssm.LabelParameterVersionInput{
		Name:             aws.String("/test/multi-label"),
		ParameterVersion: aws.Int64(2),
		Labels:           []string{"beta"},
	})
	require.NoError(t, err, "LabelParameterVersion v2")

	// Resolve by label
	alphaOut, err := client.GetParameter(ctx, &awsssm.GetParameterInput{
		Name: aws.String("/test/multi-label:alpha"),
	})
	require.NoError(t, err, "GetParameter :alpha")
	assert.Equal(t, "alpha-value", aws.ToString(alphaOut.Parameter.Value))
	assert.Equal(t, int64(1), aws.ToInt64(alphaOut.Parameter.Version))

	betaOut, err := client.GetParameter(ctx, &awsssm.GetParameterInput{
		Name: aws.String("/test/multi-label:beta"),
	})
	require.NoError(t, err, "GetParameter :beta")
	assert.Equal(t, "beta-value", aws.ToString(betaOut.Parameter.Value))
	assert.Equal(t, int64(2), aws.ToInt64(betaOut.Parameter.Version))
}

// TestSSMLabel_OverwriteLabel verifies that re-labeling a label to a different
// version correctly moves the label.
func TestSSMLabel_OverwriteLabel(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newSSMClient(t)

	// Create 2 versions
	_, err := client.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/test/overwrite-label"),
		Value: aws.String("first"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)
	_, err = client.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:      aws.String("/test/overwrite-label"),
		Value:     aws.String("second"),
		Type:      ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	// Label v1 as "live"
	_, err = client.LabelParameterVersion(ctx, &awsssm.LabelParameterVersionInput{
		Name:             aws.String("/test/overwrite-label"),
		ParameterVersion: aws.Int64(1),
		Labels:           []string{"live"},
	})
	require.NoError(t, err)

	// Move "live" label to v2
	_, err = client.LabelParameterVersion(ctx, &awsssm.LabelParameterVersionInput{
		Name:             aws.String("/test/overwrite-label"),
		ParameterVersion: aws.Int64(2),
		Labels:           []string{"live"},
	})
	require.NoError(t, err)

	// "live" should now resolve to v2
	out, err := client.GetParameter(ctx, &awsssm.GetParameterInput{
		Name: aws.String("/test/overwrite-label:live"),
	})
	require.NoError(t, err)
	assert.Equal(t, "second", aws.ToString(out.Parameter.Value))
	assert.Equal(t, int64(2), aws.ToInt64(out.Parameter.Version))
}
