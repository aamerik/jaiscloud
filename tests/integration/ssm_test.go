package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsssm "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSM_PutGetParameter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/app/db-url"),
		Type:  types.ParameterTypeString,
		Value: aws.String("postgres://localhost:5432/app"),
	})
	require.NoError(t, err)

	getOut, err := c.GetParameter(ctx, &awsssm.GetParameterInput{
		Name: aws.String("/app/db-url"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Parameter)
	assert.Equal(t, "postgres://localhost:5432/app", aws.ToString(getOut.Parameter.Value))
	assert.Equal(t, types.ParameterTypeString, getOut.Parameter.Type)
	assert.Equal(t, int64(1), getOut.Parameter.Version)
}

func TestSSM_PutParameterOverwrite(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/app/setting"),
		Type:  types.ParameterTypeString,
		Value: aws.String("v1"),
	})
	require.NoError(t, err)

	// No overwrite should fail.
	_, err = c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/app/setting"),
		Type:  types.ParameterTypeString,
		Value: aws.String("v2"),
	})
	require.Error(t, err, "should fail without Overwrite")

	// With overwrite should succeed.
	_, err = c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:      aws.String("/app/setting"),
		Type:      types.ParameterTypeString,
		Value:     aws.String("v2"),
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	getOut, err := c.GetParameter(ctx, &awsssm.GetParameterInput{Name: aws.String("/app/setting")})
	require.NoError(t, err)
	assert.Equal(t, "v2", aws.ToString(getOut.Parameter.Value))
	assert.Equal(t, int64(2), getOut.Parameter.Version)
}

func TestSSM_GetParameters_MultipleNames(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	for _, n := range []string{"/a", "/b", "/c"} {
		_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:  aws.String(n),
			Type:  types.ParameterTypeString,
			Value: aws.String("val"),
		})
		require.NoError(t, err)
	}

	getOut, err := c.GetParameters(ctx, &awsssm.GetParametersInput{
		Names: []string{"/a", "/b", "/nosuch"},
	})
	require.NoError(t, err)
	assert.Len(t, getOut.Parameters, 2)
	assert.Equal(t, []string{"/nosuch"}, getOut.InvalidParameters)
}

func TestSSM_GetParametersByPath_Recursive(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	for _, n := range []string{"/app/db", "/app/api/key", "/app/api/secret", "/other/x"} {
		_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:  aws.String(n),
			Type:  types.ParameterTypeString,
			Value: aws.String("v"),
		})
		require.NoError(t, err)
	}

	pathOut, err := c.GetParametersByPath(ctx, &awsssm.GetParametersByPathInput{
		Path:      aws.String("/app/"),
		Recursive: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, pathOut.Parameters, 3)
}

func TestSSM_GetParametersByPath_NonRecursive(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	for _, n := range []string{"/app/db", "/app/api/key"} {
		_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:  aws.String(n),
			Type:  types.ParameterTypeString,
			Value: aws.String("v"),
		})
		require.NoError(t, err)
	}

	pathOut, err := c.GetParametersByPath(ctx, &awsssm.GetParametersByPathInput{
		Path:      aws.String("/app/"),
		Recursive: aws.Bool(false),
	})
	require.NoError(t, err)
	// Only /app/db is a direct child of /app/.
	assert.Len(t, pathOut.Parameters, 1)
}

func TestSSM_DeleteParameter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/del-me"),
		Type:  types.ParameterTypeString,
		Value: aws.String("x"),
	})
	require.NoError(t, err)

	_, err = c.DeleteParameter(ctx, &awsssm.DeleteParameterInput{Name: aws.String("/del-me")})
	require.NoError(t, err)

	_, err = c.GetParameter(ctx, &awsssm.GetParameterInput{Name: aws.String("/del-me")})
	require.Error(t, err, "deleted parameter should not be found")
}

func TestSSM_DeleteParameters_Batch(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	for _, n := range []string{"/x", "/y"} {
		_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:  aws.String(n),
			Type:  types.ParameterTypeString,
			Value: aws.String("v"),
		})
		require.NoError(t, err)
	}

	delOut, err := c.DeleteParameters(ctx, &awsssm.DeleteParametersInput{
		Names: []string{"/x", "/y", "/nosuch"},
	})
	require.NoError(t, err)
	assert.Len(t, delOut.DeletedParameters, 2)
	assert.Equal(t, []string{"/nosuch"}, delOut.InvalidParameters)
}

func TestSSM_GetParameterHistory(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/hist"),
		Type:  types.ParameterTypeString,
		Value: aws.String("v1"),
	})
	require.NoError(t, err)
	for _, v := range []string{"v2", "v3"} {
		_, err = c.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:      aws.String("/hist"),
			Type:      types.ParameterTypeString,
			Value:     aws.String(v),
			Overwrite: aws.Bool(true),
		})
		require.NoError(t, err)
	}

	histOut, err := c.GetParameterHistory(ctx, &awsssm.GetParameterHistoryInput{
		Name: aws.String("/hist"),
	})
	require.NoError(t, err)
	// History contains archived versions (all but current = 2 entries).
	assert.Len(t, histOut.Parameters, 2)
}

func TestSSM_DescribeParameters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	for _, n := range []string{"/p1", "/p2"} {
		_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
			Name:  aws.String(n),
			Type:  types.ParameterTypeString,
			Value: aws.String("v"),
		})
		require.NoError(t, err)
	}

	descOut, err := c.DescribeParameters(ctx, &awsssm.DescribeParametersInput{})
	require.NoError(t, err)
	assert.Len(t, descOut.Parameters, 2)
}

func TestSSM_StringListParameter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/list-param"),
		Type:  types.ParameterTypeStringList,
		Value: aws.String("a,b,c"),
	})
	require.NoError(t, err)

	getOut, err := c.GetParameter(ctx, &awsssm.GetParameterInput{Name: aws.String("/list-param")})
	require.NoError(t, err)
	assert.Equal(t, "a,b,c", aws.ToString(getOut.Parameter.Value))
	assert.Equal(t, types.ParameterTypeStringList, getOut.Parameter.Type)
}

// TestSSMLabelInvalid verifies that labels starting with "aws" or "ssm" are
// returned in InvalidLabels — both lite and full mode (fix 1.1.7).
func TestSSMLabelInvalid(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newSSMClient(t)

	// Put a parameter to label.
	_, err := c.PutParameter(ctx, &awsssm.PutParameterInput{
		Name:  aws.String("/label-test"),
		Value: aws.String("v1"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	// Label with one valid label and two reserved prefixes.
	out, err := c.LabelParameterVersion(ctx, &awsssm.LabelParameterVersionInput{
		Name:          aws.String("/label-test"),
		ParameterVersion: aws.Int64(1),
		Labels:        []string{"prod", "aws-internal", "ssm-reserved"},
	})
	require.NoError(t, err)
	assert.Contains(t, out.InvalidLabels, "aws-internal", "labels starting with aws must be invalid")
	assert.Contains(t, out.InvalidLabels, "ssm-reserved", "labels starting with ssm must be invalid")
	assert.NotContains(t, out.InvalidLabels, "prod", "valid label must not be in InvalidLabels")
}
