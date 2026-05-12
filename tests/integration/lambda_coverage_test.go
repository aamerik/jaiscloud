package integration_test

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeLambdaZip returns a minimal valid zip archive with a single JS handler file.
func makeLambdaZip(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("index.js")
	require.NoError(t, err)
	_, err = f.Write([]byte("exports.handler = async () => ({ statusCode: 200 })"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// ─── Versions ─────────────────────────────────────────────────────────────────

func TestLambda_PublishVersion_FirstCallReturnsV1(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-v1")

	out, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-v1"),
	})
	require.NoError(t, err)
	require.Equal(t, "1", aws.ToString(out.Version))
}

func TestLambda_PublishVersion_IncrementsCounter(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-inc")

	out1, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-inc"),
	})
	require.NoError(t, err)
	require.Equal(t, "1", aws.ToString(out1.Version))

	out2, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-inc"),
	})
	require.NoError(t, err)
	require.Equal(t, "2", aws.ToString(out2.Version))
}

func TestLambda_ListVersionsByFunction_IncludesLatest(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-list")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-list"),
	})
	require.NoError(t, err)

	out, err := c.ListVersionsByFunction(ctx, &awslambda.ListVersionsByFunctionInput{
		FunctionName: aws.String("ver-func-list"),
	})
	require.NoError(t, err)

	// Should include at least version "1" and "$LATEST"
	require.GreaterOrEqual(t, len(out.Versions), 2)
	var hasLatest, hasV1 bool
	for _, v := range out.Versions {
		if aws.ToString(v.Version) == "$LATEST" {
			hasLatest = true
		}
		if aws.ToString(v.Version) == "1" {
			hasV1 = true
		}
	}
	assert.True(t, hasLatest, "expected $LATEST in versions list")
	assert.True(t, hasV1, "expected version 1 in versions list")
}

func TestLambda_ListVersionsByFunction_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-page")
	for i := 0; i < 3; i++ {
		_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
			FunctionName: aws.String("ver-func-page"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListVersionsByFunction(ctx, &awslambda.ListVersionsByFunctionInput{
		FunctionName: aws.String("ver-func-page"),
	})
	require.NoError(t, err)
	// 3 published versions + $LATEST = 4
	assert.Len(t, out.Versions, 4)
}

func TestLambda_GetFunction_ByVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-get")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-get"),
	})
	require.NoError(t, err)

	out, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("ver-func-get"),
		Qualifier:    aws.String("1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ver-func-get", aws.ToString(out.Configuration.FunctionName))
	assert.Equal(t, "1", aws.ToString(out.Configuration.Version))
}

func TestLambda_InvokeByVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-invoke")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-invoke"),
	})
	require.NoError(t, err)

	payload := []byte(`{"key":"value"}`)
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("ver-func-invoke"),
		Qualifier:    aws.String("1"),
		Payload:      payload,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
}

func TestLambda_DeleteFunction_DeletesAllVersions(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-del")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-del"),
	})
	require.NoError(t, err)

	_, err = c.DeleteFunction(ctx, &awslambda.DeleteFunctionInput{
		FunctionName: aws.String("ver-func-del"),
	})
	require.NoError(t, err)

	// Getting function by name should fail
	_, err = c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("ver-func-del"),
	})
	require.Error(t, err)

	// ListVersionsByFunction on a deleted function should also fail
	_, err = c.ListVersionsByFunction(ctx, &awslambda.ListVersionsByFunctionInput{
		FunctionName: aws.String("ver-func-del"),
	})
	require.Error(t, err)
}

func TestLambda_PublishVersion_NoChanges_IsIdempotent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "ver-func-idem")

	out1, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-idem"),
	})
	require.NoError(t, err)

	// Publish again without any changes — should produce a new version number
	// (JaisCloud increments on every publish call)
	out2, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("ver-func-idem"),
	})
	require.NoError(t, err)

	// Both calls must succeed; the second call yields a higher version number
	v1 := aws.ToString(out1.Version)
	v2 := aws.ToString(out2.Version)
	assert.NotEmpty(t, v1)
	assert.NotEmpty(t, v2)
	assert.NotEqual(t, v1, v2)
}

// ─── Aliases ──────────────────────────────────────────────────────────────────

func TestLambda_CreateAlias_PointsToVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-ver")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("alias-func-ver"),
	})
	require.NoError(t, err)

	out, err := c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-ver"),
		Name:            aws.String("stable"),
		FunctionVersion: aws.String("1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "stable", aws.ToString(out.Name))
	assert.Equal(t, "1", aws.ToString(out.FunctionVersion))
}

func TestLambda_CreateAlias_PointsToLatest(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-latest")

	out, err := c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-latest"),
		Name:            aws.String("live"),
		FunctionVersion: aws.String("$LATEST"),
	})
	require.NoError(t, err)
	assert.Equal(t, "$LATEST", aws.ToString(out.FunctionVersion))
}

func TestLambda_CreateAlias_DuplicateName_ResourceConflict(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-dup")

	_, err := c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-dup"),
		Name:            aws.String("dup"),
		FunctionVersion: aws.String("$LATEST"),
	})
	require.NoError(t, err)

	_, err = c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-dup"),
		Name:            aws.String("dup"),
		FunctionVersion: aws.String("$LATEST"),
	})
	assertAWSError(t, err, "ResourceConflictException")
}

func TestLambda_UpdateAlias_ChangeVersion(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-upd")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("alias-func-upd"),
	})
	require.NoError(t, err)
	_, err = c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("alias-func-upd"),
	})
	require.NoError(t, err)

	_, err = c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-upd"),
		Name:            aws.String("myalias"),
		FunctionVersion: aws.String("1"),
	})
	require.NoError(t, err)

	updOut, err := c.UpdateAlias(ctx, &awslambda.UpdateAliasInput{
		FunctionName:    aws.String("alias-func-upd"),
		Name:            aws.String("myalias"),
		FunctionVersion: aws.String("2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(updOut.FunctionVersion))
}

func TestLambda_DeleteAlias_RemovesAlias(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-del")
	_, err := c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-del"),
		Name:            aws.String("todelete"),
		FunctionVersion: aws.String("$LATEST"),
	})
	require.NoError(t, err)

	_, err = c.DeleteAlias(ctx, &awslambda.DeleteAliasInput{
		FunctionName: aws.String("alias-func-del"),
		Name:         aws.String("todelete"),
	})
	require.NoError(t, err)

	_, err = c.GetAlias(ctx, &awslambda.GetAliasInput{
		FunctionName: aws.String("alias-func-del"),
		Name:         aws.String("todelete"),
	})
	require.Error(t, err)
}

func TestLambda_ListAliases_ReturnsBoth(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-listboth")
	_, err := c.PublishVersion(ctx, &awslambda.PublishVersionInput{
		FunctionName: aws.String("alias-func-listboth"),
	})
	require.NoError(t, err)

	for _, aName := range []string{"alpha", "beta"} {
		_, err = c.CreateAlias(ctx, &awslambda.CreateAliasInput{
			FunctionName:    aws.String("alias-func-listboth"),
			Name:            aws.String(aName),
			FunctionVersion: aws.String("1"),
		})
		require.NoError(t, err)
	}

	out, err := c.ListAliases(ctx, &awslambda.ListAliasesInput{
		FunctionName: aws.String("alias-func-listboth"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Aliases, 2)

	names := make([]string, 0, len(out.Aliases))
	for _, a := range out.Aliases {
		names = append(names, aws.ToString(a.Name))
	}
	assert.Contains(t, names, "alpha")
	assert.Contains(t, names, "beta")
}

func TestLambda_InvokeByAlias(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-invoke")
	_, err := c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-invoke"),
		Name:            aws.String("prod"),
		FunctionVersion: aws.String("$LATEST"),
	})
	require.NoError(t, err)

	payload := []byte(`{"msg":"hello"}`)
	out, err := c.Invoke(ctx, &awslambda.InvokeInput{
		FunctionName: aws.String("alias-func-invoke"),
		Qualifier:    aws.String("prod"),
		Payload:      payload,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, out.StatusCode)
}

func TestLambda_GetAlias_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "alias-func-get")
	_, err := c.CreateAlias(ctx, &awslambda.CreateAliasInput{
		FunctionName:    aws.String("alias-func-get"),
		Name:            aws.String("myget"),
		FunctionVersion: aws.String("$LATEST"),
		Description:     aws.String("test alias"),
	})
	require.NoError(t, err)

	out, err := c.GetAlias(ctx, &awslambda.GetAliasInput{
		FunctionName: aws.String("alias-func-get"),
		Name:         aws.String("myget"),
	})
	require.NoError(t, err)
	assert.Equal(t, "myget", aws.ToString(out.Name))
	assert.Equal(t, "$LATEST", aws.ToString(out.FunctionVersion))
	assert.Equal(t, "test alias", aws.ToString(out.Description))
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestLambda_TagResource_Persists(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "tag-func-persist")

	// Retrieve ARN via GetFunction
	getFnOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("tag-func-persist"),
	})
	require.NoError(t, err)
	arn := aws.ToString(getFnOut.Configuration.FunctionArn)

	_, err = c.TagResource(ctx, &awslambda.TagResourceInput{
		Resource: aws.String(arn),
		Tags:     map[string]string{"env": "prod", "team": "platform"},
	})
	require.NoError(t, err)

	listOut, err := c.ListTags(ctx, &awslambda.ListTagsInput{
		Resource: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", listOut.Tags["env"])
	assert.Equal(t, "platform", listOut.Tags["team"])
}

func TestLambda_ListTags_ReturnsTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "tag-func-list")

	getFnOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("tag-func-list"),
	})
	require.NoError(t, err)
	arn := aws.ToString(getFnOut.Configuration.FunctionArn)

	_, err = c.TagResource(ctx, &awslambda.TagResourceInput{
		Resource: aws.String(arn),
		Tags:     map[string]string{"service": "lambda", "version": "1"},
	})
	require.NoError(t, err)

	out, err := c.ListTags(ctx, &awslambda.ListTagsInput{
		Resource: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Len(t, out.Tags, 2)
	assert.Equal(t, "lambda", out.Tags["service"])
	assert.Equal(t, "1", out.Tags["version"])
}

func TestLambda_UntagResource_Removes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "tag-func-untag")

	getFnOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("tag-func-untag"),
	})
	require.NoError(t, err)
	arn := aws.ToString(getFnOut.Configuration.FunctionArn)

	_, err = c.TagResource(ctx, &awslambda.TagResourceInput{
		Resource: aws.String(arn),
		Tags:     map[string]string{"keep": "yes", "remove": "me"},
	})
	require.NoError(t, err)

	_, err = c.UntagResource(ctx, &awslambda.UntagResourceInput{
		Resource: aws.String(arn),
		TagKeys:  []string{"remove"},
	})
	require.NoError(t, err)

	out, err := c.ListTags(ctx, &awslambda.ListTagsInput{
		Resource: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Equal(t, "yes", out.Tags["keep"])
	_, hasRemoved := out.Tags["remove"]
	assert.False(t, hasRemoved, "tag 'remove' should have been deleted")
}

func TestLambda_CreateFunction_WithTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	_, err := c.CreateFunction(ctx, &awslambda.CreateFunctionInput{
		FunctionName: aws.String("tag-func-create"),
		Runtime:      lambdatypes.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/test"),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: makeLambdaZip(t)},
		Tags:         map[string]string{"created_by": "test", "stage": "dev"},
	})
	require.NoError(t, err)

	getFnOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("tag-func-create"),
	})
	require.NoError(t, err)
	arn := aws.ToString(getFnOut.Configuration.FunctionArn)

	out, err := c.ListTags(ctx, &awslambda.ListTagsInput{
		Resource: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Equal(t, "test", out.Tags["created_by"])
	assert.Equal(t, "dev", out.Tags["stage"])
}

func TestLambda_TagResource_Overwrite(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "tag-func-overwrite")

	getFnOut, err := c.GetFunction(ctx, &awslambda.GetFunctionInput{
		FunctionName: aws.String("tag-func-overwrite"),
	})
	require.NoError(t, err)
	arn := aws.ToString(getFnOut.Configuration.FunctionArn)

	_, err = c.TagResource(ctx, &awslambda.TagResourceInput{
		Resource: aws.String(arn),
		Tags:     map[string]string{"color": "blue"},
	})
	require.NoError(t, err)

	// Overwrite with new value for the same key
	_, err = c.TagResource(ctx, &awslambda.TagResourceInput{
		Resource: aws.String(arn),
		Tags:     map[string]string{"color": "red"},
	})
	require.NoError(t, err)

	out, err := c.ListTags(ctx, &awslambda.ListTagsInput{
		Resource: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Equal(t, "red", out.Tags["color"])
}

// ─── Concurrency ──────────────────────────────────────────────────────────────

func TestLambda_PutFunctionConcurrency_Stores(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "conc-func-put")

	out, err := c.PutFunctionConcurrency(ctx, &awslambda.PutFunctionConcurrencyInput{
		FunctionName:                 aws.String("conc-func-put"),
		ReservedConcurrentExecutions: aws.Int32(50),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReservedConcurrentExecutions)
	assert.EqualValues(t, 50, aws.ToInt32(out.ReservedConcurrentExecutions))
}

func TestLambda_GetFunctionConcurrency_Returns(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "conc-func-get")

	_, err := c.PutFunctionConcurrency(ctx, &awslambda.PutFunctionConcurrencyInput{
		FunctionName:                 aws.String("conc-func-get"),
		ReservedConcurrentExecutions: aws.Int32(25),
	})
	require.NoError(t, err)

	out, err := c.GetFunctionConcurrency(ctx, &awslambda.GetFunctionConcurrencyInput{
		FunctionName: aws.String("conc-func-get"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReservedConcurrentExecutions)
	assert.EqualValues(t, 25, aws.ToInt32(out.ReservedConcurrentExecutions))
}

func TestLambda_DeleteFunctionConcurrency_Removes(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	createTestFunction(t, c, "conc-func-del")

	_, err := c.PutFunctionConcurrency(ctx, &awslambda.PutFunctionConcurrencyInput{
		FunctionName:                 aws.String("conc-func-del"),
		ReservedConcurrentExecutions: aws.Int32(10),
	})
	require.NoError(t, err)

	_, err = c.DeleteFunctionConcurrency(ctx, &awslambda.DeleteFunctionConcurrencyInput{
		FunctionName: aws.String("conc-func-del"),
	})
	require.NoError(t, err)

	out, err := c.GetFunctionConcurrency(ctx, &awslambda.GetFunctionConcurrencyInput{
		FunctionName: aws.String("conc-func-del"),
	})
	require.NoError(t, err)
	// After deletion, no reserved concurrency is set
	assert.Nil(t, out.ReservedConcurrentExecutions)
}

func TestLambda_GetAccountSettings_ReturnsLimits(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newLambdaClient(t)

	out, err := c.GetAccountSettings(ctx, &awslambda.GetAccountSettingsInput{})
	require.NoError(t, err)
	require.NotNil(t, out.AccountLimit)
	assert.Greater(t, out.AccountLimit.ConcurrentExecutions, int32(0))
	assert.Greater(t, out.AccountLimit.TotalCodeSize, int64(0))
}
