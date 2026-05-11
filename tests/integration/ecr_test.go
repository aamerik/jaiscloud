package integration

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ecrClient(t *testing.T) *awsecr.Client {
	t.Helper()
	cfg := newAWSConfig(t)
	return awsecr.NewFromConfig(cfg, func(o *awsecr.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint())
	})
}

// ─── Repository CRUD ──────────────────────────────────────────────────────────

func TestECR_CreateAndDescribeRepository(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	out, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("my-app"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-app", *out.Repository.RepositoryName)
	assert.Contains(t, *out.Repository.RepositoryArn, "arn:aws:ecr:")
	assert.Contains(t, *out.Repository.RepositoryUri, "my-app")

	desc, err := client.DescribeRepositories(ctx, &awsecr.DescribeRepositoriesInput{
		RepositoryNames: []string{"my-app"},
	})
	require.NoError(t, err)
	require.Len(t, desc.Repositories, 1)
	assert.Equal(t, "my-app", *desc.Repositories[0].RepositoryName)
	assert.Contains(t, *desc.Repositories[0].RepositoryArn, "arn:aws:ecr:")
}

func TestECR_CreateRepository_AlreadyExists(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("dup-repo"),
	})
	require.NoError(t, err)

	_, err = client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("dup-repo"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RepositoryAlreadyExistsException")
}

func TestECR_DeleteRepository_NotEmpty_RequiresForce(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("to-delete"),
	})
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[]}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("to-delete"),
		ImageManifest:  aws.String(manifest),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)

	// Delete without force should fail
	_, err = client.DeleteRepository(ctx, &awsecr.DeleteRepositoryInput{
		RepositoryName: aws.String("to-delete"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RepositoryNotEmptyException")

	// Delete with force should succeed
	_, err = client.DeleteRepository(ctx, &awsecr.DeleteRepositoryInput{
		RepositoryName: aws.String("to-delete"),
		Force:          true,
	})
	require.NoError(t, err)
}

func TestECR_DescribeRepositories_Pagination(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
			RepositoryName: aws.String(strings.ToLower("repo" + string(rune('a'+i)))),
		})
		require.NoError(t, err)
	}

	// Page 1 — maxResults=3
	page1, err := client.DescribeRepositories(ctx, &awsecr.DescribeRepositoriesInput{
		MaxResults: aws.Int32(3),
	})
	require.NoError(t, err)
	require.Len(t, page1.Repositories, 3)
	require.NotNil(t, page1.NextToken)

	// Page 2
	page2, err := client.DescribeRepositories(ctx, &awsecr.DescribeRepositoriesInput{
		MaxResults: aws.Int32(3),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Repositories, 2)
	assert.Nil(t, page2.NextToken)
}

// ─── Image Operations ─────────────────────────────────────────────────────────

func TestECR_PutAndGetImage(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("img-repo"),
	})
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[]}`
	putOut, err := client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("img-repo"),
		ImageManifest:  aws.String(manifest),
		ImageTag:       aws.String("latest"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *putOut.Image.ImageId.ImageDigest)
	assert.Equal(t, "latest", *putOut.Image.ImageId.ImageTag)
	assert.True(t, strings.HasPrefix(*putOut.Image.ImageId.ImageDigest, "sha256:"))

	getOut, err := client.BatchGetImage(ctx, &awsecr.BatchGetImageInput{
		RepositoryName: aws.String("img-repo"),
		ImageIds:       []ecrtypes.ImageIdentifier{{ImageTag: aws.String("latest")}},
	})
	require.NoError(t, err)
	require.Len(t, getOut.Images, 1)
	assert.Equal(t, manifest, *getOut.Images[0].ImageManifest)
}

func TestECR_PutImage_Immutable_RejectsOverwrite(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName:     aws.String("immutable-repo"),
		ImageTagMutability: ecrtypes.ImageTagMutabilityImmutable,
	})
	require.NoError(t, err)

	manifest1 := `{"schemaVersion":2,"tag":"v1"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("immutable-repo"),
		ImageManifest:  aws.String(manifest1),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)

	manifest2 := `{"schemaVersion":2,"tag":"v1-different"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("immutable-repo"),
		ImageManifest:  aws.String(manifest2),
		ImageTag:       aws.String("v1"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ImageTagAlreadyExistsException")
}

func TestECR_BatchDeleteImage(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("del-repo"),
	})
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"id":"test-delete"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("del-repo"),
		ImageManifest:  aws.String(manifest),
		ImageTag:       aws.String("to-delete"),
	})
	require.NoError(t, err)

	delOut, err := client.BatchDeleteImage(ctx, &awsecr.BatchDeleteImageInput{
		RepositoryName: aws.String("del-repo"),
		ImageIds:       []ecrtypes.ImageIdentifier{{ImageTag: aws.String("to-delete")}},
	})
	require.NoError(t, err)
	assert.Len(t, delOut.ImageIds, 1)
	assert.Empty(t, delOut.Failures)
}

func TestECR_ListImages_TagStatusFilter(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("list-repo"),
	})
	require.NoError(t, err)

	manifest1 := `{"schemaVersion":2,"id":"tagged"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("list-repo"),
		ImageManifest:  aws.String(manifest1),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)

	manifest2 := `{"schemaVersion":2,"id":"untagged"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("list-repo"),
		ImageManifest:  aws.String(manifest2),
	})
	require.NoError(t, err)

	taggedOut, err := client.ListImages(ctx, &awsecr.ListImagesInput{
		RepositoryName: aws.String("list-repo"),
		Filter:         &ecrtypes.ListImagesFilter{TagStatus: ecrtypes.TagStatusTagged},
	})
	require.NoError(t, err)
	assert.Len(t, taggedOut.ImageIds, 1)
	assert.Equal(t, "v1", *taggedOut.ImageIds[0].ImageTag)

	anyOut, err := client.ListImages(ctx, &awsecr.ListImagesInput{
		RepositoryName: aws.String("list-repo"),
	})
	require.NoError(t, err)
	assert.Len(t, anyOut.ImageIds, 2)
}

// ─── Auth Token ───────────────────────────────────────────────────────────────

func TestECR_GetAuthorizationToken_Default(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	out, err := client.GetAuthorizationToken(ctx, &awsecr.GetAuthorizationTokenInput{})
	require.NoError(t, err)
	require.Len(t, out.AuthorizationData, 1)

	token := *out.AuthorizationData[0].AuthorizationToken
	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(decoded), "AWS:"), "token should decode to AWS:...")

	// ExpiresAt should be ~12h from now
	assert.NotNil(t, out.AuthorizationData[0].ExpiresAt)
}

// ─── Lifecycle Policy ─────────────────────────────────────────────────────────

func TestECR_LifecyclePolicy_CRUD(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("lc-repo"),
	})
	require.NoError(t, err)

	policyText := `{"rules":[{"rulePriority":1,"selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":5},"action":{"type":"expire"}}]}`
	_, err = client.PutLifecyclePolicy(ctx, &awsecr.PutLifecyclePolicyInput{
		RepositoryName:      aws.String("lc-repo"),
		LifecyclePolicyText: aws.String(policyText),
	})
	require.NoError(t, err)

	getOut, err := client.GetLifecyclePolicy(ctx, &awsecr.GetLifecyclePolicyInput{
		RepositoryName: aws.String("lc-repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, policyText, *getOut.LifecyclePolicyText)

	_, err = client.DeleteLifecyclePolicy(ctx, &awsecr.DeleteLifecyclePolicyInput{
		RepositoryName: aws.String("lc-repo"),
	})
	require.NoError(t, err)

	_, err = client.GetLifecyclePolicy(ctx, &awsecr.GetLifecyclePolicyInput{
		RepositoryName: aws.String("lc-repo"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LifecyclePolicyNotFoundException")
}

// ─── Repository Policy ────────────────────────────────────────────────────────

func TestECR_RepositoryPolicy_CRUD(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("policy-repo"),
	})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	_, err = client.SetRepositoryPolicy(ctx, &awsecr.SetRepositoryPolicyInput{
		RepositoryName: aws.String("policy-repo"),
		PolicyText:     aws.String(policy),
	})
	require.NoError(t, err)

	getOut, err := client.GetRepositoryPolicy(ctx, &awsecr.GetRepositoryPolicyInput{
		RepositoryName: aws.String("policy-repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, policy, *getOut.PolicyText)
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestECR_Tags_AddRemoveList(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	createOut, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("tag-repo"),
	})
	require.NoError(t, err)
	arn := *createOut.Repository.RepositoryArn

	_, err = client.TagResource(ctx, &awsecr.TagResourceInput{
		ResourceArn: aws.String(arn),
		Tags: []ecrtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &awsecr.ListTagsForResourceInput{
		ResourceArn: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	_, err = client.UntagResource(ctx, &awsecr.UntagResourceInput{
		ResourceArn: aws.String(arn),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &awsecr.ListTagsForResourceInput{
		ResourceArn: aws.String(arn),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "team", *listOut2.Tags[0].Key)
}

// ─── Scanning Stubs ───────────────────────────────────────────────────────────

func TestECR_StartImageScan_CompletesImmediately(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("scan-repo"),
	})
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"id":"scan-test"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("scan-repo"),
		ImageManifest:  aws.String(manifest),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)

	scanOut, err := client.StartImageScan(ctx, &awsecr.StartImageScanInput{
		RepositoryName: aws.String("scan-repo"),
		ImageId:        &ecrtypes.ImageIdentifier{ImageTag: aws.String("v1")},
	})
	require.NoError(t, err)
	assert.Equal(t, ecrtypes.ScanStatusComplete, scanOut.ImageScanStatus.Status)
}

func TestECR_DescribeImageScanFindings_EmptyFindings(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String("findings-repo"),
	})
	require.NoError(t, err)

	manifest := `{"schemaVersion":2,"id":"findings-test"}`
	_, err = client.PutImage(ctx, &awsecr.PutImageInput{
		RepositoryName: aws.String("findings-repo"),
		ImageManifest:  aws.String(manifest),
		ImageTag:       aws.String("latest"),
	})
	require.NoError(t, err)

	findingsOut, err := client.DescribeImageScanFindings(ctx, &awsecr.DescribeImageScanFindingsInput{
		RepositoryName: aws.String("findings-repo"),
		ImageId:        &ecrtypes.ImageIdentifier{ImageTag: aws.String("latest")},
	})
	require.NoError(t, err)
	assert.Equal(t, ecrtypes.ScanStatusComplete, findingsOut.ImageScanStatus.Status)
	assert.Empty(t, findingsOut.ImageScanFindings.Findings)
}

// ─── Pull-Through Cache ───────────────────────────────────────────────────────

func TestECR_PullThroughCache_CRUD(t *testing.T) {
	resetState(t)
	client := ecrClient(t)
	ctx := context.Background()

	_, err := client.CreatePullThroughCacheRule(ctx, &awsecr.CreatePullThroughCacheRuleInput{
		EcrRepositoryPrefix: aws.String("docker-hub"),
		UpstreamRegistryUrl: aws.String("public.ecr.aws"),
	})
	require.NoError(t, err)

	listOut, err := client.DescribePullThroughCacheRules(ctx, &awsecr.DescribePullThroughCacheRulesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.PullThroughCacheRules, 1)
	assert.Equal(t, "docker-hub", *listOut.PullThroughCacheRules[0].EcrRepositoryPrefix)

	_, err = client.DeletePullThroughCacheRule(ctx, &awsecr.DeletePullThroughCacheRuleInput{
		EcrRepositoryPrefix: aws.String("docker-hub"),
	})
	require.NoError(t, err)

	listOut2, err := client.DescribePullThroughCacheRules(ctx, &awsecr.DescribePullThroughCacheRulesInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.PullThroughCacheRules)
}
