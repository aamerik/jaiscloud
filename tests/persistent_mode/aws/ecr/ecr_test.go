//go:build ecr_e2e

package ecr_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsecr "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:4566"
}

func ecrClient(t *testing.T) *awsecr.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)
	return awsecr.NewFromConfig(cfg, func(o *awsecr.Options) {
		o.BaseEndpoint = aws.String(jaiscloudHost())
	})
}

func repoName(t *testing.T) string {
	return fmt.Sprintf("e2e-%s-%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), clock.RealNow().UnixNano()%100000)
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestE2E_ECR_PushPullImage(t *testing.T) {
	client := ecrClient(t)
	ctx := context.Background()
	name := repoName(t)

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String(name),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteRepository(ctx, &awsecr.DeleteRepositoryInput{
			RepositoryName: aws.String(name),
			Force:          true,
		})
	}()

	// Use crane to copy a small public image to the local registry.
	// crane must be installed: go install github.com/google/go-containerregistry/cmd/crane@latest
	host := strings.TrimPrefix(jaiscloudHost(), "http://")
	host = strings.TrimPrefix(host, "https://")
	target := fmt.Sprintf("%s/%s:test", host, name)

	pushCmd := exec.CommandContext(ctx, "crane", "copy",
		"--insecure",
		"alpine:3.18",
		target,
	)
	out, err := pushCmd.CombinedOutput()
	require.NoError(t, err, "crane copy failed: %s", out)

	// Verify via AWS API
	listOut, err := client.ListImages(ctx, &awsecr.ListImagesInput{
		RepositoryName: aws.String(name),
	})
	require.NoError(t, err)

	found := false
	for _, id := range listOut.ImageIds {
		if id.ImageTag != nil && *id.ImageTag == "test" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected tag 'test' to appear in ListImages after push")

	// Pull back via crane
	pullCmd := exec.CommandContext(ctx, "crane", "manifest", "--insecure", target)
	pullOut, err := pullCmd.CombinedOutput()
	require.NoError(t, err, "crane manifest failed: %s", pullOut)
	assert.Contains(t, string(pullOut), "schemaVersion")
}

func TestE2E_ECR_GetAuthorizationToken_AllowsDockerLogin(t *testing.T) {
	client := ecrClient(t)
	ctx := context.Background()

	out, err := client.GetAuthorizationToken(ctx, &awsecr.GetAuthorizationTokenInput{})
	require.NoError(t, err)
	require.Len(t, out.AuthorizationData, 1)
	assert.NotEmpty(t, *out.AuthorizationData[0].AuthorizationToken)
	assert.NotNil(t, out.AuthorizationData[0].ExpiresAt)
}

func TestE2E_ECR_DeleteImage_RemovesFromRegistry(t *testing.T) {
	client := ecrClient(t)
	ctx := context.Background()
	name := repoName(t)

	_, err := client.CreateRepository(ctx, &awsecr.CreateRepositoryInput{
		RepositoryName: aws.String(name),
	})
	require.NoError(t, err)
	defer func() {
		_, _ = client.DeleteRepository(ctx, &awsecr.DeleteRepositoryInput{
			RepositoryName: aws.String(name),
			Force:          true,
		})
	}()

	host := strings.TrimPrefix(jaiscloudHost(), "http://")
	target := fmt.Sprintf("%s/%s:v1", host, name)

	pushCmd := exec.CommandContext(ctx, "crane", "copy", "--insecure", "alpine:3.18", target)
	out, err := pushCmd.CombinedOutput()
	require.NoError(t, err, "crane copy failed: %s", out)

	// Get the image IDs via ListImages
	listOut, err := client.ListImages(ctx, &awsecr.ListImagesInput{
		RepositoryName: aws.String(name),
	})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.ImageIds)

	// Delete by tag
	delOut, err := client.BatchDeleteImage(ctx, &awsecr.BatchDeleteImageInput{
		RepositoryName: aws.String(name),
		ImageIds: []ecrtypes.ImageIdentifier{
			{ImageTag: aws.String("v1")},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, delOut.ImageIds)
	assert.Empty(t, delOut.Failures)

	// Verify tag is gone from lite metadata
	listOut2, err := client.ListImages(ctx, &awsecr.ListImagesInput{
		RepositoryName: aws.String(name),
		Filter:         &ecrtypes.ListImagesFilter{TagStatus: ecrtypes.TagStatusTagged},
	})
	require.NoError(t, err)
	for _, id := range listOut2.ImageIds {
		assert.NotEqual(t, "v1", aws.ToString(id.ImageTag))
	}
}
