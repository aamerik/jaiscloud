// Package integration provides CloudFront round-trip integration tests.
// NOTE: CloudFront is not yet implemented in JaisCloud; these tests are skipped
// until the provider is added.
package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscloudfront "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	awscftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMinimalDistributionConfig(callerRef string) *awscftypes.DistributionConfig {
	return &awscftypes.DistributionConfig{
		CallerReference: aws.String(callerRef),
		Comment:         aws.String("test distribution"),
		Enabled:         aws.Bool(true),
		DefaultCacheBehavior: &awscftypes.DefaultCacheBehavior{
			ViewerProtocolPolicy: awscftypes.ViewerProtocolPolicyAllowAll,
			TargetOriginId:       aws.String("origin-1"),
			ForwardedValues: &awscftypes.ForwardedValues{
				QueryString: aws.Bool(false),
				Cookies:     &awscftypes.CookiePreference{Forward: awscftypes.ItemSelectionNone},
			},
			MinTTL: aws.Int64(0),
		},
		Origins: &awscftypes.Origins{
			Quantity: aws.Int32(1),
			Items: []awscftypes.Origin{{
				Id:         aws.String("origin-1"),
				DomainName: aws.String("example.com"),
				S3OriginConfig: &awscftypes.S3OriginConfig{
					OriginAccessIdentity: aws.String(""),
				},
			}},
		},
	}
}

func TestCloudFront_CreateDistribution(t *testing.T) {
	t.Skip("CloudFront not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newCloudfrontClient(t)

	out, err := c.CreateDistribution(ctx, &awscloudfront.CreateDistributionInput{
		DistributionConfig: newMinimalDistributionConfig("ref-create"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Distribution)
	assert.NotEmpty(t, aws.ToString(out.Distribution.Id))
}

func TestCloudFront_GetDistribution(t *testing.T) {
	t.Skip("CloudFront not yet implemented")
	resetState(t)
	ctx := context.Background()
	c := newCloudfrontClient(t)

	createOut, err := c.CreateDistribution(ctx, &awscloudfront.CreateDistributionInput{
		DistributionConfig: newMinimalDistributionConfig("ref-get"),
	})
	require.NoError(t, err)
	distID := aws.ToString(createOut.Distribution.Id)

	getOut, err := c.GetDistribution(ctx, &awscloudfront.GetDistributionInput{
		Id: aws.String(distID),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Distribution)
	assert.Equal(t, distID, aws.ToString(getOut.Distribution.Id))
	assert.NotEmpty(t, aws.ToString(getOut.Distribution.DomainName))
}
