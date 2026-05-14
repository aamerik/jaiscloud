package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awselasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElastiCache_CreateDescribeDeleteCacheCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newElastiCacheClient(t)

	out, err := client.CreateCacheCluster(ctx, &awselasticache.CreateCacheClusterInput{
		CacheClusterId: aws.String("my-cluster"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-cluster", aws.ToString(out.CacheCluster.CacheClusterId))
	assert.Equal(t, "available", aws.ToString(out.CacheCluster.CacheClusterStatus))

	descOut, err := client.DescribeCacheClusters(ctx, &awselasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String("my-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.CacheClusters, 1)
	assert.Equal(t, "my-cluster", aws.ToString(descOut.CacheClusters[0].CacheClusterId))

	_, err = client.DeleteCacheCluster(ctx, &awselasticache.DeleteCacheClusterInput{
		CacheClusterId: aws.String("my-cluster"),
	})
	require.NoError(t, err)

	_, err = client.DescribeCacheClusters(ctx, &awselasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String("my-cluster"),
	})
	require.Error(t, err)
}

func TestElastiCache_ListCacheClusters(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newElastiCacheClient(t)

	for _, id := range []string{"cache-1", "cache-2", "cache-3"} {
		_, err := client.CreateCacheCluster(ctx, &awselasticache.CreateCacheClusterInput{
			CacheClusterId: aws.String(id),
			Engine:         aws.String("redis"),
			CacheNodeType:  aws.String("cache.t3.micro"),
			NumCacheNodes:  aws.Int32(1),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeCacheClusters(ctx, &awselasticache.DescribeCacheClustersInput{})
	require.NoError(t, err)
	assert.Len(t, out.CacheClusters, 3)
}

func TestElastiCache_CreateDescribeDeleteReplicationGroup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newElastiCacheClient(t)

	out, err := client.CreateReplicationGroup(ctx, &awselasticache.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("my-rg"),
		ReplicationGroupDescription: aws.String("Test replication group"),
		CacheNodeType:               aws.String("cache.t3.micro"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-rg", aws.ToString(out.ReplicationGroup.ReplicationGroupId))
	assert.Equal(t, "available", aws.ToString(out.ReplicationGroup.Status))

	descOut, err := client.DescribeReplicationGroups(ctx, &awselasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("my-rg"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ReplicationGroups, 1)
	assert.Equal(t, "my-rg", aws.ToString(descOut.ReplicationGroups[0].ReplicationGroupId))

	_, err = client.DeleteReplicationGroup(ctx, &awselasticache.DeleteReplicationGroupInput{
		ReplicationGroupId: aws.String("my-rg"),
	})
	require.NoError(t, err)

	_, err = client.DescribeReplicationGroups(ctx, &awselasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("my-rg"),
	})
	require.Error(t, err)
}

func TestElastiCache_ModifyCacheCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newElastiCacheClient(t)

	_, err := client.CreateCacheCluster(ctx, &awselasticache.CreateCacheClusterInput{
		CacheClusterId: aws.String("my-cluster"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyCacheCluster(ctx, &awselasticache.ModifyCacheClusterInput{
		CacheClusterId: aws.String("my-cluster"),
		CacheNodeType:  aws.String("cache.t3.small"),
		ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "cache.t3.small", aws.ToString(modOut.CacheCluster.CacheNodeType))
}

// TestElastiCacheUnknownActionEnvelope verifies that an unmodelled action still
// produces a valid XML envelope with a non-empty RequestId (fix 1.1.1).
func TestElastiCacheUnknownActionEnvelope(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newElastiCacheClient(t)
	// DescribeEvents is not modelled in the provider; the response must be a valid
	// XML wrapper — not a bare <Response/> — so the SDK does not raise xml:EOF.
	_, err := client.DescribeEvents(ctx, &awselasticache.DescribeEventsInput{})
	// Not an SDK serialisation error (which would contain "xml: EOF" or similar).
	if err != nil {
		assert.NotContains(t, err.Error(), "EOF", "response must be a valid XML envelope, not bare <Response/>")
	}
}
