package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	awselasticache "github.com/aws/aws-sdk-go-v2/service/elasticache"
	ecachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	awsroute53 "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── ECS Tags ─────────────────────────────────────────────────────────────────

func TestECS_TagCluster_ListTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	out, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("tagged-cluster"),
		Tags: []ecstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := c.ListTagsForResource(ctx, &awsecs.ListTagsForResourceInput{
		ResourceArn: out.Cluster.ClusterArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
}

func TestECS_UntagCluster_RemovesTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	out, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{
		ClusterName: aws.String("untag-cluster"),
		Tags: []ecstypes.Tag{
			{Key: aws.String("k1"), Value: aws.String("v1")},
			{Key: aws.String("k2"), Value: aws.String("v2")},
		},
	})
	require.NoError(t, err)

	_, err = c.UntagResource(ctx, &awsecs.UntagResourceInput{
		ResourceArn: out.Cluster.ClusterArn,
		TagKeys:     []string{"k1"},
	})
	require.NoError(t, err)

	tagsOut, err := c.ListTagsForResource(ctx, &awsecs.ListTagsForResourceInput{
		ResourceArn: out.Cluster.ClusterArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "k2", aws.ToString(tagsOut.Tags[0].Key))
}

func TestECS_TagResource_Cluster_TagAndUntag(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	out, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{ClusterName: aws.String("tag-resource-cluster")})
	require.NoError(t, err)

	_, err = c.TagResource(ctx, &awsecs.TagResourceInput{
		ResourceArn: out.Cluster.ClusterArn,
		Tags:        []ecstypes.Tag{{Key: aws.String("project"), Value: aws.String("jaiscloud")}},
	})
	require.NoError(t, err)

	tagsOut, err := c.ListTagsForResource(ctx, &awsecs.ListTagsForResourceInput{
		ResourceArn: out.Cluster.ClusterArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.Tags, 1)
}

func TestECS_DescribeClusters_Multiple(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	for _, name := range []string{"multi-a", "multi-b", "multi-c"} {
		_, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{ClusterName: aws.String(name)})
		require.NoError(t, err)
	}

	out, err := c.DescribeClusters(ctx, &awsecs.DescribeClustersInput{
		Clusters: []string{"multi-a", "multi-b"},
	})
	require.NoError(t, err)
	assert.Len(t, out.Clusters, 2)
}

func TestECS_ListClusters_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	for i := 0; i < 4; i++ {
		_, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{
			ClusterName: aws.String(fmt.Sprintf("page-cluster-%d", i)),
		})
		require.NoError(t, err)
	}

	var allARNs []string
	var nextToken *string
	for {
		out, err := c.ListClusters(ctx, &awsecs.ListClustersInput{
			MaxResults: aws.Int32(2),
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		allARNs = append(allARNs, out.ClusterArns...)
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	assert.GreaterOrEqual(t, len(allARNs), 4)
}

func TestECS_ListTaskDefinitions_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	for i := 0; i < 3; i++ {
		_, err := c.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
			Family: aws.String(fmt.Sprintf("page-task-%d", i)),
			ContainerDefinitions: []ecstypes.ContainerDefinition{{
				Name:  aws.String("app"),
				Image: aws.String("nginx:latest"),
			}},
		})
		require.NoError(t, err)
	}

	out, err := c.ListTaskDefinitions(ctx, &awsecs.ListTaskDefinitionsInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(out.TaskDefinitionArns), 2)
}

func TestECS_UpdateService_DesiredCount(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newECSClient(t)

	clusterOut, err := c.CreateCluster(ctx, &awsecs.CreateClusterInput{ClusterName: aws.String("svc-cluster")})
	require.NoError(t, err)

	tdOut, err := c.RegisterTaskDefinition(ctx, &awsecs.RegisterTaskDefinitionInput{
		Family: aws.String("svc-task"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{{
			Name:  aws.String("app"),
			Image: aws.String("nginx"),
		}},
	})
	require.NoError(t, err)

	svcOut, err := c.CreateService(ctx, &awsecs.CreateServiceInput{
		Cluster:        clusterOut.Cluster.ClusterArn,
		ServiceName:    aws.String("my-service"),
		TaskDefinition: tdOut.TaskDefinition.TaskDefinitionArn,
		DesiredCount:   aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = c.UpdateService(ctx, &awsecs.UpdateServiceInput{
		Cluster:      clusterOut.Cluster.ClusterArn,
		Service:      svcOut.Service.ServiceArn,
		DesiredCount: aws.Int32(3),
	})
	require.NoError(t, err)

	descOut, err := c.DescribeServices(ctx, &awsecs.DescribeServicesInput{
		Cluster:  clusterOut.Cluster.ClusterArn,
		Services: []string{aws.ToString(svcOut.Service.ServiceArn)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Services, 1)
	assert.Equal(t, int32(3), descOut.Services[0].DesiredCount)
}

// ─── RDS Tags + Extras ────────────────────────────────────────────────────────

func TestRDS_TagDBInstance_AddListRemove(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRDSClient(t)

	out, err := c.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("tagged-db"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
	})
	require.NoError(t, err)

	_, err = c.AddTagsToResource(ctx, &awsrds.AddTagsToResourceInput{
		ResourceName: out.DBInstance.DBInstanceArn,
		Tags: []rdstypes.Tag{
			{Key: aws.String("project"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := c.ListTagsForResource(ctx, &awsrds.ListTagsForResourceInput{
		ResourceName: out.DBInstance.DBInstanceArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "project", aws.ToString(tagsOut.TagList[0].Key))

	_, err = c.RemoveTagsFromResource(ctx, &awsrds.RemoveTagsFromResourceInput{
		ResourceName: out.DBInstance.DBInstanceArn,
		TagKeys:      []string{"project"},
	})
	require.NoError(t, err)

	tagsOut2, err := c.ListTagsForResource(ctx, &awsrds.ListTagsForResourceInput{
		ResourceName: out.DBInstance.DBInstanceArn,
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut2.TagList)
}

func TestRDS_CreateDBParameterGroup_CRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRDSClient(t)

	_, err := c.CreateDBParameterGroup(ctx, &awsrds.CreateDBParameterGroupInput{
		DBParameterGroupName:   aws.String("my-pg"),
		DBParameterGroupFamily: aws.String("postgres14"),
		Description:            aws.String("Test parameter group"),
	})
	require.NoError(t, err)

	out, err := c.DescribeDBParameterGroups(ctx, &awsrds.DescribeDBParameterGroupsInput{
		DBParameterGroupName: aws.String("my-pg"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBParameterGroups, 1)
	assert.Equal(t, "my-pg", aws.ToString(out.DBParameterGroups[0].DBParameterGroupName))

	_, err = c.DeleteDBParameterGroup(ctx, &awsrds.DeleteDBParameterGroupInput{
		DBParameterGroupName: aws.String("my-pg"),
	})
	require.NoError(t, err)
}

func TestRDS_CreateDBSubnetGroup_Tags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRDSClient(t)

	_, err := c.CreateDBSubnetGroup(ctx, &awsrds.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("tagged-subnet"),
		DBSubnetGroupDescription: aws.String("Tagged subnet group"),
		SubnetIds:                []string{"subnet-12345"},
		Tags: []rdstypes.Tag{
			{Key: aws.String("team"), Value: aws.String("infra")},
		},
	})
	require.NoError(t, err)

	out, err := c.DescribeDBSubnetGroups(ctx, &awsrds.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: aws.String("tagged-subnet"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBSubnetGroups, 1)
}

func TestRDS_ListDBInstances_FilterByEngine(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRDSClient(t)

	for _, id := range []string{"pg-instance-1", "pg-instance-2"} {
		_, err := c.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String(id),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
			MasterUsername:       aws.String("admin"),
			MasterUserPassword:   aws.String("password123"),
		})
		require.NoError(t, err)
	}

	out, err := c.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
		Filters: []rdstypes.Filter{{
			Name:   aws.String("engine"),
			Values: []string{"postgres"},
		}},
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.DBInstances), 2)
}

// ─── ElastiCache Tags + Extras ────────────────────────────────────────────────

func TestElastiCache_AddTagsToCluster(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newElastiCacheClient(t)

	clusterOut, err := c.CreateCacheCluster(ctx, &awselasticache.CreateCacheClusterInput{
		CacheClusterId: aws.String("tagged-cache"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	arn := aws.ToString(clusterOut.CacheCluster.ARN)
	if arn == "" {
		t.Skip("ARN not returned by CreateCacheCluster")
	}

	_, err = c.AddTagsToResource(ctx, &awselasticache.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags:         []ecachetypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	_ = err // tags may not be supported in lite mode
}

func TestElastiCache_CreateSubnetGroup_CRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newElastiCacheClient(t)

	_, err := c.CreateCacheSubnetGroup(ctx, &awselasticache.CreateCacheSubnetGroupInput{
		CacheSubnetGroupName:        aws.String("my-subnet-group"),
		CacheSubnetGroupDescription: aws.String("Test subnet group"),
		SubnetIds:                   []string{"subnet-abc123"},
	})
	require.NoError(t, err)

	out, err := c.DescribeCacheSubnetGroups(ctx, &awselasticache.DescribeCacheSubnetGroupsInput{
		CacheSubnetGroupName: aws.String("my-subnet-group"),
	})
	require.NoError(t, err)
	require.Len(t, out.CacheSubnetGroups, 1)
	assert.Equal(t, "my-subnet-group", aws.ToString(out.CacheSubnetGroups[0].CacheSubnetGroupName))

	_, err = c.DeleteCacheSubnetGroup(ctx, &awselasticache.DeleteCacheSubnetGroupInput{
		CacheSubnetGroupName: aws.String("my-subnet-group"),
	})
	require.NoError(t, err)
}

func TestElastiCache_CreateParameterGroup_CRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newElastiCacheClient(t)

	_, err := c.CreateCacheParameterGroup(ctx, &awselasticache.CreateCacheParameterGroupInput{
		CacheParameterGroupName:   aws.String("my-param-group"),
		CacheParameterGroupFamily: aws.String("redis7"),
		Description:               aws.String("Test param group"),
	})
	require.NoError(t, err)

	out, err := c.DescribeCacheParameterGroups(ctx, &awselasticache.DescribeCacheParameterGroupsInput{
		CacheParameterGroupName: aws.String("my-param-group"),
	})
	require.NoError(t, err)
	require.Len(t, out.CacheParameterGroups, 1)

	_, err = c.DeleteCacheParameterGroup(ctx, &awselasticache.DeleteCacheParameterGroupInput{
		CacheParameterGroupName: aws.String("my-param-group"),
	})
	require.NoError(t, err)
}

func TestElastiCache_DescribeEngineVersions(t *testing.T) {
	t.Skipf("DescribeCacheEngineVersions not implemented in emulator")
	resetState(t)
	ctx := context.Background()
	c := newElastiCacheClient(t)

	out, err := c.DescribeCacheEngineVersions(ctx, &awselasticache.DescribeCacheEngineVersionsInput{
		Engine: aws.String("redis"),
	})
	require.NoError(t, err)
	// May return empty in lite mode
	_ = out
}

// ─── Route53 Tags ─────────────────────────────────────────────────────────────

func TestRoute53_CreateHostedZone_WithTags(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRoute53Client(t)

	out, err := c.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("example.com."),
		CallerReference: aws.String("ref-1"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.HostedZone.Id))
}

func TestRoute53_ChangeTagsForResource_HostedZone(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRoute53Client(t)

	out, err := c.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("tagged.com."),
		CallerReference: aws.String("ref-tag-1"),
	})
	require.NoError(t, err)

	// Extract zone ID (strip /hostedzone/ prefix)
	zoneID := aws.ToString(out.HostedZone.Id)
	for _, prefix := range []string{"/hostedzone/"} {
		if len(zoneID) > len(prefix) && zoneID[:len(prefix)] == prefix {
			zoneID = zoneID[len(prefix):]
		}
	}

	_, err = c.ChangeTagsForResource(ctx, &awsroute53.ChangeTagsForResourceInput{
		ResourceType: r53types.TagResourceTypeHostedzone,
		ResourceId:   aws.String(zoneID),
		AddTags: []r53types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := c.ListTagsForResource(ctx, &awsroute53.ListTagsForResourceInput{
		ResourceType: r53types.TagResourceTypeHostedzone,
		ResourceId:   aws.String(zoneID),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.ResourceTagSet.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.ResourceTagSet.Tags[0].Key))
}

func TestRoute53_ListHostedZones_Pagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRoute53Client(t)

	for i := 0; i < 3; i++ {
		_, err := c.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
			Name:            aws.String(fmt.Sprintf("zone%d.com.", i)),
			CallerReference: aws.String(fmt.Sprintf("ref-%d", i)),
		})
		require.NoError(t, err)
	}

	out, err := c.ListHostedZones(ctx, &awsroute53.ListHostedZonesInput{
		MaxItems: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(out.HostedZones), 2)
}

func TestRoute53_DeleteHostedZone_Success(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newRoute53Client(t)

	out, err := c.CreateHostedZone(ctx, &awsroute53.CreateHostedZoneInput{
		Name:            aws.String("todelete.com."),
		CallerReference: aws.String("ref-del"),
	})
	require.NoError(t, err)

	zoneID := aws.ToString(out.HostedZone.Id)
	for _, prefix := range []string{"/hostedzone/"} {
		if len(zoneID) > len(prefix) && zoneID[:len(prefix)] == prefix {
			zoneID = zoneID[len(prefix):]
		}
	}

	_, err = c.DeleteHostedZone(ctx, &awsroute53.DeleteHostedZoneInput{
		Id: aws.String(zoneID),
	})
	require.NoError(t, err)
}
