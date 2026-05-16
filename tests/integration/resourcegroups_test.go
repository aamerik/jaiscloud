package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsresourcegroups "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	rgtypes "github.com/aws/aws-sdk-go-v2/service/resourcegroups/types"
	awstagging "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceGroups_CreateAndList(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newResourceGroupsClient(t)

	// Create a group
	createOut, err := client.CreateGroup(ctx, &awsresourcegroups.CreateGroupInput{
		Name:        aws.String("my-test-group"),
		Description: aws.String("A test resource group"),
		ResourceQuery: &rgtypes.ResourceQuery{
			Type:  rgtypes.QueryTypeTagFilters10,
			Query: aws.String(`{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[]}`),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-test-group", aws.ToString(createOut.Group.Name))
	assert.NotEmpty(t, aws.ToString(createOut.Group.GroupArn))
	assert.Equal(t, "A test resource group", aws.ToString(createOut.Group.Description))

	// Create another group
	_, err = client.CreateGroup(ctx, &awsresourcegroups.CreateGroupInput{
		Name:        aws.String("second-group"),
		Description: aws.String("Second test group"),
	})
	require.NoError(t, err)

	// List groups
	listOut, err := client.ListGroups(ctx, &awsresourcegroups.ListGroupsInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.GroupIdentifiers, 2)

	// Get specific group
	getOut, err := client.GetGroup(ctx, &awsresourcegroups.GetGroupInput{
		Group: aws.String("my-test-group"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-test-group", aws.ToString(getOut.Group.Name))
	assert.Equal(t, "A test resource group", aws.ToString(getOut.Group.Description))

	// Update group description
	updateOut, err := client.UpdateGroup(ctx, &awsresourcegroups.UpdateGroupInput{
		Group:       aws.String("my-test-group"),
		Description: aws.String("Updated description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Updated description", aws.ToString(updateOut.Group.Description))

	// Delete a group
	deleteOut, err := client.DeleteGroup(ctx, &awsresourcegroups.DeleteGroupInput{
		Group: aws.String("my-test-group"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-test-group", aws.ToString(deleteOut.Group.Name))

	// Confirm only 1 group remains
	listOut2, err := client.ListGroups(ctx, &awsresourcegroups.ListGroupsInput{})
	require.NoError(t, err)
	assert.Len(t, listOut2.GroupIdentifiers, 1)
}

func TestResourceGroups_GetNotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newResourceGroupsClient(t)

	_, err := client.GetGroup(ctx, &awsresourcegroups.GetGroupInput{
		Group: aws.String("nonexistent-group"),
	})
	require.Error(t, err)
}

func TestTaggingAPI_GetResources(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newTaggingClient(t)

	// GetResources with no resources in the store — should succeed with empty list
	getOut, err := client.GetResources(ctx, &awstagging.GetResourcesInput{})
	require.NoError(t, err)
	assert.NotNil(t, getOut.ResourceTagMappingList)

	// GetTagKeys — should succeed with empty list
	keysOut, err := client.GetTagKeys(ctx, &awstagging.GetTagKeysInput{})
	require.NoError(t, err)
	assert.NotNil(t, keysOut.TagKeys)
}

func TestTaggingAPI_TagAndUntagResources(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	client := newTaggingClient(t)

	fakeArn := "arn:aws:s3:::my-test-bucket"

	// Tag resources
	tagOut, err := client.TagResources(ctx, &awstagging.TagResourcesInput{
		ResourceARNList: []string{fakeArn},
		Tags: map[string]string{
			"Environment": "test",
			"Team":        "platform",
		},
	})
	require.NoError(t, err)
	assert.Empty(t, tagOut.FailedResourcesMap)

	// GetResources — should find the tagged resource
	getOut, err := client.GetResources(ctx, &awstagging.GetResourcesInput{})
	require.NoError(t, err)
	require.Len(t, getOut.ResourceTagMappingList, 1)
	assert.Equal(t, fakeArn, aws.ToString(getOut.ResourceTagMappingList[0].ResourceARN))
	assert.Len(t, getOut.ResourceTagMappingList[0].Tags, 2)

	// GetTagKeys
	keysOut, err := client.GetTagKeys(ctx, &awstagging.GetTagKeysInput{})
	require.NoError(t, err)
	assert.Len(t, keysOut.TagKeys, 2)

	// GetTagValues for "Environment"
	valOut, err := client.GetTagValues(ctx, &awstagging.GetTagValuesInput{
		Key: aws.String("Environment"),
	})
	require.NoError(t, err)
	assert.Contains(t, valOut.TagValues, "test")

	// Untag resources
	untagOut, err := client.UntagResources(ctx, &awstagging.UntagResourcesInput{
		ResourceARNList: []string{fakeArn},
		TagKeys:         []string{"Team"},
	})
	require.NoError(t, err)
	assert.Empty(t, untagOut.FailedResourcesMap)

	// GetResources with tag filter for remaining tag
	getFiltered, err := client.GetResources(ctx, &awstagging.GetResourcesInput{})
	require.NoError(t, err)
	require.Len(t, getFiltered.ResourceTagMappingList, 1)
	// Should only have 1 tag now
	assert.Len(t, getFiltered.ResourceTagMappingList[0].Tags, 1)
}
