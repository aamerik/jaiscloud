package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsapigw "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIGateway_CreateGetDeleteRestApi(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{
		Name:        aws.String("my-api"),
		Description: aws.String("integration test API"),
	})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)
	require.NotEmpty(t, apiID)
	assert.Equal(t, "my-api", aws.ToString(createOut.Name))

	getOut, err := c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Equal(t, apiID, aws.ToString(getOut.Id))

	_, err = c.DeleteRestApi(ctx, &awsapigw.DeleteRestApiInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)

	_, err = c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String(apiID)})
	require.Error(t, err, "deleted API should not be found")
}

func TestAPIGateway_GetRestApis(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	for _, n := range []string{"api-a", "api-b", "api-c"} {
		_, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String(n)})
		require.NoError(t, err)
	}

	listOut, err := c.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 3)
}

func TestAPIGateway_UpdateRestApi(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("to-update")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	_, err = c.UpdateRestApi(ctx, &awsapigw.UpdateRestApiInput{
		RestApiId: aws.String(apiID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/name"), Value: aws.String("updated-api")},
			{Op: "replace", Path: aws.String("/description"), Value: aws.String("new description")},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Equal(t, "updated-api", aws.ToString(getOut.Name))
}

func TestAPIGateway_GetResources_RootCreatedAutomatically(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("auto-root")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	// Root "/" resource must be auto-created.
	assert.Len(t, resOut.Items, 1)
	assert.Equal(t, "/", aws.ToString(resOut.Items[0].Path))
}

func TestAPIGateway_CreateResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("res-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	// Get root resource ID.
	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	// Create /items under root.
	newRes, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  aws.String(rootID),
		PathPart:  aws.String("items"),
	})
	require.NoError(t, err)
	assert.Equal(t, "/items", aws.ToString(newRes.Path))

	// GetResources should now return 2 (root + /items).
	resOut2, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, resOut2.Items, 2)
}

func TestAPIGateway_PutMethod_PutIntegration(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("method-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	// PutMethod on root resource.
	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	// PutIntegration (MOCK).
	_, err = c.PutIntegration(ctx, &awsapigw.PutIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
		Type:       "MOCK",
	})
	require.NoError(t, err)

	// GetIntegration.
	intOut, err := c.GetIntegration(ctx, &awsapigw.GetIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
	})
	require.NoError(t, err)
	assert.Equal(t, "MOCK", string(intOut.Type))
}

func TestAPIGateway_CreateDeployment_CreateStage(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("deploy-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	// CreateDeployment with a stage name — should auto-create stage.
	deployOut, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId:   aws.String(apiID),
		StageName:   aws.String("prod"),
		Description: aws.String("first deployment"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(deployOut.Id))

	// GetDeployments.
	deploys, err := c.GetDeployments(ctx, &awsapigw.GetDeploymentsInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, deploys.Items, 1)

	// GetStage auto-created by deployment.
	stageOut, err := c.GetStage(ctx, &awsapigw.GetStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, "prod", aws.ToString(stageOut.StageName))
}

func TestAPIGateway_GetStages(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("stages-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	for _, stage := range []string{"dev", "staging", "prod"} {
		_, err = c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
			RestApiId: aws.String(apiID),
			StageName: aws.String(stage),
		})
		require.NoError(t, err)
	}

	stagesOut, err := c.GetStages(ctx, &awsapigw.GetStagesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, stagesOut.Item, 3)
}

func TestAPIGateway_UpdateStage(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("upd-stage-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	_, err = c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	_, err = c.UpdateStage(ctx, &awsapigw.UpdateStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/description"), Value: aws.String("updated stage")},
			{Op: "replace", Path: aws.String("/variables/backendUrl"), Value: aws.String("http://backend:8080")},
		},
	})
	require.NoError(t, err)

	stageOut, err := c.GetStage(ctx, &awsapigw.GetStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated stage", aws.ToString(stageOut.Description))
	assert.Equal(t, "http://backend:8080", stageOut.Variables["backendUrl"])
}

func TestAPIGateway_DeleteStage(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("del-stage-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	_, err = c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("dev"),
	})
	require.NoError(t, err)

	_, err = c.DeleteStage(ctx, &awsapigw.DeleteStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("dev"),
	})
	require.NoError(t, err)

	_, err = c.GetStage(ctx, &awsapigw.GetStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("dev"),
	})
	require.Error(t, err, "deleted stage should not be found")
}

func TestAPIGateway_DeleteResource(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("del-res-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	newRes, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  aws.String(rootID),
		PathPart:  aws.String("temp"),
	})
	require.NoError(t, err)
	newResID := aws.ToString(newRes.Id)

	_, err = c.DeleteResource(ctx, &awsapigw.DeleteResourceInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(newResID),
	})
	require.NoError(t, err)

	_, err = c.GetResource(ctx, &awsapigw.GetResourceInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(newResID),
	})
	require.Error(t, err, "deleted resource should not be found")
}
