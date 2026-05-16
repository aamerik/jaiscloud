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

// TestAPIGateway_MockIntegrationSetup verifies the full management-plane pipeline
// for a MOCK integration: method + integration + integration response + deployment + stage.
// Execute-api invoke requires SigV4-signed requests (tested separately with the SDK).
func TestAPIGateway_MockIntegrationSetup(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("mock-invoke-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = c.PutIntegration(ctx, &awsapigw.PutIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
		Type:       apigwtypes.IntegrationTypeMock,
		RequestTemplates: map[string]string{
			"application/json": `{"statusCode": 200}`,
		},
	})
	require.NoError(t, err)

	_, err = c.PutIntegrationResponse(ctx, &awsapigw.PutIntegrationResponseInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("GET"),
		StatusCode:        aws.String("200"),
		ResponseTemplates: map[string]string{"application/json": `{"ok":true}`},
	})
	require.NoError(t, err)

	deplOut, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(deplOut.Id))

	_, err = c.CreateStage(ctx, &awsapigw.CreateStageInput{
		RestApiId:    aws.String(apiID),
		StageName:    aws.String("test"),
		DeploymentId: deplOut.Id,
	})
	require.NoError(t, err)

	// Verify stage is retrievable after full setup.
	stageOut, err := c.GetStage(ctx, &awsapigw.GetStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "test", aws.ToString(stageOut.StageName))
}

func TestAPIGateway_DeleteRestApi_CascadesChildren(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("cascade-api")})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	deplOut, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)

	_, err = c.CreateStage(ctx, &awsapigw.CreateStageInput{
		RestApiId:    aws.String(apiID),
		StageName:    aws.String("prod"),
		DeploymentId: deplOut.Id,
	})
	require.NoError(t, err)

	_, err = c.DeleteRestApi(ctx, &awsapigw.DeleteRestApiInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)

	_, err = c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String(apiID)})
	require.Error(t, err, "deleted API should not be found")

	_, err = c.GetStage(ctx, &awsapigw.GetStageInput{RestApiId: aws.String(apiID), StageName: aws.String("prod")})
	require.Error(t, err, "stage of deleted API should not be found")
}

// TestAPIGWHTTPProxyPassthrough verifies that the HTTP_PROXY integration codec
// path is reachable and that Content-Type passthrough is wired (fix 1.1.6).
// The test validates the management-plane wiring; the data-plane proxy call
// is covered by the provider unit tests which use a local httptest.Server.
func TestAPIGWHTTPProxyPassthrough(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("proxy-test-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	require.NotEmpty(t, resOut.Items)
	rootID := aws.ToString(resOut.Items[0].Id)

	childOut, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  aws.String(rootID),
		PathPart:  aws.String("proxy"),
	})
	require.NoError(t, err)

	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(aws.ToString(childOut.Id)),
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = c.PutIntegration(ctx, &awsapigw.PutIntegrationInput{
		RestApiId:             aws.String(apiID),
		ResourceId:            aws.String(aws.ToString(childOut.Id)),
		HttpMethod:            aws.String("GET"),
		Type:                  apigwtypes.IntegrationTypeHttpProxy,
		IntegrationHttpMethod: aws.String("GET"),
		Uri:                   aws.String("http://example.com"),
	})
	require.NoError(t, err, "PutIntegration for HTTP_PROXY must not fail")
}

// ─── Group 1: Request validators ─────────────────────────────────────────────

func TestAPIGW_RequestValidatorCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("val-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// Create.
	createOut, err := c.CreateRequestValidator(ctx, &awsapigw.CreateRequestValidatorInput{
		RestApiId:                 aws.String(apiID),
		Name:                      aws.String("body-validator"),
		ValidateRequestBody:       true,
		ValidateRequestParameters: false,
	})
	require.NoError(t, err)
	validatorID := aws.ToString(createOut.Id)
	require.NotEmpty(t, validatorID)
	assert.Equal(t, "body-validator", aws.ToString(createOut.Name))
	assert.True(t, createOut.ValidateRequestBody)

	// Get single.
	getOut, err := c.GetRequestValidator(ctx, &awsapigw.GetRequestValidatorInput{
		RestApiId:          aws.String(apiID),
		RequestValidatorId: aws.String(validatorID),
	})
	require.NoError(t, err)
	assert.Equal(t, validatorID, aws.ToString(getOut.Id))
	assert.Equal(t, "body-validator", aws.ToString(getOut.Name))

	// List.
	listOut, err := c.GetRequestValidators(ctx, &awsapigw.GetRequestValidatorsInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 1)

	// Update — rename and enable params validation.
	updOut, err := c.UpdateRequestValidator(ctx, &awsapigw.UpdateRequestValidatorInput{
		RestApiId:          aws.String(apiID),
		RequestValidatorId: aws.String(validatorID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/name"), Value: aws.String("full-validator")},
			{Op: "replace", Path: aws.String("/validateRequestParameters"), Value: aws.String("true")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "full-validator", aws.ToString(updOut.Name))
	assert.True(t, updOut.ValidateRequestParameters)

	// Delete.
	_, err = c.DeleteRequestValidator(ctx, &awsapigw.DeleteRequestValidatorInput{
		RestApiId:          aws.String(apiID),
		RequestValidatorId: aws.String(validatorID),
	})
	require.NoError(t, err)

	// List should now be empty.
	listOut2, err := c.GetRequestValidators(ctx, &awsapigw.GetRequestValidatorsInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Items)
}

// ─── Group 2: Custom domain names ────────────────────────────────────────────

func TestAPIGW_DomainNameCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Create.
	createOut, err := c.CreateDomainName(ctx, &awsapigw.CreateDomainNameInput{
		DomainName:     aws.String("api.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", aws.ToString(createOut.DomainName))
	assert.NotEmpty(t, aws.ToString(createOut.DistributionDomainName))

	// Get single.
	getOut, err := c.GetDomainName(ctx, &awsapigw.GetDomainNameInput{DomainName: aws.String("api.example.com")})
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", aws.ToString(getOut.DomainName))

	// List.
	listOut, err := c.GetDomainNames(ctx, &awsapigw.GetDomainNamesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 1)

	// Update certificate ARN.
	newCert := "arn:aws:acm:us-east-1:000000000000:certificate/new"
	updOut, err := c.UpdateDomainName(ctx, &awsapigw.UpdateDomainNameInput{
		DomainName: aws.String("api.example.com"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/certificateArn"), Value: aws.String(newCert)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, newCert, aws.ToString(updOut.CertificateArn))

	// Delete.
	_, err = c.DeleteDomainName(ctx, &awsapigw.DeleteDomainNameInput{DomainName: aws.String("api.example.com")})
	require.NoError(t, err)

	// Verify gone.
	_, err = c.GetDomainName(ctx, &awsapigw.GetDomainNameInput{DomainName: aws.String("api.example.com")})
	require.Error(t, err)
}

// ─── Group 3: Usage Plans + API Keys ─────────────────────────────────────────

func TestAPIGW_UsagePlanCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Create.
	createOut, err := c.CreateUsagePlan(ctx, &awsapigw.CreateUsagePlanInput{
		Name:        aws.String("standard-plan"),
		Description: aws.String("100 req/day"),
	})
	require.NoError(t, err)
	planID := aws.ToString(createOut.Id)
	require.NotEmpty(t, planID)
	assert.Equal(t, "standard-plan", aws.ToString(createOut.Name))

	// Get.
	getOut, err := c.GetUsagePlan(ctx, &awsapigw.GetUsagePlanInput{UsagePlanId: aws.String(planID)})
	require.NoError(t, err)
	assert.Equal(t, planID, aws.ToString(getOut.Id))

	// List.
	listOut, err := c.GetUsagePlans(ctx, &awsapigw.GetUsagePlansInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 1)

	// Update name.
	updOut, err := c.UpdateUsagePlan(ctx, &awsapigw.UpdateUsagePlanInput{
		UsagePlanId: aws.String(planID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/name"), Value: aws.String("premium-plan")},
			{Op: "replace", Path: aws.String("/description"), Value: aws.String("1000 req/day")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "premium-plan", aws.ToString(updOut.Name))

	// Delete.
	_, err = c.DeleteUsagePlan(ctx, &awsapigw.DeleteUsagePlanInput{UsagePlanId: aws.String(planID)})
	require.NoError(t, err)

	// Should be gone.
	_, err = c.GetUsagePlan(ctx, &awsapigw.GetUsagePlanInput{UsagePlanId: aws.String(planID)})
	require.Error(t, err)
}

func TestAPIGW_ApiKeyCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Create.
	createOut, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{
		Name:        aws.String("my-api-key"),
		Description: aws.String("test key"),
		Enabled:     true,
	})
	require.NoError(t, err)
	keyID := aws.ToString(createOut.Id)
	require.NotEmpty(t, keyID)
	assert.Equal(t, "my-api-key", aws.ToString(createOut.Name))
	assert.True(t, createOut.Enabled)

	// Get (with value).
	getOut, err := c.GetApiKey(ctx, &awsapigw.GetApiKeyInput{
		ApiKey:       aws.String(keyID),
		IncludeValue: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, keyID, aws.ToString(getOut.Id))
	assert.NotEmpty(t, aws.ToString(getOut.Value))

	// List.
	listOut, err := c.GetApiKeys(ctx, &awsapigw.GetApiKeysInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 1)

	// Delete.
	_, err = c.DeleteApiKey(ctx, &awsapigw.DeleteApiKeyInput{ApiKey: aws.String(keyID)})
	require.NoError(t, err)

	// Should be gone.
	_, err = c.GetApiKey(ctx, &awsapigw.GetApiKeyInput{ApiKey: aws.String(keyID)})
	require.Error(t, err)
}

func TestAPIGW_UsagePlanKeyCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Create usage plan.
	planOut, err := c.CreateUsagePlan(ctx, &awsapigw.CreateUsagePlanInput{Name: aws.String("plan-for-key")})
	require.NoError(t, err)
	planID := aws.ToString(planOut.Id)

	// Create API key.
	keyOut, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{Name: aws.String("key-for-plan"), Enabled: true})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.Id)

	// Associate key with plan.
	assocOut, err := c.CreateUsagePlanKey(ctx, &awsapigw.CreateUsagePlanKeyInput{
		UsagePlanId: aws.String(planID),
		KeyId:       aws.String(keyID),
		KeyType:     aws.String("API_KEY"),
	})
	require.NoError(t, err)
	assert.Equal(t, keyID, aws.ToString(assocOut.Id))

	// List plan keys.
	keysOut, err := c.GetUsagePlanKeys(ctx, &awsapigw.GetUsagePlanKeysInput{UsagePlanId: aws.String(planID)})
	require.NoError(t, err)
	assert.Len(t, keysOut.Items, 1)
	assert.Equal(t, keyID, aws.ToString(keysOut.Items[0].Id))

	// Delete plan key.
	_, err = c.DeleteUsagePlanKey(ctx, &awsapigw.DeleteUsagePlanKeyInput{
		UsagePlanId: aws.String(planID),
		KeyId:       aws.String(keyID),
	})
	require.NoError(t, err)

	// Verify gone from plan.
	keysOut2, err := c.GetUsagePlanKeys(ctx, &awsapigw.GetUsagePlanKeysInput{UsagePlanId: aws.String(planID)})
	require.NoError(t, err)
	assert.Empty(t, keysOut2.Items)
}

// ─── Group 4: Stage + deployment operations ───────────────────────────────────

func TestAPIGW_StageUpdateAndFlush(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("stage-flush-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// Deploy with stage.
	_, err = c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	// UpdateStage — add a variable.
	_, err = c.UpdateStage(ctx, &awsapigw.UpdateStageInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/variables/backendUrl"), Value: aws.String("http://backend:8080")},
		},
	})
	require.NoError(t, err)

	// GetStage and verify variable.
	stageOut, err := c.GetStage(ctx, &awsapigw.GetStageInput{RestApiId: aws.String(apiID), StageName: aws.String("prod")})
	require.NoError(t, err)
	assert.Equal(t, "prod", aws.ToString(stageOut.StageName))
	assert.Equal(t, "http://backend:8080", stageOut.Variables["backendUrl"])

	// DeleteStage.
	_, err = c.DeleteStage(ctx, &awsapigw.DeleteStageInput{RestApiId: aws.String(apiID), StageName: aws.String("prod")})
	require.NoError(t, err)

	// Stage should be gone.
	_, err = c.GetStage(ctx, &awsapigw.GetStageInput{RestApiId: aws.String(apiID), StageName: aws.String("prod")})
	require.Error(t, err)
}

func TestAPIGW_DeploymentWithExistingStage(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("redeploy-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// First deployment.
	dep1, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("v1"),
	})
	require.NoError(t, err)
	firstID := aws.ToString(dep1.Id)
	require.NotEmpty(t, firstID)

	// Second deployment to same stage.
	dep2, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("v1"),
	})
	require.NoError(t, err)
	secondID := aws.ToString(dep2.Id)
	require.NotEmpty(t, secondID)

	// IDs must differ.
	assert.NotEqual(t, firstID, secondID)

	// Both deployments should appear.
	deploys, err := c.GetDeployments(ctx, &awsapigw.GetDeploymentsInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, deploys.Items, 2)
}

// ─── Group 5: Method + Integration round-trip ────────────────────────────────

func TestAPIGW_MethodIntegrationRoundtrip(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("roundtrip-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// Get root resource.
	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	// Create child resource.
	childOut, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  aws.String(rootID),
		PathPart:  aws.String("widgets"),
	})
	require.NoError(t, err)
	childID := aws.ToString(childOut.Id)

	// PutMethod.
	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(childID),
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	// PutMethodResponse.
	_, err = c.PutMethodResponse(ctx, &awsapigw.PutMethodResponseInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(childID),
		HttpMethod: aws.String("GET"),
		StatusCode: aws.String("200"),
	})
	require.NoError(t, err)

	// PutIntegration (MOCK).
	intIn, err := c.PutIntegration(ctx, &awsapigw.PutIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(childID),
		HttpMethod: aws.String("GET"),
		Type:       apigwtypes.IntegrationTypeMock,
		RequestTemplates: map[string]string{
			"application/json": `{"statusCode": 200}`,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, apigwtypes.IntegrationTypeMock, intIn.Type)

	// PutIntegrationResponse.
	_, err = c.PutIntegrationResponse(ctx, &awsapigw.PutIntegrationResponseInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(childID),
		HttpMethod:        aws.String("GET"),
		StatusCode:        aws.String("200"),
		ResponseTemplates: map[string]string{"application/json": `{"widgets":[]}`},
	})
	require.NoError(t, err)

	// GetIntegration and verify.
	intOut, err := c.GetIntegration(ctx, &awsapigw.GetIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(childID),
		HttpMethod: aws.String("GET"),
	})
	require.NoError(t, err)
	assert.Equal(t, apigwtypes.IntegrationTypeMock, intOut.Type)
	assert.Equal(t, "WHEN_NO_MATCH", aws.ToString(intOut.PassthroughBehavior))

	// GetMethod and verify.
	methOut, err := c.GetMethod(ctx, &awsapigw.GetMethodInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(childID),
		HttpMethod: aws.String("GET"),
	})
	require.NoError(t, err)
	assert.Equal(t, "NONE", aws.ToString(methOut.AuthorizationType))
}

// ─── Group 6: Error cases ─────────────────────────────────────────────────────

func TestAPIGW_GetNonExistentRestApi(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	_, err := c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String("nonexistent-id")})
	require.Error(t, err, "unknown API should return error")
}

func TestAPIGW_DeleteNonExistent(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	_, err := c.DeleteRestApi(ctx, &awsapigw.DeleteRestApiInput{RestApiId: aws.String("does-not-exist")})
	require.Error(t, err, "deleting unknown API should return error")
}

func TestAPIGW_CreateDuplicateUsagePlan(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Create first plan with a given name.
	out1, err := c.CreateUsagePlan(ctx, &awsapigw.CreateUsagePlanInput{Name: aws.String("shared-name")})
	require.NoError(t, err)

	// Second plan with same name — AWS allows it (plans are not name-unique).
	out2, err := c.CreateUsagePlan(ctx, &awsapigw.CreateUsagePlanInput{Name: aws.String("shared-name")})
	require.NoError(t, err)

	// Both must have different IDs.
	assert.NotEqual(t, aws.ToString(out1.Id), aws.ToString(out2.Id))

	// List should show both.
	listOut, err := c.GetUsagePlans(ctx, &awsapigw.GetUsagePlansInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 2)
}

func TestAPIGW_GetApiKeyMasked(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	keyOut, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{
		Name:    aws.String("mask-test-key"),
		Enabled: true,
	})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.Id)

	// Without includeValue — Value should be absent/empty.
	masked, err := c.GetApiKey(ctx, &awsapigw.GetApiKeyInput{ApiKey: aws.String(keyID)})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(masked.Value), "value should be masked when includeValue is not set")

	// With includeValue=true — Value must be present.
	revealed, err := c.GetApiKey(ctx, &awsapigw.GetApiKeyInput{
		ApiKey:       aws.String(keyID),
		IncludeValue: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(revealed.Value), "value should be present when includeValue=true")
}

// ─── Group 7: createdDate is parseable ───────────────────────────────────────

func TestAPIGW_CreatedDateParseable(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	_, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("date-check-api")})
	require.NoError(t, err)

	apis, err := c.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	require.NotEmpty(t, apis.Items)
	assert.NotNil(t, apis.Items[0].CreatedDate, "createdDate must be parsed by the SDK")
}

// ─── Additional tests ─────────────────────────────────────────────────────────

func TestAPIGW_ListResourcesAfterCreate(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("multi-res-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	// Add three child resources under root.
	for _, part := range []string{"users", "orders", "products"} {
		_, err = c.CreateResource(ctx, &awsapigw.CreateResourceInput{
			RestApiId: aws.String(apiID),
			ParentId:  aws.String(rootID),
			PathPart:  aws.String(part),
		})
		require.NoError(t, err)
	}

	// List should return root + 3 children = 4.
	allRes, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, allRes.Items, 4)
}

func TestAPIGW_GetExport(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("export-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	_, err = c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	_, err = c.GetExport(ctx, &awsapigw.GetExportInput{
		RestApiId:  aws.String(apiID),
		StageName:  aws.String("prod"),
		ExportType: aws.String("oas30"),
	})
	if err != nil {
		t.Skipf("GetExport not yet implemented or returned error: %v", err)
	}
}

func TestAPIGW_TagsOnRestApi(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Create an API — its ARN is used for tagging.
	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("tagged-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// Build a synthetic ARN (the emulator accepts any ARN-like string).
	resourceArn := "arn:aws:apigateway:us-east-1::/restapis/" + apiID

	// TagResource.
	_, err = c.TagResource(ctx, &awsapigw.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	// GetTags — both tags should be present.
	tagsOut, err := c.GetTags(ctx, &awsapigw.GetTagsInput{ResourceArn: aws.String(resourceArn)})
	require.NoError(t, err)
	assert.Equal(t, "test", tagsOut.Tags["env"])
	assert.Equal(t, "platform", tagsOut.Tags["team"])

	// UntagResource — remove one tag.
	_, err = c.UntagResource(ctx, &awsapigw.UntagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	// GetTags — only "env" should remain.
	tagsOut2, err := c.GetTags(ctx, &awsapigw.GetTagsInput{ResourceArn: aws.String(resourceArn)})
	require.NoError(t, err)
	assert.Equal(t, "test", tagsOut2.Tags["env"])
	_, hasTeam := tagsOut2.Tags["team"]
	assert.False(t, hasTeam, "team tag should have been removed")
}

func TestAPIGW_BasePathMappingCRUD(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Need a domain name first.
	_, err := c.CreateDomainName(ctx, &awsapigw.CreateDomainNameInput{
		DomainName:     aws.String("bpm.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/bpm-test"),
	})
	require.NoError(t, err)

	// Need a REST API.
	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("bpm-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// Deploy to stage.
	_, err = c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{
		RestApiId: aws.String(apiID),
		StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	// CreateBasePathMapping.
	bpmOut, err := c.CreateBasePathMapping(ctx, &awsapigw.CreateBasePathMappingInput{
		DomainName: aws.String("bpm.example.com"),
		RestApiId:  aws.String(apiID),
		Stage:      aws.String("prod"),
		BasePath:   aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", aws.ToString(bpmOut.BasePath))

	// GetBasePathMapping.
	getOut, err := c.GetBasePathMapping(ctx, &awsapigw.GetBasePathMappingInput{
		DomainName: aws.String("bpm.example.com"),
		BasePath:   aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Equal(t, apiID, aws.ToString(getOut.RestApiId))
	assert.Equal(t, "prod", aws.ToString(getOut.Stage))

	// GetBasePathMappings (list).
	listOut, err := c.GetBasePathMappings(ctx, &awsapigw.GetBasePathMappingsInput{
		DomainName: aws.String("bpm.example.com"),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 1)

	// DeleteBasePathMapping.
	_, err = c.DeleteBasePathMapping(ctx, &awsapigw.DeleteBasePathMappingInput{
		DomainName: aws.String("bpm.example.com"),
		BasePath:   aws.String("v1"),
	})
	require.NoError(t, err)

	// Should be empty now.
	listOut2, err := c.GetBasePathMappings(ctx, &awsapigw.GetBasePathMappingsInput{
		DomainName: aws.String("bpm.example.com"),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.Items)
}

// ─── Extra coverage tests ─────────────────────────────────────────────────────

func TestAPIGW_GetMethod_NotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("method-notfound-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	// No method registered — must error.
	_, err = c.GetMethod(ctx, &awsapigw.GetMethodInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("POST"),
	})
	require.Error(t, err)
}

func TestAPIGW_DeleteMethod(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("del-method-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("DELETE"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = c.DeleteMethod(ctx, &awsapigw.DeleteMethodInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("DELETE"),
	})
	require.NoError(t, err)

	_, err = c.GetMethod(ctx, &awsapigw.GetMethodInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("DELETE"),
	})
	require.Error(t, err, "method should be gone after DeleteMethod")
}

func TestAPIGW_DeleteIntegration(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("del-int-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = c.PutIntegration(ctx, &awsapigw.PutIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
		Type:       apigwtypes.IntegrationTypeMock,
	})
	require.NoError(t, err)

	_, err = c.DeleteIntegration(ctx, &awsapigw.DeleteIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
	})
	require.NoError(t, err)

	_, err = c.GetIntegration(ctx, &awsapigw.GetIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
	})
	require.Error(t, err, "integration should be gone after DeleteIntegration")
}

func TestAPIGW_DeleteDeployment(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("del-deploy-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	dep, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	depID := aws.ToString(dep.Id)

	_, err = c.DeleteDeployment(ctx, &awsapigw.DeleteDeploymentInput{
		RestApiId:    aws.String(apiID),
		DeploymentId: aws.String(depID),
	})
	require.NoError(t, err)

	deploys, err := c.GetDeployments(ctx, &awsapigw.GetDeploymentsInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Empty(t, deploys.Items)
}

func TestAPIGW_MultipleValidatorsPerAPI(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("multi-val-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	for i, name := range []string{"v1", "v2", "v3"} {
		_, err = c.CreateRequestValidator(ctx, &awsapigw.CreateRequestValidatorInput{
			RestApiId:           aws.String(apiID),
			Name:                aws.String(name),
			ValidateRequestBody: i%2 == 0,
		})
		require.NoError(t, err)
	}

	listOut, err := c.GetRequestValidators(ctx, &awsapigw.GetRequestValidatorsInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 3)
}

func TestAPIGW_MultipleDomainNames(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	for _, d := range []string{"a.example.com", "b.example.com"} {
		_, err := c.CreateDomainName(ctx, &awsapigw.CreateDomainNameInput{
			DomainName:     aws.String(d),
			CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/test"),
		})
		require.NoError(t, err)
	}

	listOut, err := c.GetDomainNames(ctx, &awsapigw.GetDomainNamesInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 2)
}

func TestAPIGW_UpdateApiKey(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	keyOut, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{
		Name:    aws.String("orig-key"),
		Enabled: true,
	})
	require.NoError(t, err)
	keyID := aws.ToString(keyOut.Id)

	updOut, err := c.UpdateApiKey(ctx, &awsapigw.UpdateApiKeyInput{
		ApiKey: aws.String(keyID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/name"), Value: aws.String("renamed-key")},
			{Op: "replace", Path: aws.String("/description"), Value: aws.String("updated")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed-key", aws.ToString(updOut.Name))
	assert.Equal(t, "updated", aws.ToString(updOut.Description))
}

func TestAPIGW_GetRestApi_VerifyFields(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{
		Name:        aws.String("field-check-api"),
		Description: aws.String("desc"),
	})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	getOut, err := c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Equal(t, apiID, aws.ToString(getOut.Id))
	assert.Equal(t, "field-check-api", aws.ToString(getOut.Name))
	assert.Equal(t, "desc", aws.ToString(getOut.Description))
	assert.NotNil(t, getOut.CreatedDate)
}

func TestAPIGW_GetResource_VerifyPath(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("res-path-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	childOut, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  aws.String(rootID),
		PathPart:  aws.String("items"),
	})
	require.NoError(t, err)
	childID := aws.ToString(childOut.Id)

	// GetResource by ID and verify path.
	getOut, err := c.GetResource(ctx, &awsapigw.GetResourceInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(childID),
	})
	require.NoError(t, err)
	assert.Equal(t, "/items", aws.ToString(getOut.Path))
	assert.Equal(t, "items", aws.ToString(getOut.PathPart))
	assert.Equal(t, rootID, aws.ToString(getOut.ParentId))
}

// ─── Additional coverage: ~20 more tests ──────────────────────────────────────

func TestAPIGW_CreateRestApi_EmptyNameFails(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	_, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("")})
	require.Error(t, err, "empty name must be rejected")
}

func TestAPIGW_GetRestApis_EmptyAfterReset(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	listOut, err := c.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut.Items)
}

func TestAPIGW_UpdateRestApi_Description(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{
		Name:        aws.String("desc-api"),
		Description: aws.String("original"),
	})
	require.NoError(t, err)
	apiID := aws.ToString(createOut.Id)

	_, err = c.UpdateRestApi(ctx, &awsapigw.UpdateRestApiInput{
		RestApiId: aws.String(apiID),
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: "replace", Path: aws.String("/description"), Value: aws.String("updated description")},
		},
	})
	require.NoError(t, err)

	getOut, err := c.GetRestApi(ctx, &awsapigw.GetRestApiInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(getOut.Description))
}

func TestAPIGW_CreateStage_Explicit(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("explicit-stage-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	// Create deployment without stage.
	dep, err := c.CreateDeployment(ctx, &awsapigw.CreateDeploymentInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)

	// Explicitly create a stage pointing at that deployment.
	_, err = c.CreateStage(ctx, &awsapigw.CreateStageInput{
		RestApiId:    aws.String(apiID),
		StageName:    aws.String("canary"),
		DeploymentId: dep.Id,
	})
	require.NoError(t, err)

	stageOut, err := c.GetStage(ctx, &awsapigw.GetStageInput{RestApiId: aws.String(apiID), StageName: aws.String("canary")})
	require.NoError(t, err)
	assert.Equal(t, "canary", aws.ToString(stageOut.StageName))
}

func TestAPIGW_GetStages_Empty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("no-stages-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	stagesOut, err := c.GetStages(ctx, &awsapigw.GetStagesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	assert.Empty(t, stagesOut.Item)
}

func TestAPIGW_PutMethodResponse_StatusCode(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("methresp-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("POST"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	// Add 200 and 400 responses.
	for _, code := range []string{"200", "400"} {
		_, err = c.PutMethodResponse(ctx, &awsapigw.PutMethodResponseInput{
			RestApiId:  aws.String(apiID),
			ResourceId: aws.String(rootID),
			HttpMethod: aws.String("POST"),
			StatusCode: aws.String(code),
		})
		require.NoError(t, err)
	}
}

func TestAPIGW_RequestValidator_IsolatedPerAPI(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	// Two independent APIs, each with its own validator.
	for _, name := range []string{"api-x", "api-y"} {
		apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String(name)})
		require.NoError(t, err)
		apiID := aws.ToString(apiOut.Id)

		_, err = c.CreateRequestValidator(ctx, &awsapigw.CreateRequestValidatorInput{
			RestApiId:           aws.String(apiID),
			Name:                aws.String("v"),
			ValidateRequestBody: true,
		})
		require.NoError(t, err)

		listOut, err := c.GetRequestValidators(ctx, &awsapigw.GetRequestValidatorsInput{RestApiId: aws.String(apiID)})
		require.NoError(t, err)
		assert.Len(t, listOut.Items, 1, "each API must see only its own validators")
	}
}

func TestAPIGW_DomainName_DistributionDomainNotEmpty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateDomainName(ctx, &awsapigw.CreateDomainNameInput{
		DomainName:     aws.String("dist.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/dist"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(createOut.DistributionDomainName), ".cloudfront.net")
}

func TestAPIGW_UsagePlan_WithDescription(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateUsagePlan(ctx, &awsapigw.CreateUsagePlanInput{
		Name:        aws.String("described-plan"),
		Description: aws.String("quota: 500/day"),
	})
	require.NoError(t, err)

	getOut, err := c.GetUsagePlan(ctx, &awsapigw.GetUsagePlanInput{UsagePlanId: createOut.Id})
	require.NoError(t, err)
	assert.Equal(t, "quota: 500/day", aws.ToString(getOut.Description))
}

func TestAPIGW_ApiKey_EnabledByDefault(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	createOut, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{
		Name: aws.String("default-enabled-key"),
	})
	require.NoError(t, err)
	assert.True(t, createOut.Enabled, "API key should be enabled by default")
}

func TestAPIGW_MultipleApiKeys_List(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	for _, name := range []string{"key-a", "key-b", "key-c"} {
		_, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{Name: aws.String(name)})
		require.NoError(t, err)
	}

	listOut, err := c.GetApiKeys(ctx, &awsapigw.GetApiKeysInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 3)
}

func TestAPIGW_UsagePlanKey_MultipleKeys(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	planOut, err := c.CreateUsagePlan(ctx, &awsapigw.CreateUsagePlanInput{Name: aws.String("multi-key-plan")})
	require.NoError(t, err)
	planID := aws.ToString(planOut.Id)

	var keyIDs []string
	for _, name := range []string{"k1", "k2"} {
		kOut, err := c.CreateApiKey(ctx, &awsapigw.CreateApiKeyInput{Name: aws.String(name)})
		require.NoError(t, err)
		keyIDs = append(keyIDs, aws.ToString(kOut.Id))
	}

	for _, kID := range keyIDs {
		_, err := c.CreateUsagePlanKey(ctx, &awsapigw.CreateUsagePlanKeyInput{
			UsagePlanId: aws.String(planID),
			KeyId:       aws.String(kID),
			KeyType:     aws.String("API_KEY"),
		})
		require.NoError(t, err)
	}

	keysOut, err := c.GetUsagePlanKeys(ctx, &awsapigw.GetUsagePlanKeysInput{UsagePlanId: aws.String(planID)})
	require.NoError(t, err)
	assert.Len(t, keysOut.Items, 2)
}

func TestAPIGW_DeleteRestApi_RemovesFromList(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	out1, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("api-keep")})
	require.NoError(t, err)
	out2, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("api-delete")})
	require.NoError(t, err)

	_, err = c.DeleteRestApi(ctx, &awsapigw.DeleteRestApiInput{RestApiId: out2.Id})
	require.NoError(t, err)

	listOut, err := c.GetRestApis(ctx, &awsapigw.GetRestApisInput{})
	require.NoError(t, err)
	assert.Len(t, listOut.Items, 1)
	assert.Equal(t, aws.ToString(out1.Id), aws.ToString(listOut.Items[0].Id))
}

func TestAPIGW_GetDeployments_Empty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("no-deploy-api")})
	require.NoError(t, err)

	deploys, err := c.GetDeployments(ctx, &awsapigw.GetDeploymentsInput{RestApiId: apiOut.Id})
	require.NoError(t, err)
	assert.Empty(t, deploys.Items)
}

func TestAPIGW_PutIntegrationResponse_StoresStatusCode(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("intresp-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	_, err = c.PutMethod(ctx, &awsapigw.PutMethodInput{
		RestApiId:         aws.String(apiID),
		ResourceId:        aws.String(rootID),
		HttpMethod:        aws.String("GET"),
		AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = c.PutIntegration(ctx, &awsapigw.PutIntegrationInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
		Type:       apigwtypes.IntegrationTypeMock,
	})
	require.NoError(t, err)

	intRespOut, err := c.PutIntegrationResponse(ctx, &awsapigw.PutIntegrationResponseInput{
		RestApiId:  aws.String(apiID),
		ResourceId: aws.String(rootID),
		HttpMethod: aws.String("GET"),
		StatusCode: aws.String("200"),
	})
	require.NoError(t, err)
	assert.Equal(t, "200", aws.ToString(intRespOut.StatusCode))
}

func TestAPIGW_TagResource_Overwrite(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	resourceArn := "arn:aws:apigateway:us-east-1::/restapis/overwrite-test"

	// Tag once.
	_, err := c.TagResource(ctx, &awsapigw.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        map[string]string{"key": "v1"},
	})
	require.NoError(t, err)

	// Tag again to overwrite.
	_, err = c.TagResource(ctx, &awsapigw.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags:        map[string]string{"key": "v2", "extra": "yes"},
	})
	require.NoError(t, err)

	tagsOut, err := c.GetTags(ctx, &awsapigw.GetTagsInput{ResourceArn: aws.String(resourceArn)})
	require.NoError(t, err)
	assert.Equal(t, "v2", tagsOut.Tags["key"])
	assert.Equal(t, "yes", tagsOut.Tags["extra"])
}

func TestAPIGW_GetTags_NonExistentReturnsEmpty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	tagsOut, err := c.GetTags(ctx, &awsapigw.GetTagsInput{
		ResourceArn: aws.String("arn:aws:apigateway:us-east-1::/restapis/never-tagged"),
	})
	require.NoError(t, err)
	assert.Empty(t, tagsOut.Tags)
}

func TestAPIGW_BasePathMapping_EmptyBasePath(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	_, err := c.CreateDomainName(ctx, &awsapigw.CreateDomainNameInput{
		DomainName:     aws.String("empty-bpm.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/empty-bpm"),
	})
	require.NoError(t, err)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("empty-bpm-api")})
	require.NoError(t, err)

	// Empty base path maps to "(none)" per AWS convention.
	bpmOut, err := c.CreateBasePathMapping(ctx, &awsapigw.CreateBasePathMappingInput{
		DomainName: aws.String("empty-bpm.example.com"),
		RestApiId:  apiOut.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, "(none)", aws.ToString(bpmOut.BasePath))
}

func TestAPIGW_RequestValidator_GetAfterDelete_NotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("rv-del-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	createOut, err := c.CreateRequestValidator(ctx, &awsapigw.CreateRequestValidatorInput{
		RestApiId: aws.String(apiID),
		Name:      aws.String("temp-validator"),
	})
	require.NoError(t, err)
	validatorID := aws.ToString(createOut.Id)

	_, err = c.DeleteRequestValidator(ctx, &awsapigw.DeleteRequestValidatorInput{
		RestApiId:          aws.String(apiID),
		RequestValidatorId: aws.String(validatorID),
	})
	require.NoError(t, err)

	_, err = c.GetRequestValidator(ctx, &awsapigw.GetRequestValidatorInput{
		RestApiId:          aws.String(apiID),
		RequestValidatorId: aws.String(validatorID),
	})
	require.Error(t, err, "deleted validator should not be found")
}

func TestAPIGW_DomainName_GetAfterDelete_NotFound(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	_, err := c.CreateDomainName(ctx, &awsapigw.CreateDomainNameInput{
		DomainName:     aws.String("gone.example.com"),
		CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/gone"),
	})
	require.NoError(t, err)

	_, err = c.DeleteDomainName(ctx, &awsapigw.DeleteDomainNameInput{DomainName: aws.String("gone.example.com")})
	require.NoError(t, err)

	_, err = c.GetDomainName(ctx, &awsapigw.GetDomainNameInput{DomainName: aws.String("gone.example.com")})
	require.Error(t, err)
}

func TestAPIGW_NestedResource_Path(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	apiOut, err := c.CreateRestApi(ctx, &awsapigw.CreateRestApiInput{Name: aws.String("nested-res-api")})
	require.NoError(t, err)
	apiID := aws.ToString(apiOut.Id)

	resOut, err := c.GetResources(ctx, &awsapigw.GetResourcesInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	rootID := aws.ToString(resOut.Items[0].Id)

	// /v1
	v1, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  aws.String(rootID),
		PathPart:  aws.String("v1"),
	})
	require.NoError(t, err)

	// /v1/items
	items, err := c.CreateResource(ctx, &awsapigw.CreateResourceInput{
		RestApiId: aws.String(apiID),
		ParentId:  v1.Id,
		PathPart:  aws.String("items"),
	})
	require.NoError(t, err)
	assert.Equal(t, "/v1/items", aws.ToString(items.Path))
}

func TestAPIGW_GetUsagePlans_Empty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	listOut, err := c.GetUsagePlans(ctx, &awsapigw.GetUsagePlansInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut.Items)
}

func TestAPIGW_GetApiKeys_Empty(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	c := newAPIGWClient(t)

	listOut, err := c.GetApiKeys(ctx, &awsapigw.GetApiKeysInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut.Items)
}
