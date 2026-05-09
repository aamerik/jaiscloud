package services

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func apigwParts(path string) []string {
	return strings.Split(strings.TrimPrefix(path, "/"), "/")
}

func TestApigwDetectAction_RestApis(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"POST", "/restapis", "CreateRestApi"},
		{"GET", "/restapis", "GetRestApis"},
		{"GET", "/restapis/abc123", "GetRestApi"},
		{"PATCH", "/restapis/abc123", "UpdateRestApi"},
		{"DELETE", "/restapis/abc123", "DeleteRestApi"},
	}
	for _, tc := range tests {
		params := map[string]any{}
		got := apigwDetectAction(tc.method, apigwParts(tc.path), params)
		assert.Equal(t, tc.want, got, tc.path)
	}
}

func TestApigwDetectAction_Resources(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"GET", "/restapis/a1/resources", "GetResources"},
		{"GET", "/restapis/a1/resources/r1", "GetResource"},
		{"POST", "/restapis/a1/resources/r1", "CreateResource"},
		{"DELETE", "/restapis/a1/resources/r1", "DeleteResource"},
	}
	for _, tc := range tests {
		params := map[string]any{}
		got := apigwDetectAction(tc.method, apigwParts(tc.path), params)
		assert.Equal(t, tc.want, got, tc.path)
	}
}

func TestApigwDetectAction_Methods(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"PUT", "/restapis/a1/resources/r1/methods/GET", "PutMethod"},
		{"GET", "/restapis/a1/resources/r1/methods/GET", "GetMethod"},
		{"DELETE", "/restapis/a1/resources/r1/methods/GET", "DeleteMethod"},
	}
	for _, tc := range tests {
		params := map[string]any{}
		got := apigwDetectAction(tc.method, apigwParts(tc.path), params)
		assert.Equal(t, tc.want, got, tc.path)
		// httpMethod param should be injected
		assert.Equal(t, "GET", params["httpMethod"])
	}
}

func TestApigwDetectAction_Integration(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"PUT", "/restapis/a1/resources/r1/methods/POST/integration", "PutIntegration"},
		{"GET", "/restapis/a1/resources/r1/methods/POST/integration", "GetIntegration"},
		{"DELETE", "/restapis/a1/resources/r1/methods/POST/integration", "DeleteIntegration"},
		{"PUT", "/restapis/a1/resources/r1/methods/POST/integration/responses/200", "PutIntegrationResponse"},
		{"PUT", "/restapis/a1/resources/r1/methods/POST/responses/200", "PutMethodResponse"},
	}
	for _, tc := range tests {
		params := map[string]any{}
		got := apigwDetectAction(tc.method, apigwParts(tc.path), params)
		assert.Equal(t, tc.want, got, tc.path)
	}
}

func TestApigwDetectAction_Deployments(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"POST", "/restapis/a1/deployments", "CreateDeployment"},
		{"GET", "/restapis/a1/deployments", "GetDeployments"},
		{"DELETE", "/restapis/a1/deployments/d1", "DeleteDeployment"},
	}
	for _, tc := range tests {
		params := map[string]any{}
		got := apigwDetectAction(tc.method, apigwParts(tc.path), params)
		assert.Equal(t, tc.want, got, tc.path)
	}
}

func TestApigwDetectAction_Stages(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"POST", "/restapis/a1/stages", "CreateStage"},
		{"GET", "/restapis/a1/stages", "GetStages"},
		{"GET", "/restapis/a1/stages/prod", "GetStage"},
		{"PATCH", "/restapis/a1/stages/prod", "UpdateStage"},
		{"DELETE", "/restapis/a1/stages/prod", "DeleteStage"},
	}
	for _, tc := range tests {
		params := map[string]any{}
		got := apigwDetectAction(tc.method, apigwParts(tc.path), params)
		assert.Equal(t, tc.want, got, tc.path)
		if len(apigwParts(tc.path)) >= 4 {
			assert.Equal(t, "prod", params["stageName"])
		}
	}
}

func TestApigwDetectAction_PathParamsInjected(t *testing.T) {
	params := map[string]any{}
	apigwDetectAction("GET", apigwParts("/restapis/myapi/resources/res42"), params)
	assert.Equal(t, "myapi", params["restApiId"])
	assert.Equal(t, "res42", params["resourceId"])
}
