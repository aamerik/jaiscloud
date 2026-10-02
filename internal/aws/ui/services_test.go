package ui

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/model"
)

func servicesFor(t *testing.T, cloud model.Cloud, providers *AWSProviders) ServicesResponse {
	t.Helper()
	handler := buildServicesHandler(providers, cloud)
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest("GET", "/api/ui/v1/services", nil))

	var resp ServicesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func TestServicesHandler_NonAWSCloudIsEmpty(t *testing.T) {
	for _, cloud := range []model.Cloud{model.CloudGCP, model.CloudAzure} {
		resp := servicesFor(t, cloud, &AWSProviders{Queue: &queue.QueueProvider{}})
		if len(resp.Services) != 0 {
			t.Fatalf("cloud %q: expected no services, got %d", cloud, len(resp.Services))
		}
	}
}

func TestServicesHandler_OnlyWiredProvidersListed(t *testing.T) {
	resp := servicesFor(t, model.CloudAWS, &AWSProviders{Queue: &queue.QueueProvider{}})
	if len(resp.Services) != 1 || resp.Services[0].ID != "sqs" {
		t.Fatalf("expected [sqs], got %+v", resp.Services)
	}
	if resp.Services[0].Category == "" || resp.Services[0].RootPath == "" {
		t.Fatalf("descriptor missing category/rootPath: %+v", resp.Services[0])
	}
}

func TestServicesHandler_NoProvidersIsEmpty(t *testing.T) {
	resp := servicesFor(t, model.CloudAWS, &AWSProviders{})
	if len(resp.Services) != 0 {
		t.Fatalf("expected no services, got %d", len(resp.Services))
	}
}

func TestServicesHandler_TiersAreReported(t *testing.T) {
	resp := servicesFor(t, model.CloudAWS, &AWSProviders{
		Queue:   &queue.QueueProvider{},
		Compute: &compute.ComputeProvider{},
	})

	byID := map[string]ServiceDescriptor{}
	for _, service := range resp.Services {
		byID[service.ID] = service
	}

	if got := byID["sqs"].Tier; got != TierFull {
		t.Fatalf("sqs tier = %q, want %q", got, TierFull)
	}
	if got := byID["ec2"].Tier; got != TierMetadata {
		t.Fatalf("ec2 tier = %q, want %q", got, TierMetadata)
	}
	if byID["ec2"].Note == "" {
		t.Fatal("metadata service should carry a note")
	}
}
