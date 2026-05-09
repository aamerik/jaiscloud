package azure_test

import (
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/azure/adapter"
	"jaiscloud/internal/model"
)

func TestAzureAdapter_Cloud(t *testing.T) {
	a := azure.New()
	if a.Cloud() != model.CloudAzure {
		t.Errorf("expected CloudAzure, got %s", a.Cloud())
	}
}

func TestAzureAdapter_DetectAndDecode_ReturnsNotImplemented(t *testing.T) {
	a := azure.New()
	r := httptest.NewRequest("POST", "/", nil)
	_, _, err := a.DetectAndDecode(r, nil)
	if err == nil {
		t.Fatal("expected UnsupportedOperation error")
	}
}
