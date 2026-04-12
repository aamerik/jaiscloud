package gcp_test

import (
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/adapter/gcp"
	"jaiscloud/internal/model"
)

func TestGCPAdapter_Cloud(t *testing.T) {
	a := gcp.New()
	if a.Cloud() != model.CloudGCP {
		t.Errorf("expected CloudGCP, got %s", a.Cloud())
	}
}

func TestGCPAdapter_DetectAndDecode_ReturnsNotImplemented(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/", nil)
	_, _, err := a.DetectAndDecode(r, nil)
	if err == nil {
		t.Fatal("expected UnsupportedOperation error")
	}
}
