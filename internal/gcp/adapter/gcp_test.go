package gcp_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/model"
)

func TestGCPAdapter_Cloud(t *testing.T) {
	a := gcp.New()
	if a.Cloud() != model.CloudGCP {
		t.Errorf("expected CloudGCP, got %s", a.Cloud())
	}
}

func TestGCPAdapter_DetectAndDecode_Storage(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("GET", "/storage/v1/b/my-bucket/o", nil)
	nr, codec, err := a.DetectAndDecode(r, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if nr.Service != "storage" {
		t.Errorf("expected service storage, got %q", nr.Service)
	}
	if nr.Action != "ObjectsList" {
		t.Errorf("expected action ObjectsList, got %q", nr.Action)
	}
	if codec == nil {
		t.Fatal("expected non-nil codec")
	}
	if got := a.ServiceToProvider("storage"); got != "Storage" {
		t.Errorf("expected provider prefix Storage, got %q", got)
	}
}

func TestGCPAdapter_DetectAndDecode_Unknown(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/", nil)
	_, _, err := a.DetectAndDecode(r, nil)
	if err == nil {
		t.Fatal("expected UnknownService error")
	}
	if !strings.Contains(err.Error(), "GCP") {
		t.Errorf("expected GCP detection error, got %v", err)
	}
}
