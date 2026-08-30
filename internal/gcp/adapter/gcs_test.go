package gcp

import (
	"net/http/httptest"
	"testing"

	"encoding/json"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

func TestGCSCodecDownloadForcesMedia(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("GET", "/download/storage/v1/b/bkt/o/obj", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsGetMedia" {
		t.Errorf("expected ObjectsGetMedia, got %q", nr.Action)
	}
}

func TestGCSCodecResumableStartCapturesContentType(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=resumable&name=obj", nil)
	r.Header.Set("X-Upload-Content-Type", "text/plain")
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertStartResumable" {
		t.Fatalf("expected ObjectsInsertStartResumable, got %q", nr.Action)
	}
	if ct, _ := nr.Params[wire.ContentTypeKey].(string); ct != "text/plain" {
		t.Errorf("expected content type text/plain, got %q", ct)
	}
}

func TestGCSCodecResumablePutNoContentTypeOverride(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/upload/storage/v1/b/bkt/o?uploadType=resumable&upload_id=1", nil)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Content-Range", "bytes 0-4/*")
	nr, err := c.Decode(r, []byte("chunk"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := nr.Params[wire.ContentTypeKey]; ok {
		t.Error("resumable PUT must not set object content type (chunk Content-Type is the media type)")
	}
	if cr, _ := nr.Params["contentRange"].(string); cr != "bytes 0-4/*" {
		t.Errorf("expected contentRange bytes 0-4/*, got %q", cr)
	}
}

func TestSplitEscapedPreservesEncodedSlash(t *testing.T) {
	// %2F within a segment (a slash in an object name) survives as part of the name.
	seg := splitEscaped("/storage/v1/b/bkt/o/a%2Fb/c")
	if len(seg) != 7 || seg[5] != "a/b" {
		t.Errorf("expected object name 'a/b' at seg[5], got %v", seg)
	}
}

func TestGCSCodecEncodeError(t *testing.T) {
	c := &GCSCodec{}
	perr := model.NewProviderError("NotFound", "object not found", 404)
	status, _, body := c.EncodeError(nil, perr)
	if status != 404 {
		t.Fatalf("expected status 404, got %d", status)
	}
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	errObj, _ := env["error"].(map[string]any)
	errorsArr, ok := errObj["errors"].([]any)
	if !ok || len(errorsArr) == 0 {
		t.Fatal("expected non-empty errors[] array in GCS error envelope")
	}
	first := errorsArr[0].(map[string]any)
	if first["reason"] != "notFound" {
		t.Errorf("expected reason notFound, got %v", first["reason"])
	}
	if _, ok := errObj["status"]; ok {
		t.Error("GCS error envelope must not contain a 'status' field")
	}
}
