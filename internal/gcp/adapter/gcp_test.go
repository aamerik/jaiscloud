package gcp_test

import (
	"bytes"
	"compress/gzip"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/model"
)

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestGCPAdapter_GzipJSONBody(t *testing.T) {
	a := gcp.New()
	payload := []byte(`{"name":"gzip-bucket"}`)
	r := httptest.NewRequest("POST", "/storage/v1/b?project=test-project", bytes.NewReader(payload))
	r.Header.Set("Content-Encoding", "gzip")

	nr, _, err := a.DetectAndDecode(r, gzipBytes(t, payload))
	if err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	body, ok := nr.Params["body"].(map[string]any)
	if !ok {
		t.Fatalf("expected decoded JSON body, got %#v", nr.Params["body"])
	}
	if body["name"] != "gzip-bucket" {
		t.Errorf("expected name gzip-bucket, got %v", body["name"])
	}
	if got := r.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("expected transport Content-Encoding cleared, got %q", got)
	}
}

func TestGCPAdapter_GzipMediaUploadNotDecoded(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=media", nil)
	r.Header.Set("Content-Encoding", "gzip")

	// uploadType=media treats Content-Encoding as the object's own encoding, so
	// the body must be left untouched and the header preserved.
	_, _, _ = a.DetectAndDecode(r, gzipBytes(t, []byte("object bytes")))
	if got := r.Header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("expected media Content-Encoding preserved, got %q", got)
	}
}

func TestGCPAdapter_GzipMalformedBody(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/storage/v1/b?project=test-project", nil)
	r.Header.Set("Content-Encoding", "gzip")

	_, _, err := a.DetectAndDecode(r, []byte("not actually gzip"))
	if err == nil {
		t.Fatal("expected malformed gzip error")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.HTTPStatus != 400 {
		t.Errorf("expected 400, got %d", pe.HTTPStatus)
	}
}

func TestGCPAdapter_GzipStreamingMultipartWrapped(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=multipart", bytes.NewReader(gzipBytes(t, []byte("x"))))
	r.Header.Set("Content-Encoding", "gzip")

	// The codec may reject the tiny body, but the transport must already be
	// unwrapped: header cleared and length unknown.
	_, _, _ = a.DetectAndDecode(r, nil)
	if got := r.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("expected multipart Content-Encoding cleared, got %q", got)
	}
	if r.ContentLength != -1 {
		t.Errorf("expected ContentLength -1 for streamed body, got %d", r.ContentLength)
	}
}

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
