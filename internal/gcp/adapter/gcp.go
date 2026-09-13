// Package gcp implements the GCP CloudAdapter.
//
// GCP identifies requests by URL path (e.g. /storage/v1/b/{bucket}/o, or
// /v1/projects/{project}/topics/{topic}) rather than by a SigV4 header scope.
// Identity comes from an OAuth2 bearer token plus the project ID embedded in
// the path — see internal/gcp/identity.
package gcp

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/gcp/identity"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
)

// GCPAdapter routes incoming HTTP requests to the appropriate service codec.
// Implements adapter.CloudAdapter.
type GCPAdapter struct {
	codecs         map[string]adapter.Codec
	serviceAccount string // default SA identity when the token carries none
}

// New returns a GCPAdapter with the default codec set and a blank default
// service account (identity falls back to identity.DefaultServiceAccount).
func New() *GCPAdapter {
	return NewAdapter("")
}

// NewAdapter returns a GCPAdapter with the codecs derived from gcpServices and
// the given default service-account identity.
func NewAdapter(serviceAccount string) *GCPAdapter {
	codecs := make(map[string]adapter.Codec, len(gcpServices))
	for _, svc := range gcpServices {
		if svc.Codec != nil {
			codecs[svc.ServiceName] = svc.Codec()
		}
	}
	return &GCPAdapter{
		codecs:         codecs,
		serviceAccount: serviceAccount,
	}
}

// Cloud implements adapter.CloudAdapter.
func (a *GCPAdapter) Cloud() model.Cloud { return model.CloudGCP }

// CodecFor returns the codec for the given service name.
func (a *GCPAdapter) CodecFor(service string) (adapter.Codec, error) {
	c, ok := a.codecs[service]
	if !ok {
		return nil, model.NewProviderError("UnknownService",
			fmt.Sprintf("no codec for service %q", service), 404)
	}
	return c, nil
}

// ServiceToProvider implements adapter.CloudAdapter.
// Looks up the provider registry prefix for a GCP wire service name.
// Driven by gcpServices in services.go — no hardcoded cases here.
func (a *GCPAdapter) ServiceToProvider(service string) string {
	if prefix, ok := serviceProviderMap[service]; ok {
		return prefix
	}
	return service
}

// DetectAndDecode implements adapter.CloudAdapter.
// Identifies the service from the URL path, selects the codec, and decodes.
func (a *GCPAdapter) DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	body, err := decodeGzippedBody(r, body)
	if err != nil {
		return nil, nil, err
	}
	service, source := DetectService(r)
	if service == "" {
		return nil, nil, model.NewProviderError("UnknownService", "cannot detect target GCP service", 404)
	}
	_ = source

	codec, err := a.CodecFor(service)
	if err != nil {
		return nil, nil, err
	}
	nr, err := codec.Decode(r, body)
	if err != nil {
		return nil, codec, err
	}
	return nr, codec, nil
}

// decodeGzippedBody transparently decompresses a request whose
// Content-Encoding is gzip. Google's JSON APIs accept gzip request bodies, and
// the official SDKs rely on it: the Java google-http-client enables gzip by
// default for every request body, so GCS bucket/object metadata writes,
// multipart uploads and BigQuery jobs all arrive gzip-encoded. Without this a
// JSON body fails to parse ("malformed JSON body", 400).
//
// GCS media uploads (uploadType=media) are deliberately excluded: there
// Content-Encoding describes the stored object's own encoding (transcoding),
// not the transport, so the bytes must reach the provider untouched.
//
// For non-streaming requests the decoded bytes are returned; for streaming
// requests (e.g. multipart/related uploads) the request body is wrapped so the
// codec reads decompressed bytes while Close still closes the original body.
func decodeGzippedBody(r *http.Request, body []byte) ([]byte, error) {
	if !strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "gzip") {
		return body, nil
	}
	if r.URL.Query().Get("uploadType") == "media" {
		return body, nil
	}
	r.Header.Del("Content-Encoding")

	// Non-streaming: the gateway already buffered the body.
	if body != nil {
		if len(body) == 0 {
			return body, nil
		}
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed gzip request body", 400)
		}
		defer zr.Close()
		out, err := io.ReadAll(zr)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed gzip request body", 400)
		}
		return out, nil
	}

	// Streaming: leave r.Body readable by the codec, decompressed.
	if r.Body != nil {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed gzip request body", 400)
		}
		r.Body = &gzipBody{Reader: zr, underlying: r.Body}
		r.ContentLength = -1
	}
	return body, nil
}

// gzipBody adapts a gzip.Reader over an underlying request body so closing it
// releases both (gzip.Reader.Close does not close its source).
type gzipBody struct {
	*gzip.Reader
	underlying io.ReadCloser
}

func (g *gzipBody) Close() error {
	err := g.Reader.Close()
	if cerr := g.underlying.Close(); err == nil {
		err = cerr
	}
	return err
}

// EnrichRequest implements adapter.CloudAdapter.
// Resolves project ID (path → token → config default) and returns the bearer
// token as the access key. The service-account email is decoded from the token
// when present (exposed to providers via identity, not here).
func (a *GCPAdapter) EnrichRequest(r *http.Request, defaultRegion, defaultAccountID string) (region, accountID, accessKey string) {
	ident := identity.FromRequest(r)

	// The identity package falls back to its own hardcoded default. Prefer the
	// configured project (defaultAccountID) when the request carried no explicit
	// project in the URL path or bearer token.
	project := ident.ProjectID
	if ident.Source == identity.SourceDefault ||
		(ident.Source == identity.SourceBearer && ident.ProjectID == identity.DefaultProjectID) {
		if defaultAccountID != "" {
			project = defaultAccountID
		}
	}

	region = defaultRegion
	if region == "" {
		region = "global"
	}
	return region, project, ident.AccessKey
}

// ResourceIDFor implements adapter.CloudAdapter.
// Returns a GCP resource-name formatter for the given project.
func (a *GCPAdapter) ResourceIDFor(_, accountID string) func(resourceType, name string) string {
	return resource.ResourceID(accountID)
}
