package aws

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/aws/arn"
	"jaiscloud/internal/aws/identity"
	"jaiscloud/internal/model"
)

// AWSAdapter routes incoming HTTP requests to the appropriate service codec.
// Implements adapter.CloudAdapter.
type AWSAdapter struct {
	codecs map[string]adapter.Codec // keyed by service name, e.g. "sqs"
}

func NewAdapter(codecs map[string]adapter.Codec) *AWSAdapter {
	return &AWSAdapter{codecs: codecs}
}

// Cloud implements adapter.CloudAdapter.
func (a *AWSAdapter) Cloud() model.Cloud { return model.CloudAWS }

// CodecFor returns the codec for the given service name.
func (a *AWSAdapter) CodecFor(service string) (adapter.Codec, error) {
	c, ok := a.codecs[service]
	if !ok {
		return nil, model.NewProviderError("UnknownService",
			fmt.Sprintf("no codec for service %q", service), 400)
	}
	return c, nil
}

// ServiceToProvider implements adapter.CloudAdapter.
// Looks up the provider registry prefix for an AWS wire service name.
// Driven by awsServices in services.go — no hardcoded cases here.
func (a *AWSAdapter) ServiceToProvider(service string) string {
	if prefix, ok := serviceProviderMap[service]; ok {
		return prefix
	}
	return service
}

// DetectAndDecode implements adapter.CloudAdapter.
// Identifies the service, selects the codec, and decodes the request.
func (a *AWSAdapter) DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	service, source := DetectService(r, body)
	if service == "" {
		slog.Error("aws: service detection failed",
			"method", r.Method,
			"path", r.URL.Path,
			"x_amz_target", r.Header.Get("X-Amz-Target"),
			"sigv4_service", sigv4Service(r.Header.Get("Authorization")),
			"auth_prefix", authPrefix(r.Header.Get("Authorization")),
			"content_type", r.Header.Get("Content-Type"),
		)
		return nil, nil, model.NewProviderError("UnknownService", "cannot detect target AWS service", 400)
	}
	slog.Debug("aws: service detected", "service", service, "source", source) // success path — debug only

	codec, err := a.CodecFor(service)
	if err != nil {
		slog.Error("aws: no codec for service", "service", service, "err", err)
		return nil, nil, err
	}
	nr, err := codec.Decode(r, body)
	if err != nil {
		slog.Error("aws: decode failed", "service", service, "err", err,
			"method", r.Method, "path", r.URL.Path)
		return nil, codec, err
	}
	return nr, codec, nil
}

// EnrichRequest implements adapter.CloudAdapter.
// Extracts account ID, region, and access key from SigV4/SigV2 credentials.
func (a *AWSAdapter) EnrichRequest(r *http.Request, defaultRegion, defaultAccountID string) (region, accountID, accessKey string) {
	ident := identity.FromRequest(r)
	accountID = ident.AccountID
	region = identity.NormaliseRegion(ident.Region, defaultRegion)
	accessKey = ident.AccessKey
	return
}

// ResourceIDFor implements adapter.CloudAdapter.
// Returns an AWS ARN formatter for the given region and account.
func (a *AWSAdapter) ResourceIDFor(region, accountID string) func(resourceType, name string) string {
	return arn.ResourceID(region, accountID)
}

// authPrefix returns only the first 40 chars of the Authorization header for
// safe logging — enough to identify the scheme without leaking credentials.
func authPrefix(auth string) string {
	if len(auth) > 40 {
		return auth[:40] + "..."
	}
	return auth
}

// sigv4Service extract service name from a sigV4 authorization header.
// returns "" if the header is not in the expected format.
// Format: AWS4-HMAC-SHA256 Credential=<key>/<date>/<region>/<service>/aws4_request, SignedHeaders=..., Signature=...
func sigv4Service(auth string) string {
	// SigV4 Authorization header format is:
	// "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/service/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=..."
	// We want to extract the "service" segment from the Credential scope.
	const prefix = "Credential="
	idx := strings.Index(auth, prefix)
	if idx < 0 {
		return ""
	}
	cred := auth[idx+len(prefix):]
	if end := strings.Index(cred, ","); end >= 0 {
		cred = cred[:end]
	}
	// cred = <access_key>/<date>/<region>/<service>/aws4_request
	parts := strings.Split(cred, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}
