package aws

import (
	"fmt"
	"log/slog"
	"net/http"

	"jaiscloud/internal/adapter"
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
			"authorization_prefix", authPrefix(r.Header.Get("Authorization")),
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

// authPrefix returns only the first 40 chars of the Authorization header for
// safe logging — enough to identify the scheme without leaking credentials.
func authPrefix(auth string) string {
	if len(auth) > 40 {
		return auth[:40] + "..."
	}
	return auth
}
