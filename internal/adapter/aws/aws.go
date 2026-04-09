package aws

import (
	"fmt"
	"net/http"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// AWSAdapter routes incoming HTTP requests to the appropriate service codec.
type AWSAdapter struct {
	codecs map[string]adapter.Codec // keyed by service name, e.g. "sqs"
}

func NewAdapter(codecs map[string]adapter.Codec) *AWSAdapter {
	return &AWSAdapter{codecs: codecs}
}

// CodecFor returns the codec for the given service name.
func (a *AWSAdapter) CodecFor(service string) (adapter.Codec, error) {
	c, ok := a.codecs[service]
	if !ok {
		return nil, model.NewProviderError("UnknownService",
			fmt.Sprintf("no codec for service %q", service), 400)
	}
	return c, nil
}

// DetectAndDecode identifies the service, selects the codec, and decodes the request.
func (a *AWSAdapter) DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	service, _ := DetectService(r, body)
	if service == "" {
		return nil, nil, model.NewProviderError("UnknownService", "cannot detect target AWS service", 400)
	}
	codec, err := a.CodecFor(service)
	if err != nil {
		return nil, nil, err
	}
	nr, err := codec.Decode(r, body)
	if err != nil {
		return nil, nil, err
	}
	return nr, codec, nil
}
