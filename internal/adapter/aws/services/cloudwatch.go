package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// CloudWatchCodec handles the CloudWatch Query/XML wire protocol.
// AWS SDK v2 sends requests as either:
//   - POST / with body Action=GetMetricStatistics&Namespace=... (SDK v1 form)
//   - POST /service/GraniteServiceVersion20100801/operation/GetMetricStatistics
//     with body Namespace=... (SDK v2 Granite form — Action embedded in URL).
type CloudWatchCodec struct{}

var _ adapter.Codec = (*CloudWatchCodec)(nil)

func (c *CloudWatchCodec) ServiceName() string { return "monitoring" }

func (c *CloudWatchCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	// Extract Action: first from query/body, then from Granite URL path.
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		action = parseGraniteActionFromPath(r.URL.Path)
	}
	if action == "" {
		return nil, fmt.Errorf("cloudwatch: missing Action (neither Action= param nor Granite URL)")
	}

	params := flattenQueryValues(values)
	return &model.NormalizedRequest{
		Service: "monitoring",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *CloudWatchCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := buildCloudWatchXML(resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *CloudWatchCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">`+
			`<Error><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>jaiscloud-cloudwatch</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

// parseGraniteActionFromPath extracts the action name from an SDK v2 Granite URL:
// /service/<GraniteServiceVersion>/operation/<Action>.
// Returns "" if the path doesn't match that shape.
func parseGraniteActionFromPath(path string) string {
	const opMarker = "/operation/"
	if !strings.HasPrefix(path, "/service/") {
		return ""
	}
	i := strings.Index(path, opMarker)
	if i < 0 {
		return ""
	}
	return path[i+len(opMarker):]
}

// buildCloudWatchXML renders a CloudWatch Query-protocol XML response.
// Provider handlers set data["__action__"] to name the wrapper.
func buildCloudWatchXML(data map[string]any) string {
	if data == nil {
		return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
	}
	action, _ := data["__action__"].(string)
	if action == "" {
		action = "CloudWatch"
	}

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	fmt.Fprintf(&b, `<%sResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">`, action)
	b.WriteString(`<` + action + `Result>`)
	writeCWResult(&b, data)
	b.WriteString(`</` + action + `Result>`)
	fmt.Fprintf(&b, `<ResponseMetadata><RequestId>jaiscloud-cloudwatch</RequestId></ResponseMetadata>`)
	fmt.Fprintf(&b, `</%sResponse>`, action)
	return b.String()
}

func writeCWResult(b *strings.Builder, data map[string]any) {
	for k, v := range data {
		if strings.HasPrefix(k, "__") {
			continue
		}
		writeCWElement(b, k, v)
	}
}

func writeCWElement(b *strings.Builder, name string, v any) {
	switch val := v.(type) {
	case nil:
		// omit nil values
	case string:
		fmt.Fprintf(b, `<%s>%s</%s>`, name, xmlEscape(val), name)
	case bool, int, int64, float64:
		fmt.Fprintf(b, `<%s>%v</%s>`, name, val, name)
	case []any:
		fmt.Fprintf(b, `<%s>`, name)
		for _, item := range val {
			b.WriteString(`<member>`)
			switch m := item.(type) {
			case map[string]any:
				for k2, v2 := range m {
					writeCWElement(b, k2, v2)
				}
			default:
				b.WriteString(xmlEscape(fmt.Sprintf("%v", m)))
			}
			b.WriteString(`</member>`)
		}
		fmt.Fprintf(b, `</%s>`, name)
	case map[string]any:
		fmt.Fprintf(b, `<%s>`, name)
		for k2, v2 := range val {
			writeCWElement(b, k2, v2)
		}
		fmt.Fprintf(b, `</%s>`, name)
	default:
		fmt.Fprintf(b, `<%s>%s</%s>`, name, xmlEscape(fmt.Sprintf("%v", val)), name)
	}
}
