package services

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	smithycbor "github.com/aws/smithy-go/encoding/cbor"
	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/reqctx"
)

// CloudWatchCodec handles two CloudWatch wire protocols:
//
//  1. Legacy awsQuery (SDK v1 or older v2): form-encoded body with Action= param,
//     or Granite URL path with form-encoded body.
//
//  2. smithy-rpc-v2-cbor (SDK v2 cloudwatch >= v1.57): smithy-protocol header
//     set to "rpc-v2-cbor", body is CBOR-encoded.
type CloudWatchCodec struct{}

var _ adapter.Codec = (*CloudWatchCodec)(nil)

func (c *CloudWatchCodec) ServiceName() string { return "monitoring" }

func (c *CloudWatchCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	if isCBORRequest(r) {
		return c.decodeCBOR(r, body)
	}
	// Legacy: AWS Query / Granite path with form-encoded body.
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

func isCBORRequest(r *http.Request) bool {
	return r.Header.Get("smithy-protocol") == "rpc-v2-cbor" ||
		strings.HasPrefix(r.Header.Get("Content-Type"), "application/cbor")
}

func (c *CloudWatchCodec) decodeCBOR(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	action := parseGraniteActionFromPath(r.URL.Path)
	if action == "" {
		return nil, fmt.Errorf("cloudwatch: CBOR request missing Granite path action")
	}

	params := make(map[string]any)
	if len(body) > 0 {
		v, err := smithycbor.Decode(body)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch: failed to decode CBOR body: %w", err)
		}
		if m, ok := v.(smithycbor.Map); ok {
			flattenSmithyCBORMap(m, "", params)
		}
	}

	nr := &model.NormalizedRequest{
		Service: "monitoring",
		Action:  action,
		Params:  params,
		Raw:     r,
	}
	nr.SetMeta("protocol", "cbor")
	return nr, nil
}

// flattenSmithyCBORMap flattens a nested CBOR map into dot-notation params
// matching the Query-protocol format expected by provider handlers.
// Example: {"MetricData":[{"MetricName":"X"}]} → {"MetricData.member.1.MetricName":"X"}
func flattenSmithyCBORMap(m smithycbor.Map, prefix string, out map[string]any) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		flattenSmithyCBORValue(key, v, out)
	}
}

func flattenSmithyCBORValue(key string, v smithycbor.Value, out map[string]any) {
	switch val := v.(type) {
	case smithycbor.Map:
		flattenSmithyCBORMap(val, key, out)
	case smithycbor.List:
		for i, item := range val {
			flattenSmithyCBORValue(key+".member."+strconv.Itoa(i+1), item, out)
		}
	case smithycbor.String:
		out[key] = string(val)
	case smithycbor.Float64:
		out[key] = float64(val)
	case smithycbor.Float32:
		out[key] = float64(val)
	case smithycbor.Uint:
		out[key] = float64(val)
	case smithycbor.NegInt:
		out[key] = -float64(val)
	case smithycbor.Bool:
		out[key] = bool(val)
	case *smithycbor.Tag:
		// CBOR tag 1 = epoch seconds timestamp
		if val.ID == 1 {
			if t, err := smithycbor.AsTime(v); err == nil {
				out[key] = t.UTC().Format(time.RFC3339)
			}
		}
	case *smithycbor.Nil:
		// omit nil values
	}
}

func (c *CloudWatchCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	if nr != nil && nr.GetMeta("protocol") == "cbor" {
		return encodeCBORResponse(resp.HTTPStatus, resp.Data)
	}
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	reqID := reqctx.GetRequestID(nr.Raw.Context())
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	body := buildCloudWatchXML(resp.Data, reqID)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *CloudWatchCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	if nr != nil && nr.GetMeta("protocol") == "cbor" {
		h := http.Header{}
		h.Set("Content-Type", "application/cbor")
		h.Set("Smithy-Protocol", "rpc-v2-cbor")
		errMap := smithycbor.Map{
			"__type":  smithycbor.String(perr.Code),
			"message": smithycbor.String(perr.Message),
		}
		return perr.HTTPStatus, h, smithycbor.Encode(errMap)
	}
	reqID := ""
	if nr != nil && nr.Raw != nil {
		reqID = reqctx.GetRequestID(nr.Raw.Context())
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">`+
			`<Error><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>%s</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message), reqID,
	)
	return perr.HTTPStatus, h, []byte(body)
}

func encodeCBORResponse(status int, data map[string]any) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/cbor")
	h.Set("Smithy-Protocol", "rpc-v2-cbor")

	var cborVal smithycbor.Value
	if data != nil {
		cborVal = goToSmithyCBOR(data)
	} else {
		cborVal = smithycbor.Map{}
	}
	return status, h, smithycbor.Encode(cborVal)
}

// goToSmithyCBOR converts a Go value to a smithy-go CBOR Value for wire encoding.
// - float64 integers encode as Uint/NegInt (compatible with AsInt32/AsInt64 deserializers)
// - RFC3339 strings encode as CBOR Tag(1, Float64(epochSeconds)) for time.Time deserializers
// - map keys beginning with "__" are stripped (internal provider metadata)
func goToSmithyCBOR(v any) smithycbor.Value {
	switch val := v.(type) {
	case nil:
		return &smithycbor.Nil{}
	case bool:
		return smithycbor.Bool(val)
	case float64:
		if isIntegerFloat(val) {
			if val >= 0 {
				return smithycbor.Uint(uint64(val))
			}
			return smithycbor.NegInt(uint64(-val))
		}
		return smithycbor.Float64(val)
	case float32:
		return goToSmithyCBOR(float64(val))
	case int:
		if val >= 0 {
			return smithycbor.Uint(uint64(val))
		}
		return smithycbor.NegInt(uint64(-val))
	case int64:
		if val >= 0 {
			return smithycbor.Uint(uint64(val))
		}
		return smithycbor.NegInt(uint64(-val))
	case uint64:
		return smithycbor.Uint(val)
	case string:
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return &smithycbor.Tag{ID: 1, Value: smithycbor.Float64(float64(t.UnixMilli()) / 1000)}
		}
		return smithycbor.String(val)
	case []any:
		list := make(smithycbor.List, len(val))
		for i, item := range val {
			list[i] = goToSmithyCBOR(item)
		}
		return list
	case []string:
		list := make(smithycbor.List, len(val))
		for i, item := range val {
			list[i] = goToSmithyCBOR(item)
		}
		return list
	case []float64:
		list := make(smithycbor.List, len(val))
		for i, item := range val {
			list[i] = goToSmithyCBOR(item)
		}
		return list
	case map[string]any:
		m := smithycbor.Map{}
		for k, vv := range val {
			if strings.HasPrefix(k, "__") {
				continue
			}
			m[k] = goToSmithyCBOR(vv)
		}
		return m
	default:
		return smithycbor.String(fmt.Sprintf("%v", val))
	}
}

func isIntegerFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v == math.Trunc(v)
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
func buildCloudWatchXML(data map[string]any, reqID string) string {
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
	fmt.Fprintf(&b, `<ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata>`, xmlEscape(reqID))
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
