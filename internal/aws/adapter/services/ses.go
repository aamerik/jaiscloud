package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/reqctx"
)

// SESCodec handles the SES v1 wire protocol (Query/XML).
type SESCodec struct{}

var _ adapter.Codec = (*SESCodec)(nil)

func (c *SESCodec) ServiceName() string { return "email" }

func (c *SESCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter")
	}

	params := map[string]any{}
	for k, vs := range values {
		if len(vs) == 1 {
			params[k] = vs[0]
		} else if len(vs) > 1 {
			params[k] = vs
		}
	}

	return &model.NormalizedRequest{
		Service: "email",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

func (c *SESCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	reqID := reqctx.GetRequestID(nr.Raw.Context())
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	body := buildSESXML(nr.Action, resp.Data, reqID)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *SESCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	reqID := ""
	if nr != nil && nr.Raw != nil {
		reqID = reqctx.GetRequestID(nr.Raw.Context())
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse>`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>%s</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message), reqID,
	)
	return perr.HTTPStatus, h, []byte(body)
}

// buildSESXML builds a Query/XML response envelope for SES.
func buildSESXML(action string, data map[string]any, reqID string) string {
	var inner strings.Builder
	resultTag := action + "Result"
	inner.WriteString("<" + resultTag + ">")
	for k, v := range data {
		inner.WriteString(sesXMLValue(k, v))
	}
	inner.WriteString("</" + resultTag + ">")

	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<%sResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">`+
			`%s`+
			`<ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata>`+
			`</%sResponse>`,
		action, inner.String(), reqID, action,
	)
}

// isValidXMLName returns true if s can be used as an XML element name.
func isValidXMLName(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c == '@' || c == '.' || c == '#' || c == '/' {
			return false
		}
	}
	return true
}

func sesXMLValue(key string, val any) string {
	switch v := val.(type) {
	case map[string]any:
		var sb strings.Builder
		sb.WriteString("<" + key + ">")
		for k2, v2 := range v {
			if !isValidXMLName(k2) {
				// Use entry/key/value encoding for maps with non-XML keys (e.g. email addresses).
				sb.WriteString("<entry>")
				sb.WriteString("<key>" + xmlEscape(k2) + "</key>")
				sb.WriteString("<value>")
				if vm, ok := v2.(map[string]any); ok {
					for k3, v3 := range vm {
						sb.WriteString(sesXMLValue(k3, v3))
					}
				} else {
					sb.WriteString(xmlEscape(fmt.Sprint(v2)))
				}
				sb.WriteString("</value>")
				sb.WriteString("</entry>")
			} else {
				sb.WriteString(sesXMLValue(k2, v2))
			}
		}
		sb.WriteString("</" + key + ">")
		return sb.String()
	case []any:
		var sb strings.Builder
		sb.WriteString("<" + key + ">")
		for _, item := range v {
			sb.WriteString(sesXMLValue("member", item))
		}
		sb.WriteString("</" + key + ">")
		return sb.String()
	case []string:
		var sb strings.Builder
		sb.WriteString("<" + key + ">")
		for _, s := range v {
			sb.WriteString("<member>" + xmlEscape(s) + "</member>")
		}
		sb.WriteString("</" + key + ">")
		return sb.String()
	default:
		return "<" + key + ">" + xmlEscape(fmt.Sprint(v)) + "</" + key + ">"
	}
}
