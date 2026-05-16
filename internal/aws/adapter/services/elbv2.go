package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/reqctx"
)

// ELBv2Codec handles the ELBv2 (elasticloadbalancing) Query/XML wire protocol.
// Same protocol family as RDS, ElastiCache: Action= query param, XML response.
type ELBv2Codec struct{}

var _ adapter.Codec = (*ELBv2Codec)(nil)

func (c *ELBv2Codec) ServiceName() string { return "elasticloadbalancing" }

func (c *ELBv2Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter for ELBv2 request")
	}
	return &model.NormalizedRequest{
		Service: "elasticloadbalancing",
		Action:  action,
		Params:  flattenQueryValues(values),
		Raw:     r,
	}, nil
}

func (c *ELBv2Codec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	action := ""
	reqID := ""
	if nr != nil {
		action = nr.Action
		if nr.Raw != nil {
			reqID = reqctx.GetRequestID(nr.Raw.Context())
		}
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}
	body := buildELBv2XML(action, resp.Data, reqID)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *ELBv2Codec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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
			`<ErrorResponse xmlns="http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/">`+
			`<Error><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>%s</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message), reqID,
	)
	return perr.HTTPStatus, h, []byte(body)
}

const elbNS = `xmlns="http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"`

func buildELBv2XML(action string, data map[string]any, reqID string) string {
	if data == nil {
		data = map[string]any{}
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}

	wrap := func(action, inner string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + action + `Response ` + elbNS + `>` +
			`<` + action + `Result>` + inner + `</` + action + `Result>` +
			`<ResponseMetadata><RequestId>` + reqID + `</RequestId></ResponseMetadata>` +
			`</` + action + `Response>`
	}

	if lbs, ok := data["LoadBalancers"]; ok {
		var sb strings.Builder
		sb.WriteString(`<LoadBalancers>`)
		if items, ok := lbs.([]any); ok {
			for _, item := range items {
				sb.WriteString(encodeELBLoadBalancer(item))
			}
		}
		sb.WriteString(`</LoadBalancers>`)
		return wrap(action, sb.String())
	}

	if tgs, ok := data["TargetGroups"]; ok {
		var sb strings.Builder
		sb.WriteString(`<TargetGroups>`)
		if items, ok := tgs.([]any); ok {
			for _, item := range items {
				sb.WriteString(encodeELBTargetGroup(item))
			}
		}
		sb.WriteString(`</TargetGroups>`)
		return wrap(action, sb.String())
	}

	if listeners, ok := data["Listeners"]; ok {
		var sb strings.Builder
		sb.WriteString(`<Listeners>`)
		if items, ok := listeners.([]any); ok {
			for _, item := range items {
				sb.WriteString(encodeELBListener(item))
			}
		}
		sb.WriteString(`</Listeners>`)
		return wrap(action, sb.String())
	}

	if ths, ok := data["TargetHealthDescriptions"]; ok {
		var sb strings.Builder
		sb.WriteString(`<TargetHealthDescriptions>`)
		if items, ok := ths.([]any); ok {
			for _, item := range items {
				sb.WriteString(encodeELBTargetHealth(item))
			}
		}
		sb.WriteString(`</TargetHealthDescriptions>`)
		return wrap(action, sb.String())
	}

	if attrs, ok := data["Attributes"]; ok {
		var sb strings.Builder
		sb.WriteString(`<Attributes>`)
		if items, ok := attrs.([]any); ok {
			for _, item := range items {
				if m, ok := item.(map[string]any); ok {
					sb.WriteString(`<member>`)
					sb.WriteString(xmlTag("Key", str(m["Key"])))
					sb.WriteString(xmlTag("Value", str(m["Value"])))
					sb.WriteString(`</member>`)
				}
			}
		}
		sb.WriteString(`</Attributes>`)
		return wrap(action, sb.String())
	}

	if tagDescriptions, ok := data["TagDescriptions"]; ok {
		var sb strings.Builder
		sb.WriteString(`<TagDescriptions>`)
		if items, ok := tagDescriptions.([]any); ok {
			for _, item := range items {
				if m, ok := item.(map[string]any); ok {
					sb.WriteString(`<member>`)
					sb.WriteString(xmlTag("ResourceArn", str(m["ResourceArn"])))
					sb.WriteString(`<Tags>`)
					if tags, ok := m["Tags"].([]any); ok {
						for _, t := range tags {
							if tm, ok := t.(map[string]any); ok {
								sb.WriteString(`<member>`)
								sb.WriteString(xmlTag("Key", str(tm["Key"])))
								sb.WriteString(xmlTag("Value", str(tm["Value"])))
								sb.WriteString(`</member>`)
							}
						}
					}
					sb.WriteString(`</Tags>`)
					sb.WriteString(`</member>`)
				}
			}
		}
		sb.WriteString(`</TagDescriptions>`)
		return wrap(action, sb.String())
	}

	// Empty action response (e.g. Delete)
	if action != "" {
		return wrap(action, "")
	}

	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<Response ` + elbNS + `>` +
		`<ResponseMetadata><RequestId>` + xmlEscape(reqID) + `</RequestId></ResponseMetadata>` +
		`</Response>`
}

func encodeELBLoadBalancer(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<member>`)
	sb.WriteString(xmlTag("LoadBalancerArn", str(m["LoadBalancerArn"])))
	sb.WriteString(xmlTag("LoadBalancerName", str(m["LoadBalancerName"])))
	sb.WriteString(xmlTag("DNSName", str(m["DNSName"])))
	sb.WriteString(xmlTag("Scheme", str(m["Scheme"])))
	sb.WriteString(xmlTag("Type", str(m["Type"])))
	if state, ok := m["State"].(map[string]any); ok {
		sb.WriteString(`<State>`)
		sb.WriteString(xmlTag("Code", str(state["Code"])))
		sb.WriteString(`</State>`)
	}
	sb.WriteString(`</member>`)
	return sb.String()
}

func encodeELBTargetGroup(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<member>`)
	sb.WriteString(xmlTag("TargetGroupArn", str(m["TargetGroupArn"])))
	sb.WriteString(xmlTag("TargetGroupName", str(m["TargetGroupName"])))
	sb.WriteString(xmlTag("Protocol", str(m["Protocol"])))
	sb.WriteString(xmlTag("Port", str(m["Port"])))
	sb.WriteString(xmlTag("VpcId", str(m["VpcId"])))
	sb.WriteString(xmlTag("TargetType", str(m["TargetType"])))
	sb.WriteString(`</member>`)
	return sb.String()
}

func encodeELBListener(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<member>`)
	sb.WriteString(xmlTag("ListenerArn", str(m["ListenerArn"])))
	sb.WriteString(xmlTag("LoadBalancerArn", str(m["LoadBalancerArn"])))
	sb.WriteString(xmlTag("Protocol", str(m["Protocol"])))
	sb.WriteString(xmlTag("Port", str(m["Port"])))
	if actions, ok := m["DefaultActions"].([]any); ok {
		sb.WriteString(`<DefaultActions>`)
		for _, a := range actions {
			if am, ok := a.(map[string]any); ok {
				sb.WriteString(`<member>`)
				sb.WriteString(xmlTag("Type", str(am["Type"])))
				if tga := str(am["TargetGroupArn"]); tga != "" {
					sb.WriteString(xmlTag("TargetGroupArn", tga))
				}
				sb.WriteString(`</member>`)
			}
		}
		sb.WriteString(`</DefaultActions>`)
	}
	sb.WriteString(`</member>`)
	return sb.String()
}

func encodeELBTargetHealth(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<member>`)
	if target, ok := m["Target"].(map[string]any); ok {
		sb.WriteString(`<Target>`)
		sb.WriteString(xmlTag("Id", str(target["Id"])))
		sb.WriteString(`</Target>`)
	}
	sb.WriteString(xmlTag("HealthCheckPort", str(m["HealthCheckPort"])))
	if th, ok := m["TargetHealth"].(map[string]any); ok {
		sb.WriteString(`<TargetHealth>`)
		sb.WriteString(xmlTag("State", str(th["State"])))
		sb.WriteString(`</TargetHealth>`)
	}
	sb.WriteString(`</member>`)
	return sb.String()
}
