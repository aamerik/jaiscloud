package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/reqctx"
)

// CloudFormationCodec handles the CloudFormation Query/XML wire protocol.
type CloudFormationCodec struct{}

var _ adapter.Codec = (*CloudFormationCodec)(nil)

func (c *CloudFormationCodec) ServiceName() string { return "cloudformation" }

func (c *CloudFormationCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter for CloudFormation request")
	}
	return &model.NormalizedRequest{
		Service: "cloudformation",
		Action:  action,
		Params:  flattenQueryValues(values),
		Raw:     r,
	}, nil
}

func (c *CloudFormationCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
	body := buildCFXML(action, resp.Data, reqID)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *CloudFormationCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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
			`<ErrorResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>%s</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message), reqID,
	)
	return perr.HTTPStatus, h, []byte(body)
}

const cfNS = `xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/"`

func buildCFXML(action string, data map[string]any, reqID string) string {
	if data == nil {
		return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
	}
	if reqID == "" {
		reqID = "00000000-0000-0000-0000-000000000000"
	}

	meta := `<ResponseMetadata><RequestId>` + reqID + `</RequestId></ResponseMetadata>`

	wrap := func(act, inner string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + act + `Response ` + cfNS + `>` +
			`<` + act + `Result>` + inner + `</` + act + `Result>` +
			meta +
			`</` + act + `Response>`
	}
	wrapNoResult := func(act string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + act + `Response ` + cfNS + `>` + meta + `</` + act + `Response>`
	}

	switch action {
	case "CreateStack":
		return wrap("CreateStack", xmlTag("StackId", str(data["StackId"])))
	case "UpdateStack":
		return wrap("UpdateStack", xmlTag("StackId", str(data["StackId"])))
	case "DeleteStack":
		return wrapNoResult("DeleteStack")
	case "DescribeStacks":
		var sb strings.Builder
		sb.WriteString(`<Stacks>`)
		if stacks, ok := data["Stacks"].([]map[string]any); ok {
			for _, s := range stacks {
				sb.WriteString(encodeCFStack(s))
			}
		}
		sb.WriteString(`</Stacks>`)
		return wrap("DescribeStacks", sb.String())
	case "ListStacks":
		var sb strings.Builder
		sb.WriteString(`<StackSummaries>`)
		if sums, ok := data["StackSummaries"].([]map[string]any); ok {
			for _, s := range sums {
				sb.WriteString(encodeCFStackSummary(s))
			}
		}
		sb.WriteString(`</StackSummaries>`)
		return wrap("ListStacks", sb.String())
	case "DescribeStackResources":
		var sb strings.Builder
		sb.WriteString(`<StackResources>`)
		if resources, ok := data["StackResources"].([]map[string]any); ok {
			for _, r := range resources {
				sb.WriteString(encodeCFStackResource(r))
			}
		}
		sb.WriteString(`</StackResources>`)
		return wrap("DescribeStackResources", sb.String())
	case "ValidateTemplate":
		inner := xmlTag("Parameters", "")
		if params, ok := data["Parameters"].([]any); ok && len(params) > 0 {
			var pb strings.Builder
			for _, p := range params {
				if pm, ok := p.(map[string]any); ok {
					pb.WriteString(`<member>`)
					pb.WriteString(xmlTag("ParameterKey", str(pm["ParameterKey"])))
					pb.WriteString(xmlTag("Description", str(pm["Description"])))
					pb.WriteString(`</member>`)
				}
			}
			inner = xmlTag("Parameters", pb.String())
		}
		return wrap("ValidateTemplate", inner)
	case "GetTemplate":
		return wrap("GetTemplate", xmlTag("TemplateBody", str(data["TemplateBody"])))
	}
	return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
}

func encodeCFStack(s map[string]any) string {
	var sb strings.Builder
	sb.WriteString(`<member>`)
	sb.WriteString(xmlTag("StackId", str(s["StackId"])))
	sb.WriteString(xmlTag("StackName", str(s["StackName"])))
	sb.WriteString(xmlTag("StackStatus", str(s["StackStatus"])))
	sb.WriteString(xmlTag("Description", str(s["Description"])))
	sb.WriteString(xmlTag("CreationTime", str(s["CreationTime"])))
	if params, ok := s["Parameters"].([]map[string]any); ok {
		sb.WriteString(`<Parameters>`)
		for _, p := range params {
			sb.WriteString(`<member>`)
			sb.WriteString(xmlTag("ParameterKey", str(p["ParameterKey"])))
			sb.WriteString(xmlTag("ParameterValue", str(p["ParameterValue"])))
			sb.WriteString(`</member>`)
		}
		sb.WriteString(`</Parameters>`)
	}
	if outputs, ok := s["Outputs"].([]map[string]any); ok {
		sb.WriteString(`<Outputs>`)
		for _, o := range outputs {
			sb.WriteString(`<member>`)
			sb.WriteString(xmlTag("OutputKey", str(o["OutputKey"])))
			sb.WriteString(xmlTag("OutputValue", str(o["OutputValue"])))
			sb.WriteString(`</member>`)
		}
		sb.WriteString(`</Outputs>`)
	}
	sb.WriteString(`</member>`)
	return sb.String()
}

func encodeCFStackSummary(s map[string]any) string {
	var sb strings.Builder
	sb.WriteString(`<member>`)
	sb.WriteString(xmlTag("StackId", str(s["StackId"])))
	sb.WriteString(xmlTag("StackName", str(s["StackName"])))
	sb.WriteString(xmlTag("StackStatus", str(s["StackStatus"])))
	sb.WriteString(xmlTag("CreationTime", str(s["CreationTime"])))
	sb.WriteString(`</member>`)
	return sb.String()
}

func encodeCFStackResource(r map[string]any) string {
	var sb strings.Builder
	sb.WriteString(`<member>`)
	sb.WriteString(xmlTag("StackName", str(r["StackName"])))
	sb.WriteString(xmlTag("LogicalResourceId", str(r["LogicalResourceId"])))
	sb.WriteString(xmlTag("PhysicalResourceId", str(r["PhysicalResourceId"])))
	sb.WriteString(xmlTag("ResourceType", str(r["ResourceType"])))
	sb.WriteString(xmlTag("ResourceStatus", str(r["ResourceStatus"])))
	sb.WriteString(`</member>`)
	return sb.String()
}
