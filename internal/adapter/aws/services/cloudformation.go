package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
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

func (c *CloudFormationCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := buildCFXML(resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

func (c *CloudFormationCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>jaiscloud-cf</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

const cfNS = `xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/"`

func buildCFXML(data map[string]any) string {
	if data == nil {
		return `<?xml version="1.0" encoding="UTF-8"?><Response/>`
	}

	meta := `<ResponseMetadata><RequestId>jaiscloud-cf</RequestId></ResponseMetadata>`

	wrap := func(action, inner string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + action + `Response ` + cfNS + `>` +
			`<` + action + `Result>` + inner + `</` + action + `Result>` +
			meta +
			`</` + action + `Response>`
	}
	wrapNoResult := func(action string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>` +
			`<` + action + `Response ` + cfNS + `>` + meta + `</` + action + `Response>`
	}

	if v, ok := data["StackId"]; ok {
		if _, create := data["CreateStack"]; create {
			return wrap("CreateStack", xmlTag("StackId", str(v)))
		}
		if _, update := data["UpdateStack"]; update {
			return wrap("UpdateStack", xmlTag("StackId", str(v)))
		}
	}
	if _, ok := data["DeleteStack"]; ok {
		return wrapNoResult("DeleteStack")
	}
	if list, ok := data["Stacks"]; ok {
		var sb strings.Builder
		sb.WriteString(`<Stacks>`)
		if stacks, ok := list.([]map[string]any); ok {
			for _, s := range stacks {
				sb.WriteString(encodeCFStack(s))
			}
		}
		sb.WriteString(`</Stacks>`)
		return wrap("DescribeStacks", sb.String())
	}
	if list, ok := data["StackSummaries"]; ok {
		var sb strings.Builder
		sb.WriteString(`<StackSummaries>`)
		if sums, ok := list.([]map[string]any); ok {
			for _, s := range sums {
				sb.WriteString(encodeCFStackSummary(s))
			}
		}
		sb.WriteString(`</StackSummaries>`)
		return wrap("ListStacks", sb.String())
	}
	if list, ok := data["StackResources"]; ok {
		var sb strings.Builder
		sb.WriteString(`<StackResources>`)
		if resources, ok := list.([]map[string]any); ok {
			for _, r := range resources {
				sb.WriteString(encodeCFStackResource(r))
			}
		}
		sb.WriteString(`</StackResources>`)
		return wrap("DescribeStackResources", sb.String())
	}
	if v, ok := data["StackStatus"]; ok {
		if _, ok := data["ValidateTemplate"]; ok {
			return wrap("ValidateTemplate", xmlTag("Description", str(v)))
		}
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
