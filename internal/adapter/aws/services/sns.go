package services

import (
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// SNSCodec handles SNS wire format (Query protocol + XML responses).
type SNSCodec struct{}

var _ adapter.Codec = (*SNSCodec)(nil)

func (c *SNSCodec) ServiceName() string { return "sns" }

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *SNSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter")
	}

	params := flattenSNSParams(values)

	nr := &model.NormalizedRequest{
		Service: "sns",
		Action:  action,
		Params:  params,
		Raw:     r,
	}
	return nr, nil
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *SNSCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	body := buildSNSXML(nr.Action, resp.Data)
	return resp.HTTPStatus, h, []byte(body)
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *SNSCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	code := snsErrorCodeMap[perr.Code]
	if code == "" {
		code = perr.Code
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error>`+
			`<RequestId>00000000-0000-0000-0000-000000000000</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(code), xmlEscape(perr.Message),
	)
	return perr.HTTPStatus, h, []byte(body)
}

var snsErrorCodeMap = map[string]string{
	"NotFound":         "NotFound",
	"InvalidParameter": "InvalidParameter",
}

// ─── Params ───────────────────────────────────────────────────────────────────

func flattenSNSParams(values interface{ Get(string) string }) map[string]any {
	type getter interface{ Get(string) string }
	v := values.(getter)
	params := map[string]any{}
	for _, k := range []string{
		"TopicArn", "Name", "SubscriptionArn", "Protocol", "Endpoint",
		"Message", "Subject", "MessageStructure", "PhoneNumber",
		"AttributeName", "AttributeValue", "ResourceArn",
	} {
		if val := v.Get(k); val != "" {
			params[k] = val
		}
	}
	return params
}

// ─── XML builder ──────────────────────────────────────────────────────────────

func buildSNSXML(action string, data map[string]any) string {
	xmlns := "http://sns.amazonaws.com/doc/2010-03-31/"
	inner := buildSNSResult(action, data)

	resultXML := ""
	if inner != "" {
		resultXML = "<" + action + "Result>" + inner + "</" + action + "Result>"
	}

	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<` + action + `Response xmlns="` + xmlns + `">` +
		resultXML +
		`<ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata>` +
		`</` + action + `Response>`
}

func buildSNSResult(action string, data map[string]any) string {
	if data == nil {
		return ""
	}
	var sb strings.Builder
	switch action {
	case "CreateTopic":
		sb.WriteString(xmlTag("TopicArn", str(data["TopicArn"])))
	case "Subscribe":
		sb.WriteString(xmlTag("SubscriptionArn", str(data["SubscriptionArn"])))
	case "ListTopics":
		sb.WriteString("<Topics>")
		if topics, ok := data["Topics"].([]map[string]any); ok {
			for _, t := range topics {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("TopicArn", str(t["TopicArn"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Topics>")
	case "ListSubscriptions", "ListSubscriptionsByTopic":
		sb.WriteString("<Subscriptions>")
		if subs, ok := data["Subscriptions"].([]map[string]any); ok {
			for _, s := range subs {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("SubscriptionArn", str(s["SubscriptionArn"])))
				sb.WriteString(xmlTag("TopicArn", str(s["TopicArn"])))
				sb.WriteString(xmlTag("Protocol", str(s["Protocol"])))
				sb.WriteString(xmlTag("Endpoint", str(s["Endpoint"])))
				sb.WriteString(xmlTag("Owner", str(s["Owner"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Subscriptions>")
	case "GetTopicAttributes", "GetSubscriptionAttributes":
		sb.WriteString("<Attributes>")
		if attrs, ok := data["Attributes"].(map[string]string); ok {
			for k, v := range attrs {
				sb.WriteString("<entry>")
				sb.WriteString(xmlTag("key", k))
				sb.WriteString(xmlTag("value", v))
				sb.WriteString("</entry>")
			}
		}
		sb.WriteString("</Attributes>")
	case "Publish":
		sb.WriteString(xmlTag("MessageId", str(data["MessageId"])))
	case "ListTagsForResource":
		sb.WriteString("<Tags>")
		if tags, ok := data["Tags"].([]map[string]any); ok {
			for _, t := range tags {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("Key", str(t["Key"])))
				sb.WriteString(xmlTag("Value", str(t["Value"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Tags>")
	}
	return sb.String()
}
