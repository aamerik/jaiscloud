package services

import (
	"fmt"
	"net/http"
	"net/url"
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
	var extra strings.Builder
	for k, v := range perr.Data {
		extra.WriteString(fmt.Sprintf("<%s>%s</%s>", xmlEscape(k), xmlEscape(fmt.Sprint(v)), xmlEscape(k)))
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">`+
			`<Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message>%s</Error>`+
			`<RequestId>00000000-0000-0000-0000-000000000000</RequestId>`+
			`</ErrorResponse>`,
		xmlEscape(code), xmlEscape(perr.Message), extra.String(),
	)
	return perr.HTTPStatus, h, []byte(body)
}

var snsErrorCodeMap = map[string]string{
	"NotFound":         "NotFound",
	"InvalidParameter": "InvalidParameter",
}

// ─── Params ───────────────────────────────────────────────────────────────────

func flattenSNSParams(values url.Values) map[string]any {
	params := map[string]any{}
	for _, k := range []string{
		"TopicArn", "Name", "SubscriptionArn", "Protocol", "Endpoint",
		"Message", "Subject", "MessageStructure", "PhoneNumber",
		"AttributeName", "AttributeValue", "ResourceArn",
		// FIFO publish fields
		"MessageGroupId", "MessageDeduplicationId",
		// ConfirmSubscription
		"Token",
		// AddPermission
		"Label",
		// Pagination
		"NextToken",
	} {
		if val := values.Get(k); val != "" {
			params[k] = val
		}
	}
	// Parse MessageAttributes.entry.N.{Name,Value.DataType,Value.StringValue}
	attrs := extractSNSMessageAttributes(values)
	if len(attrs) > 0 {
		params["MessageAttributes"] = attrs
	}
	// Parse Attributes.entry.N.{key,value} (subscription/topic attribute map — different shape than MessageAttributes).
	if snsAttrs := extractSNSAttributesEntry(values); len(snsAttrs) > 0 {
		params["Attributes"] = snsAttrs
	}
	// Parse AWSAccountId.member.N and ActionName.member.N for AddPermission.
	if accts := extractMemberStrings(values, "AWSAccountId"); len(accts) > 0 {
		params["AWSAccountId"] = accts
	}
	if actions := extractMemberStrings(values, "ActionName"); len(actions) > 0 {
		params["ActionName"] = actions
	}
	// Parse Tags.member.N.{Key,Value} for TagResource/ListTagsForResource.
	if tags := extractSNSTags(values); len(tags) > 0 {
		params["Tags"] = tags
	}
	// Parse TagKeys.member.N for UntagResource.
	if keys := extractSNSTagKeys(values); len(keys) > 0 {
		params["TagKeys"] = keys
	}
	// Parse PublishBatchRequestEntries.member.N.{Id,Message,...} for PublishBatch.
	if entries := extractSNSBatchEntries(values); len(entries) > 0 {
		params["PublishBatchRequestEntries"] = entries
	}
	return params
}

// extractSNSAttributesEntry parses Attributes.entry.N.key / Attributes.entry.N.value
// into a map[string]string (used for SetTopicAttributes, SetSubscriptionAttributes, etc.).
func extractSNSAttributesEntry(values url.Values) map[string]string {
	result := map[string]string{}
	for i := 1; ; i++ {
		k := values.Get(fmt.Sprintf("Attributes.entry.%d.key", i))
		if k == "" {
			break
		}
		v := values.Get(fmt.Sprintf("Attributes.entry.%d.value", i))
		result[k] = v
	}
	return result
}

// extractMemberStrings parses {prefix}.member.N from SNS Query protocol into a []string.
func extractMemberStrings(values url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := values.Get(fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			break
		}
		result = append(result, v)
	}
	return result
}

// extractSNSMessageAttributes parses SNS MessageAttributes from Query protocol form:
// MessageAttributes.entry.1.Name, MessageAttributes.entry.1.Value.DataType, ...
func extractSNSMessageAttributes(values url.Values) map[string]any {
	result := map[string]any{}
	for i := 1; ; i++ {
		name := values.Get(fmt.Sprintf("MessageAttributes.entry.%d.Name", i))
		if name == "" {
			break
		}
		dt := values.Get(fmt.Sprintf("MessageAttributes.entry.%d.Value.DataType", i))
		sv := values.Get(fmt.Sprintf("MessageAttributes.entry.%d.Value.StringValue", i))
		result[name] = map[string]any{"DataType": dt, "StringValue": sv}
	}
	return result
}

// extractSNSTags parses Tags.member.N.{Key,Value} from SNS Query protocol.
func extractSNSTags(values url.Values) []any {
	var tags []any
	for i := 1; ; i++ {
		key := values.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if key == "" {
			break
		}
		val := values.Get(fmt.Sprintf("Tags.member.%d.Value", i))
		tags = append(tags, map[string]any{"Key": key, "Value": val})
	}
	return tags
}

// extractSNSTagKeys parses TagKeys.member.N from SNS Query protocol.
func extractSNSTagKeys(values url.Values) []any {
	var keys []any
	for i := 1; ; i++ {
		k := values.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	return keys
}

// extractSNSBatchEntries parses PublishBatchRequestEntries.member.N.{Id,Message,...}.
func extractSNSBatchEntries(values url.Values) []any {
	var entries []any
	for i := 1; ; i++ {
		id := values.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i))
		if id == "" {
			break
		}
		entry := map[string]any{"Id": id}
		if msg := values.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i)); msg != "" {
			entry["Message"] = msg
		}
		if subj := values.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Subject", i)); subj != "" {
			entry["Subject"] = subj
		}
		entries = append(entries, entry)
	}
	return entries
}

// ─── XML builder ──────────────────────────────────────────────────────────────

func buildSNSXML(action string, data map[string]any) string {
	xmlns := "http://sns.amazonaws.com/doc/2010-03-31/"
	inner := buildSNSResult(action, data)

	resultXML := "<" + action + "Result>" + inner + "</" + action + "Result>"

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
	case "PublishBatch":
		sb.WriteString("<Successful>")
		if successful, ok := data["Successful"].([]map[string]any); ok {
			for _, s := range successful {
				sb.WriteString("<member>")
				sb.WriteString(xmlTag("Id", str(s["Id"])))
				sb.WriteString(xmlTag("MessageId", str(s["MessageId"])))
				sb.WriteString("</member>")
			}
		}
		sb.WriteString("</Successful>")
		sb.WriteString("<Failed/>")
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
