package services

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// SQSCodec handles SQS wire format in two protocols:
//
//	JSON  — modern aws-sdk-go-v2 uses X-Amz-Target: AmazonSQS.<Action>
//	Query — legacy SDKs use Action=<Action> as query/form parameter, XML responses
type SQSCodec struct{}

var _ adapter.Codec = (*SQSCodec)(nil)

func (c *SQSCodec) ServiceName() string { return "sqs" }

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *SQSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	if target := r.Header.Get("X-Amz-Target"); strings.HasPrefix(target, "AmazonSQS.") {
		return c.decodeJSON(r, body, target)
	}
	return c.decodeQuery(r, body)
}

func (c *SQSCodec) decodeJSON(r *http.Request, body []byte, target string) (*model.NormalizedRequest, error) {
	action := strings.TrimPrefix(target, "AmazonSQS.")
	if action == "" {
		return nil, fmt.Errorf("empty action in X-Amz-Target")
	}

	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return nil, fmt.Errorf("invalid JSON body: %w", err)
		}
	} else {
		params = map[string]any{}
	}

	// Normalise MessageAttributes from JSON SDK format
	if ma, ok := params["MessageAttributes"]; ok {
		params["MessageAttributes"] = normaliseMessageAttributesJSON(ma)
	}
	// Batch entries: SDK sends "Entries" as array
	if entries, ok := params["Entries"]; ok {
		params["Entries"] = normaliseEntries(entries)
	}
	// AttributeNames / MessageAttributeNames come as JSON arrays — leave as-is

	nr := &model.NormalizedRequest{
		Service: "sqs",
		Action:  action,
		Params:  params,
		Raw:     r,
	}
	nr.SetMeta("sqs_protocol", "json")
	return nr, nil
}

func (c *SQSCodec) decodeQuery(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	// Merge URL query + form body into a single Values map
	values := mergeQueryAndForm(r, body)
	action := values.Get("Action")
	if action == "" {
		return nil, fmt.Errorf("missing Action parameter")
	}

	params := flattenQueryParams(values, action)

	nr := &model.NormalizedRequest{
		Service: "sqs",
		Action:  action,
		Params:  params,
		Raw:     r,
	}
	nr.SetMeta("sqs_protocol", "query")
	return nr, nil
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *SQSCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	if nr.GetMeta("sqs_protocol") == "json" {
		return c.encodeJSON(resp)
	}
	return c.encodeXML(nr.Action, resp)
}

func (c *SQSCodec) encodeJSON(resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.0")
	b, _ := json.Marshal(resp.Data)
	return resp.HTTPStatus, h, b
}

// encodeXML wraps the response data in the canonical SQS XML envelope:
//
//	<CreateQueueResponse>
//	  <CreateQueueResult>...</CreateQueueResult>
//	  <ResponseMetadata><RequestId>...</RequestId></ResponseMetadata>
//	</CreateQueueResponse>
func (c *SQSCodec) encodeXML(action string, resp *model.ProviderResponse) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")

	result := buildXMLResult(action, resp.Data)

	type ResponseMetadata struct {
		RequestId string `xml:"RequestId"`
	}
	type Envelope struct {
		XMLName        xml.Name         `xml:""`
		Result         any              `xml:",omitempty"`
		ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	}

	// We build XML manually to get the right element names
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString("<" + action + "Response>")
	sb.WriteString("<" + action + "Result>")
	sb.WriteString(result)
	sb.WriteString("</" + action + "Result>")
	sb.WriteString("<ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata>")
	sb.WriteString("</" + action + "Response>")

	return resp.HTTPStatus, h, []byte(sb.String())
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *SQSCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	awsCode := sqsErrorCodeMap[perr.Code]
	if awsCode == "" {
		awsCode = perr.Code
	}

	if nr != nil && nr.GetMeta("sqs_protocol") == "json" {
		return encodeJSONError(awsCode, perr.Message, perr.HTTPStatus, perr.Data)
	}
	return encodeXMLError(awsCode, perr.Message, perr.HTTPStatus, perr.Data)
}

func encodeJSONError(code, msg string, status int, data map[string]any) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/x-amz-json-1.0")
	out := map[string]any{
		"__type":  code,
		"message": msg,
	}
	for k, v := range data {
		out[k] = v
	}
	body, _ := json.Marshal(out)
	return status, h, body
}

func encodeXMLError(code, msg string, status int, data map[string]any) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/xml")
	var extra strings.Builder
	for k, v := range data {
		extra.WriteString(fmt.Sprintf("<%s>%s</%s>", xmlEscape(k), xmlEscape(fmt.Sprint(v)), xmlEscape(k)))
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ErrorResponse><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message>%s</Error>`+
			`<RequestId>00000000-0000-0000-0000-000000000000</RequestId></ErrorResponse>`,
		xmlEscape(code), xmlEscape(msg), extra.String(),
	)
	return status, h, []byte(body)
}

var sqsErrorCodeMap = map[string]string{
	"NotFound":         "AWS.SimpleQueueService.NonExistentQueue",
	"AlreadyExists":    "QueueAlreadyExists",
	"InvalidParameter": "InvalidParameterValue",
	"ValidationError":  "InvalidParameterValue",
	"LimitExceeded":    "OverLimit",
	"EmptyBatch":       "AWS.SimpleQueueService.EmptyBatchRequest",
	"BatchTooLarge":    "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
	"UnknownAction":    "InvalidAction",
	"UnknownService":   "InvalidClientTokenId",
}

// ─── Query protocol helpers ───────────────────────────────────────────────────

func mergeQueryAndForm(r *http.Request, body []byte) url.Values {
	values := r.URL.Query()
	if len(body) > 0 {
		formValues, err := url.ParseQuery(string(body))
		if err == nil {
			for k, vs := range formValues {
				for _, v := range vs {
					values.Add(k, v)
				}
			}
		}
	}
	return values
}

// flattenQueryParams converts the numbered AWS Query Protocol format into Go structures.
//
// Examples:
//
//	SendMessageBatchRequestEntry.1.Id=m1
//	SendMessageBatchRequestEntry.1.MessageBody=hello
//	MessageAttribute.1.Name=color
//	MessageAttribute.1.Value.DataType=String
//	MessageAttribute.1.Value.StringValue=blue
//	AttributeName.1=All
func flattenQueryParams(values url.Values, action string) map[string]any {
	params := map[string]any{}

	// Simple scalar keys
	scalarKeys := []string{
		"QueueUrl", "QueueName", "QueueOwnerAWSAccountId",
		"MessageBody", "ReceiptHandle", "DelaySeconds",
		"MaxNumberOfMessages", "VisibilityTimeout", "WaitTimeSeconds",
	}
	for _, k := range scalarKeys {
		if v := values.Get(k); v != "" {
			params[k] = v
		}
	}

	// Attributes map: Attribute.N.Name / Attribute.N.Value
	attrs := extractNumberedMap(values, "Attribute")
	if len(attrs) > 0 {
		params["Attributes"] = attrs
	}

	// MessageAttributes: MessageAttribute.N.Name + MessageAttribute.N.Value.DataType + ...
	msgAttrs := extractMessageAttributes(values)
	if len(msgAttrs) > 0 {
		params["MessageAttributes"] = msgAttrs
	}

	// AttributeNames list: AttributeName.1, AttributeName.2, ...
	attrNames := extractNumberedList(values, "AttributeName")
	if len(attrNames) > 0 {
		params["AttributeNames"] = attrNames
	}

	// MessageAttributeNames: MessageAttributeName.1, ...
	maNames := extractNumberedList(values, "MessageAttributeName")
	if len(maNames) > 0 {
		params["MessageAttributeNames"] = maNames
	}

	// Batch entries: SendMessageBatchRequestEntry.N.* or DeleteMessageBatchRequestEntry.N.*
	entries := extractBatchEntries(values)
	if len(entries) > 0 {
		params["Entries"] = entries
	}

	// Tags: Tag.N.Key / Tag.N.Value
	tags := extractNumberedMap(values, "Tag")
	if len(tags) > 0 {
		params["Tags"] = tags
	}

	// TagKey.N (UntagQueue)
	tagKeys := extractNumberedList(values, "TagKey")
	if len(tagKeys) > 0 {
		params["TagKeys"] = tagKeys
	}

	// QueueNamePrefix (ListQueues)
	if v := values.Get("QueueNamePrefix"); v != "" {
		params["QueueNamePrefix"] = v
	}

	// FIFO / group fields
	for _, k := range []string{"MessageGroupId", "MessageDeduplicationId"} {
		if v := values.Get(k); v != "" {
			params[k] = v
		}
	}

	return params
}

func extractNumberedList(values url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		k := fmt.Sprintf("%s.%d", prefix, i)
		v := values.Get(k)
		if v == "" {
			break
		}
		result = append(result, v)
	}
	return result
}

func extractNumberedMap(values url.Values, prefix string) map[string]string {
	result := map[string]string{}
	for i := 1; ; i++ {
		name := values.Get(fmt.Sprintf("%s.%d.Name", prefix, i))
		val := values.Get(fmt.Sprintf("%s.%d.Value", prefix, i))
		if name == "" {
			break
		}
		result[name] = val
	}
	return result
}

func extractMessageAttributes(values url.Values) map[string]any {
	result := map[string]any{}
	for i := 1; ; i++ {
		name := values.Get(fmt.Sprintf("MessageAttribute.%d.Name", i))
		if name == "" {
			break
		}
		dt := values.Get(fmt.Sprintf("MessageAttribute.%d.Value.DataType", i))
		sv := values.Get(fmt.Sprintf("MessageAttribute.%d.Value.StringValue", i))
		result[name] = map[string]any{"DataType": dt, "StringValue": sv}
	}
	return result
}

func extractBatchEntries(values url.Values) []map[string]any {
	// Try both SendMessageBatchRequestEntry and DeleteMessageBatchRequestEntry prefixes
	for _, prefix := range []string{
		"SendMessageBatchRequestEntry",
		"DeleteMessageBatchRequestEntry",
		"ChangeMessageVisibilityBatchRequestEntry",
	} {
		entries := extractPrefixedEntries(values, prefix)
		if len(entries) > 0 {
			return entries
		}
	}
	return nil
}

func extractPrefixedEntries(values url.Values, prefix string) []map[string]any {
	var entries []map[string]any
	for i := 1; ; i++ {
		id := values.Get(fmt.Sprintf("%s.%d.Id", prefix, i))
		if id == "" {
			break
		}
		entry := map[string]any{"Id": id}
		for _, field := range []string{
			"MessageBody", "DelaySeconds", "ReceiptHandle",
			"VisibilityTimeout", "MessageGroupId", "MessageDeduplicationId",
		} {
			if v := values.Get(fmt.Sprintf("%s.%d.%s", prefix, i, field)); v != "" {
				entry[field] = v
			}
		}
		// MessageAttributes within batch entry
		msgAttrs := map[string]any{}
		for j := 1; ; j++ {
			maName := values.Get(fmt.Sprintf("%s.%d.MessageAttribute.%d.Name", prefix, i, j))
			if maName == "" {
				break
			}
			dt := values.Get(fmt.Sprintf("%s.%d.MessageAttribute.%d.Value.DataType", prefix, i, j))
			sv := values.Get(fmt.Sprintf("%s.%d.MessageAttribute.%d.Value.StringValue", prefix, i, j))
			msgAttrs[maName] = map[string]any{"DataType": dt, "StringValue": sv}
		}
		if len(msgAttrs) > 0 {
			entry["MessageAttributes"] = msgAttrs
		}
		entries = append(entries, entry)
	}
	return entries
}

// ─── XML result builder ───────────────────────────────────────────────────────

// buildXMLResult converts provider response data into inner XML for each action.
func buildXMLResult(action string, data map[string]any) string {
	var sb strings.Builder
	switch action {
	case "CreateQueue":
		sb.WriteString(xmlTag("QueueUrl", str(data["QueueUrl"])))
	case "GetQueueUrl":
		sb.WriteString(xmlTag("QueueUrl", str(data["QueueUrl"])))
	case "ListQueues":
		if urls, ok := data["QueueUrls"].([]string); ok {
			for _, u := range urls {
				sb.WriteString(xmlTag("QueueUrl", u))
			}
		}
	case "GetQueueAttributes":
		if attrs, ok := data["Attributes"].(map[string]string); ok {
			// Sort for deterministic output
			keys := make([]string, 0, len(attrs))
			for k := range attrs {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				sb.WriteString("<Attribute>")
				sb.WriteString(xmlTag("Name", k))
				sb.WriteString(xmlTag("Value", attrs[k]))
				sb.WriteString("</Attribute>")
			}
		}
	case "SendMessage":
		sb.WriteString(xmlTag("MessageId", str(data["MessageId"])))
		sb.WriteString(xmlTag("MD5OfMessageBody", str(data["MD5OfMessageBody"])))
		if v, ok := data["MD5OfMessageAttributes"]; ok && str(v) != "" {
			sb.WriteString(xmlTag("MD5OfMessageAttributes", str(v)))
		}
	case "ReceiveMessage":
		if msgs, ok := data["Messages"].([]map[string]any); ok {
			for _, m := range msgs {
				sb.WriteString("<Message>")
				sb.WriteString(xmlTag("MessageId", str(m["MessageId"])))
				sb.WriteString(xmlTag("ReceiptHandle", str(m["ReceiptHandle"])))
				sb.WriteString(xmlTag("MD5OfBody", str(m["MD5OfBody"])))
				sb.WriteString(xmlTag("Body", str(m["Body"])))
				if attrs, ok := m["Attributes"].(map[string]string); ok {
					for k, v := range attrs {
						sb.WriteString("<Attribute>")
						sb.WriteString(xmlTag("Name", k))
						sb.WriteString(xmlTag("Value", v))
						sb.WriteString("</Attribute>")
					}
				}
				if msgAttrs, ok := m["MessageAttributes"]; ok {
					sb.WriteString(encodeMessageAttributesXML(msgAttrs))
				}
				sb.WriteString("</Message>")
			}
		}
	case "SendMessageBatch":
		for _, s := range batchResultList(data["Successful"]) {
			sb.WriteString("<SendMessageBatchResultEntry>")
			sb.WriteString(xmlTag("Id", str(s["Id"])))
			sb.WriteString(xmlTag("MessageId", str(s["MessageId"])))
			sb.WriteString(xmlTag("MD5OfMessageBody", str(s["MD5OfMessageBody"])))
			if v, ok := s["MD5OfMessageAttributes"]; ok && str(v) != "" {
				sb.WriteString(xmlTag("MD5OfMessageAttributes", str(v)))
			}
			sb.WriteString("</SendMessageBatchResultEntry>")
		}
		for _, f := range batchResultList(data["Failed"]) {
			sb.WriteString("<BatchResultErrorEntry>")
			sb.WriteString(xmlTag("Id", str(f["Id"])))
			sb.WriteString(xmlTag("Code", str(f["Code"])))
			sb.WriteString(xmlTag("Message", str(f["Message"])))
			sb.WriteString(xmlTag("SenderFault", fmt.Sprintf("%v", f["SenderFault"])))
			sb.WriteString("</BatchResultErrorEntry>")
		}
	case "DeleteMessageBatch", "ChangeMessageVisibilityBatch":
		for _, s := range batchResultList(data["Successful"]) {
			sb.WriteString("<DeleteMessageBatchResultEntry>")
			sb.WriteString(xmlTag("Id", str(s["Id"])))
			sb.WriteString("</DeleteMessageBatchResultEntry>")
		}
		for _, f := range batchResultList(data["Failed"]) {
			sb.WriteString("<BatchResultErrorEntry>")
			sb.WriteString(xmlTag("Id", str(f["Id"])))
			sb.WriteString(xmlTag("Code", str(f["Code"])))
			sb.WriteString(xmlTag("Message", str(f["Message"])))
			sb.WriteString(xmlTag("SenderFault", fmt.Sprintf("%v", f["SenderFault"])))
			sb.WriteString("</BatchResultErrorEntry>")
		}
	case "ListQueueTags":
		if tags, ok := data["Tags"].(map[string]string); ok {
			keys := make([]string, 0, len(tags))
			for k := range tags {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				sb.WriteString("<Tag>")
				sb.WriteString(xmlTag("Key", k))
				sb.WriteString(xmlTag("Value", tags[k]))
				sb.WriteString("</Tag>")
			}
		}
	}
	// Empty result for: DeleteQueue, DeleteMessage, PurgeQueue, SetQueueAttributes, TagQueue, UntagQueue, ChangeMessageVisibility
	return sb.String()
}

func encodeMessageAttributesXML(v any) string {
	var sb strings.Builder
	switch m := v.(type) {
	case map[string]any:
		for name, attr := range m {
			sb.WriteString("<MessageAttribute>")
			sb.WriteString(xmlTag("Name", name))
			sb.WriteString("<Value>")
			switch a := attr.(type) {
			case map[string]any:
				sb.WriteString(xmlTag("DataType", str(a["DataType"])))
				if sv, ok := a["StringValue"]; ok {
					sb.WriteString(xmlTag("StringValue", str(sv)))
				}
			}
			sb.WriteString("</Value>")
			sb.WriteString("</MessageAttribute>")
		}
	}
	return sb.String()
}

// ─── JSON normalisation ───────────────────────────────────────────────────────

// normaliseMessageAttributesJSON converts the SDK MessageAttributes format to our internal format.
// SDK format: {"color": {"DataType": "String", "StringValue": "blue"}}
func normaliseMessageAttributesJSON(v any) map[string]any {
	result := map[string]any{}
	switch m := v.(type) {
	case map[string]any:
		for k, val := range m {
			if attr, ok := val.(map[string]any); ok {
				result[k] = attr
			}
		}
	}
	return result
}

func normaliseEntries(v any) []map[string]any {
	switch e := v.(type) {
	case []any:
		result := make([]map[string]any, 0, len(e))
		for _, item := range e {
			if m, ok := item.(map[string]any); ok {
				result = append(result, m)
			}
		}
		return result
	case []map[string]any:
		return e
	}
	return nil
}

// ─── Misc helpers ─────────────────────────────────────────────────────────────

func xmlTag(name, value string) string {
	return "<" + name + ">" + xmlEscape(value) + "</" + name + ">"
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

func str(v any) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case bool:
		return strconv.FormatBool(s)
	}
	return fmt.Sprintf("%v", v)
}

func batchResultList(v any) []map[string]any {
	if v == nil {
		return nil
	}
	if l, ok := v.([]map[string]any); ok {
		return l
	}
	return nil
}
