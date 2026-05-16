// Package notification implements the SNS provider.
package notification

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	sqsstore "jaiscloud/internal/store/aws/sqs"
)

// FunctionInvoker is the narrow interface SNS uses to invoke Lambda functions.
type FunctionInvoker interface {
	InvokeInternal(ctx context.Context, functionName string, payload []byte) ([]byte, error)
}

// SQSSender is the narrow interface SNS uses to deliver messages to SQS queues.
// Implementations must resolve ARNs to queue URLs before writing.
type SQSSender interface {
	InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]SQSMessageAttribute, src SQSSourceContext) error
}

// SQSMessageAttribute carries a single SQS message attribute for cross-service delivery.
type SQSMessageAttribute struct {
	DataType    string
	StringValue string
}

// SQSSourceContext carries caller identity for internal SQS deliveries.
type SQSSourceContext struct {
	SourceArn        string
	ServicePrincipal string
}

// SNSProvider handles SNS operations.
type SNSProvider struct {
	resources  store.ResourceStore
	messages   sqsstore.SQSMessageStore
	bus        *events.EventBus
	invoker    FunctionInvoker
	sqsSender  SQSSender
	httpClient *http.Client
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, bus *events.EventBus) *SNSProvider {
	return &SNSProvider{
		resources:  resources,
		messages:   messages,
		bus:        bus,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// SetLambdaInvoker wires the Lambda invoker for SNS→Lambda protocol delivery.
func (p *SNSProvider) SetLambdaInvoker(inv FunctionInvoker) { p.invoker = inv }

// SetSQSSender wires the SQS sender for SNS→SQS protocol delivery with ARN resolution.
func (p *SNSProvider) SetSQSSender(s SQSSender) { p.sqsSender = s }

func (p *SNSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Notification.CreateTopic":               p.CreateTopic,
		"Notification.DeleteTopic":               p.DeleteTopic,
		"Notification.GetTopicAttributes":        p.GetTopicAttributes,
		"Notification.SetTopicAttributes":        p.SetTopicAttributes,
		"Notification.ListTopics":                p.ListTopics,
		"Notification.Subscribe":                 p.Subscribe,
		"Notification.Unsubscribe":               p.Unsubscribe,
		"Notification.ConfirmSubscription":       p.ConfirmSubscription,
		"Notification.ListSubscriptions":         p.ListSubscriptions,
		"Notification.ListSubscriptionsByTopic":  p.ListSubscriptionsByTopic,
		"Notification.GetSubscriptionAttributes": p.GetSubscriptionAttributes,
		"Notification.SetSubscriptionAttributes": p.SetSubscriptionAttributes,
		"Notification.Publish":                   p.Publish,
		"Notification.PublishBatch":              p.PublishBatch,
		"Notification.TagResource":               p.TagResource,
		"Notification.UntagResource":             p.UntagResource,
		"Notification.ListTagsForResource":       p.ListTagsForResource,
		// Permissions
		"Notification.AddPermission":    p.AddPermission,
		"Notification.RemovePermission": p.RemovePermission,
		// Platform applications and endpoints
		"Notification.CreatePlatformApplication":          p.CreatePlatformApplication,
		"Notification.GetPlatformApplicationAttributes":   p.GetPlatformApplicationAttributes,
		"Notification.SetPlatformApplicationAttributes":   p.SetPlatformApplicationAttributes,
		"Notification.DeletePlatformApplication":          p.DeletePlatformApplication,
		"Notification.ListPlatformApplications":           p.ListPlatformApplications,
		"Notification.CreatePlatformEndpoint":             p.CreatePlatformEndpoint,
		"Notification.GetEndpointAttributes":              p.GetEndpointAttributes,
		"Notification.SetEndpointAttributes":              p.SetEndpointAttributes,
		"Notification.DeleteEndpoint":                     p.DeleteEndpoint,
		"Notification.ListEndpointsByPlatformApplication": p.ListEndpointsByPlatformApplication,
		// SMS
		"Notification.SetSMSAttributes":              p.SetSMSAttributes,
		"Notification.GetSMSAttributes":              p.GetSMSAttributes,
		"Notification.OptInPhoneNumber":              p.OptInPhoneNumber,
		"Notification.CheckIfPhoneNumberIsOptedOut":  p.CheckIfPhoneNumberIsOptedOut,
		"Notification.ListPhoneNumbersOptedOut":      p.ListPhoneNumbersOptedOut,
		"Notification.ListOriginationNumbers":        p.ListOriginationNumbers,
		// Data protection
		"Notification.PutDataProtectionPolicy": p.PutDataProtectionPolicy,
		"Notification.GetDataProtectionPolicy": p.GetDataProtectionPolicy,
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func topicArn(nr *model.NormalizedRequest, name string) string {
	return nr.ResourceID("sns-topic", name)
}

func subArn(nr *model.NormalizedRequest, topicName string) string {
	suffix := fmt.Sprintf("%x", md5.Sum([]byte(topicName+time.Now().String())))
	return nr.ResourceID("sns-subscription", topicName+":"+suffix)
}

func saveEntry(ctx context.Context, rs store.ResourceStore, resType, id string, data any) error {
	raw, _ := json.Marshal(data)
	entry := store.ResourceEntry{Type: resType, ID: id, Data: json.RawMessage(raw)}
	if err := rs.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			return rs.Update(ctx, entry)
		}
		return err
	}
	return nil
}

func loadEntry(ctx context.Context, rs store.ResourceStore, resType, id string, out any) error {
	e, err := rs.Get(ctx, resType, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(e.Data, out)
}

// ─── Topic data ───────────────────────────────────────────────────────────────

type topicData struct {
	TopicArn   string            `json:"TopicArn"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"Tags"`
	CreatedAt  time.Time         `json:"CreatedAt"`
	// DedupCache maps deduplication ID → unix expiry (FIFO topics only).
	DedupCache map[string]int64 `json:"DedupCache,omitempty"`
}

func (p *SNSProvider) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, model.NewProviderError("InvalidParameter", "Name is required", 400)
	}
	arn := topicArn(nr, name)

	isFIFO := strings.HasSuffix(name, ".fifo")
	attrs := map[string]string{
		"TopicArn":                 arn,
		"DisplayName":              name,
		"SubscriptionsConfirmed":   "0",
		"SubscriptionsPending":     "0",
		"SubscriptionsDeleted":     "0",
		"EffectiveDeliveryPolicy":  `{"defaultHealthyRetryPolicy":{"numRetries":3}}`,
	}
	if isFIFO {
		attrs["FifoTopic"] = "true"
		attrs["ContentBasedDeduplication"] = "false"
		// Allow caller to override ContentBasedDeduplication via Attributes
	}
	// Apply caller-provided attributes (e.g., ContentBasedDeduplication)
	if attrsMap, ok := nr.Params["Attributes"].(map[string]any); ok {
		for k, v := range attrsMap {
			attrs[k] = fmt.Sprint(v)
		}
	}

	td := topicData{
		TopicArn:  arn,
		Attributes: attrs,
		Tags:      map[string]string{},
		CreatedAt: time.Now().UTC(),
	}
	if err := saveEntry(ctx, p.resources, "sns_topics", arn, td); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"TopicArn": arn}), nil
}

func (p *SNSProvider) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	if err := p.resources.Delete(ctx, "sns_topics", arn); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "Topic not found", 404)
		}
		return nil, err
	}
	// Delete all subscriptions for this topic.
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	for _, e := range entries {
		var s subscriptionData
		if json.Unmarshal(e.Data, &s) == nil && s.TopicArn == arn {
			_ = p.resources.Delete(ctx, "sns_subscriptions", s.SubscriptionArn)
		}
	}
	return provider.OK(nil), nil
}

func (p *SNSProvider) GetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	return provider.OK(map[string]any{"Attributes": td.Attributes}), nil
}

func (p *SNSProvider) SetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	attr := strParam(nr.Params, "AttributeName")
	val := strParam(nr.Params, "AttributeValue")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	td.Attributes[attr] = val
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "sns_topics", "")
	var topics []map[string]any
	for _, e := range entries {
		var td topicData
		if json.Unmarshal(e.Data, &td) == nil {
			topics = append(topics, map[string]any{"TopicArn": td.TopicArn})
		}
	}
	if topics == nil {
		topics = []map[string]any{}
	}
	return provider.OK(map[string]any{"Topics": topics}), nil
}

// ─── Subscriptions ────────────────────────────────────────────────────────────

type subscriptionData struct {
	SubscriptionArn string            `json:"SubscriptionArn"`
	TopicArn        string            `json:"TopicArn"`
	Protocol        string            `json:"Protocol"`
	Endpoint        string            `json:"Endpoint"`
	Owner           string            `json:"Owner"`
	Attributes      map[string]string `json:"Attributes"`
	Token           string            `json:"Token,omitempty"` // confirmation token for http/https
	Confirmed       bool              `json:"Confirmed"`
}

func (p *SNSProvider) Subscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	protocol := strParam(nr.Params, "Protocol")
	endpoint := strParam(nr.Params, "Endpoint")

	if tArn == "" || protocol == "" {
		return nil, model.NewProviderError("InvalidParameter", "TopicArn and Protocol are required", 400)
	}

	parts := strings.Split(tArn, ":")
	topicName := parts[len(parts)-1]
	sArn := subArn(nr, topicName)

	// Generate confirmation token for http/https; auto-confirm everything else.
	token := ""
	confirmed := true
	if protocol == "http" || protocol == "https" {
		token = fmt.Sprintf("%x", md5.Sum([]byte(sArn+time.Now().String())))
		confirmed = true // auto-confirm for local dev
	}

	attrs := map[string]string{"SubscriptionArn": sArn}
	if reqAttrs, ok := nr.Params["Attributes"].(map[string]string); ok {
		for k, v := range reqAttrs {
			attrs[k] = v
		}
	}
	sd := subscriptionData{
		SubscriptionArn: sArn,
		TopicArn:        tArn,
		Protocol:        protocol,
		Endpoint:        endpoint,
		Owner:           nr.AccountID,
		Attributes:      attrs,
		Token:           token,
		Confirmed:       confirmed,
	}
	if err := saveEntry(ctx, p.resources, "sns_subscriptions", sArn, sd); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"SubscriptionArn": sArn}), nil
}

func (p *SNSProvider) Unsubscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	_ = p.resources.Delete(ctx, "sns_subscriptions", sArn)
	return provider.OK(nil), nil
}

// ConfirmSubscription validates the token generated at Subscribe time.
func (p *SNSProvider) ConfirmSubscription(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	token := strParam(nr.Params, "Token")

	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != tArn {
			continue
		}
		if sd.Token == token || token == "" {
			sd.Confirmed = true
			_ = saveEntry(ctx, p.resources, "sns_subscriptions", sd.SubscriptionArn, sd)
			return provider.OK(map[string]any{"SubscriptionArn": sd.SubscriptionArn}), nil
		}
	}
	return nil, model.NewProviderError("AuthorizationError", "invalid confirmation token", 403)
}

func (p *SNSProvider) ListSubscriptions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	return provider.OK(map[string]any{"Subscriptions": subscriptionList(entries)}), nil
}

func (p *SNSProvider) ListSubscriptionsByTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	var filtered []store.ResourceEntry
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) == nil && sd.TopicArn == tArn {
			filtered = append(filtered, e)
		}
	}
	return provider.OK(map[string]any{"Subscriptions": subscriptionList(filtered)}), nil
}

func (p *SNSProvider) GetSubscriptionAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	var sd subscriptionData
	if err := loadEntry(ctx, p.resources, "sns_subscriptions", sArn, &sd); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Subscription not found")
	}
	attrs := map[string]string{
		"SubscriptionArn": sd.SubscriptionArn,
		"TopicArn":        sd.TopicArn,
		"Protocol":        sd.Protocol,
		"Endpoint":        sd.Endpoint,
		"Owner":           sd.Owner,
	}
	for k, v := range sd.Attributes {
		attrs[k] = v
	}
	return provider.OK(map[string]any{"Attributes": attrs}), nil
}

func (p *SNSProvider) SetSubscriptionAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	attr := strParam(nr.Params, "AttributeName")
	val := strParam(nr.Params, "AttributeValue")
	if attr == "FilterPolicy" && val != "" {
		var check map[string]any
		if err := json.Unmarshal([]byte(val), &check); err != nil {
			return nil, model.NewProviderError("InvalidParameter", "Invalid JSON in FilterPolicy: "+err.Error(), http.StatusBadRequest)
		}
	}
	var sd subscriptionData
	if err := loadEntry(ctx, p.resources, "sns_subscriptions", sArn, &sd); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Subscription not found")
	}
	sd.Attributes[attr] = val
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_subscriptions", sArn, sd)
}

func subscriptionList(entries []store.ResourceEntry) []map[string]any {
	var subs []map[string]any
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) == nil {
			subs = append(subs, map[string]any{
				"SubscriptionArn": sd.SubscriptionArn,
				"TopicArn":        sd.TopicArn,
				"Protocol":        sd.Protocol,
				"Endpoint":        sd.Endpoint,
				"Owner":           sd.Owner,
			})
		}
	}
	if subs == nil {
		subs = []map[string]any{}
	}
	return subs
}

// ─── Publish ──────────────────────────────────────────────────────────────────

func (p *SNSProvider) Publish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	message := strParam(nr.Params, "Message")
	subject := strParam(nr.Params, "Subject")
	messageID := fmt.Sprintf("%x", md5.Sum([]byte(message+time.Now().String())))

	if tArn == "" {
		return nil, model.NewProviderError("InvalidParameter", "TopicArn is required", 400)
	}

	var msgAttrs map[string]any
	if ma, ok := nr.Params["MessageAttributes"].(map[string]any); ok {
		msgAttrs = ma
	}

	// ── FIFO dedup ────────────────────────────────────────────────────────────
	var td topicData
	if loadErr := loadEntry(ctx, p.resources, "sns_topics", tArn, &td); loadErr == nil {
		if td.Attributes["FifoTopic"] == "true" {
			dedupID := strParam(nr.Params, "MessageDeduplicationId")
			if dedupID == "" && td.Attributes["ContentBasedDeduplication"] == "true" {
				dedupID = fmt.Sprintf("%x", md5.Sum([]byte(message)))
			}
			if dedupID != "" {
				now := time.Now().Unix()
				if td.DedupCache == nil {
					td.DedupCache = make(map[string]int64)
				}
				// Prune expired entries.
				for k, exp := range td.DedupCache {
					if exp < now {
						delete(td.DedupCache, k)
					}
				}
				if _, seen := td.DedupCache[dedupID]; seen {
					return provider.OK(map[string]any{"MessageId": messageID}), nil
				}
				td.DedupCache[dedupID] = now + 300 // 5-min window
				_ = saveEntry(ctx, p.resources, "sns_topics", tArn, td)
			}
		}
	}

	// ── Deliver to each matching subscription ─────────────────────────────────
	entries, _ := p.resources.List(ctx, "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != tArn {
			continue
		}
		// Evaluate filter policy.
		if fp := sd.Attributes["FilterPolicy"]; fp != "" {
			if !matchesFilterPolicy(fp, msgAttrs) {
				continue
			}
		}
		rawDelivery := sd.Attributes["RawMessageDelivery"] == "true"
		switch sd.Protocol {
		case "sqs":
			p.deliverToSQS(ctx, sd.Endpoint, tArn, messageID, message, subject,
				nr.Region, nr.AccountID, msgAttrs, rawDelivery)
		case "lambda":
			go p.deliverToLambda(ctx, sd.SubscriptionArn, tArn, messageID, message, subject, sd.Endpoint, msgAttrs)
		case "http", "https":
			go p.deliverToHTTP(tArn, messageID, message, subject, sd.Endpoint, msgAttrs)
		}
	}

	return provider.OK(map[string]any{"MessageId": messageID}), nil
}

func (p *SNSProvider) deliverToSQS(ctx context.Context, queueURL, tArn, messageID, message, subject, region, accountID string, msgAttrs map[string]any, rawDelivery bool) {
	var bodyStr string
	if rawDelivery {
		bodyStr = message
	} else {
		envelope := map[string]any{
			"Type":      "Notification",
			"MessageId": messageID,
			"TopicArn":  tArn,
			"Subject":   subject,
			"Message":   message,
			"Timestamp": time.Now().UTC().Format(time.RFC3339),
		}
		if len(msgAttrs) > 0 {
			envelope["MessageAttributes"] = msgAttrs
		}
		b, _ := json.Marshal(envelope)
		bodyStr = string(b)
	}

	// Prefer sqsSender (resolves ARN → URL correctly) over raw message store.
	if p.sqsSender != nil {
		sqsAttrs := make(map[string]SQSMessageAttribute, len(msgAttrs))
		for k, v := range msgAttrs {
			if m, ok := v.(map[string]any); ok {
				dt, _ := m["DataType"].(string)
				sv, _ := m["StringValue"].(string)
				sqsAttrs[k] = SQSMessageAttribute{DataType: dt, StringValue: sv}
			}
		}
		_ = p.sqsSender.InternalSend(ctx, queueURL, bodyStr, sqsAttrs, SQSSourceContext{
			SourceArn:        tArn,
			ServicePrincipal: "sns.amazonaws.com",
		})
		return
	}

	if p.messages == nil {
		return
	}
	sqsMsgID := fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), rand.Int31())
	msg := sqsstore.SQSMessage{
		MessageID: sqsMsgID,
		QueueURL:  queueURL,
		Body:      bodyStr,
		SentAt:    time.Now(),
	}
	_, _, _ = p.messages.Send(ctx, msg)
}

func (p *SNSProvider) deliverToLambda(ctx context.Context, subArn, tArn, messageID, message, subject, functionName string, msgAttrs map[string]any) {
	if p.invoker == nil {
		return
	}
	// Extract bare function name from ARN or use as-is.
	fnName := functionName
	if parts := strings.Split(functionName, ":"); len(parts) >= 7 {
		fnName = parts[6]
	}
	record := map[string]any{
		"EventSource":          "aws:sns",
		"EventVersion":         "1.0",
		"EventSubscriptionArn": subArn,
		"Sns": map[string]any{
			"Type":              "Notification",
			"MessageId":         messageID,
			"TopicArn":          tArn,
			"Subject":           subject,
			"Message":           message,
			"Timestamp":         time.Now().UTC().Format(time.RFC3339),
			"SignatureVersion":  "1",
			"Signature":         "EXAMPLE",
			"SigningCertUrl":    "EXAMPLE",
			"UnsubscribeUrl":   "EXAMPLE",
			"MessageAttributes": msgAttrs,
		},
	}
	payload, _ := json.Marshal(map[string]any{"Records": []any{record}})
	_, _ = p.invoker.InvokeInternal(ctx, fnName, payload)
}

func (p *SNSProvider) deliverToHTTP(tArn, messageID, message, subject, endpoint string, msgAttrs map[string]any) {
	envelope := map[string]any{
		"Type":             "Notification",
		"MessageId":        messageID,
		"TopicArn":         tArn,
		"Subject":          subject,
		"Message":          message,
		"Timestamp":        time.Now().UTC().Format(time.RFC3339),
		"SignatureVersion": "1",
		"Signature":        "EXAMPLE",
		"SigningCertURL":   "EXAMPLE",
	}
	if len(msgAttrs) > 0 {
		envelope["MessageAttributes"] = msgAttrs
	}
	body, _ := json.Marshal(envelope)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-sns-message-type", "Notification")
	req.Header.Set("x-amz-sns-topic-arn", tArn)
	req.Header.Set("x-amz-sns-message-id", messageID)
	resp, err := p.httpClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func (p *SNSProvider) PublishBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	entries, _ := nr.Params["PublishBatchRequestEntries"].([]any)
	var successful []map[string]any
	for _, e := range entries {
		m, _ := e.(map[string]any)
		id, _ := m["Id"].(string)
		message, _ := m["Message"].(string)
		msgID := fmt.Sprintf("%x", md5.Sum([]byte(message)))
		fakeNR := &model.NormalizedRequest{
			Params:    map[string]any{"TopicArn": tArn, "Message": message},
			Region:    nr.Region,
			AccountID: nr.AccountID,
		}
		if _, err := p.Publish(ctx, fakeNR); err == nil {
			successful = append(successful, map[string]any{"Id": id, "MessageId": msgID})
		}
	}
	return provider.OK(map[string]any{"Successful": successful, "Failed": []map[string]any{}}), nil
}

// ─── Filter policy ────────────────────────────────────────────────────────────

// matchesFilterPolicy returns true if the message attributes satisfy the filter policy.
func matchesFilterPolicy(filterPolicyJSON string, msgAttrs map[string]any) bool {
	if filterPolicyJSON == "" {
		return true
	}
	var policy map[string][]any
	if err := json.Unmarshal([]byte(filterPolicyJSON), &policy); err != nil {
		return true // malformed policy → pass-through
	}
	for key, rules := range policy {
		attr, hasAttr := msgAttrs[key]
		if !hasAttr {
			// "exists: false" rule passes when attribute is absent.
			for _, r := range rules {
				if m, ok := r.(map[string]any); ok {
					if ex, ok := m["exists"].(bool); ok && !ex {
						goto nextKey
					}
				}
			}
			return false // attribute absent and no matching rule
		nextKey:
			continue
		}
		attrMap, _ := attr.(map[string]any)
		strVal, _ := attrMap["StringValue"].(string)
		dataType, _ := attrMap["DataType"].(string)
		isNum := strings.HasPrefix(dataType, "Number")
		var numVal float64
		if isNum {
			numVal, _ = strconv.ParseFloat(strVal, 64)
		}
		if !filterRulesMatch(rules, strVal, numVal, isNum) {
			return false
		}
	}
	return true
}

func filterRulesMatch(rules []any, strVal string, numVal float64, isNum bool) bool {
	for _, rule := range rules {
		switch r := rule.(type) {
		case string:
			if r == strVal {
				return true
			}
		case map[string]any:
			if filterSingleRuleMatch(r, strVal, numVal, isNum) {
				return true
			}
		}
	}
	return false
}

func filterSingleRuleMatch(r map[string]any, strVal string, numVal float64, isNum bool) bool {
	if prefix, ok := r["prefix"].(string); ok {
		return strings.HasPrefix(strVal, prefix)
	}
	if ab, ok := r["anything-but"]; ok {
		switch v := ab.(type) {
		case []any:
			for _, item := range v {
				if fmt.Sprint(item) == strVal {
					return false
				}
			}
			return true
		case string:
			return v != strVal
		}
	}
	if numCond, ok := r["numeric"].([]any); ok && isNum {
		return evaluateNumericCondition(numCond, numVal)
	}
	if ex, ok := r["exists"].(bool); ok {
		return ex // attribute exists, so "exists: true" matches
	}
	return false
}

func evaluateNumericCondition(cond []any, val float64) bool {
	for i := 0; i+1 < len(cond); i += 2 {
		op, _ := cond[i].(string)
		var thresh float64
		switch t := cond[i+1].(type) {
		case float64:
			thresh = t
		case string:
			thresh, _ = strconv.ParseFloat(t, 64)
		}
		switch op {
		case ">":
			if !(val > thresh) {
				return false
			}
		case ">=":
			if !(val >= thresh) {
				return false
			}
		case "<":
			if !(val < thresh) {
				return false
			}
		case "<=":
			if !(val <= thresh) {
				return false
			}
		case "=":
			if val != thresh {
				return false
			}
		}
	}
	return true
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *SNSProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	if tags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range tags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				td.Tags[k] = v
			}
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(td.Tags, fmt.Sprintf("%v", k))
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	var tags []map[string]any
	for k, v := range td.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	if tags == nil {
		tags = []map[string]any{}
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}
