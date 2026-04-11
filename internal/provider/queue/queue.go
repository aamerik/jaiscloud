package queue

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	sqsstore "jaiscloud/internal/store/aws/sqs"
)

// QueueProvider implements all SQS operations.
// It is cloud-agnostic — the SQS codec translates wire format, this
// provider works with NormalizedRequest.Params maps.
type QueueProvider struct {
	resources store.ResourceStore
	messages  sqsstore.SQSMessageStore
	clock     clock.Clock
	bus       *events.EventBus
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, clk clock.Clock, bus *events.EventBus) *QueueProvider {
	return &QueueProvider{resources: resources, messages: messages, clock: clk, bus: bus}
}

// Routes returns the provider's route map for registration in the Registry.
func (p *QueueProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Control plane
		"Queue.CreateQueue":        p.CreateQueue,
		"Queue.DeleteQueue":        p.DeleteQueue,
		"Queue.ListQueues":         p.ListQueues,
		"Queue.GetQueueUrl":        p.GetQueueUrl,
		"Queue.GetQueueAttributes": p.GetQueueAttributes,
		"Queue.SetQueueAttributes": p.SetQueueAttributes,

		// Data plane
		"Queue.SendMessage":    p.SendMessage,
		"Queue.ReceiveMessage": p.ReceiveMessage,
		"Queue.DeleteMessage":  p.DeleteMessage,
		"Queue.ChangeMessageVisibility": p.ChangeMessageVisibility,
		"Queue.PurgeQueue":     p.PurgeQueue,

		// Batch operations
		"Queue.SendMessageBatch":              p.SendMessageBatch,
		"Queue.DeleteMessageBatch":            p.DeleteMessageBatch,
		"Queue.ChangeMessageVisibilityBatch":  p.ChangeMessageVisibilityBatch,

		// Tags
		"Queue.TagQueue":      p.TagQueue,
		"Queue.UntagQueue":    p.UntagQueue,
		"Queue.ListQueueTags": p.ListQueueTags,
	}
}

// ─── Control Plane ────────────────────────────────────────────────────────────

func (p *QueueProvider) CreateQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, ok := stringParam(nr.Params, "QueueName")
	if !ok || name == "" {
		return nil, model.NewProviderError("InvalidParameter", "QueueName is required", 400)
	}
	attrs := attrsParam(nr.Params, "Attributes")

	isFIFO := strings.HasSuffix(name, ".fifo")
	if isFIFO {
		if attrs["FifoQueue"] != "true" {
			attrs["FifoQueue"] = "true"
		}
	}

	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", nr.Port, name)

	// Idempotency: if queue exists with same name return its URL
	if _, err := p.resources.Get(ctx, "sqs_queues", queueURL); err == nil {
		return provider.OK(map[string]any{"QueueUrl": queueURL}), nil
	}

	now := p.clock.Now()
	state := map[string]any{
		"QueueName":                     name,
		"QueueUrl":                      queueURL,
		"QueueArn":                      fmt.Sprintf("arn:aws:sqs:%s:000000000000:%s", nr.Region, name),
		"IsFifo":                        isFIFO,
		"Attributes":                    attrs,
		"Tags":                          map[string]string{},
		"CreatedTimestamp":              strconv.FormatInt(now.Unix(), 10),
		"LastModifiedTimestamp":         strconv.FormatInt(now.Unix(), 10),
		"VisibilityTimeout":             attrOrDefault(attrs, "VisibilityTimeout", "30"),
		"DelaySeconds":                  attrOrDefault(attrs, "DelaySeconds", "0"),
		"MaximumMessageSize":            attrOrDefault(attrs, "MaximumMessageSize", "262144"),
		"MessageRetentionPeriod":        attrOrDefault(attrs, "MessageRetentionPeriod", "345600"),
		"ReceiveMessageWaitTimeSeconds": attrOrDefault(attrs, "ReceiveMessageWaitTimeSeconds", "0"),
	}

	if rp, ok := attrs["RedrivePolicy"]; ok {
		state["RedrivePolicy"] = rp
	}
	if mc, ok := attrs["MaxReceiveCount"]; ok {
		state["MaxReceiveCount"] = mc
	}

	data, _ := json.Marshal(state)
	if err := p.resources.Create(ctx, store.ResourceEntry{
		Type: "sqs_queues", ID: queueURL, Data: data, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return nil, err
	}

	return provider.OK(map[string]any{"QueueUrl": queueURL}), nil
}

func (p *QueueProvider) DeleteQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	if err := p.resources.Delete(ctx, "sqs_queues", queueURL); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "queue does not exist", 400)
		}
		return nil, err
	}
	p.messages.Purge(ctx, queueURL)
	return provider.OK(map[string]any{}), nil
}

func (p *QueueProvider) ListQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix, _ := stringParam(nr.Params, "QueueNamePrefix")
	entries, err := p.resources.List(ctx, "sqs_queues", prefix)
	if err != nil {
		return nil, err
	}
	urls := make([]string, 0, len(entries))
	for _, e := range entries {
		urls = append(urls, e.ID)
	}
	return provider.OK(map[string]any{"QueueUrls": urls}), nil
}

func (p *QueueProvider) GetQueueUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, ok := stringParam(nr.Params, "QueueName")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueName is required", 400)
	}
	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", nr.Port, name)
	if _, err := p.resources.Get(ctx, "sqs_queues", queueURL); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	return provider.OK(map[string]any{"QueueUrl": queueURL}), nil
}

func (p *QueueProvider) GetQueueAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}

	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	now := p.clock.Now()
	vis, notVis, delayed, _ := p.messages.GetApproximateCounts(ctx, queueURL, now)

	attrs := buildAttributes(state, vis, notVis, delayed)
	return provider.OK(map[string]any{"Attributes": attrs}), nil
}

func (p *QueueProvider) SetQueueAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}

	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	newAttrs := attrsParam(nr.Params, "Attributes")
	existing := attrsFromState(state)
	for k, v := range newAttrs {
		existing[k] = v
		// Also update top-level fields
		state[k] = v
	}
	state["Attributes"] = existing
	state["LastModifiedTimestamp"] = strconv.FormatInt(p.clock.Now().Unix(), 10)

	data, _ := json.Marshal(state)
	entry.Data = data
	return provider.OK(map[string]any{}), p.resources.Update(ctx, entry)
}

// ─── Data Plane ───────────────────────────────────────────────────────────────

func (p *QueueProvider) SendMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	body, _ := stringParam(nr.Params, "MessageBody")

	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	now := p.clock.Now()
	msgID := newMessageID()
	delaySec := intFromState(state, "DelaySeconds", 0)
	if d, ok := nr.Params["DelaySeconds"]; ok {
		delaySec = toInt(d)
	}

	msg := sqsstore.SQSMessage{
		MessageID:  msgID,
		QueueURL:   queueURL,
		Body:       body,
		SentAt:     now,
		Attributes: map[string]string{},
	}
	if delaySec > 0 {
		msg.DelayUntil = now.Add(time.Duration(delaySec) * time.Second)
	}
	if g, ok := stringParam(nr.Params, "MessageGroupId"); ok {
		msg.GroupID = g
	}
	if d, ok := stringParam(nr.Params, "MessageDeduplicationId"); ok {
		msg.DeduplicationID = d
	}
	if ma, ok := nr.Params["MessageAttributes"]; ok {
		msg.MessageAttributes = parseMessageAttributes(ma)
	}

	origID, err := p.messages.Send(ctx, msg)
	if err != nil {
		return nil, err
	}
	if origID != "" {
		msgID = origID // FIFO dedup: return the original message's ID
	}

	md5Body := fmt.Sprintf("%x", md5.Sum([]byte(body)))
	resp := map[string]any{
		"MessageId":        msgID,
		"MD5OfMessageBody": md5Body,
	}
	if len(msg.MessageAttributes) > 0 {
		resp["MD5OfMessageAttributes"] = md5MessageAttributes(msg.MessageAttributes)
	}
	return provider.OK(resp), nil
}

func (p *QueueProvider) ReceiveMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}

	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	maxMessages := 1
	if m, ok := nr.Params["MaxNumberOfMessages"]; ok {
		maxMessages = toInt(m)
	}
	if maxMessages > 10 {
		maxMessages = 10
	}
	if maxMessages < 1 {
		maxMessages = 1
	}

	defaultVisTimeout := intFromState(state, "VisibilityTimeout", 30)
	visTimeout := defaultVisTimeout
	if v, ok := nr.Params["VisibilityTimeout"]; ok {
		visTimeout = toInt(v)
	}

	// Which message attributes to return
	attrNames := stringSliceParam(nr.Params, "MessageAttributeNames")
	sysAttrNames := stringSliceParam(nr.Params, "AttributeNames")

	now := p.clock.Now()
	msgs, err := p.messages.Receive(ctx, queueURL, maxMessages, now)
	if err != nil {
		return nil, err
	}

	// Apply the caller-specified visibility timeout
	for i := range msgs {
		p.messages.ChangeVisibility(ctx, queueURL, msgs[i].ReceiptHandle, visTimeout, now)
		msgs[i].VisibleAt = now.Add(time.Duration(visTimeout) * time.Second)

		// Check DLQ redrive
		p.checkDLQ(ctx, state, queueURL, &msgs[i])
	}

	// Build wire-format messages
	var wireMessages []map[string]any
	for _, m := range msgs {
		wm := map[string]any{
			"MessageId":     m.MessageID,
			"ReceiptHandle": m.ReceiptHandle,
			"Body":          m.Body,
			"MD5OfBody":     m.MD5OfBody,
		}
		// System attributes
		sysAttrs := buildSysAttributes(m)
		if len(sysAttrNames) > 0 {
			filtered := map[string]string{}
			for _, n := range sysAttrNames {
				if n == "All" {
					filtered = sysAttrs
					break
				}
				if v, ok := sysAttrs[n]; ok {
					filtered[n] = v
				}
			}
			if len(filtered) > 0 {
				wm["Attributes"] = filtered
			}
		}
		// Message attributes
		if len(attrNames) > 0 && len(m.MessageAttributes) > 0 {
			filtered := map[string]sqsstore.MessageAttribute{}
			for _, n := range attrNames {
				if n == "All" {
					filtered = m.MessageAttributes
					break
				}
				if v, ok := m.MessageAttributes[n]; ok {
					filtered[n] = v
				}
			}
			if len(filtered) > 0 {
				wm["MessageAttributes"] = filtered
				wm["MD5OfMessageAttributes"] = md5MessageAttributes(filtered)
			}
		}
		wireMessages = append(wireMessages, wm)
	}

	if wireMessages == nil {
		wireMessages = []map[string]any{}
	}
	return provider.OK(map[string]any{"Messages": wireMessages}), nil
}

func (p *QueueProvider) DeleteMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	receiptHandle, _ := stringParam(nr.Params, "ReceiptHandle")
	if err := p.messages.Delete(ctx, queueURL, receiptHandle); err != nil {
		// SQS returns success even for invalid handles (fire-and-forget)
		return provider.OK(map[string]any{}), nil
	}
	return provider.OK(map[string]any{}), nil
}

func (p *QueueProvider) ChangeMessageVisibility(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	receiptHandle, _ := stringParam(nr.Params, "ReceiptHandle")
	timeout := 0
	if v, ok := nr.Params["VisibilityTimeout"]; ok {
		timeout = toInt(v)
	}
	now := p.clock.Now()
	// AWS silently succeeds for invalid/expired receipt handles — do not return error.
	p.messages.ChangeVisibility(ctx, queueURL, receiptHandle, timeout, now)
	return provider.OK(map[string]any{}), nil
}

func (p *QueueProvider) PurgeQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	if _, err := p.resources.Get(ctx, "sqs_queues", queueURL); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	p.messages.Purge(ctx, queueURL)
	return provider.OK(map[string]any{}), nil
}

// ─── Batch Operations ─────────────────────────────────────────────────────────

func (p *QueueProvider) SendMessageBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}

	entries := batchEntries(nr.Params, "Entries")
	if len(entries) == 0 {
		return nil, model.NewProviderError("EmptyBatch", "batch must contain at least one message", 400)
	}
	if len(entries) > 10 {
		return nil, model.NewProviderError("BatchTooLarge", "maximum 10 messages per batch", 400)
	}

	if _, err := p.resources.Get(ctx, "sqs_queues", queueURL); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}

	var successful []map[string]any
	var failed []map[string]any

	for _, e := range entries {
		id, _ := e["Id"].(string)
		body, _ := e["MessageBody"].(string)
		msgID := newMessageID()
		now := p.clock.Now()

		msg := sqsstore.SQSMessage{
			MessageID: msgID,
			QueueURL:  queueURL,
			Body:      body,
			SentAt:    now,
		}
		if g, ok := e["MessageGroupId"].(string); ok {
			msg.GroupID = g
		}
		if d, ok := e["MessageDeduplicationId"].(string); ok {
			msg.DeduplicationID = d
		}
		if ds, ok := e["DelaySeconds"]; ok {
			if sec := toInt(ds); sec > 0 {
				msg.DelayUntil = now.Add(time.Duration(sec) * time.Second)
			}
		}
		if ma, ok := e["MessageAttributes"]; ok {
			msg.MessageAttributes = parseMessageAttributes(ma)
		}

		origID, sendErr := p.messages.Send(ctx, msg)
		if sendErr != nil {
			failed = append(failed, map[string]any{"Id": id, "Code": "InternalError", "Message": sendErr.Error(), "SenderFault": false})
			continue
		}
		if origID != "" {
			msgID = origID
		}
		md5Body := fmt.Sprintf("%x", md5.Sum([]byte(body)))
		entry := map[string]any{
			"Id":               id,
			"MessageId":        msgID,
			"MD5OfMessageBody": md5Body,
		}
		if len(msg.MessageAttributes) > 0 {
			entry["MD5OfMessageAttributes"] = md5MessageAttributes(msg.MessageAttributes)
		}
		successful = append(successful, entry)
	}

	return provider.OK(map[string]any{
		"Successful": successful,
		"Failed":     failed,
	}), nil
}

func (p *QueueProvider) DeleteMessageBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entries := batchEntries(nr.Params, "Entries")
	if len(entries) == 0 {
		return nil, model.NewProviderError("EmptyBatch", "batch must contain at least one entry", 400)
	}

	var successful []map[string]any
	var failed []map[string]any

	for _, e := range entries {
		id, _ := e["Id"].(string)
		rh, _ := e["ReceiptHandle"].(string)
		if err := p.messages.Delete(ctx, queueURL, rh); err != nil {
			failed = append(failed, map[string]any{
				"Id": id, "Code": "ReceiptHandleIsInvalid",
				"Message": "The input receipt handle is invalid.", "SenderFault": true,
			})
		} else {
			successful = append(successful, map[string]any{"Id": id})
		}
	}

	return provider.OK(map[string]any{"Successful": successful, "Failed": failed}), nil
}

func (p *QueueProvider) ChangeMessageVisibilityBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entries := batchEntries(nr.Params, "Entries")

	now := p.clock.Now()
	var successful []map[string]any
	var failed []map[string]any

	for _, e := range entries {
		id, _ := e["Id"].(string)
		rh, _ := e["ReceiptHandle"].(string)
		timeout := toInt(e["VisibilityTimeout"])
		if err := p.messages.ChangeVisibility(ctx, queueURL, rh, timeout, now); err != nil {
			failed = append(failed, map[string]any{"Id": id, "Code": "InvalidParameterValue", "Message": err.Error(), "SenderFault": true})
		} else {
			successful = append(successful, map[string]any{"Id": id})
		}
	}

	return provider.OK(map[string]any{"Successful": successful, "Failed": failed}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *QueueProvider) TagQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	tags := tagsFromState(state)
	for k, v := range attrsParam(nr.Params, "Tags") {
		tags[k] = v
	}
	state["Tags"] = tags
	data, _ := json.Marshal(state)
	entry.Data = data
	return provider.OK(map[string]any{}), p.resources.Update(ctx, entry)
}

func (p *QueueProvider) UntagQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	tags := tagsFromState(state)
	keys := stringSliceParam(nr.Params, "TagKeys")
	for _, k := range keys {
		delete(tags, k)
	}
	state["Tags"] = tags
	data, _ := json.Marshal(state)
	entry.Data = data
	return provider.OK(map[string]any{}), p.resources.Update(ctx, entry)
}

func (p *QueueProvider) ListQueueTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entry, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)
	return provider.OK(map[string]any{"Tags": tagsFromState(state)}), nil
}

// ─── DLQ ──────────────────────────────────────────────────────────────────────

func (p *QueueProvider) checkDLQ(ctx context.Context, state map[string]any, queueURL string, msg *sqsstore.SQSMessage) {
	rp, ok := state["RedrivePolicy"].(string)
	if !ok || rp == "" {
		return
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(rp), &policy); err != nil {
		return
	}
	maxCount := toInt(policy["maxReceiveCount"])
	if maxCount <= 0 {
		maxCount = 1
	}
	dlqArn, _ := policy["deadLetterTargetArn"].(string)
	if dlqArn == "" || msg.ReceiveCount < maxCount {
		return
	}

	// Move message to DLQ — reset delivery metadata so it is immediately visible.
	dlqURL := arnToURL(dlqArn, nr_port(state))
	dlqMsg := *msg
	dlqMsg.QueueURL = dlqURL
	dlqMsg.ReceiptHandle = ""
	dlqMsg.VisibleAt = time.Time{}   // clear in-flight timeout
	dlqMsg.DelayUntil = time.Time{}  // no delay in DLQ
	dlqMsg.ReceiveCount = 0
	p.messages.Send(ctx, dlqMsg)
	p.messages.Delete(ctx, queueURL, msg.ReceiptHandle)

	p.bus.Publish(events.Event{
		Type: events.EventMessageDLQ,
		Payload: events.DLQEvent{
			SourceQueueURL: queueURL,
			DLQQueueURL:    dlqURL,
			MessageID:      msg.MessageID,
		},
	})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func stringParam(params map[string]any, key string) (string, bool) {
	v, ok := params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func attrsParam(params map[string]any, key string) map[string]string {
	v, ok := params[key]
	if !ok {
		return map[string]string{}
	}
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		result := make(map[string]string, len(m))
		for k, val := range m {
			result[k] = fmt.Sprintf("%v", val)
		}
		return result
	}
	return map[string]string{}
}

func stringSliceParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		result := make([]string, 0, len(s))
		for _, item := range s {
			result = append(result, fmt.Sprintf("%v", item))
		}
		return result
	}
	return nil
}

func batchEntries(params map[string]any, key string) []map[string]any {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch entries := v.(type) {
	case []map[string]any:
		return entries
	case []any:
		result := make([]map[string]any, 0, len(entries))
		for _, e := range entries {
			if m, ok := e.(map[string]any); ok {
				result = append(result, m)
			}
		}
		return result
	}
	return nil
}

func attrOrDefault(attrs map[string]string, key, def string) string {
	if v, ok := attrs[key]; ok && v != "" {
		return v
	}
	return def
}

func attrsFromState(state map[string]any) map[string]string {
	if v, ok := state["Attributes"]; ok {
		return attrsParam(map[string]any{"a": v}, "a")
	}
	return map[string]string{}
}

func tagsFromState(state map[string]any) map[string]string {
	if v, ok := state["Tags"]; ok {
		switch t := v.(type) {
		case map[string]string:
			return t
		case map[string]any:
			result := map[string]string{}
			for k, val := range t {
				result[k] = fmt.Sprintf("%v", val)
			}
			return result
		}
	}
	return map[string]string{}
}

func intFromState(state map[string]any, key string, def int) int {
	v, ok := state[key]
	if !ok {
		return def
	}
	return toInt(v)
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func buildAttributes(state map[string]any, visible, notVisible, delayed int) map[string]string {
	a := map[string]string{
		"QueueArn":                      str(state["QueueArn"]),
		"CreatedTimestamp":               str(state["CreatedTimestamp"]),
		"LastModifiedTimestamp":          str(state["LastModifiedTimestamp"]),
		"VisibilityTimeout":              str(state["VisibilityTimeout"]),
		"DelaySeconds":                   str(state["DelaySeconds"]),
		"MaximumMessageSize":             str(state["MaximumMessageSize"]),
		"MessageRetentionPeriod":         str(state["MessageRetentionPeriod"]),
		"ReceiveMessageWaitTimeSeconds":  str(state["ReceiveMessageWaitTimeSeconds"]),
		"ApproximateNumberOfMessages":                    strconv.Itoa(visible),
		"ApproximateNumberOfMessagesNotVisible":          strconv.Itoa(notVisible),
		"ApproximateNumberOfMessagesDelayed":             strconv.Itoa(delayed),
	}
	if rp, ok := state["RedrivePolicy"]; ok {
		a["RedrivePolicy"] = str(rp)
	}
	if isFIFO, _ := state["IsFifo"].(bool); isFIFO {
		a["FifoQueue"] = "true"
	}
	return a
}

func buildSysAttributes(m sqsstore.SQSMessage) map[string]string {
	a := map[string]string{
		"SenderId":                      "000000000000",
		"SentTimestamp":                 strconv.FormatInt(m.SentAt.UnixMilli(), 10),
		"ApproximateReceiveCount":       strconv.Itoa(m.ReceiveCount),
		"ApproximateFirstReceiveTimestamp": "0",
	}
	if m.FirstReceivedAt != nil {
		a["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(m.FirstReceivedAt.UnixMilli(), 10)
	}
	return a
}

func parseMessageAttributes(v any) map[string]sqsstore.MessageAttribute {
	result := map[string]sqsstore.MessageAttribute{}
	switch m := v.(type) {
	case map[string]any:
		for k, val := range m {
			if attr, ok := val.(map[string]any); ok {
				ma := sqsstore.MessageAttribute{
					DataType: str(attr["DataType"]),
				}
				if sv, ok := attr["StringValue"]; ok {
					ma.StringValue = str(sv)
				}
				result[k] = ma
			}
		}
	case map[string]sqsstore.MessageAttribute:
		return m
	}
	return result
}

// md5MessageAttributes computes the AWS-compatible MD5 over message attributes.
// The algorithm: for each attribute sorted by name, write:
//
//	4-byte big-endian len(name) + name bytes
//	4-byte big-endian len(dataType) + dataType bytes
//	1-byte transport type: 1 = String/Number, 2 = Binary
//	4-byte big-endian len(value) + value bytes
func md5MessageAttributes(attrs map[string]sqsstore.MessageAttribute) string {
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names)

	h := md5.New()
	buf4 := make([]byte, 4)
	writeBytes := func(b []byte) {
		binary.BigEndian.PutUint32(buf4, uint32(len(b)))
		h.Write(buf4)
		h.Write(b)
	}
	for _, name := range names {
		attr := attrs[name]
		writeBytes([]byte(name))
		writeBytes([]byte(attr.DataType))
		if strings.HasPrefix(attr.DataType, "Binary") {
			h.Write([]byte{2})
			writeBytes(attr.BinaryValue)
		} else {
			// String or Number
			h.Write([]byte{1})
			writeBytes([]byte(attr.StringValue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func arnToURL(arn string, port int) string {
	// arn:aws:sqs:us-east-1:000000000000:queue-name → http://localhost:port/000000000000/queue-name
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return arn
	}
	return fmt.Sprintf("http://localhost:%d/%s/%s", port, parts[4], parts[5])
}

func nr_port(state map[string]any) int {
	url, _ := state["QueueUrl"].(string)
	if url == "" {
		return 4566
	}
	// http://localhost:4566/...
	parts := strings.Split(url, ":")
	if len(parts) >= 3 {
		portStr := strings.Split(parts[2], "/")[0]
		p, _ := strconv.Atoi(portStr)
		if p > 0 {
			return p
		}
	}
	return 4566
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func newMessageID() string {
	return fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), rand.Int31())
}
