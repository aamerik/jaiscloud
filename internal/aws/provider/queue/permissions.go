package queue

import (
	"context"
	"encoding/json"
	"net/http"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func (p *QueueProvider) AddPermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	state, err := p.getQueueByURL(ctx, queueURL)
	if err != nil {
		return nil, &model.ProviderError{Code: "NonExistentQueue", Message: "Queue does not exist", HTTPStatus: http.StatusBadRequest}
	}
	label, _ := stringParam(nr.Params, "Label")

	attrs, _ := state["Attributes"].(map[string]string)
	if attrs == nil {
		attrs = map[string]string{}
	}
	var doc map[string]any
	if pol := attrs["Policy"]; pol != "" {
		json.Unmarshal([]byte(pol), &doc)
	}
	if doc == nil {
		doc = map[string]any{"Version": "2012-10-17", "Statement": []any{}}
	}
	stmts, _ := doc["Statement"].([]any)
	filtered := stmts[:0]
	for _, s := range stmts {
		if sm, ok := s.(map[string]any); ok && sm["Sid"] == label {
			continue
		}
		filtered = append(filtered, s)
	}
	stmts = append(filtered, map[string]any{
		"Sid":       label,
		"Effect":    "Allow",
		"Action":    extractMemberList(nr.Params, "ActionName"),
		"Principal": extractMemberList(nr.Params, "AWSAccountId"),
	})
	doc["Statement"] = stmts
	raw, _ := json.Marshal(doc)
	attrs["Policy"] = string(raw)
	state["Attributes"] = attrs

	saveErr := p.saveQueueState(ctx, queueURL, state)
	return provider.OK(map[string]any{}), saveErr
}

func (p *QueueProvider) RemovePermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	state, err := p.getQueueByURL(ctx, queueURL)
	if err != nil {
		return nil, &model.ProviderError{Code: "NonExistentQueue", Message: "Queue does not exist", HTTPStatus: http.StatusBadRequest}
	}
	label, _ := stringParam(nr.Params, "Label")

	attrs, _ := state["Attributes"].(map[string]string)
	if attrs != nil {
		var doc map[string]any
		if pol := attrs["Policy"]; pol != "" {
			json.Unmarshal([]byte(pol), &doc)
		}
		if doc != nil {
			stmts, _ := doc["Statement"].([]any)
			filtered := stmts[:0]
			for _, s := range stmts {
				if sm, ok := s.(map[string]any); ok && sm["Sid"] == label {
					continue
				}
				filtered = append(filtered, s)
			}
			doc["Statement"] = filtered
			raw, _ := json.Marshal(doc)
			attrs["Policy"] = string(raw)
			state["Attributes"] = attrs
		}
	}
	return provider.OK(map[string]any{}), p.saveQueueState(ctx, queueURL, state)
}

// getQueueByURL loads queue state by URL.
func (p *QueueProvider) getQueueByURL(ctx context.Context, queueURL string) (map[string]any, error) {
	e, err := p.resources.Get(ctx, "sqs_queues", queueURL)
	if err != nil {
		return nil, err
	}
	var state map[string]any
	if err := json.Unmarshal(e.Data, &state); err != nil {
		return nil, err
	}
	return state, nil
}

// saveQueueState persists queue state back to the store.
func (p *QueueProvider) saveQueueState(ctx context.Context, queueURL string, state map[string]any) error {
	data, _ := json.Marshal(state)
	return p.resources.Update(ctx, store.ResourceEntry{Type: "sqs_queues", ID: queueURL, Data: data})
}

// extractMemberList reads a []any or string param as a []string.
func extractMemberList(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return []string{}
	}
	switch val := v.(type) {
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return val
	case string:
		if val != "" {
			return []string{val}
		}
	}
	return []string{}
}
