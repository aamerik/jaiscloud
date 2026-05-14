package notification

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

func (p *SNSProvider) AddPermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	label := strParam(nr.Params, "Label")

	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	if td.Attributes == nil {
		td.Attributes = map[string]string{}
	}

	var doc map[string]any
	if pol := td.Attributes["Policy"]; pol != "" {
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
		"Action":    extractSNSMemberList(nr.Params, "ActionName"),
		"Principal": extractSNSMemberList(nr.Params, "AWSAccountId"),
	})
	doc["Statement"] = stmts
	raw, _ := json.Marshal(doc)
	td.Attributes["Policy"] = string(raw)

	return provider.OK(map[string]any{}), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func (p *SNSProvider) RemovePermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	label := strParam(nr.Params, "Label")

	var td topicData
	if err := loadEntry(ctx, p.resources, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	if td.Attributes != nil {
		var doc map[string]any
		if pol := td.Attributes["Policy"]; pol != "" {
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
			td.Attributes["Policy"] = string(raw)
		}
	}
	return provider.OK(map[string]any{}), saveEntry(ctx, p.resources, "sns_topics", arn, td)
}

func extractSNSMemberList(params map[string]any, key string) []string {
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
