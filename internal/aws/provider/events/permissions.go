package events

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtBusPolicy = "eb_bus_policy"

func (p *EventBridgeProvider) PutPermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	busName := strParam(nr.Params, "EventBusName")
	if busName == "" {
		busName = "default"
	}
	statementID := strParam(nr.Params, "StatementId")
	principal := strParam(nr.Params, "Principal")
	action := strParam(nr.Params, "Action")

	var doc map[string]any
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtBusPolicy, busName); err == nil {
		json.Unmarshal(e.Data, &doc)
	}
	if doc == nil {
		doc = map[string]any{"Version": "2012-10-17", "Statement": []any{}}
	}
	stmts, _ := doc["Statement"].([]any)
	filtered := stmts[:0]
	for _, s := range stmts {
		if sm, ok := s.(map[string]any); ok && sm["Sid"] == statementID {
			continue
		}
		filtered = append(filtered, s)
	}
	stmt := map[string]any{
		"Sid":       statementID,
		"Effect":    "Allow",
		"Action":    action,
		"Principal": map[string]any{"AWS": principal},
	}
	doc["Statement"] = append(filtered, stmt)
	raw, _ := json.Marshal(doc)
	entry := store.ResourceEntry{Type: rtBusPolicy, ID: busName, Data: raw}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(map[string]any{}), nil
}

func (p *EventBridgeProvider) RemovePermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	busName := strParam(nr.Params, "EventBusName")
	if busName == "" {
		busName = "default"
	}
	statementID := strParam(nr.Params, "StatementId")

	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtBusPolicy, busName)
	if err != nil {
		return provider.OK(map[string]any{}), nil
	}
	var doc map[string]any
	if err := json.Unmarshal(e.Data, &doc); err != nil {
		return provider.OK(map[string]any{}), nil
	}
	stmts, _ := doc["Statement"].([]any)
	filtered := stmts[:0]
	for _, s := range stmts {
		if sm, ok := s.(map[string]any); ok && sm["Sid"] == statementID {
			continue
		}
		filtered = append(filtered, s)
	}
	doc["Statement"] = filtered
	raw, _ := json.Marshal(doc)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtBusPolicy, ID: busName, Data: raw})
	return provider.OK(map[string]any{}), nil
}
