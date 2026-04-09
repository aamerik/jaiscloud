// Package table implements the DynamoDB provider (TableProvider).
package table

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	dynamostore "jaiscloud/internal/store/aws/dynamodb"
	streamstore "jaiscloud/internal/store/stream"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// TableProvider handles all DynamoDB operations.
type TableProvider struct {
	resources store.ResourceStore
	items     dynamostore.DynamoDBItemStore
	streams   *streamstore.MemoryStreamStore
}

func New(resources store.ResourceStore, items dynamostore.DynamoDBItemStore) *TableProvider {
	return &TableProvider{resources: resources, items: items}
}

func NewWithStreams(resources store.ResourceStore, items dynamostore.DynamoDBItemStore, streams *streamstore.MemoryStreamStore) *TableProvider {
	return &TableProvider{resources: resources, items: items, streams: streams}
}

func (p *TableProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Control plane
		"Table.CreateTable":   p.CreateTable,
		"Table.DescribeTable": p.DescribeTable,
		"Table.DeleteTable":   p.DeleteTable,
		"Table.ListTables":    p.ListTables,
		"Table.UpdateTable":   p.UpdateTable,
		// Data plane
		"Table.PutItem":    p.PutItem,
		"Table.GetItem":    p.GetItem,
		"Table.DeleteItem": p.DeleteItem,
		"Table.UpdateItem": p.UpdateItem,
		"Table.Query":      p.Query,
		"Table.Scan":       p.Scan,
		// Batch
		"Table.BatchWriteItem": p.BatchWriteItem,
		"Table.BatchGetItem":   p.BatchGetItem,
		// Transact (serial, no ACID guarantee)
		"Table.TransactWriteItems": p.TransactWriteItems,
		"Table.TransactGetItems":   p.TransactGetItems,
		// Tags
		"Table.TagResource":       p.TagResource,
		"Table.UntagResource":     p.UntagResource,
		"Table.ListTagsOfResource": p.ListTagsOfResource,
	}
}

// ─── Table metadata ───────────────────────────────────────────────────────────

type tableSchema struct {
	TableName              string                   `json:"TableName"`
	TableArn               string                   `json:"TableArn"`
	TableStatus            string                   `json:"TableStatus"`
	KeySchema              []map[string]string      `json:"KeySchema"`
	AttributeDefinitions   []map[string]string      `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []map[string]any         `json:"GlobalSecondaryIndexes,omitempty"`
	BillingMode            string                   `json:"BillingMode"`
	ItemCount              int                      `json:"ItemCount"`
	TableSizeBytes         int                      `json:"TableSizeBytes"`
	CreationDateTime       time.Time                `json:"CreationDateTime"`
	Tags                   map[string]string        `json:"Tags"`
	StreamEnabled          bool                     `json:"StreamEnabled"`
	StreamViewType         string                   `json:"StreamViewType"`
	LatestStreamArn        string                   `json:"LatestStreamArn"`
}

func tableArn(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, accountID, name)
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case string:
			i, _ := strconv.Atoi(n)
			return i
		}
	}
	return def
}

func (p *TableProvider) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "TableName is required", 400)
	}
	arn := tableArn(nr.Region, nr.AccountID, name)

	// Parse KeySchema and AttributeDefinitions.
	keySchema := parseKeySchema(nr.Params["KeySchema"])
	attrDefs := parseAttrDefs(nr.Params["AttributeDefinitions"])
	gsis := parseGSIs(nr.Params["GlobalSecondaryIndexes"])
	billing := strParam(nr.Params, "BillingMode")
	if billing == "" {
		billing = "PROVISIONED"
	}

	ts := tableSchema{
		TableName:              name,
		TableArn:               arn,
		TableStatus:            "ACTIVE",
		KeySchema:              keySchema,
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
		BillingMode:            billing,
		CreationDateTime:       time.Now().UTC(),
		Tags:                   map[string]string{},
	}

	raw, _ := json.Marshal(ts)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: "dynamodb_tables", ID: name, Data: json.RawMessage(raw)}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("ResourceInUseException", "Table already exists", 400)
		}
		return nil, err
	}
	return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}

func (p *TableProvider) DescribeTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	return provider.OK(map[string]any{"Table": tableDesc(ts)}), nil
}

func (p *TableProvider) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	_ = p.resources.Delete(ctx, "dynamodb_tables", name)
	p.items.Reset() // simplification: reset all items (acceptable for emulator)
	return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}

func (p *TableProvider) ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, "dynamodb_tables", "")
	var names []string
	for _, e := range entries {
		var ts tableSchema
		if json.Unmarshal(e.Data, &ts) == nil {
			names = append(names, ts.TableName)
		}
	}
	if names == nil {
		names = []string{}
	}
	return provider.OK(map[string]any{"TableNames": names}), nil
}

func (p *TableProvider) UpdateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	if billing := strParam(nr.Params, "BillingMode"); billing != "" {
		ts.BillingMode = billing
	}
	// Handle StreamSpecification
	if ss, ok := nr.Params["StreamSpecification"].(map[string]any); ok {
		enabled, _ := ss["StreamEnabled"].(bool)
		viewType, _ := ss["StreamViewType"].(string)
		if viewType == "" {
			viewType = "NEW_AND_OLD_IMAGES"
		}
		ts.StreamEnabled = enabled
		ts.StreamViewType = viewType
		if enabled && p.streams != nil {
			streamArn := fmt.Sprintf("arn:aws:dynamodb:us-east-1:000000000000:table/%s/stream/%d", name, time.Now().UnixNano())
			ts.LatestStreamArn = streamArn
			p.streams.Enable(name, streamArn)
		} else if !enabled && p.streams != nil {
			p.streams.Disable(name)
		}
	}
	_ = p.saveTable(ctx, ts)
	return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}

// ─── Item operations ──────────────────────────────────────────────────────────

func (p *TableProvider) PutItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	item := itemParam(nr.Params, "Item")
	if item == nil {
		return nil, model.NewProviderError("ValidationException", "Item is required", 400)
	}
	ts, _ := p.loadTable(ctx, name)
	pkHash := computePKHash(item, ts)
	oldItem, _ := p.items.GetItem(ctx, name, pkHash)
	if err := p.items.PutItem(ctx, name, pkHash, item); err != nil {
		return nil, err
	}
	eventName := "INSERT"
	if oldItem != nil {
		eventName = "MODIFY"
	}
	p.appendStreamRecord(name, eventName, pkHash, extractKeys(item, ts), item, oldItem)
	return provider.OK(map[string]any{}), nil
}

func (p *TableProvider) GetItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	key := itemParam(nr.Params, "Key")
	if key == nil {
		return nil, model.NewProviderError("ValidationException", "Key is required", 400)
	}
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	pkHash := computePKHash(key, ts)
	item, err := p.items.GetItem(ctx, name, pkHash)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	if item != nil {
		result["Item"] = item
	}
	return provider.OK(result), nil
}

func (p *TableProvider) DeleteItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	key := itemParam(nr.Params, "Key")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	pkHash := computePKHash(key, ts)
	oldItem, _ := p.items.GetItem(ctx, name, pkHash)
	if err := p.items.DeleteItem(ctx, name, pkHash); err != nil {
		return provider.OK(map[string]any{}), err
	}
	p.appendStreamRecord(name, "REMOVE", pkHash, key, nil, oldItem)
	return provider.OK(map[string]any{}), nil
}

func (p *TableProvider) UpdateItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	key := itemParam(nr.Params, "Key")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	pkHash := computePKHash(key, ts)

	spec := dynamostore.UpdateSpec{
		UpdateExpression:          strParam(nr.Params, "UpdateExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ReturnValues:              strParam(nr.Params, "ReturnValues"),
	}

	oldItem, _ := p.items.GetItem(ctx, name, pkHash)
	updated, err := p.items.UpdateItem(ctx, name, pkHash, key, spec)
	if err != nil {
		return nil, err
	}
	p.appendStreamRecord(name, "MODIFY", pkHash, key, updated, oldItem)
	result := map[string]any{}
	if spec.ReturnValues == "ALL_NEW" {
		result["Attributes"] = updated
	}
	return provider.OK(result), nil
}

func (p *TableProvider) Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	q := dynamostore.QuerySpec{
		IndexName:                 strParam(nr.Params, "IndexName"),
		KeyConditionExpression:    strParam(nr.Params, "KeyConditionExpression"),
		FilterExpression:          strParam(nr.Params, "FilterExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ScanIndexForward:          true,
		Limit:                     intParam(nr.Params, "Limit", 0),
	}
	items, lastKey, err := p.items.Query(ctx, name, q)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"Items": items,
		"Count": len(items),
	}
	if lastKey != "" {
		result["LastEvaluatedKey"] = lastKey
	}
	return provider.OK(result), nil
}

func (p *TableProvider) Scan(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	sc := dynamostore.ScanSpec{
		FilterExpression:          strParam(nr.Params, "FilterExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		Limit:                     intParam(nr.Params, "Limit", 0),
	}
	items, lastKey, err := p.items.Scan(ctx, name, sc)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"Items": items,
		"Count": len(items),
	}
	if lastKey != "" {
		result["LastEvaluatedKey"] = lastKey
	}
	return provider.OK(result), nil
}

// ─── Batch operations ─────────────────────────────────────────────────────────

func (p *TableProvider) BatchWriteItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	requestItems, _ := nr.Params["RequestItems"].(map[string]any)
	var reqs []dynamostore.BatchWriteRequest
	for tableName, v := range requestItems {
		ts, _ := p.loadTable(ctx, tableName)
		writeReqs, _ := v.([]any)
		for _, wr := range writeReqs {
			m, _ := wr.(map[string]any)
			if put, ok := m["PutRequest"].(map[string]any); ok {
				item := itemParam(put, "Item")
				reqs = append(reqs, dynamostore.BatchWriteRequest{
					Table:   tableName,
					PutItem: item,
					PutHash: computePKHash(item, ts),
				})
			}
			if del, ok := m["DeleteRequest"].(map[string]any); ok {
				key := itemParam(del, "Key")
				reqs = append(reqs, dynamostore.BatchWriteRequest{
					Table:      tableName,
					DeleteKey:  key,
					DeleteHash: computePKHash(key, ts),
				})
			}
		}
	}
	_, err := p.items.BatchWriteItems(ctx, reqs)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"UnprocessedItems": map[string]any{}}), nil
}

func (p *TableProvider) BatchGetItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	requestItems, _ := nr.Params["RequestItems"].(map[string]any)
	var reqs []dynamostore.BatchGetRequest
	for table, v := range requestItems {
		m, _ := v.(map[string]any)
		rawKeys, _ := m["Keys"].([]any)
		var keys []map[string]any
		for _, k := range rawKeys {
			if km, ok := k.(map[string]any); ok {
				keys = append(keys, km)
			}
		}
		reqs = append(reqs, dynamostore.BatchGetRequest{Table: table, Keys: keys})
	}
	result, err := p.items.BatchGetItems(ctx, reqs)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Responses": result, "UnprocessedKeys": map[string]any{}}), nil
}

func (p *TableProvider) TransactWriteItems(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	transactItems, _ := nr.Params["TransactItems"].([]any)
	for _, ti := range transactItems {
		m, _ := ti.(map[string]any)
		if put, ok := m["Put"].(map[string]any); ok {
			table := strParam(put, "TableName")
			item := itemParam(put, "Item")
			ts, _ := p.loadTable(ctx, table)
			if err := p.items.PutItem(ctx, table, computePKHash(item, ts), item); err != nil {
				return nil, err
			}
		}
		if del, ok := m["Delete"].(map[string]any); ok {
			table := strParam(del, "TableName")
			key := itemParam(del, "Key")
			ts, err := p.loadTable(ctx, table)
			if err == nil {
				_ = p.items.DeleteItem(ctx, table, computePKHash(key, ts))
			}
		}
		if upd, ok := m["Update"].(map[string]any); ok {
			table := strParam(upd, "TableName")
			key := itemParam(upd, "Key")
			spec := dynamostore.UpdateSpec{
				UpdateExpression:          strParam(upd, "UpdateExpression"),
				ExpressionAttributeNames:  exprNamesFrom(upd),
				ExpressionAttributeValues: exprValuesFrom(upd),
			}
			ts, err := p.loadTable(ctx, table)
			if err == nil {
				_, _ = p.items.UpdateItem(ctx, table, computePKHash(key, ts), key, spec)
			}
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *TableProvider) TransactGetItems(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	transactItems, _ := nr.Params["TransactItems"].([]any)
	var responses []map[string]any
	for _, ti := range transactItems {
		m, _ := ti.(map[string]any)
		if get, ok := m["Get"].(map[string]any); ok {
			table := strParam(get, "TableName")
			key := itemParam(get, "Key")
			ts, _ := p.loadTable(ctx, table)
			h := computePKHash(key, ts)
			item, _ := p.items.GetItem(ctx, table, h)
			if item != nil {
				responses = append(responses, map[string]any{"Item": item})
			} else {
				responses = append(responses, map[string]any{})
			}
		}
	}
	return provider.OK(map[string]any{"Responses": responses}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *TableProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	name := arnToTableName(arn)
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	if tags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range tags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				ts.Tags[k] = v
			}
		}
	}
	return provider.OK(nil), p.saveTable(ctx, ts)
}

func (p *TableProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	name := arnToTableName(arn)
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(ts.Tags, fmt.Sprintf("%v", k))
		}
	}
	return provider.OK(nil), p.saveTable(ctx, ts)
}

func (p *TableProvider) ListTagsOfResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	name := arnToTableName(arn)
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Table not found", 400)
	}
	var tags []map[string]any
	for k, v := range ts.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	if tags == nil {
		tags = []map[string]any{}
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (p *TableProvider) loadTable(ctx context.Context, name string) (tableSchema, error) {
	e, err := p.resources.Get(ctx, "dynamodb_tables", name)
	if err != nil {
		return tableSchema{}, err
	}
	var ts tableSchema
	return ts, json.Unmarshal(e.Data, &ts)
}

func (p *TableProvider) saveTable(ctx context.Context, ts tableSchema) error {
	raw, _ := json.Marshal(ts)
	return p.resources.Update(ctx, store.ResourceEntry{Type: "dynamodb_tables", ID: ts.TableName, Data: json.RawMessage(raw)})
}

func tableDesc(ts tableSchema) map[string]any {
	d := map[string]any{
		"TableName":              ts.TableName,
		"TableArn":               ts.TableArn,
		"TableStatus":            ts.TableStatus,
		"KeySchema":              ts.KeySchema,
		"AttributeDefinitions":   ts.AttributeDefinitions,
		"GlobalSecondaryIndexes": ts.GlobalSecondaryIndexes,
		"BillingModeSummary":     map[string]any{"BillingMode": ts.BillingMode},
		"ItemCount":              ts.ItemCount,
		"TableSizeBytes":         ts.TableSizeBytes,
		"CreationDateTime":       ts.CreationDateTime.Unix(),
	}
	if ts.StreamEnabled {
		d["StreamSpecification"] = map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": ts.StreamViewType,
		}
		d["LatestStreamArn"] = ts.LatestStreamArn
	}
	return d
}

func parseKeySchema(v any) []map[string]string {
	var result []map[string]string
	switch ks := v.(type) {
	case []any:
		for _, k := range ks {
			if m, ok := k.(map[string]any); ok {
				entry := map[string]string{}
				if n, ok := m["AttributeName"].(string); ok {
					entry["AttributeName"] = n
				}
				if t, ok := m["KeyType"].(string); ok {
					entry["KeyType"] = t
				}
				result = append(result, entry)
			}
		}
	}
	return result
}

func parseAttrDefs(v any) []map[string]string {
	var result []map[string]string
	switch defs := v.(type) {
	case []any:
		for _, d := range defs {
			if m, ok := d.(map[string]any); ok {
				entry := map[string]string{}
				if n, ok := m["AttributeName"].(string); ok {
					entry["AttributeName"] = n
				}
				if t, ok := m["AttributeType"].(string); ok {
					entry["AttributeType"] = t
				}
				result = append(result, entry)
			}
		}
	}
	return result
}

func parseGSIs(v any) []map[string]any {
	if v == nil {
		return nil
	}
	switch g := v.(type) {
	case []any:
		var result []map[string]any
		for _, gi := range g {
			if m, ok := gi.(map[string]any); ok {
				gsi := map[string]any{
					"IndexName":  m["IndexName"],
					"KeySchema":  parseKeySchema(m["KeySchema"]),
					"Projection": m["Projection"],
					"IndexStatus": "ACTIVE",
				}
				result = append(result, gsi)
			}
		}
		return result
	}
	return nil
}

func itemParam(params map[string]any, key string) map[string]any {
	if v, ok := params[key]; ok {
		if m, ok := v.(map[string]any); ok {
			return m
		}
	}
	return nil
}

// computePKHash builds a stable hash from the key attributes as defined in the table schema.
// Falls back to itemPKHash if schema is unavailable.
func computePKHash(key map[string]any, ts tableSchema) string {
	if key == nil {
		return ""
	}
	// Extract only the key attributes in schema order for a stable hash.
	var parts []string
	for _, ks := range ts.KeySchema {
		attr := ks["AttributeName"]
		if v, ok := key[attr]; ok {
			parts = append(parts, fmt.Sprintf("%s=%v", attr, v))
		}
	}
	if len(parts) == 0 {
		return dynamostore.MemoryItemPKHash(key)
	}
	return strings.Join(parts, "|")
}

func exprNames(params map[string]any) map[string]string {
	return exprNamesFrom(params)
}

func exprNamesFrom(m map[string]any) map[string]string {
	if v, ok := m["ExpressionAttributeNames"]; ok {
		if raw, ok := v.(map[string]any); ok {
			result := make(map[string]string, len(raw))
			for k, val := range raw {
				result[k] = fmt.Sprintf("%v", val)
			}
			return result
		}
	}
	return nil
}

func exprValues(params map[string]any) map[string]any {
	return exprValuesFrom(params)
}

func exprValuesFrom(m map[string]any) map[string]any {
	if v, ok := m["ExpressionAttributeValues"]; ok {
		if raw, ok := v.(map[string]any); ok {
			return raw
		}
	}
	return nil
}

// extractKeys returns only the primary key attributes from an item.
func extractKeys(item map[string]any, ts tableSchema) map[string]any {
	if len(ts.KeySchema) == 0 {
		return item
	}
	keys := make(map[string]any, len(ts.KeySchema))
	for _, ks := range ts.KeySchema {
		attr := ks["AttributeName"]
		if v, ok := item[attr]; ok {
			keys[attr] = v
		}
	}
	return keys
}

func arnToTableName(arn string) string {
	// arn:aws:dynamodb:region:accountID:table/tableName
	parts := strings.Split(arn, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return arn
}
