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
		// TTL
		"Table.DescribeTimeToLive": p.DescribeTimeToLive,
		"Table.UpdateTimeToLive":   p.UpdateTimeToLive,
		// PITR
		"Table.DescribeContinuousBackups": p.DescribeContinuousBackups,
		"Table.UpdateContinuousBackups":   p.UpdateContinuousBackups,
	}
}

// ─── Table metadata ───────────────────────────────────────────────────────────

// TTLSpecification mirrors DynamoDB's TimeToLiveSpecification.
type TTLSpecification struct {
	AttributeName string `json:"AttributeName"`
	Enabled       bool   `json:"Enabled"`
}

type tableSchema struct {
	TableName              string              `json:"TableName"`
	TableArn               string              `json:"TableArn"`
	TableStatus            string              `json:"TableStatus"`
	KeySchema              []map[string]string `json:"KeySchema"`
	AttributeDefinitions   []map[string]string `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []map[string]any    `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []map[string]any    `json:"LocalSecondaryIndexes,omitempty"`
	BillingMode            string              `json:"BillingMode"`
	ItemCount              int                 `json:"ItemCount"`
	TableSizeBytes         int                 `json:"TableSizeBytes"`
	CreationDateTime       time.Time           `json:"CreationDateTime"`
	Tags                   map[string]string   `json:"Tags"`
	StreamEnabled          bool                `json:"StreamEnabled"`
	StreamViewType         string              `json:"StreamViewType"`
	LatestStreamArn        string              `json:"LatestStreamArn"`
	TTLSpec                *TTLSpecification   `json:"TTLSpec,omitempty"`
	PITREnabled            bool                `json:"PITREnabled"`
}

func tableArn(nr *model.NormalizedRequest, name string) string {
	if nr.ResourceID != nil {
		return nr.ResourceID("dynamodb-table", name)
	}
	return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", nr.Region, nr.AccountID, name)
}

func streamResourceID(nr *model.NormalizedRequest, tableName, label string) string {
	if nr.ResourceID != nil {
		return nr.ResourceID("dynamodb-stream", tableName+"/stream/"+label)
	}
	return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s/stream/%s", nr.Region, nr.AccountID, tableName, label)
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
	arn := tableArn(nr, name)

	// Parse KeySchema and AttributeDefinitions.
	keySchema := parseKeySchema(nr.Params["KeySchema"])
	attrDefs := parseAttrDefs(nr.Params["AttributeDefinitions"])
	gsis := parseGSIs(nr.Params["GlobalSecondaryIndexes"])
	lsis := parseLSIs(nr.Params["LocalSecondaryIndexes"])
	billing := strParam(nr.Params, "BillingMode")
	if billing == "" {
		billing = "PROVISIONED"
	}

	if len(gsis) > 20 {
		return nil, model.NewProviderError("ValidationException", "Number of GlobalSecondaryIndexes exceeds the limit of 20", 400)
	}
	if len(lsis) > 5 {
		return nil, model.NewProviderError("ValidationException", "Number of LocalSecondaryIndexes exceeds the limit of 5", 400)
	}

	ts := tableSchema{
		TableName:              name,
		TableArn:               arn,
		TableStatus:            "ACTIVE",
		KeySchema:              keySchema,
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
		LocalSecondaryIndexes:  lsis,
		BillingMode:            billing,
		CreationDateTime:       time.Now().UTC(),
		Tags:                   map[string]string{},
	}

	// Handle StreamSpecification at table creation time.
	if ss, ok := nr.Params["StreamSpecification"].(map[string]any); ok {
		enabled, _ := ss["StreamEnabled"].(bool)
		viewType, _ := ss["StreamViewType"].(string)
		if viewType == "" {
			viewType = "NEW_AND_OLD_IMAGES"
		}
		ts.StreamEnabled = enabled
		ts.StreamViewType = viewType
		if enabled && p.streams != nil {
			label := fmt.Sprintf("%d", time.Now().UnixNano())
			streamArn := streamResourceID(nr, name, label)
			ts.LatestStreamArn = streamArn
			p.streams.Enable(name, streamArn)
		}
	}

	raw, _ := json.Marshal(ts)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: "dynamodb_tables", ID: name, Data: json.RawMessage(raw)}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, model.NewProviderError("ResourceInUseException", "Table already exists", 400)
		}
		return nil, err
	}

	// Initialise per-table item storage (no-op for memory store, DDL for postgres store).
	storeSchema := toStoreSchema(ts, attrDefs)
	if err := p.items.CreateTableSchema(ctx, storeSchema); err != nil {
		// Non-fatal: legacy store doesn't need this. Log and continue.
		_ = err
	}

	return provider.OK(map[string]any{"TableDescription": tableDesc(ts)}), nil
}

func (p *TableProvider) DescribeTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	return provider.OK(map[string]any{"Table": tableDesc(ts)}), nil
}

func (p *TableProvider) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	_ = p.resources.Delete(ctx, "dynamodb_tables", name)
	_ = p.items.DropTableSchema(ctx, name)
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
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	if billing := strParam(nr.Params, "BillingMode"); billing != "" {
		ts.BillingMode = billing
	}
	// Handle StreamSpecification.
	if ss, ok := nr.Params["StreamSpecification"].(map[string]any); ok {
		enabled, _ := ss["StreamEnabled"].(bool)
		viewType, _ := ss["StreamViewType"].(string)
		if viewType == "" {
			viewType = "NEW_AND_OLD_IMAGES"
		}
		ts.StreamEnabled = enabled
		ts.StreamViewType = viewType
		if enabled && p.streams != nil {
			streamArn := streamResourceID(nr, name, fmt.Sprintf("%d", time.Now().UnixNano()))
			ts.LatestStreamArn = streamArn
			p.streams.Enable(name, streamArn)
		} else if !enabled && p.streams != nil {
			p.streams.Disable(name)
		}
	}
	// Handle GlobalSecondaryIndexUpdates.
	if updates, ok := nr.Params["GlobalSecondaryIndexUpdates"].([]any); ok {
		for _, u := range updates {
			m, ok := u.(map[string]any)
			if !ok {
				continue
			}
			if create, ok := m["Create"].(map[string]any); ok {
				if len(ts.GlobalSecondaryIndexes) >= 20 {
					return nil, model.NewProviderError("ValidationException", "Number of GlobalSecondaryIndexes exceeds the limit of 20", 400)
				}
				newGSIWire := map[string]any{
					"IndexName":   create["IndexName"],
					"KeySchema":   create["KeySchema"],
					"Projection":  create["Projection"],
					"IndexStatus": "ACTIVE",
				}
				ts.GlobalSecondaryIndexes = append(ts.GlobalSecondaryIndexes, newGSIWire)
				schema := toStoreSchema(ts, ts.AttributeDefinitions)
				idx := indexDefFromWire(newGSIWire, func() map[string]string {
					m := make(map[string]string)
					for _, a := range ts.AttributeDefinitions {
						m[a["AttributeName"]] = a["AttributeType"]
					}
					return m
				}(), false)
				if err := p.items.AddGSI(ctx, name, schema, idx); err != nil {
					return nil, err
				}
			}
			if del, ok := m["Delete"].(map[string]any); ok {
				indexName := strParam(del, "IndexName")
				newGSIs := ts.GlobalSecondaryIndexes[:0]
				for _, g := range ts.GlobalSecondaryIndexes {
					if fmt.Sprintf("%v", g["IndexName"]) != indexName {
						newGSIs = append(newGSIs, g)
					}
				}
				ts.GlobalSecondaryIndexes = newGSIs
				schema := toStoreSchema(ts, ts.AttributeDefinitions)
				if err := p.items.DeleteGSI(ctx, name, schema, indexName); err != nil {
					return nil, err
				}
			}
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
	// Peek at existing item for stream event type — before the conditional write.
	existingForStream, _ := p.items.GetItem(ctx, name, pkHash)
	schema := buildItemSchema(ts)
	cond := dynamostore.ConditionSpec{
		ConditionExpression:       strParam(nr.Params, "ConditionExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ReturnValues:              strParam(nr.Params, "ReturnValues"),
		Schema:                    schema,
	}
	oldItem, err := p.items.PutItem(ctx, name, pkHash, item, cond)
	if err != nil {
		if isConditionFailed(err) {
			return nil, model.NewProviderError("ConditionalCheckFailedException", "The conditional request failed", 400)
		}
		return nil, err
	}
	eventName := "INSERT"
	if existingForStream != nil {
		eventName = "MODIFY"
	}
	p.appendStreamRecord(name, eventName, pkHash, extractKeys(item, ts), item, existingForStream)
	result := map[string]any{}
	if cond.ReturnValues == "ALL_OLD" && oldItem != nil {
		result["Attributes"] = oldItem
	}
	return provider.OK(result), nil
}

func (p *TableProvider) GetItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	key := itemParam(nr.Params, "Key")
	if key == nil {
		return nil, model.NewProviderError("ValidationException", "Key is required", 400)
	}
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
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
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	pkHash := computePKHash(key, ts)
	cond := dynamostore.ConditionSpec{
		ConditionExpression:       strParam(nr.Params, "ConditionExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ReturnValues:              strParam(nr.Params, "ReturnValues"),
		Schema:                    buildItemSchema(ts),
	}
	oldItem, err := p.items.DeleteItem(ctx, name, pkHash, cond)
	if err != nil {
		if isConditionFailed(err) {
			return nil, model.NewProviderError("ConditionalCheckFailedException", "The conditional request failed", 400)
		}
		return nil, err
	}
	p.appendStreamRecord(name, "REMOVE", pkHash, key, nil, oldItem)
	result := map[string]any{}
	if cond.ReturnValues == "ALL_OLD" && oldItem != nil {
		result["Attributes"] = oldItem
	}
	return provider.OK(result), nil
}

func (p *TableProvider) UpdateItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	key := itemParam(nr.Params, "Key")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	pkHash := computePKHash(key, ts)

	spec := dynamostore.UpdateSpec{
		UpdateExpression:          strParam(nr.Params, "UpdateExpression"),
		ConditionExpression:       strParam(nr.Params, "ConditionExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ReturnValues:              strParam(nr.Params, "ReturnValues"),
		Schema:                    buildItemSchema(ts),
	}

	oldItem, _ := p.items.GetItem(ctx, name, pkHash)
	updated, err := p.items.UpdateItem(ctx, name, pkHash, key, spec)
	if err != nil {
		if isConditionFailed(err) {
			return nil, model.NewProviderError("ConditionalCheckFailedException", "The conditional request failed", 400)
		}
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
	ts, _ := p.loadTable(ctx, name)
	indexName := strParam(nr.Params, "IndexName")
	scanFwd := true
	if v, ok := nr.Params["ScanIndexForward"].(bool); ok {
		scanFwd = v
	}
	q := dynamostore.QuerySpec{
		IndexName:                 indexName,
		IndexSchema:               resolveIndexSchema(ts, indexName),
		KeyConditionExpression:    strParam(nr.Params, "KeyConditionExpression"),
		FilterExpression:          strParam(nr.Params, "FilterExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ScanIndexForward:          scanFwd,
		Limit:                     intParam(nr.Params, "Limit", 0),
		ExclusiveStartKey:         exclusiveStartKey(nr.Params),
	}
	items, scannedCount, lastKey, err := p.items.Query(ctx, name, q)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"Items":        items,
		"Count":        len(items),
		"ScannedCount": scannedCount,
	}
	if lastKey != "" {
		result["LastEvaluatedKey"] = unmarshalKey(lastKey)
	}
	return provider.OK(result), nil
}

func (p *TableProvider) Scan(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, _ := p.loadTable(ctx, name)
	indexName := strParam(nr.Params, "IndexName")
	sc := dynamostore.ScanSpec{
		IndexName:                 indexName,
		IndexSchema:               resolveIndexSchema(ts, indexName),
		FilterExpression:          strParam(nr.Params, "FilterExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		Limit:                     intParam(nr.Params, "Limit", 0),
		ExclusiveStartKey:         exclusiveStartKey(nr.Params),
	}
	items, scannedCount, lastKey, err := p.items.Scan(ctx, name, sc)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"Items":        items,
		"Count":        len(items),
		"ScannedCount": scannedCount,
	}
	if lastKey != "" {
		result["LastEvaluatedKey"] = unmarshalKey(lastKey)
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
					Schema:  buildItemSchema(ts),
					PutItem: item,
					PutHash: computePKHash(item, ts),
				})
			}
			if del, ok := m["DeleteRequest"].(map[string]any); ok {
				key := itemParam(del, "Key")
				reqs = append(reqs, dynamostore.BatchWriteRequest{
					Table:      tableName,
					Schema:     buildItemSchema(ts),
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
			if _, err := p.items.PutItem(ctx, table, computePKHash(item, ts), item, dynamostore.ConditionSpec{}); err != nil {
				return nil, err
			}
		}
		if del, ok := m["Delete"].(map[string]any); ok {
			table := strParam(del, "TableName")
			key := itemParam(del, "Key")
			ts, err := p.loadTable(ctx, table)
			if err == nil {
				_, _ = p.items.DeleteItem(ctx, table, computePKHash(key, ts), dynamostore.ConditionSpec{})
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
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
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
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
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
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
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

// ─── TTL ──────────────────────────────────────────────────────────────────────

func (p *TableProvider) DescribeTimeToLive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	ttlDesc := map[string]any{"TimeToLiveStatus": "DISABLED"}
	if ts.TTLSpec != nil && ts.TTLSpec.Enabled {
		ttlDesc = map[string]any{
			"TimeToLiveStatus": "ENABLED",
			"AttributeName":    ts.TTLSpec.AttributeName,
		}
	} else if ts.TTLSpec != nil && ts.TTLSpec.AttributeName != "" {
		ttlDesc["AttributeName"] = ts.TTLSpec.AttributeName
	}
	return provider.OK(map[string]any{"TimeToLiveDescription": ttlDesc}), nil
}

func (p *TableProvider) UpdateTimeToLive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Table not found")
	}
	spec, _ := nr.Params["TimeToLiveSpecification"].(map[string]any)
	attrName, _ := spec["AttributeName"].(string)
	if attrName == "" {
		return nil, model.NewProviderError("ValidationException",
			"TimeToLiveSpecification.AttributeName is required", 400)
	}
	enabled, _ := spec["Enabled"].(bool)
	ts.TTLSpec = &TTLSpecification{AttributeName: attrName, Enabled: enabled}
	if err := p.saveTable(ctx, ts); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"TimeToLiveSpecification": map[string]any{
			"Enabled":       enabled,
			"AttributeName": attrName,
		},
	}), nil
}

// ─── PITR ─────────────────────────────────────────────────────────────────────

func (p *TableProvider) DescribeContinuousBackups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "TableNotFoundException", "Table not found")
	}
	pitrStatus := "DISABLED"
	if ts.PITREnabled {
		pitrStatus = "ENABLED"
	}
	return provider.OK(map[string]any{
		"ContinuousBackupsDescription": map[string]any{
			"ContinuousBackupsStatus": "AVAILABLE",
			"PointInTimeRecoveryDescription": map[string]any{
				"PointInTimeRecoveryStatus": pitrStatus,
			},
		},
	}), nil
}

func (p *TableProvider) UpdateContinuousBackups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, err := p.loadTable(ctx, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "TableNotFoundException", "Table not found")
	}
	spec, _ := nr.Params["PointInTimeRecoverySpecification"].(map[string]any)
	enabled, _ := spec["PointInTimeRecoveryEnabled"].(bool)
	ts.PITREnabled = enabled
	if err := p.saveTable(ctx, ts); err != nil {
		return nil, err
	}
	pitrStatus := "DISABLED"
	if enabled {
		pitrStatus = "ENABLED"
	}
	return provider.OK(map[string]any{
		"ContinuousBackupsDescription": map[string]any{
			"ContinuousBackupsStatus": "AVAILABLE",
			"PointInTimeRecoveryDescription": map[string]any{
				"PointInTimeRecoveryStatus": pitrStatus,
			},
		},
	}), nil
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
		"LocalSecondaryIndexes":  ts.LocalSecondaryIndexes,
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
	case []map[string]string:
		return ks
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

// buildItemSchema converts a loaded tableSchema into a *dynamostore.TableSchema for index maintenance.
func buildItemSchema(ts tableSchema) *dynamostore.TableSchema {
	s := toStoreSchema(ts, ts.AttributeDefinitions)
	return &s
}

// resolveIndexSchema finds the IndexKeyRef for the given indexName in the table schema.
func resolveIndexSchema(ts tableSchema, indexName string) *dynamostore.IndexKeyRef {
	if indexName == "" {
		return nil
	}
	attrTypes := make(map[string]string)
	for _, a := range ts.AttributeDefinitions {
		attrTypes[a["AttributeName"]] = a["AttributeType"]
	}
	for _, g := range ts.GlobalSecondaryIndexes {
		if fmt.Sprintf("%v", g["IndexName"]) != indexName {
			continue
		}
		ks := parseKeySchema(g["KeySchema"])
		var pkAttr, skAttr string
		for _, k := range ks {
			switch k["KeyType"] {
			case "HASH":
				pkAttr = k["AttributeName"]
			case "RANGE":
				skAttr = k["AttributeName"]
			}
		}
		return &dynamostore.IndexKeyRef{
			IndexName: indexName,
			PKAttr:    pkAttr,
			SKAttr:    skAttr,
			PKType:    attrTypes[pkAttr],
			SKType:    attrTypes[skAttr],
			IsLSI:     false,
		}
	}
	for _, l := range ts.LocalSecondaryIndexes {
		if fmt.Sprintf("%v", l["IndexName"]) != indexName {
			continue
		}
		ks := parseKeySchema(l["KeySchema"])
		var pkAttr, skAttr string
		for _, k := range ks {
			switch k["KeyType"] {
			case "HASH":
				pkAttr = k["AttributeName"]
			case "RANGE":
				skAttr = k["AttributeName"]
			}
		}
		if pkAttr == "" {
			// KeySchema stored as []map[string]string from parseKeySchema
			pkAttr = tableMainPK(ts)
		}
		return &dynamostore.IndexKeyRef{
			IndexName: indexName,
			PKAttr:    pkAttr,
			SKAttr:    skAttr,
			PKType:    attrTypes[pkAttr],
			SKType:    attrTypes[skAttr],
			IsLSI:     true,
		}
	}
	return nil
}

func tableMainPK(ts tableSchema) string {
	for _, k := range ts.KeySchema {
		if k["KeyType"] == "HASH" {
			return k["AttributeName"]
		}
	}
	return ""
}

func parseLSIs(v any) []map[string]any {
	if v == nil {
		return nil
	}
	ls, ok := v.([]any)
	if !ok {
		return nil
	}
	var result []map[string]any
	for _, li := range ls {
		if m, ok := li.(map[string]any); ok {
			lsi := map[string]any{
				"IndexName":   m["IndexName"],
				"KeySchema":   parseKeySchema(m["KeySchema"]),
				"Projection":  m["Projection"],
				"IndexStatus": "ACTIVE",
			}
			result = append(result, lsi)
		}
	}
	return result
}

// toStoreSchema converts the wire tableSchema into a dynamostore.TableSchema
// so the item store knows key attribute names and index definitions.
func toStoreSchema(ts tableSchema, attrDefs []map[string]string) dynamostore.TableSchema {
	// Build attribute type map from AttributeDefinitions.
	attrTypes := make(map[string]string, len(attrDefs))
	for _, a := range attrDefs {
		attrTypes[a["AttributeName"]] = a["AttributeType"]
	}

	var pkAttr, skAttr, pkType, skType string
	for _, ks := range ts.KeySchema {
		switch ks["KeyType"] {
		case "HASH":
			pkAttr = ks["AttributeName"]
			pkType = attrTypes[pkAttr]
		case "RANGE":
			skAttr = ks["AttributeName"]
			skType = attrTypes[skAttr]
		}
	}

	schema := dynamostore.TableSchema{
		TableName: ts.TableName,
		PKAttr:    pkAttr,
		SKAttr:    skAttr,
		PKType:    pkType,
		SKType:    skType,
	}

	for _, g := range ts.GlobalSecondaryIndexes {
		idx := indexDefFromWire(g, attrTypes, false)
		schema.GSIs = append(schema.GSIs, idx)
	}
	for _, l := range ts.LocalSecondaryIndexes {
		idx := indexDefFromWire(l, attrTypes, true)
		schema.LSIs = append(schema.LSIs, idx)
	}

	return schema
}

func indexDefFromWire(m map[string]any, attrTypes map[string]string, isLSI bool) dynamostore.IndexDef {
	name, _ := m["IndexName"].(string)
	ks := parseKeySchema(m["KeySchema"])
	var pkAttr, skAttr, pkType, skType string
	for _, k := range ks {
		switch k["KeyType"] {
		case "HASH":
			pkAttr = k["AttributeName"]
			pkType = attrTypes[pkAttr]
		case "RANGE":
			skAttr = k["AttributeName"]
			skType = attrTypes[skAttr]
		}
	}
	var proj dynamostore.ProjectionDef
	if p, ok := m["Projection"].(map[string]any); ok {
		proj.Type, _ = p["ProjectionType"].(string)
		if na, ok := p["NonKeyAttributes"].([]any); ok {
			for _, a := range na {
				if s, ok := a.(string); ok {
					proj.NonKeyAttrs = append(proj.NonKeyAttrs, s)
				}
			}
		}
	}
	if proj.Type == "" {
		proj.Type = "ALL"
	}
	return dynamostore.IndexDef{
		IndexName:  name,
		PKAttr:     pkAttr,
		SKAttr:     skAttr,
		PKType:     pkType,
		SKType:     skType,
		Projection: proj,
		IsLSI:      isLSI,
	}
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

// unmarshalKey parses a JSON-encoded key string back to a map for wire encoding.
func unmarshalKey(s string) map[string]any {
	var m map[string]any
	json.Unmarshal([]byte(s), &m)
	return m
}

// isConditionFailed returns true when err is a ConditionalCheckFailedException from the store.
func isConditionFailed(err error) bool {
	return err != nil && err.Error() == "ConditionalCheckFailedException"
}

func arnToTableName(arn string) string {
	// arn:aws:dynamodb:region:accountID:table/tableName
	parts := strings.Split(arn, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return arn
}

// exclusiveStartKey returns a JSON-encoded ExclusiveStartKey from request params,
// or an empty string when the param is absent or not a map.
func exclusiveStartKey(params map[string]any) string {
	v, ok := params["ExclusiveStartKey"]
	if !ok {
		return ""
	}
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}
