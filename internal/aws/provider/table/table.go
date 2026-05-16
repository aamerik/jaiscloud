// Package table implements the DynamoDB provider (TableProvider).
package table

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
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
	ttlWorker *TTLWorker
}

func New(resources store.ResourceStore, items dynamostore.DynamoDBItemStore) *TableProvider {
	return &TableProvider{resources: resources, items: items}
}

func NewWithStreams(resources store.ResourceStore, items dynamostore.DynamoDBItemStore, streams *streamstore.MemoryStreamStore) *TableProvider {
	return &TableProvider{resources: resources, items: items, streams: streams}
}

// NewWithTTL constructs a TableProvider with an active TTL reaper.
// cloud/region/accountID are injected so background goroutines never hold a NormalizedRequest.
func NewWithTTL(
	resources store.ResourceStore,
	items dynamostore.DynamoDBItemStore,
	streams *streamstore.MemoryStreamStore,
	cloud, region, accountID string,
	ttlInterval time.Duration,
) *TableProvider {
	p := &TableProvider{resources: resources, items: items, streams: streams}
	p.ttlWorker = NewTTLWorker(resources, items, streams, cloud, region, accountID, ttlInterval)
	p.ttlWorker.Start(context.Background())
	return p
}

// Shutdown stops the TTL reaper goroutine (if running).
func (p *TableProvider) Shutdown() {
	if p.ttlWorker != nil {
		p.ttlWorker.Shutdown()
	}
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
		// PartiQL stubs (not supported)
		"Table.ExecuteStatement":      p.ExecuteStatement,
		"Table.ExecuteTransaction":    p.ExecuteTransaction,
		"Table.BatchExecuteStatement": p.BatchExecuteStatement,
		// Global Tables (metadata-only)
		"Table.CreateGlobalTable":   p.CreateGlobalTable,
		"Table.DescribeGlobalTable": p.DescribeGlobalTable,
		"Table.ListGlobalTables":    p.ListGlobalTables,
		"Table.UpdateGlobalTable":   p.UpdateGlobalTable,
		// Kinesis Streaming Destinations (metadata-only)
		"Table.EnableKinesisStreamingDestination":  p.EnableKinesisStreamingDestination,
		"Table.DisableKinesisStreamingDestination": p.DisableKinesisStreamingDestination,
		"Table.DescribeKinesisStreamingDestination": p.DescribeKinesisStreamingDestination,
		"Table.UpdateKinesisStreamingDestination":  p.UpdateKinesisStreamingDestination,
		// Misc stubs
		"Table.DescribeEndpoints":           p.DescribeEndpoints,
		"Table.DescribeLimits":              p.DescribeLimits,
		"Table.DescribeContributorInsights": p.DescribeContributorInsights,
		"Table.ListContributorInsights":     p.ListContributorInsights,
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
	return nr.ResourceID("dynamodb-table", name)
}

func streamResourceID(nr *model.NormalizedRequest, tableName, label string) string {
	return nr.ResourceID("dynamodb-stream", tableName+"/stream/"+label)
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
	wcu, rcu := parseProvisionedThroughput(nr.Params["ProvisionedThroughput"])
	storeSchema := toStoreSchema(ts, attrDefs)
	storeSchema.BillingMode = billing
	storeSchema.WCU = wcu
	storeSchema.RCU = rcu
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
	if p.streams != nil {
		p.streams.Disable(name)
	}
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
	if dynItemSize(item) > maxItemSizeBytes {
		return nil, model.NewProviderError("ValidationException",
			"Item size has exceeded the maximum allowed size of 400 KB", 400)
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
		if isThrottled(err) {
			return nil, storeErrToProvider(err)
		}
		if isConditionFailed(err) {
			return nil, model.NewProviderError("ConditionalCheckFailedException", "The conditional request failed", 400)
		}
		return nil, storeErrToProvider(err)
	}
	eventName := "INSERT"
	if existingForStream != nil {
		eventName = "MODIFY"
	}
	p.appendStreamRecord(name, eventName, newEventID(), extractKeys(item, ts), item, existingForStream)
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
		projAttrs := dynamostore.ParseProjection(strParam(nr.Params, "ProjectionExpression"), exprNames(nr.Params))
		result["Item"] = dynamostore.ApplyProjection(item, projAttrs)
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
		if isThrottled(err) {
			return nil, storeErrToProvider(err)
		}
		if isConditionFailed(err) {
			return nil, model.NewProviderError("ConditionalCheckFailedException", "The conditional request failed", 400)
		}
		return nil, storeErrToProvider(err)
	}
	p.appendStreamRecord(name, "REMOVE", newEventID(), key, nil, oldItem)
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
		if isThrottled(err) {
			return nil, storeErrToProvider(err)
		}
		if isConditionFailed(err) {
			return nil, model.NewProviderError("ConditionalCheckFailedException", "The conditional request failed", 400)
		}
		return nil, storeErrToProvider(err)
	}
	p.appendStreamRecord(name, "MODIFY", newEventID(), key, updated, oldItem)
	result := map[string]any{}
	switch spec.ReturnValues {
	case "ALL_NEW":
		result["Attributes"] = updated
	case "ALL_OLD":
		if oldItem != nil {
			result["Attributes"] = oldItem
		}
	case "UPDATED_NEW":
		attrs := computeUpdatedAttrs(updated, oldItem)
		if len(attrs) > 0 {
			result["Attributes"] = attrs
		}
	case "UPDATED_OLD":
		attrs := computeUpdatedAttrs(oldItem, updated)
		if len(attrs) > 0 {
			result["Attributes"] = attrs
		}
	}
	return provider.OK(result), nil
}

// computeUpdatedAttrs returns the attributes from source that differ from reference.
func applyProjectionSlice(items []map[string]any, attrs []string) []map[string]any {
	if len(attrs) == 0 {
		return items
	}
	out := make([]map[string]any, len(items))
	for i, item := range items {
		out[i] = dynamostore.ApplyProjection(item, attrs)
	}
	return out
}

func computeUpdatedAttrs(source, reference map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	out := map[string]any{}
	for k, v := range source {
		if rv, ok := reference[k]; !ok || !deepEqualDynamo(v, rv) {
			out[k] = v
		}
	}
	return out
}

func deepEqualDynamo(a, b any) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}

func (p *TableProvider) Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, _ := p.loadTable(ctx, name)
	indexName := strParam(nr.Params, "IndexName")
	scanFwd := true
	if v, ok := nr.Params["ScanIndexForward"].(bool); ok {
		scanFwd = v
	}
	selectVal := strParam(nr.Params, "Select")
	projExpr := strParam(nr.Params, "ProjectionExpression")

	// Validate: SELECT=COUNT and ProjectionExpression cannot both be set.
	if selectVal == "COUNT" && projExpr != "" {
		return nil, model.NewProviderError("ValidationException",
			"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Select} Expression parameters: {ProjectionExpression}", 400)
	}

	keyCondExpr := strParam(nr.Params, "KeyConditionExpression")
	esk, eskErr := exclusiveStartKeyE(nr.Params)
	if eskErr != nil {
		return nil, model.NewProviderError("ValidationException", "invalid pagination token", 400)
	}

	// Validate KeyConditionExpression operators.
	if keyCondExpr != "" {
		idxSchema := resolveIndexSchema(ts, indexName)
		var pkAttr, skAttr string
		if idxSchema != nil {
			pkAttr = idxSchema.PKAttr
			skAttr = idxSchema.SKAttr
		} else {
			// Use table primary key.
			for _, k := range ts.KeySchema {
				switch k["KeyType"] {
				case "HASH":
					pkAttr = k["AttributeName"]
				case "RANGE":
					skAttr = k["AttributeName"]
				}
			}
		}
		if validErr := dynamostore.ValidateKeyConditionExpression(keyCondExpr, pkAttr, skAttr, exprNames(nr.Params), exprValues(nr.Params)); validErr != nil {
			return nil, model.NewProviderError("ValidationException", validErr.Error(), 400)
		}
	}

	q := dynamostore.QuerySpec{
		IndexName:                 indexName,
		IndexSchema:               resolveIndexSchema(ts, indexName),
		KeyConditionExpression:    keyCondExpr,
		FilterExpression:          strParam(nr.Params, "FilterExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		ScanIndexForward:          scanFwd,
		Limit:                     intParam(nr.Params, "Limit", 0),
		ExclusiveStartKey:         esk,
	}
	items, scannedCount, lastKey, err := p.items.Query(ctx, name, q)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	// Apply GSI projection if applicable.
	if indexName != "" {
		for _, g := range ts.GlobalSecondaryIndexes {
			if fmt.Sprintf("%v", g["IndexName"]) == indexName {
				items = filterByGSIProjection(items, g, ts.KeySchema)
				break
			}
		}
	}

	projAttrs := dynamostore.ParseProjection(projExpr, exprNames(nr.Params))
	projected := applyProjectionSlice(items, projAttrs)

	var lek map[string]any
	if lastKey != "" {
		lek = buildLastEvaluatedKey(lastKey, ts, indexName)
	}

	if selectVal == "COUNT" {
		result := map[string]any{
			"Count":        len(projected),
			"ScannedCount": scannedCount,
		}
		if lek != nil {
			result["LastEvaluatedKey"] = lek
		}
		return provider.OK(result), nil
	}

	result := map[string]any{
		"Items":        projected,
		"Count":        len(projected),
		"ScannedCount": scannedCount,
	}
	if lek != nil {
		result["LastEvaluatedKey"] = lek
	}
	return provider.OK(result), nil
}

func (p *TableProvider) Scan(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "TableName")
	ts, _ := p.loadTable(ctx, name)
	indexName := strParam(nr.Params, "IndexName")
	selectVal := strParam(nr.Params, "Select")
	projExpr := strParam(nr.Params, "ProjectionExpression")

	// Validate: SELECT=COUNT and ProjectionExpression cannot both be set.
	if selectVal == "COUNT" && projExpr != "" {
		return nil, model.NewProviderError("ValidationException",
			"Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Select} Expression parameters: {ProjectionExpression}", 400)
	}

	eskScan, eskScanErr := exclusiveStartKeyE(nr.Params)
	if eskScanErr != nil {
		return nil, model.NewProviderError("ValidationException", "invalid pagination token", 400)
	}

	sc := dynamostore.ScanSpec{
		IndexName:                 indexName,
		IndexSchema:               resolveIndexSchema(ts, indexName),
		FilterExpression:          strParam(nr.Params, "FilterExpression"),
		ExpressionAttributeNames:  exprNames(nr.Params),
		ExpressionAttributeValues: exprValues(nr.Params),
		Limit:                     intParam(nr.Params, "Limit", 0),
		ExclusiveStartKey:         eskScan,
	}

	// Parallel scan validation and wiring.
	_, segPresent := nr.Params["Segment"]
	_, totalPresent := nr.Params["TotalSegments"]
	segParam := intParam(nr.Params, "Segment", 0)
	totalParam := intParam(nr.Params, "TotalSegments", 0)
	if segPresent && !totalPresent {
		return nil, model.NewProviderError("ValidationException",
			"The TotalSegments parameter is required when you specify the Segment parameter", 400)
	}
	if totalPresent && !segPresent {
		return nil, model.NewProviderError("ValidationException",
			"The Segment parameter is required when you specify the TotalSegments parameter", 400)
	}
	if totalPresent && segPresent && segParam >= totalParam {
		return nil, model.NewProviderError("ValidationException",
			"The Segment parameter must be between 0 and the TotalSegments parameter", 400)
	}
	if totalPresent && segPresent {
		sc.Segment = segParam
		sc.TotalSegments = totalParam
	}

	items, scannedCount, lastKey, err := p.items.Scan(ctx, name, sc)
	if err != nil {
		return nil, storeErrToProvider(err)
	}

	projAttrs := dynamostore.ParseProjection(projExpr, exprNames(nr.Params))
	projected := applyProjectionSlice(items, projAttrs)

	var lek map[string]any
	if lastKey != "" {
		lek = buildLastEvaluatedKey(lastKey, ts, indexName)
	}

	if selectVal == "COUNT" {
		result := map[string]any{
			"Count":        len(projected),
			"ScannedCount": scannedCount,
		}
		if lek != nil {
			result["LastEvaluatedKey"] = lek
		}
		return provider.OK(result), nil
	}

	result := map[string]any{
		"Items":        projected,
		"Count":        len(projected),
		"ScannedCount": scannedCount,
	}
	if lek != nil {
		result["LastEvaluatedKey"] = lek
	}
	return provider.OK(result), nil
}

// ─── Batch operations ─────────────────────────────────────────────────────────

func (p *TableProvider) BatchWriteItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	requestItems, _ := nr.Params["RequestItems"].(map[string]any)

	// Validate total request count across all tables (cap: 25).
	totalRequests := 0
	for _, v := range requestItems {
		if writeReqs, ok := v.([]any); ok {
			totalRequests += len(writeReqs)
		}
	}
	if totalRequests > 25 {
		return nil, model.NewProviderError("ValidationException",
			"Too many items requested for the BatchWriteItem call", 400)
	}

	var reqs []dynamostore.BatchWriteRequest
	for tableName, v := range requestItems {
		ts, _ := p.loadTable(ctx, tableName)
		writeReqs, _ := v.([]any)
		for _, wr := range writeReqs {
			m, _ := wr.(map[string]any)
			if put, ok := m["PutRequest"].(map[string]any); ok {
				item := itemParam(put, "Item")
				if dynItemSize(item) > maxItemSizeBytes {
					return nil, model.NewProviderError("ValidationException",
						"Item size has exceeded the maximum allowed size of 400 KB", 400)
				}
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
	unprocessed, err := p.items.BatchWriteItems(ctx, reqs)
	if err != nil {
		return nil, err
	}
	unprocessedItems := map[string]any{}
	if len(unprocessed) > 0 {
		// Group unprocessed requests back by table in the wire format.
		tableReqs := make(map[string][]any)
		for _, req := range unprocessed {
			if req.PutItem != nil {
				tableReqs[req.Table] = append(tableReqs[req.Table], map[string]any{
					"PutRequest": map[string]any{"Item": req.PutItem},
				})
			} else if req.DeleteKey != nil {
				tableReqs[req.Table] = append(tableReqs[req.Table], map[string]any{
					"DeleteRequest": map[string]any{"Key": req.DeleteKey},
				})
			}
		}
		for t, rs := range tableReqs {
			unprocessedItems[t] = rs
		}
	}
	return provider.OK(map[string]any{"UnprocessedItems": unprocessedItems}), nil
}

func (p *TableProvider) BatchGetItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	requestItems, _ := nr.Params["RequestItems"].(map[string]any)

	// Validate total key count across all tables (cap: 100).
	totalKeys := 0
	for _, v := range requestItems {
		if m, ok := v.(map[string]any); ok {
			if rawKeys, ok := m["Keys"].([]any); ok {
				totalKeys += len(rawKeys)
			}
		}
	}
	if totalKeys > 100 {
		return nil, model.NewProviderError("ValidationException",
			"Too many items requested for the BatchGetItem call", 400)
	}

	var reqs []dynamostore.BatchGetRequest
	type tableProjection struct{ attrs []string }
	projections := map[string]tableProjection{}
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
		projExpr, _ := m["ProjectionExpression"].(string)
		names := map[string]string{}
		if en, ok := m["ExpressionAttributeNames"].(map[string]any); ok {
			for k, nv := range en {
				names[k], _ = nv.(string)
			}
		}
		projections[table] = tableProjection{dynamostore.ParseProjection(projExpr, names)}
	}
	result, err := p.items.BatchGetItems(ctx, reqs)
	if err != nil {
		return nil, err
	}
	// Apply per-table projections.
	for table, items := range result {
		if proj := projections[table]; len(proj.attrs) > 0 {
			projected := make([]map[string]any, len(items))
			for i, item := range items {
				projected[i] = dynamostore.ApplyProjection(item, proj.attrs)
			}
			result[table] = projected
		}
	}
	return provider.OK(map[string]any{"Responses": result, "UnprocessedKeys": map[string]any{}}), nil
}

func (p *TableProvider) TransactWriteItems(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	transactItems, _ := nr.Params["TransactItems"].([]any)
	if len(transactItems) == 0 {
		return nil, model.NewProviderError("ValidationException", "Request must contain at least one TransactItem", 400)
	}
	if len(transactItems) > 100 {
		return nil, model.NewProviderError("ValidationException", "Too many items in TransactItems. Maximum allowed is 100", 400)
	}

	// Build typed ops; validate tables exist and check for duplicate item keys.
	type itemKey struct{ table, pkHash string }
	seen := map[itemKey]bool{}
	var ops []dynamostore.TransactWriteOp
	for _, ti := range transactItems {
		m, _ := ti.(map[string]any)
		var op dynamostore.TransactWriteOp

		switch {
		case m["Put"] != nil:
			put, _ := m["Put"].(map[string]any)
			table := strParam(put, "TableName")
			item := itemParam(put, "Item")
			ts, err := p.loadTable(ctx, table)
			if err != nil {
				return nil, model.NewProviderError("ResourceNotFoundException", "Table not found: "+table, 400)
			}
			pkHash := computePKHash(item, ts)
			op = dynamostore.TransactWriteOp{
				Type: "Put", Table: table, PKHash: pkHash, Item: item,
				Cond: dynamostore.ConditionSpec{
					ConditionExpression:       strParam(put, "ConditionExpression"),
					ExpressionAttributeNames:  exprNamesFrom(put),
					ExpressionAttributeValues: exprValuesFrom(put),
					Schema:                    buildItemSchema(ts),
				},
				ReturnValuesOnConditionCheckFailure: strParam(put, "ReturnValuesOnConditionCheckFailure"),
			}
		case m["Delete"] != nil:
			del, _ := m["Delete"].(map[string]any)
			table := strParam(del, "TableName")
			key := itemParam(del, "Key")
			ts, err := p.loadTable(ctx, table)
			if err != nil {
				return nil, model.NewProviderError("ResourceNotFoundException", "Table not found: "+table, 400)
			}
			pkHash := computePKHash(key, ts)
			op = dynamostore.TransactWriteOp{
				Type: "Delete", Table: table, PKHash: pkHash, Key: key,
				Cond: dynamostore.ConditionSpec{
					ConditionExpression:       strParam(del, "ConditionExpression"),
					ExpressionAttributeNames:  exprNamesFrom(del),
					ExpressionAttributeValues: exprValuesFrom(del),
					Schema:                    buildItemSchema(ts),
				},
				ReturnValuesOnConditionCheckFailure: strParam(del, "ReturnValuesOnConditionCheckFailure"),
			}
		case m["Update"] != nil:
			upd, _ := m["Update"].(map[string]any)
			table := strParam(upd, "TableName")
			key := itemParam(upd, "Key")
			ts, err := p.loadTable(ctx, table)
			if err != nil {
				return nil, model.NewProviderError("ResourceNotFoundException", "Table not found: "+table, 400)
			}
			pkHash := computePKHash(key, ts)
			op = dynamostore.TransactWriteOp{
				Type: "Update", Table: table, PKHash: pkHash, Key: key,
				Cond: dynamostore.ConditionSpec{
					ConditionExpression:       strParam(upd, "ConditionExpression"),
					ExpressionAttributeNames:  exprNamesFrom(upd),
					ExpressionAttributeValues: exprValuesFrom(upd),
					Schema:                    buildItemSchema(ts),
				},
				Update: dynamostore.UpdateSpec{
					UpdateExpression:          strParam(upd, "UpdateExpression"),
					ExpressionAttributeNames:  exprNamesFrom(upd),
					ExpressionAttributeValues: exprValuesFrom(upd),
					Schema:                    buildItemSchema(ts),
				},
				ReturnValuesOnConditionCheckFailure: strParam(upd, "ReturnValuesOnConditionCheckFailure"),
			}
		case m["ConditionCheck"] != nil:
			cc, _ := m["ConditionCheck"].(map[string]any)
			table := strParam(cc, "TableName")
			key := itemParam(cc, "Key")
			ts, err := p.loadTable(ctx, table)
			if err != nil {
				return nil, model.NewProviderError("ResourceNotFoundException", "Table not found: "+table, 400)
			}
			pkHash := computePKHash(key, ts)
			op = dynamostore.TransactWriteOp{
				Type: "ConditionCheck", Table: table, PKHash: pkHash, Key: key,
				Cond: dynamostore.ConditionSpec{
					ConditionExpression:       strParam(cc, "ConditionExpression"),
					ExpressionAttributeNames:  exprNamesFrom(cc),
					ExpressionAttributeValues: exprValuesFrom(cc),
				},
				ReturnValuesOnConditionCheckFailure: strParam(cc, "ReturnValuesOnConditionCheckFailure"),
			}
		default:
			continue
		}

		ik := itemKey{op.Table, op.PKHash}
		if seen[ik] {
			// AWS returns TransactionCanceledException with DuplicateItem for all ops.
			dupReasons := make([]map[string]any, len(ops)+1)
			for j := range dupReasons {
				dupReasons[j] = map[string]any{"Code": dynamostore.CancelCodeDuplicateItem, "Message": "Transaction request cannot include multiple operations on one item"}
			}
			return nil, model.NewProviderError("TransactionCanceledException",
				"Transaction cancelled, please refer cancellation reasons for specific reasons [DuplicateItem]", 400).
				WithData(map[string]any{"CancellationReasons": dupReasons})
		}
		seen[ik] = true
		ops = append(ops, op)
	}

	reasons, err := p.items.TransactWriteItems(ctx, ops)
	if err != nil {
		return nil, err
	}
	if reasons != nil {
		cancelReasons := make([]map[string]any, len(reasons))
		for i, r := range reasons {
			entry := map[string]any{"Code": r.Code, "Message": r.Message}
			if r.Item != nil {
				entry["Item"] = r.Item
			}
			cancelReasons[i] = entry
		}
		codes := make([]string, len(reasons))
		for i, r := range reasons {
			codes[i] = r.Code
		}
		return nil, model.NewProviderError("TransactionCanceledException",
			"Transaction cancelled, please refer cancellation reasons for specific reasons ["+
				strings.Join(codes, ", ")+"]", 400).
			WithData(map[string]any{"CancellationReasons": cancelReasons})
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
				projExpr, _ := get["ProjectionExpression"].(string)
				names := map[string]string{}
				if en, ok := get["ExpressionAttributeNames"].(map[string]any); ok {
					for k, nv := range en {
						names[k], _ = nv.(string)
					}
				}
				projAttrs := dynamostore.ParseProjection(projExpr, names)
				responses = append(responses, map[string]any{"Item": dynamostore.ApplyProjection(item, projAttrs)})
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
			"ContinuousBackupsStatus": "ENABLED",
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
			"ContinuousBackupsStatus": "ENABLED",
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

// parseProvisionedThroughput extracts WCU and RCU from a ProvisionedThroughput param map.
// Returns (0, 0) when pt is nil or not a map.
func parseProvisionedThroughput(pt any) (wcu, rcu int64) {
	m, ok := pt.(map[string]any)
	if !ok {
		return 0, 0
	}
	switch v := m["WriteCapacityUnits"].(type) {
	case float64:
		wcu = int64(v)
	case int64:
		wcu = v
	case int:
		wcu = int64(v)
	}
	switch v := m["ReadCapacityUnits"].(type) {
	case float64:
		rcu = int64(v)
	case int64:
		rcu = v
	case int:
		rcu = int64(v)
	}
	return wcu, rcu
}

func itemParam(params map[string]any, key string) map[string]any {
	if v, ok := params[key]; ok {
		if m, ok := v.(map[string]any); ok {
			return m
		}
	}
	return nil
}

// newEventID returns a UUID-like string for DynamoDB stream event IDs.
func newEventID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
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

// buildLastEvaluatedKey extracts only the key attributes from a full-item JSON string.
// For index queries it also includes the index key attributes.
// An HMAC (_jc_mac) is appended to the key map so that we can detect tampered tokens.
func buildLastEvaluatedKey(lastKeyJSON string, ts tableSchema, indexName string) map[string]any {
	var item map[string]any
	if err := json.Unmarshal([]byte(lastKeyJSON), &item); err != nil {
		return nil
	}
	keyAttrs := make(map[string]bool)
	for _, k := range ts.KeySchema {
		keyAttrs[k["AttributeName"]] = true
	}
	if indexName != "" {
		for _, g := range ts.GlobalSecondaryIndexes {
			if fmt.Sprintf("%v", g["IndexName"]) != indexName {
				continue
			}
			for _, k := range parseKeySchema(g["KeySchema"]) {
				keyAttrs[k["AttributeName"]] = true
			}
		}
		for _, l := range ts.LocalSecondaryIndexes {
			if fmt.Sprintf("%v", l["IndexName"]) != indexName {
				continue
			}
			for _, k := range parseKeySchema(l["KeySchema"]) {
				keyAttrs[k["AttributeName"]] = true
			}
		}
	}
	lek := make(map[string]any, len(keyAttrs)+1)
	for attr := range keyAttrs {
		if v, ok := item[attr]; ok {
			lek[attr] = v
		}
	}
	// Append HMAC so we can verify the token on the next page request.
	lek["_jc_mac"] = map[string]any{"S": computeLEKMac(lek)}
	return lek
}

// computeLEKMac computes an HMAC-SHA256 over the canonical JSON of the key map
// (excluding the _jc_mac field itself), keyed by the instance ID or a fallback string.
func computeLEKMac(keyMap map[string]any) string {
	// Collect all keys except _jc_mac, sort them for a canonical representation.
	keys := make([]string, 0, len(keyMap))
	for k := range keyMap {
		if k != "_jc_mac" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	canonical := make(map[string]any, len(keys))
	for _, k := range keys {
		canonical[k] = keyMap[k]
	}
	b, _ := json.Marshal(canonical)
	instanceID := os.Getenv("JAISCLOUD_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "jaiscloud-default-lek-key"
	}
	mac := hmac.New(sha256.New, []byte(instanceID))
	mac.Write(b)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// verifyAndStripLEKMac verifies the _jc_mac on an ExclusiveStartKey and returns
// the key map without _jc_mac on success.  If _jc_mac is present and invalid,
// returns an error.  If _jc_mac is absent, the key is passed through unchanged
// (for compatibility with keys from before this feature was added).
func verifyAndStripLEKMac(m map[string]any) (map[string]any, error) {
	macVal, hasMac := m["_jc_mac"]
	if !hasMac {
		// Old token without MAC — allow through for backwards compat.
		return m, nil
	}
	// Extract the MAC string value.
	var providedMac string
	if macMap, ok := macVal.(map[string]any); ok {
		providedMac, _ = macMap["S"].(string)
	}
	// Build the stripped map for recomputation.
	stripped := make(map[string]any, len(m)-1)
	for k, v := range m {
		if k != "_jc_mac" {
			stripped[k] = v
		}
	}
	expected := computeLEKMac(stripped)
	if !hmac.Equal([]byte(providedMac), []byte(expected)) {
		return nil, fmt.Errorf("invalid pagination token")
	}
	return stripped, nil
}

// ─── Item size validation ─────────────────────────────────────────────────────

const maxItemSizeBytes = 409600 // 400 KB

func dynAttrSize(name string, v any) int {
	size := len(name)
	m, ok := v.(map[string]any)
	if !ok {
		return size + 1
	}
	for typ, val := range m {
		switch typ {
		case "S":
			s, _ := val.(string)
			size += len(s) + 3
		case "N":
			s, _ := val.(string)
			size += len(s) + 1
		case "B":
			s, _ := val.(string)
			b, _ := base64.StdEncoding.DecodeString(s)
			size += len(b) + 1
		case "BOOL", "NULL":
			size += 1
		case "L":
			size += 3
			if elems, ok := val.([]any); ok {
				for _, e := range elems {
					size += dynAttrSize("", e)
				}
			}
		case "M":
			size += 3
			if mm, ok := val.(map[string]any); ok {
				for k, mv := range mm {
					size += dynAttrSize(k, mv)
				}
			}
		case "SS", "NS", "BS":
			elems, _ := val.([]any)
			for _, e := range elems {
				es, _ := e.(string)
				size += len(es)
			}
		}
	}
	return size
}

func dynItemSize(item map[string]any) int {
	total := 0
	for k, v := range item {
		total += dynAttrSize(k, v)
	}
	return total
}

// ─── GSI projection enforcement ───────────────────────────────────────────────

func filterByGSIProjection(items []map[string]any, gsi map[string]any, tableKeySchema []map[string]string) []map[string]any {
	proj, _ := gsi["Projection"].(map[string]any)
	if proj == nil {
		return items
	}
	projType, _ := proj["ProjectionType"].(string)
	if projType == "" || projType == "ALL" {
		return items
	}

	// Collect allowed attribute names.
	allowed := make(map[string]bool)
	for _, k := range tableKeySchema {
		allowed[k["AttributeName"]] = true
	}
	for _, k := range parseKeySchema(gsi["KeySchema"]) {
		allowed[k["AttributeName"]] = true
	}
	if projType == "INCLUDE" {
		if nonKey, ok := proj["NonKeyAttributes"].([]any); ok {
			for _, a := range nonKey {
				if s, ok := a.(string); ok {
					allowed[s] = true
				}
			}
		}
	}

	out := make([]map[string]any, len(items))
	for i, item := range items {
		filtered := make(map[string]any)
		for k, v := range item {
			if allowed[k] {
				filtered[k] = v
			}
		}
		out[i] = filtered
	}
	return out
}

// isConditionFailed returns true when err is a ConditionalCheckFailedException from the store.
func isConditionFailed(err error) bool {
	return err != nil && err.Error() == "ConditionalCheckFailedException"
}

// isThrottled returns true when err indicates a provisioned throughput exceeded error.
func isThrottled(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "ProvisionedThroughputExceededException")
}

// storeErrToProvider converts known store sentinel errors into ProviderErrors.
// Unknown errors are returned as-is (become HTTP 500 at the gateway).
func storeErrToProvider(err error) error {
	if err == nil {
		return nil
	}
	if isThrottled(err) {
		return model.NewProviderError("ProvisionedThroughputExceededException",
			"The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API", 400)
	}
	var exprErr *dynamostore.ExpressionError
	if errors.As(err, &exprErr) {
		return model.NewProviderError("ValidationException", exprErr.Message, 400)
	}
	return err
}

func arnToTableName(arn string) string {
	// arn:aws:dynamodb:region:accountID:table/tableName
	parts := strings.Split(arn, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return arn
}

// ─── Kinesis Streaming Destinations (metadata-only) ──────────────────────────

type kinesisDestInfo struct {
	StreamArn         string
	DestinationStatus string
	TimePrecision     string
}

func (p *TableProvider) kinesisDestKey(tableName string) string {
	return "dynamodb_kinesis_dest_" + tableName
}

func (p *TableProvider) loadKinesisDests(ctx context.Context, tableName string) ([]kinesisDestInfo, error) {
	entry, err := p.resources.Get(ctx, p.kinesisDestKey(tableName), tableName)
	if err != nil {
		return nil, nil // not found = empty list
	}
	var dests []kinesisDestInfo
	json.Unmarshal(entry.Data, &dests)
	return dests, nil
}

func (p *TableProvider) saveKinesisDests(ctx context.Context, tableName string, dests []kinesisDestInfo) error {
	data, _ := json.Marshal(dests)
	key := p.kinesisDestKey(tableName)
	entry, err := p.resources.Get(ctx, key, tableName)
	if err != nil {
		return p.resources.Create(ctx, store.ResourceEntry{Type: key, ID: tableName, Data: json.RawMessage(data)})
	}
	entry.Data = json.RawMessage(data)
	return p.resources.Update(ctx, entry)
}

func (p *TableProvider) EnableKinesisStreamingDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tableName, _ := nr.Params["TableName"].(string)
	streamArn, _ := nr.Params["StreamArn"].(string)
	if tableName == "" || streamArn == "" {
		return nil, model.NewProviderError("ValidationException", "TableName and StreamArn are required", 400)
	}
	dests, err := p.loadKinesisDests(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: load kinesis dests: %w", err)
	}
	for _, d := range dests {
		if d.StreamArn == streamArn && d.DestinationStatus == "ACTIVE" {
			return nil, model.NewProviderError("ValidationException",
				"Kinesis streaming destination already active for stream: "+streamArn, 400)
		}
	}
	dests = append(dests, kinesisDestInfo{
		StreamArn:         streamArn,
		DestinationStatus: "ACTIVE",
		TimePrecision:     "MILLISECOND",
	})
	if err := p.saveKinesisDests(ctx, tableName, dests); err != nil {
		return nil, fmt.Errorf("dynamodb: save kinesis dests: %w", err)
	}
	return provider.OK(map[string]any{
		"TableName":         tableName,
		"StreamArn":         streamArn,
		"DestinationStatus": "ACTIVE",
	}), nil
}

func (p *TableProvider) DisableKinesisStreamingDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tableName, _ := nr.Params["TableName"].(string)
	streamArn, _ := nr.Params["StreamArn"].(string)
	dests, err := p.loadKinesisDests(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: load kinesis dests: %w", err)
	}
	found := false
	for i, d := range dests {
		if d.StreamArn == streamArn {
			if d.DestinationStatus == "DISABLED" {
				return nil, model.NewProviderError("ValidationException", "Kinesis destination not active", 400)
			}
			dests[i].DestinationStatus = "DISABLED"
			found = true
			break
		}
	}
	if !found {
		return nil, model.NewProviderError("ValidationException", "Kinesis destination not active", 400)
	}
	if err := p.saveKinesisDests(ctx, tableName, dests); err != nil {
		return nil, fmt.Errorf("dynamodb: save kinesis dests: %w", err)
	}
	return provider.OK(map[string]any{
		"TableName":         tableName,
		"StreamArn":         streamArn,
		"DestinationStatus": "DISABLED",
	}), nil
}

func (p *TableProvider) DescribeKinesisStreamingDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tableName, _ := nr.Params["TableName"].(string)
	dests, err := p.loadKinesisDests(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: load kinesis dests: %w", err)
	}
	items := make([]map[string]any, 0, len(dests))
	for _, d := range dests {
		items = append(items, map[string]any{
			"StreamArn":         d.StreamArn,
			"DestinationStatus": d.DestinationStatus,
			"ApproximateCreationDateTimePrecision": d.TimePrecision,
		})
	}
	return provider.OK(map[string]any{
		"TableName":                    tableName,
		"KinesisDataStreamDestinations": items,
	}), nil
}

func (p *TableProvider) UpdateKinesisStreamingDestination(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tableName, _ := nr.Params["TableName"].(string)
	streamArn, _ := nr.Params["StreamArn"].(string)
	var precision string
	if cfg, ok := nr.Params["UpdateKinesisStreamingConfiguration"].(map[string]any); ok {
		precision, _ = cfg["ApproximateCreationDateTimePrecision"].(string)
	}
	dests, err := p.loadKinesisDests(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: load kinesis dests: %w", err)
	}
	for i, d := range dests {
		if d.StreamArn == streamArn {
			if precision != "" {
				dests[i].TimePrecision = precision
			}
			if err := p.saveKinesisDests(ctx, tableName, dests); err != nil {
				return nil, fmt.Errorf("dynamodb: save kinesis dests: %w", err)
			}
			return provider.OK(map[string]any{
				"TableName":         tableName,
				"StreamArn":         streamArn,
				"DestinationStatus": dests[i].DestinationStatus,
			}), nil
		}
	}
	return nil, model.NewProviderError("ValidationException", "Kinesis destination not found: "+streamArn, 400)
}

// ─── Global Tables (metadata-only) ───────────────────────────────────────────

func (p *TableProvider) CreateGlobalTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["GlobalTableName"].(string)
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "GlobalTableName is required", 400)
	}
	if _, err := p.resources.Get(ctx, "dynamodb_global_tables", name); err == nil {
		return nil, model.NewProviderError("GlobalTableAlreadyExistsException", "Global table already exists: "+name, 400)
	}

	var replicas []map[string]any
	if rg, ok := nr.Params["ReplicationGroup"].([]any); ok {
		for _, r := range rg {
			if rm, ok := r.(map[string]any); ok {
				region, _ := rm["RegionName"].(string)
				replicas = append(replicas, map[string]any{
					"RegionName":    region,
					"ReplicaStatus": "ACTIVE",
				})
			}
		}
	}

	desc := map[string]any{
		"GlobalTableName":   name,
		"GlobalTableStatus": "ACTIVE",
		"CreationDateTime":  time.Now().Unix(),
		"ReplicationGroup":  replicas,
	}
	data, _ := json.Marshal(desc)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: "dynamodb_global_tables", ID: name, Data: json.RawMessage(data)}); err != nil {
		return nil, fmt.Errorf("dynamodb: create global table: %w", err)
	}
	return provider.OK(map[string]any{"GlobalTableDescription": desc}), nil
}

func (p *TableProvider) DescribeGlobalTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["GlobalTableName"].(string)
	entry, err := p.resources.Get(ctx, "dynamodb_global_tables", name)
	if err != nil {
		return nil, model.NewProviderError("GlobalTableNotFoundException", "Global table not found: "+name, 400)
	}
	var desc map[string]any
	json.Unmarshal(entry.Data, &desc)
	return provider.OK(map[string]any{"GlobalTableDescription": desc}), nil
}

func (p *TableProvider) ListGlobalTables(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, "dynamodb_global_tables", "")
	if err != nil {
		return nil, fmt.Errorf("dynamodb: list global tables: %w", err)
	}
	tables := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var desc map[string]any
		if err := json.Unmarshal(e.Data, &desc); err != nil {
			continue
		}
		rg, _ := desc["ReplicationGroup"]
		tables = append(tables, map[string]any{
			"GlobalTableName":  desc["GlobalTableName"],
			"ReplicationGroup": rg,
		})
	}
	return provider.OK(map[string]any{"GlobalTables": tables}), nil
}

func (p *TableProvider) UpdateGlobalTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["GlobalTableName"].(string)
	entry, err := p.resources.Get(ctx, "dynamodb_global_tables", name)
	if err != nil {
		return nil, model.NewProviderError("GlobalTableNotFoundException", "Global table not found: "+name, 400)
	}
	var desc map[string]any
	json.Unmarshal(entry.Data, &desc)

	// Build current replica map keyed by region.
	replicaMap := map[string]map[string]any{}
	if rg, ok := desc["ReplicationGroup"].([]any); ok {
		for _, r := range rg {
			if rm, ok := r.(map[string]any); ok {
				region, _ := rm["RegionName"].(string)
				replicaMap[region] = rm
			}
		}
	}

	if updates, ok := nr.Params["ReplicaUpdates"].([]any); ok {
		for _, u := range updates {
			um, ok := u.(map[string]any)
			if !ok {
				continue
			}
			if create, ok := um["Create"].(map[string]any); ok {
				region, _ := create["RegionName"].(string)
				replicaMap[region] = map[string]any{"RegionName": region, "ReplicaStatus": "ACTIVE"}
			}
			if del, ok := um["Delete"].(map[string]any); ok {
				region, _ := del["RegionName"].(string)
				delete(replicaMap, region)
			}
		}
	}

	replicas := make([]map[string]any, 0, len(replicaMap))
	for _, r := range replicaMap {
		replicas = append(replicas, r)
	}
	desc["ReplicationGroup"] = replicas

	data, _ := json.Marshal(desc)
	entry.Data = json.RawMessage(data)
	if err := p.resources.Update(ctx, entry); err != nil {
		return nil, fmt.Errorf("dynamodb: update global table: %w", err)
	}
	return provider.OK(map[string]any{"GlobalTableDescription": desc}), nil
}

// ─── PartiQL stubs ────────────────────────────────────────────────────────────

func (p *TableProvider) ExecuteStatement(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("ValidationException",
		"PartiQL is not supported in this emulator. Use standard DynamoDB API operations.", 400)
}

func (p *TableProvider) ExecuteTransaction(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("ValidationException",
		"PartiQL is not supported in this emulator. Use standard DynamoDB API operations.", 400)
}

func (p *TableProvider) BatchExecuteStatement(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("ValidationException",
		"PartiQL is not supported in this emulator. Use standard DynamoDB API operations.", 400)
}

// exclusiveStartKey returns a JSON-encoded ExclusiveStartKey from request params,
// or an empty string when the param is absent or not a map.
// If _jc_mac is present it is verified; an invalid MAC causes a ValidationException
// to be returned as a sentinel string that callers must handle — see exclusiveStartKeyE.
func exclusiveStartKey(params map[string]any) string {
	s, _ := exclusiveStartKeyE(params)
	return s
}

// exclusiveStartKeyE returns (jsonKey, error). error is non-nil when _jc_mac
// verification fails.
func exclusiveStartKeyE(params map[string]any) (string, error) {
	v, ok := params["ExclusiveStartKey"]
	if !ok {
		return "", nil
	}
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return "", nil
	}
	stripped, err := verifyAndStripLEKMac(m)
	if err != nil {
		return "", fmt.Errorf("invalid pagination token")
	}
	b, err := json.Marshal(stripped)
	if err != nil {
		return "", nil
	}
	return string(b), nil
}

// ─── Misc stubs (Task 1.4) ────────────────────────────────────────────────────

func (p *TableProvider) DescribeEndpoints(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              fmt.Sprintf("dynamodb.%s.amazonaws.com", nr.Region),
				"CachePeriodInMinutes": 1440,
			},
		},
	}), nil
}

func (p *TableProvider) DescribeLimits(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"AccountMaxReadCapacityUnits":  20000,
		"AccountMaxWriteCapacityUnits": 20000,
		"TableMaxReadCapacityUnits":    10000,
		"TableMaxWriteCapacityUnits":   10000,
	}), nil
}

func (p *TableProvider) DescribeContributorInsights(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"TableName":                 strParam(nr.Params, "TableName"),
		"ContributorInsightsStatus": "DISABLED",
	}), nil
}

func (p *TableProvider) ListContributorInsights(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"ContributorInsightsSummaries": []any{},
	}), nil
}
