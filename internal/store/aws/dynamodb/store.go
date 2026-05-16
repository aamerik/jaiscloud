// Package dynamodb provides the DynamoDB item data-plane store.
package dynamodb

import (
	"context"
)

// UpdateSpec describes a DynamoDB UpdateItem operation.
type UpdateSpec struct {
	UpdateExpression          string
	ConditionExpression       string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]any
	ReturnValues              string // "NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"

	// Schema is required for index maintenance.
	Schema *TableSchema
}

// QuerySpec describes a DynamoDB Query operation.
type QuerySpec struct {
	// IndexSchema is nil when querying the table's primary key.
	// Set by the provider after resolving the IndexName from TableSchema.
	IndexSchema *IndexKeyRef

	// ProjectionAttrs is nil for ProjectionType=ALL.
	// For KEYS_ONLY or INCLUDE, the provider populates this list.
	ProjectionAttrs []string

	IndexName                 string
	KeyConditionExpression    string
	FilterExpression          string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]any
	ScanIndexForward          bool
	Limit                     int
	ExclusiveStartKey         string // JSON-encoded last evaluated key
	Select                    string
}

// ScanSpec describes a DynamoDB Scan operation.
type ScanSpec struct {
	IndexSchema     *IndexKeyRef // nil = scan the main table
	ProjectionAttrs []string     // nil = return full item

	IndexName                 string
	FilterExpression          string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]any
	Limit                     int
	ExclusiveStartKey         string
	Select                    string
	Segment                   int // 0-based segment number (parallel scan)
	TotalSegments             int // total number of segments; 0 means no parallel scan
}

// BatchWriteRequest is a single put or delete within a BatchWriteItem call.
type BatchWriteRequest struct {
	Table      string
	Schema     *TableSchema   // needed so the store can maintain index tables
	PutItem    map[string]any // non-nil → put
	PutHash    string         // pk hash for PutItem
	DeleteKey  map[string]any // non-nil → delete
	DeleteHash string         // pk hash for DeleteKey
}

// BatchGetRequest describes items to fetch in a BatchGetItem call.
type BatchGetRequest struct {
	Table string
	Keys  []map[string]any
}

// ConditionSpec carries optional condition checking fields shared by PutItem and DeleteItem.
type ConditionSpec struct {
	ConditionExpression       string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]any
	// ReturnValues for PutItem/DeleteItem: "" or "ALL_OLD"
	ReturnValues string

	// Schema is required for index maintenance. When nil, index tables are not updated.
	Schema *TableSchema
}

// TransactWriteOp is a single operation within a TransactWriteItems call.
type TransactWriteOp struct {
	Type      string         // "Put", "Delete", "Update", "ConditionCheck"
	Table     string
	PKHash    string
	Item      map[string]any // Put only
	Key       map[string]any // Delete, Update, ConditionCheck
	Cond      ConditionSpec
	Update    UpdateSpec
	// ReturnValuesOnConditionCheckFailure controls what is returned in a
	// CancellationReason when this operation's condition fails.
	// Valid value: "ALL_OLD".  Empty string means do not return the item.
	ReturnValuesOnConditionCheckFailure string
}

// CancellationReason is the per-item failure detail for TransactionCanceledException.
type CancellationReason struct {
	Code    string         // "None", "ConditionalCheckFailed", "TransactionConflict", "ThrottlingError", "ValidationError", "ResourceNotFound", "ItemCollectionSizeLimitExceeded"
	Message string
	Item    map[string]any // ReturnValuesOnConditionCheckFailure — set when Code=="ConditionalCheckFailed" and the caller requested ALL_OLD
}

// Cancellation reason code constants.
const (
	CancelCodeNone                          = "None"
	CancelCodeConditionalCheckFailed        = "ConditionalCheckFailed"
	CancelCodeTransactionConflict           = "TransactionConflict"
	CancelCodeThrottlingError               = "ThrottlingError"
	CancelCodeValidationError               = "ValidationError"
	CancelCodeResourceNotFound              = "ResourceNotFound"
	CancelCodeItemCollectionSizeLimitExceed = "ItemCollectionSizeLimitExceeded"
	CancelCodeDuplicateItem                 = "DuplicateItem"
	CancelCodeProvisionedThroughput         = "ProvisionedThroughputExceeded"
	CancelCodeRequestLimitExceeded          = "RequestLimitExceeded"
	CancelCodeInternalServerError           = "InternalServerError"
)

// DynamoDBItemStore manages the DynamoDB item data plane.
type DynamoDBItemStore interface {
	// ── Data-plane methods ───────────────────────────────────────────────────
	PutItem(ctx context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error)
	GetItem(ctx context.Context, table, pkHash string) (map[string]any, error)
	DeleteItem(ctx context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error)
	UpdateItem(ctx context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error)
	// Query returns (items, scannedCount, lastKey, error).
	// scannedCount is the number of items that matched the key condition before
	// FilterExpression was applied. Equals len(items) when there is no FilterExpression.
	Query(ctx context.Context, table string, q QuerySpec) ([]map[string]any, int, string, error)
	// Scan returns (items, scannedCount, lastKey, error).
	// scannedCount is the number of items examined before FilterExpression filtering.
	Scan(ctx context.Context, table string, s ScanSpec) ([]map[string]any, int, string, error)
	BatchWriteItems(ctx context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error)
	BatchGetItems(ctx context.Context, reqs []BatchGetRequest) (map[string][]map[string]any, error)
	// TransactWriteItems evaluates all conditions then applies all writes atomically.
	// Returns (nil, nil) on success; (reasons, nil) if any condition failed (caller wraps in TransactionCanceledException).
	TransactWriteItems(ctx context.Context, ops []TransactWriteOp) ([]CancellationReason, error)
	Reset()

	// ── Table-lifecycle methods ──────────────────────────────────────────────

	// CreateTableSchema initialises per-table storage (DDL in Postgres, index maps in memory).
	// Called by TableProvider.CreateTable.
	CreateTableSchema(ctx context.Context, schema TableSchema) error

	// DropTableSchema removes per-table storage and all its items/index rows.
	// Called by TableProvider.DeleteTable.
	DropTableSchema(ctx context.Context, tableName string) error

	// AddGSI backfills index rows for a new GSI from existing items.
	// Called by TableProvider.UpdateTable when creating a GSI.
	AddGSI(ctx context.Context, tableName string, schema TableSchema, idx IndexDef) error

	// DeleteGSI removes all index rows for the named GSI.
	// Called by TableProvider.UpdateTable when deleting a GSI.
	DeleteGSI(ctx context.Context, tableName string, schema TableSchema, indexName string) error
}
