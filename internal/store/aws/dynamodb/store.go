// Package dynamodb provides the DynamoDB item data-plane store.
package dynamodb

import (
	"context"
)

// UpdateSpec describes a DynamoDB UpdateItem operation.
type UpdateSpec struct {
	UpdateExpression          string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]any
	ReturnValues              string // "NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"
}

// QuerySpec describes a DynamoDB Query operation.
type QuerySpec struct {
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
	IndexName                 string
	FilterExpression          string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]any
	Limit                     int
	ExclusiveStartKey         string
	Select                    string
}

// BatchWriteRequest is a single put or delete within a BatchWriteItem call.
type BatchWriteRequest struct {
	Table      string
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

// DynamoDBItemStore manages the DynamoDB item data plane.
type DynamoDBItemStore interface {
	PutItem(ctx context.Context, table, pkHash string, item map[string]any) error
	GetItem(ctx context.Context, table, pkHash string) (map[string]any, error)
	DeleteItem(ctx context.Context, table, pkHash string) error
	UpdateItem(ctx context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error)
	Query(ctx context.Context, table string, q QuerySpec) ([]map[string]any, string, error)
	Scan(ctx context.Context, table string, s ScanSpec) ([]map[string]any, string, error)
	BatchWriteItems(ctx context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error)
	BatchGetItems(ctx context.Context, reqs []BatchGetRequest) (map[string][]map[string]any, error)
	Reset()
}
