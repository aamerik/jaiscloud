package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDynamoDBItemStore implements DynamoDBItemStore against PostgreSQL.
type PostgresDynamoDBItemStore struct {
	pool *pgxpool.Pool
}

func NewPostgresDynamoDBItemStore(pool *pgxpool.Pool) *PostgresDynamoDBItemStore {
	return &PostgresDynamoDBItemStore{pool: pool}
}

func hashKey(table string, item map[string]any) string {
	b, _ := json.Marshal(item)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(table+":"+string(b))))
}

func (s *PostgresDynamoDBItemStore) PutItem(ctx context.Context, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error) {
	h := pkHash
	if h == "" {
		h = hashKey(table, item)
	}
	// Evaluate condition against existing item if present.
	var oldItem map[string]any
	if cond.ConditionExpression != "" || cond.ReturnValues == "ALL_OLD" {
		oldItem, _ = s.GetItem(ctx, table, h)
		if cond.ConditionExpression != "" {
			existing := oldItem
			if existing == nil {
				existing = map[string]any{}
			}
			if !matchesFilter(existing, cond.ConditionExpression, cond.ExpressionAttributeNames, cond.ExpressionAttributeValues) {
				return nil, &conditionFailedError{}
			}
		}
	}
	raw, _ := json.Marshal(item)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_dynamodb_items (table_name, pk_hash, item)
		VALUES ($1, $2, $3)
		ON CONFLICT (table_name, pk_hash) DO UPDATE
			SET item=$3, updated_at=now()
	`, table, h, json.RawMessage(raw))
	if err != nil {
		return nil, err
	}
	if cond.ReturnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

func (s *PostgresDynamoDBItemStore) GetItem(ctx context.Context, table, pkHash string) (map[string]any, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT item FROM jc_dynamodb_items WHERE table_name=$1 AND pk_hash=$2
	`, table, pkHash).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var item map[string]any
	return item, json.Unmarshal(raw, &item)
}

func (s *PostgresDynamoDBItemStore) DeleteItem(ctx context.Context, table, pkHash string, cond ConditionSpec) (map[string]any, error) {
	var oldItem map[string]any
	if cond.ConditionExpression != "" || cond.ReturnValues == "ALL_OLD" {
		oldItem, _ = s.GetItem(ctx, table, pkHash)
		if cond.ConditionExpression != "" {
			existing := oldItem
			if existing == nil {
				existing = map[string]any{}
			}
			if !matchesFilter(existing, cond.ConditionExpression, cond.ExpressionAttributeNames, cond.ExpressionAttributeValues) {
				return nil, &conditionFailedError{}
			}
		}
	}
	_, err := s.pool.Exec(ctx, `
		DELETE FROM jc_dynamodb_items WHERE table_name=$1 AND pk_hash=$2
	`, table, pkHash)
	if err != nil {
		return nil, err
	}
	if cond.ReturnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

func (s *PostgresDynamoDBItemStore) UpdateItem(ctx context.Context, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error) {
	existing, err := s.GetItem(ctx, table, pkHash)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		existing = copyItem(item)
	}
	if spec.UpdateExpression != "" {
		applyUpdateExpression(existing, spec.UpdateExpression, spec.ExpressionAttributeNames, spec.ExpressionAttributeValues)
	} else {
		for k, v := range item {
			existing[k] = v
		}
	}
	if _, err := s.PutItem(ctx, table, pkHash, existing, ConditionSpec{}); err != nil {
		return nil, err
	}
	return existing, nil
}

func (s *PostgresDynamoDBItemStore) Query(ctx context.Context, table string, q QuerySpec) ([]map[string]any, string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT item FROM jc_dynamodb_items WHERE table_name=$1 ORDER BY pk_hash
	`, table)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	return s.filterRows(rows, q.KeyConditionExpression, q.FilterExpression, q.ExpressionAttributeNames, q.ExpressionAttributeValues, q.ExclusiveStartKey, q.Limit)
}

func (s *PostgresDynamoDBItemStore) Scan(ctx context.Context, table string, sc ScanSpec) ([]map[string]any, string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT item FROM jc_dynamodb_items WHERE table_name=$1 ORDER BY pk_hash
	`, table)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	return s.filterRows(rows, "", sc.FilterExpression, sc.ExpressionAttributeNames, sc.ExpressionAttributeValues, sc.ExclusiveStartKey, sc.Limit)
}

func (s *PostgresDynamoDBItemStore) filterRows(rows pgx.Rows, keyExpr, filterExpr string, names map[string]string, values map[string]any, exclusiveStartKey string, limit int) ([]map[string]any, string, error) {
	var all []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, "", err
		}
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		if !matchesKeyCondition(item, keyExpr, names, values) {
			continue
		}
		if filterExpr != "" && !matchesFilter(item, filterExpr, names, values) {
			continue
		}
		all = append(all, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	all = paginateItems(all, exclusiveStartKey, limit)
	var lastKey string
	if limit > 0 && len(all) == limit {
		b, _ := json.Marshal(all[len(all)-1])
		lastKey = string(b)
	}
	if all == nil {
		all = []map[string]any{}
	}
	return all, lastKey, nil
}

func (s *PostgresDynamoDBItemStore) BatchWriteItems(ctx context.Context, reqs []BatchWriteRequest) ([]BatchWriteRequest, error) {
	for _, req := range reqs {
		if req.PutItem != nil {
			if _, err := s.PutItem(ctx, req.Table, req.PutHash, req.PutItem, ConditionSpec{}); err != nil {
				return nil, err
			}
		} else if req.DeleteKey != nil {
			h := req.DeleteHash
			if h == "" {
				h = hashKey(req.Table, req.DeleteKey)
			}
			if _, err := s.DeleteItem(ctx, req.Table, h, ConditionSpec{}); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (s *PostgresDynamoDBItemStore) BatchGetItems(ctx context.Context, reqs []BatchGetRequest) (map[string][]map[string]any, error) {
	result := make(map[string][]map[string]any)
	for _, req := range reqs {
		for _, key := range req.Keys {
			h := itemPKHash(key)
			item, err := s.GetItem(ctx, req.Table, h)
			if err != nil {
				return nil, err
			}
			if item != nil {
				result[req.Table] = append(result[req.Table], item)
			}
		}
	}
	return result, nil
}

func (s *PostgresDynamoDBItemStore) Reset() {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_dynamodb_items`)
}
