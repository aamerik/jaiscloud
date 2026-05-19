package dynamodb

import (
	"context"

	"jaiscloud/internal/aws/store/bundle"
)

// BundledDynamoDBItemStore wraps LocalBundle[MemoryDynamoDBItemStore] to provide
// per-(account,region) isolation. It implements DynamoDBItemStore and is used in lite mode.
type BundledDynamoDBItemStore struct {
	b *bundle.LocalBundle[MemoryDynamoDBItemStore]
}

func NewBundledDynamoDBItemStore() *BundledDynamoDBItemStore {
	return &BundledDynamoDBItemStore{
		b: bundle.NewLocal(func(_, _ string) *MemoryDynamoDBItemStore {
			return NewMemoryDynamoDBItemStore()
		}),
	}
}

func (s *BundledDynamoDBItemStore) inner(account, region string) (*MemoryDynamoDBItemStore, error) {
	return s.b.Get(account, region)
}

func (s *BundledDynamoDBItemStore) PutItem(ctx context.Context, account, region, table, pkHash string, item map[string]any, cond ConditionSpec) (map[string]any, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.PutItem(ctx, account, region, table, pkHash, item, cond)
}

func (s *BundledDynamoDBItemStore) GetItem(ctx context.Context, account, region, table, pkHash string) (map[string]any, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.GetItem(ctx, account, region, table, pkHash)
}

func (s *BundledDynamoDBItemStore) DeleteItem(ctx context.Context, account, region, table, pkHash string, cond ConditionSpec) (map[string]any, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.DeleteItem(ctx, account, region, table, pkHash, cond)
}

func (s *BundledDynamoDBItemStore) UpdateItem(ctx context.Context, account, region, table, pkHash string, item map[string]any, spec UpdateSpec) (map[string]any, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.UpdateItem(ctx, account, region, table, pkHash, item, spec)
}

func (s *BundledDynamoDBItemStore) Query(ctx context.Context, account, region, table string, q QuerySpec) ([]map[string]any, int, string, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, 0, "", err
	}
	return st.Query(ctx, account, region, table, q)
}

func (s *BundledDynamoDBItemStore) Scan(ctx context.Context, account, region, table string, sc ScanSpec) ([]map[string]any, int, string, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, 0, "", err
	}
	return st.Scan(ctx, account, region, table, sc)
}

func (s *BundledDynamoDBItemStore) BatchWriteItems(ctx context.Context, account, region string, reqs []BatchWriteRequest) ([]BatchWriteRequest, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.BatchWriteItems(ctx, account, region, reqs)
}

func (s *BundledDynamoDBItemStore) BatchGetItems(ctx context.Context, account, region string, reqs []BatchGetRequest) (map[string][]map[string]any, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.BatchGetItems(ctx, account, region, reqs)
}

func (s *BundledDynamoDBItemStore) TransactWriteItems(ctx context.Context, account, region string, ops []TransactWriteOp) ([]CancellationReason, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.TransactWriteItems(ctx, account, region, ops)
}

func (s *BundledDynamoDBItemStore) CreateTableSchema(ctx context.Context, account, region string, schema TableSchema) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.CreateTableSchema(ctx, account, region, schema)
}

func (s *BundledDynamoDBItemStore) DropTableSchema(ctx context.Context, account, region, tableName string) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.DropTableSchema(ctx, account, region, tableName)
}

func (s *BundledDynamoDBItemStore) AddGSI(ctx context.Context, account, region, tableName string, schema TableSchema, idx IndexDef) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.AddGSI(ctx, account, region, tableName, schema, idx)
}

func (s *BundledDynamoDBItemStore) DeleteGSI(ctx context.Context, account, region, tableName string, schema TableSchema, indexName string) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.DeleteGSI(ctx, account, region, tableName, schema, indexName)
}

func (s *BundledDynamoDBItemStore) Reset() { s.b.Reset() }
