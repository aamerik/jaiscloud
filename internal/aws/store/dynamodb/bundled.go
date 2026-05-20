package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

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

func (s *BundledDynamoDBItemStore) Reset(ctx context.Context) { s.b.Reset(ctx) }

// ─── Snapshotter ──────────────────────────────────────────────────────────────

type bundledDynamoScopeSnap struct {
	Account string          `json:"account"`
	Region  string          `json:"region"`
	Data    json.RawMessage `json:"data"`
}

func (s *BundledDynamoDBItemStore) IsEmpty(_ context.Context) (bool, error) {
	empty := true
	s.b.Iter(func(_, _ string, st *MemoryDynamoDBItemStore) {
		if !empty {
			return
		}
		ok, _ := st.IsEmpty(context.Background())
		if !ok {
			empty = false
		}
	})
	return empty, nil
}

func (s *BundledDynamoDBItemStore) Snapshot(_ context.Context, w io.Writer) error {
	scopes := make([]bundledDynamoScopeSnap, 0)
	var iterErr error
	s.b.Iter(func(account, region string, st *MemoryDynamoDBItemStore) {
		if iterErr != nil {
			return
		}
		var buf bytes.Buffer
		if err := st.Snapshot(context.Background(), &buf); err != nil {
			iterErr = err
			return
		}
		scopes = append(scopes, bundledDynamoScopeSnap{
			Account: account,
			Region:  region,
			Data:    json.RawMessage(buf.Bytes()),
		})
	})
	if iterErr != nil {
		return iterErr
	}
	return json.NewEncoder(w).Encode(map[string]any{"kind": "local", "scopes": scopes})
}

func (s *BundledDynamoDBItemStore) Restore(_ context.Context, r io.Reader) error {
	var envelope struct {
		Scopes []bundledDynamoScopeSnap `json:"scopes"`
	}
	if err := json.NewDecoder(r).Decode(&envelope); err != nil {
		return err
	}
	s.b.ResetAndDiscard()
	for _, scope := range envelope.Scopes {
		st, err := s.b.Get(scope.Account, scope.Region)
		if err != nil {
			return err
		}
		if err := st.Restore(context.Background(), bytes.NewReader(scope.Data)); err != nil {
			return err
		}
	}
	return nil
}
