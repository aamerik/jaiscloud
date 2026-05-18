package table

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	dynamostore "jaiscloud/internal/store/aws/dynamodb"
	streamstore "jaiscloud/internal/store/stream"

	"jaiscloud/internal/store"
)

// TTLWorker periodically scans all DynamoDB tables for expired TTL items and deletes them.
// cloud/region/accountID are captured at construction — never from a NormalizedRequest.
type TTLWorker struct {
	resources store.ResourceStore
	items     dynamostore.DynamoDBItemStore
	streams   *streamstore.MemoryStreamStore
	cloud     string
	region    string
	accountID string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	tickerDur time.Duration
}

// NewTTLWorker constructs a worker. tickerDur controls scan frequency (use time.Hour in production).
func NewTTLWorker(
	resources store.ResourceStore,
	items dynamostore.DynamoDBItemStore,
	streams *streamstore.MemoryStreamStore,
	cloud, region, accountID string,
	tickerDur time.Duration,
) *TTLWorker {
	return &TTLWorker{
		resources: resources,
		items:     items,
		streams:   streams,
		cloud:     cloud,
		region:    region,
		accountID: accountID,
		tickerDur: tickerDur,
	}
}

// Start launches the background scan goroutine.
func (w *TTLWorker) Start(ctx context.Context) {
	ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.tickerDur)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.sweep(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Shutdown stops the goroutine and waits for it to exit.
func (w *TTLWorker) Shutdown() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

// Reset satisfies admin.Resetter; state is owned by the store, not the worker.
func (w *TTLWorker) Reset() {}

func (w *TTLWorker) sweep(ctx context.Context) {
	entries, err := w.resources.List(ctx, "", "", "dynamodb_tables", "")
	if err != nil {
		return
	}
	nowUnix := time.Now().Unix()
	for _, e := range entries {
		var ts tableSchema
		if err := json.Unmarshal(e.Data, &ts); err != nil {
			continue
		}
		if ts.TTLSpec == nil || !ts.TTLSpec.Enabled {
			continue
		}
		w.expireTable(ctx, ts, nowUnix)
	}
}

func (w *TTLWorker) expireTable(ctx context.Context, ts tableSchema, nowUnix int64) {
	sc := dynamostore.ScanSpec{Limit: 0} // 0 = no limit
	items, _, _, err := w.items.Scan(ctx, "", "", ts.TableName, sc)
	if err != nil {
		return
	}
	ttlAttr := ts.TTLSpec.AttributeName
	for _, item := range items {
		v, ok := item[ttlAttr]
		if !ok {
			continue
		}
		expiry := ttlNumericValue(v)
		if expiry == 0 || expiry > nowUnix {
			continue
		}
		pkHash := dynamostore.MemoryItemPKHash(item)
		_, _ = w.items.DeleteItem(ctx, "", "", ts.TableName, pkHash, dynamostore.ConditionSpec{})
		if w.streams != nil && w.streams.IsEnabled(ts.TableName) {
			w.streams.Append(ts.TableName, streamstore.Record{
				EventName:                   "REMOVE",
				Keys:                        item,
				OldImage:                    item,
				ApproximateCreationDateTime: time.Now(),
				UserIdentity:                &streamstore.UserIdentity{Type: "Service", PrincipalId: "dynamodb.amazonaws.com"},
			})
		}
	}
}

// ttlNumericValue extracts the Unix epoch integer from a DynamoDB N attribute.
func ttlNumericValue(v any) int64 {
	switch val := v.(type) {
	case map[string]any:
		if n, ok := val["N"].(string); ok {
			return parseIntStr(n)
		}
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	}
	return 0
}

func parseIntStr(s string) int64 {
	negative := false
	start := 0
	if len(s) > 0 && s[0] == '-' {
		negative = true
		start = 1
	}
	var result int64
	for _, c := range s[start:] {
		if c < '0' || c > '9' {
			return 0
		}
		result = result*10 + int64(c-'0')
	}
	if negative {
		result = -result
	}
	return result
}
