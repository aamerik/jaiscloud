// Package kinesis implements the AWS Kinesis provider for JaisCloud.
package kinesis

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	kinesisstore "jaiscloud/internal/store/aws/kinesis"
)

var streamNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

// Provider handles Kinesis API operations.
// In lite mode it uses the in-memory store; in full mode it proxies to kinesis-mock.
type Provider struct {
	store      *kinesisstore.MemoryKinesisStore
	mockServer *MockServer
	httpClient *http.Client
	fullMode   bool
}

// New constructs a Provider in lite mode.
func New(store *kinesisstore.MemoryKinesisStore) *Provider {
	return &Provider{store: store, httpClient: &http.Client{Timeout: 30 * time.Second}}
}

// NewFull constructs a Provider in full mode backed by a kinesis-mock subprocess.
func NewFull(store *kinesisstore.MemoryKinesisStore, mock *MockServer) *Provider {
	return &Provider{
		store:      store,
		mockServer: mock,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		fullMode:   true,
	}
}

// proxyToMock forwards the current action + params to kinesis-mock subprocess.
func (p *Provider) proxyToMock(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := json.Marshal(nr.Params)
	url := fmt.Sprintf("http://localhost:%d/", p.mockServer.Port())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, model.NewProviderError("InternalFailure", err.Error(), 500)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202."+nr.Action)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20000101/us-east-1/kinesis/aws4_request, SignedHeaders=host, Signature=test")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, model.NewProviderError("InternalFailure", "kinesis-mock unreachable: "+err.Error(), 500)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		var errBody struct {
			Type    string `json:"__type"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBody, &errBody)
		if errBody.Type == "" {
			errBody.Type = "InternalFailure"
		}
		return nil, model.NewProviderError(errBody.Type, errBody.Message, resp.StatusCode)
	}
	var data map[string]any
	_ = json.Unmarshal(respBody, &data)
	if data == nil {
		data = map[string]any{}
	}
	return provider.OK(data), nil
}

// Routes returns all Kinesis handler registrations.
// In full mode every route is wrapped to proxy to kinesis-mock.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	routes := p.liteRoutes()
	if p.fullMode {
		wrapped := make(map[string]provider.HandlerFunc, len(routes))
		for k := range routes {
			k := k // capture
			wrapped[k] = func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
				return p.proxyToMock(ctx, nr)
			}
		}
		return wrapped
	}
	return routes
}

// liteRoutes returns the in-memory handler map used in lite mode.
func (p *Provider) liteRoutes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Stream lifecycle
		"Kinesis.CreateStream":          p.CreateStream,
		"Kinesis.DeleteStream":          p.DeleteStream,
		"Kinesis.DescribeStream":        p.DescribeStream,
		"Kinesis.DescribeStreamSummary": p.DescribeStreamSummary,
		"Kinesis.ListStreams":            p.ListStreams,
		"Kinesis.UpdateStreamMode":      p.UpdateStreamMode,
		// Records
		"Kinesis.PutRecord":           p.PutRecord,
		"Kinesis.PutRecords":          p.PutRecords,
		"Kinesis.GetShardIterator":    p.GetShardIterator,
		"Kinesis.GetRecords":          p.GetRecords,
		// Shards
		"Kinesis.ListShards":         p.ListShards,
		"Kinesis.SplitShard":         p.SplitShard,
		"Kinesis.MergeShards":        p.MergeShards,
		"Kinesis.UpdateShardCount":   p.UpdateShardCount,
		// Consumers (Enhanced Fan-Out)
		"Kinesis.RegisterStreamConsumer":   p.RegisterStreamConsumer,
		"Kinesis.DeregisterStreamConsumer": p.DeregisterStreamConsumer,
		"Kinesis.ListStreamConsumers":      p.ListStreamConsumers,
		"Kinesis.DescribeStreamConsumer":   p.DescribeStreamConsumer,
		"Kinesis.SubscribeToShard":         p.SubscribeToShard,
		// Retention
		"Kinesis.IncreaseStreamRetentionPeriod": p.IncreaseStreamRetentionPeriod,
		"Kinesis.DecreaseStreamRetentionPeriod": p.DecreaseStreamRetentionPeriod,
		// Tags
		"Kinesis.AddTagsToStream":      p.AddTagsToStream,
		"Kinesis.RemoveTagsFromStream":  p.RemoveTagsFromStream,
		"Kinesis.ListTagsForStream":     p.ListTagsForStream,
		// Monitoring (stubs)
		"Kinesis.EnableEnhancedMonitoring":  p.EnableEnhancedMonitoring,
		"Kinesis.DisableEnhancedMonitoring": p.DisableEnhancedMonitoring,
		// Resource Policy
		"Kinesis.PutResourcePolicy":    p.PutResourcePolicy,
		"Kinesis.GetResourcePolicy":    p.GetResourcePolicy,
		"Kinesis.DeleteResourcePolicy": p.DeleteResourcePolicy,
	}
}

// Reset wipes all state. In full mode, restarts kinesis-mock subprocess.
func (p *Provider) Reset() {
	if p.fullMode && p.mockServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.mockServer.Restart(ctx); err != nil {
			// best-effort; log but don't panic
			_ = err
		}
		return
	}
	p.store.Reset()
}

// Shutdown stops the kinesis-mock subprocess if running.
func (p *Provider) Shutdown() {
	if p.fullMode && p.mockServer != nil {
		_ = p.mockServer.Stop()
	}
}

// ─── Stream CRUD ──────────────────────────────────────────────────────────────

func (p *Provider) CreateStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	shardCount := intParam(nr.Params, "ShardCount", 1)

	if name == "" || len(name) > 128 || !streamNameRE.MatchString(name) {
		return nil, kErr("ValidationException", "StreamName must be 1-128 characters matching [a-zA-Z0-9_.\\-]+", 400)
	}
	if shardCount < 1 {
		return nil, kErr("ValidationException", "ShardCount must be at least 1", 400)
	}

	mode := kinesisstore.StreamModeProvisioned
	if md := nestedStr(nr.Params, "StreamModeDetails", "StreamMode"); md == "ON_DEMAND" {
		mode = kinesisstore.StreamModeOnDemand
	}

	arn := nr.ResourceID("kinesis-stream", name)
	stream := kinesisstore.Stream{
		Name:      name,
		ARN:       arn,
		Status:    kinesisstore.StreamStatusActive,
		Mode:      mode,
		CreatedAt: time.Now().UTC(),
	}
	if err := p.store.CreateStream(stream, shardCount); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DeleteStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, arn := streamIdentifiers(nr)
	if name != "" {
		if err := p.store.DeleteStreamInScope(nr.AccountID, nr.Region, name); err != nil {
			return nil, providerErr(err)
		}
	} else if arn != "" {
		if err := p.store.DeleteStreamByARN(arn); err != nil {
			return nil, providerErr(err)
		}
	} else {
		return nil, kErr("ValidationException", "StreamName or StreamARN required", 400)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, arn := streamIdentifiers(nr)
	var stream *kinesisstore.Stream
	var err error
	if arn != "" && name == "" {
		stream, err = p.store.GetStreamByARN(arn)
		if err != nil {
			return nil, providerErr(err)
		}
		name = stream.Name
	} else {
		stream, err = p.store.GetStreamInScope(nr.AccountID, nr.Region, name)
		if err != nil {
			return nil, providerErr(err)
		}
	}
	shards, err := p.store.ListShardsInScope(nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{
		"StreamDescription": buildStreamDescription(stream, shards, false),
	}), nil
}

func (p *Provider) DescribeStreamSummary(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, arn := streamIdentifiers(nr)
	var stream *kinesisstore.Stream
	var err error
	if arn != "" && name == "" {
		stream, err = p.store.GetStreamByARN(arn)
		if err != nil {
			return nil, providerErr(err)
		}
		name = stream.Name
	} else {
		stream, err = p.store.GetStreamInScope(nr.AccountID, nr.Region, name)
		if err != nil {
			return nil, providerErr(err)
		}
	}
	shards, err := p.store.ListShardsInScope(nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{
		"StreamDescriptionSummary": buildStreamDescription(stream, shards, true),
	}), nil
}

func (p *Provider) ListStreams(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	limit := intParam(nr.Params, "Limit", 100)
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	all := p.store.ListStreamsInScope(nr.AccountID, nr.Region)
	// sort by name for determinism
	sortStreamsByName(all)

	exclusiveStartName := strParam(nr.Params, "ExclusiveStartStreamName")
	start := 0
	if exclusiveStartName != "" {
		for i, st := range all {
			if st.Name == exclusiveStartName {
				start = i + 1
				break
			}
		}
	}
	all = all[start:]

	hasMore := false
	if len(all) > limit {
		hasMore = true
		all = all[:limit]
	}

	names := make([]string, len(all))
	arns := make([]string, len(all))
	for i, st := range all {
		names[i] = st.Name
		arns[i] = st.ARN
	}
	result := map[string]any{
		"StreamNames": names,
		"StreamSummaries": func() []map[string]any {
			out := make([]map[string]any, len(all))
			for i, st := range all {
				out[i] = map[string]any{
					"StreamName": st.Name,
					"StreamARN":  st.ARN,
					"StreamStatus": string(st.Status),
					"StreamModeDetails": map[string]any{"StreamMode": string(st.Mode)},
				}
			}
			return out
		}(),
		"HasMoreStreams": hasMore,
	}
	if hasMore {
		result["NextToken"] = names[len(names)-1]
	}
	return provider.OK(result), nil
}

func (p *Provider) UpdateStreamMode(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "StreamARN")
	mode := nestedStr(nr.Params, "StreamModeDetails", "StreamMode")
	var sm kinesisstore.StreamMode
	switch mode {
	case "ON_DEMAND":
		sm = kinesisstore.StreamModeOnDemand
	default:
		sm = kinesisstore.StreamModeProvisioned
	}
	if err := p.store.UpdateStreamModeByARN(arn, sm); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Records ──────────────────────────────────────────────────────────────────

func (p *Provider) PutRecord(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	pk := strParam(nr.Params, "PartitionKey")
	dataB64 := strParam(nr.Params, "Data")
	explicitHash := strParam(nr.Params, "ExplicitHashKey")

	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, kErr("InvalidArgumentException", "Data must be valid base64", 400)
	}

	shardID, seq, err := p.store.PutRecordInScope(nr.AccountID, nr.Region, name, data, pk, explicitHash)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{
		"ShardId":        shardID,
		"SequenceNumber": seq,
		"EncryptionType": "NONE",
	}), nil
}

func (p *Provider) PutRecords(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	rawRecords, _ := nr.Params["Records"].([]any)

	results := make([]map[string]any, len(rawRecords))
	failCount := 0
	for i, raw := range rawRecords {
		rec, _ := raw.(map[string]any)
		pk, _ := rec["PartitionKey"].(string)
		dataB64, _ := rec["Data"].(string)
		explicitHash, _ := rec["ExplicitHashKey"].(string)

		data, decErr := base64.StdEncoding.DecodeString(dataB64)
		if decErr != nil {
			results[i] = map[string]any{
				"ErrorCode":    "InvalidArgumentException",
				"ErrorMessage": "Data must be valid base64",
			}
			failCount++
			continue
		}
		shardID, seq, putErr := p.store.PutRecordInScope(nr.AccountID, nr.Region, name, data, pk, explicitHash)
		if putErr != nil {
			ke, _ := putErr.(*kinesisstore.KinesisError)
			errCode := "InternalFailure"
			errMsg := putErr.Error()
			if ke != nil {
				errCode = ke.Code
				errMsg = ke.Message
			}
			results[i] = map[string]any{
				"ErrorCode":    errCode,
				"ErrorMessage": errMsg,
			}
			failCount++
		} else {
			results[i] = map[string]any{
				"ShardId":        shardID,
				"SequenceNumber": seq,
			}
		}
	}
	return provider.OK(map[string]any{
		"FailedRecordCount": failCount,
		"Records":           results,
	}), nil
}

func (p *Provider) GetShardIterator(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	shardID := strParam(nr.Params, "ShardId")
	iterTypeStr := strParam(nr.Params, "ShardIteratorType")
	seqNum := strParam(nr.Params, "StartingSequenceNumber")
	tsRaw := strParam(nr.Params, "Timestamp")

	var ts *time.Time
	if tsRaw != "" {
		t, err := time.Parse(time.RFC3339, tsRaw)
		if err == nil {
			ts = &t
		}
	}

	iterType := kinesisstore.ShardIteratorType(iterTypeStr)
	id, err := p.store.CreateIteratorInScope(nr.AccountID, nr.Region, name, shardID, iterType, seqNum, ts)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{"ShardIterator": id}), nil
}

func (p *Provider) GetRecords(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	iterID := strParam(nr.Params, "ShardIterator")
	limit := intParam(nr.Params, "Limit", 10000)
	if limit < 1 {
		return nil, kErr("InvalidArgumentException", "Limit must be between 1 and 10000", 400)
	}

	records, nextIter, millsBehind, err := p.store.GetRecords(iterID, limit)
	if err != nil {
		return nil, providerErr(err)
	}

	outRecords := make([]map[string]any, len(records))
	for i, r := range records {
		outRecords[i] = map[string]any{
			"SequenceNumber":              r.SequenceNumber,
			"ApproximateArrivalTimestamp": r.ApproximateArrivalTime.Unix(),
			"Data":                        base64.StdEncoding.EncodeToString(r.Data),
			"PartitionKey":                r.PartitionKey,
			"EncryptionType":              r.EncryptionType,
		}
	}

	result := map[string]any{
		"Records":            outRecords,
		"MillisBehindLatest": millsBehind,
	}
	if nextIter != "" {
		result["NextShardIterator"] = nextIter
	}
	return provider.OK(result), nil
}

// ─── Shards ───────────────────────────────────────────────────────────────────

func (p *Provider) ListShards(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, arn := streamIdentifiers(nr)
	if name == "" && arn != "" {
		st, err := p.store.GetStreamByARN(arn)
		if err != nil {
			return nil, providerErr(err)
		}
		name = st.Name
	}
	shards, err := p.store.ListShardsInScope(nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{
		"Shards": buildShardList(shards),
	}), nil
}

func (p *Provider) SplitShard(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	shardID := strParam(nr.Params, "ShardToSplit")
	newKey := strParam(nr.Params, "NewStartingHashKey")
	if err := p.store.SplitShardInScope(nr.AccountID, nr.Region, name, shardID, newKey); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) MergeShards(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	shard := strParam(nr.Params, "ShardToMerge")
	adjacent := strParam(nr.Params, "AdjacentShardToMerge")
	if err := p.store.MergeShardsInScope(nr.AccountID, nr.Region, name, shard, adjacent); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UpdateShardCount(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	target := intParam(nr.Params, "TargetShardCount", 0)
	stream, err := p.store.GetStreamInScope(nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, providerErr(err)
	}
	shards, err := p.store.ListShardsInScope(nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, providerErr(err)
	}
	openCount := 0
	for _, s := range shards {
		if s.IsOpen {
			openCount++
		}
	}
	return provider.OK(map[string]any{
		"StreamName":       name,
		"CurrentShardCount": openCount,
		"TargetShardCount": target,
		"StreamARN":        stream.ARN,
	}), nil
}

// ─── Consumers ────────────────────────────────────────────────────────────────

func (p *Provider) RegisterStreamConsumer(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	streamARN := strParam(nr.Params, "StreamARN")
	consumerName := strParam(nr.Params, "ConsumerName")

	stream, err := p.store.GetStreamByARN(streamARN)
	if err != nil {
		return nil, providerErr(err)
	}

	ts := time.Now().UTC().Unix()
	// ARN: arn:aws:kinesis:region:account:stream/name/consumer/consumerName:timestamp
	consumerARN := fmt.Sprintf("%s/consumer/%s:%d", streamARN, consumerName, ts)
	_ = stream

	c, err := p.store.RegisterConsumer(streamARN, consumerName, consumerARN)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{
		"Consumer": buildConsumer(c),
	}), nil
}

func (p *Provider) DeregisterStreamConsumer(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	consumerARN := strParam(nr.Params, "ConsumerARN")
	if err := p.store.DeregisterConsumer(consumerARN); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListStreamConsumers(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	streamARN := strParam(nr.Params, "StreamARN")
	consumers, err := p.store.ListConsumers(streamARN)
	if err != nil {
		return nil, providerErr(err)
	}
	out := make([]map[string]any, len(consumers))
	for i, c := range consumers {
		out[i] = buildConsumer(c)
	}
	return provider.OK(map[string]any{"Consumers": out}), nil
}

func (p *Provider) DescribeStreamConsumer(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	consumerARN := strParam(nr.Params, "ConsumerARN")
	c, err := p.store.GetConsumer(consumerARN)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{
		"ConsumerDescription": buildConsumer(c),
	}), nil
}

func (p *Provider) SubscribeToShard(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// HTTP/2 server-push not implemented in lite mode — return empty event stream stub
	return provider.OK(map[string]any{}), nil
}

// ─── Retention ────────────────────────────────────────────────────────────────

func (p *Provider) IncreaseStreamRetentionPeriod(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	hours := intParam(nr.Params, "RetentionPeriodHours", 0)
	if err := p.store.SetRetentionPeriodInScope(nr.AccountID, nr.Region, name, hours); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DecreaseStreamRetentionPeriod(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	hours := intParam(nr.Params, "RetentionPeriodHours", 0)
	if err := p.store.SetRetentionPeriodInScope(nr.AccountID, nr.Region, name, hours); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *Provider) AddTagsToStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	tags := parseTagMap(nr.Params, "Tags")
	if err := p.store.AddTagsInScope(nr.AccountID, nr.Region, name, tags); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) RemoveTagsFromStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	keys := parseStringList(nr.Params, "TagKeys")
	if err := p.store.RemoveTagsInScope(nr.AccountID, nr.Region, name, keys); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	tags, err := p.store.GetTagsInScope(nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, providerErr(err)
	}
	tagList := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": tagList, "HasMoreTags": false}), nil
}

// ─── Monitoring (stubs) ───────────────────────────────────────────────────────

func (p *Provider) EnableEnhancedMonitoring(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	streamARN := ""
	if st, err := p.store.GetStreamInScope(nr.AccountID, nr.Region, name); err == nil {
		streamARN = st.ARN
	}
	return provider.OK(map[string]any{
		"StreamName":               name,
		"CurrentShardLevelMetrics": []string{},
		"DesiredShardLevelMetrics": []string{},
		"StreamARN":                streamARN,
	}), nil
}

func (p *Provider) DisableEnhancedMonitoring(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "StreamName")
	streamARN := ""
	if st, err := p.store.GetStreamInScope(nr.AccountID, nr.Region, name); err == nil {
		streamARN = st.ARN
	}
	return provider.OK(map[string]any{
		"StreamName":               name,
		"CurrentShardLevelMetrics": []string{},
		"DesiredShardLevelMetrics": []string{},
		"StreamARN":                streamARN,
	}), nil
}

// ─── Resource Policy ──────────────────────────────────────────────────────────

func (p *Provider) PutResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceARN")
	policy := strParam(nr.Params, "Policy")
	if err := p.store.SetResourcePolicy(arn, policy); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) GetResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceARN")
	policy, err := p.store.GetResourcePolicy(arn)
	if err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{"Policy": policy}), nil
}

func (p *Provider) DeleteResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceARN")
	if err := p.store.DeleteResourcePolicy(arn); err != nil {
		return nil, providerErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func intParam(params map[string]any, key string, def int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return def
}

func nestedStr(params map[string]any, outer, inner string) string {
	if m, ok := params[outer].(map[string]any); ok {
		v, _ := m[inner].(string)
		return v
	}
	return ""
}

func streamIdentifiers(nr *model.NormalizedRequest) (name, arn string) {
	return strParam(nr.Params, "StreamName"), strParam(nr.Params, "StreamARN")
}

func parseTagMap(params map[string]any, key string) map[string]string {
	out := make(map[string]string)
	if m, ok := params[key].(map[string]any); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
	}
	return out
}

func parseStringList(params map[string]any, key string) []string {
	if arr, ok := params[key].([]any); ok {
		out := make([]string, 0, len(arr))
		for _, v := range arr {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func buildStreamDescription(stream *kinesisstore.Stream, shards []kinesisstore.Shard, summary bool) map[string]any {
	openCount := 0
	for _, s := range shards {
		if s.IsOpen {
			openCount++
		}
	}
	d := map[string]any{
		"StreamName":              stream.Name,
		"StreamARN":               stream.ARN,
		"StreamStatus":            string(stream.Status),
		"StreamModeDetails":       map[string]any{"StreamMode": string(stream.Mode)},
		"RetentionPeriodHours":    stream.RetentionPeriodHours,
		"StreamCreationTimestamp": stream.CreatedAt.Unix(),
		"EncryptionType":          stream.EncryptionType,
		"HasMoreShards":           false,
		"OpenShardCount":          openCount,
	}
	if !summary {
		d["Shards"] = buildShardList(shards)
	}
	return d
}

func buildShardList(shards []kinesisstore.Shard) []map[string]any {
	out := make([]map[string]any, len(shards))
	for i, s := range shards {
		sh := map[string]any{
			"ShardId": s.ShardID,
			"HashKeyRange": map[string]any{
				"StartingHashKey": s.HashKeyRange.StartingHashKey,
				"EndingHashKey":   s.HashKeyRange.EndingHashKey,
			},
			"SequenceNumberRange": map[string]any{
				"StartingSequenceNumber": s.SequenceNumberRange.StartingSequenceNumber,
			},
		}
		if s.SequenceNumberRange.EndingSequenceNumber != "" {
			sh["SequenceNumberRange"].(map[string]any)["EndingSequenceNumber"] = s.SequenceNumberRange.EndingSequenceNumber
		}
		if s.ParentShardID != "" {
			sh["ParentShardId"] = s.ParentShardID
		}
		if s.AdjacentParentShardID != "" {
			sh["AdjacentParentShardId"] = s.AdjacentParentShardID
		}
		out[i] = sh
	}
	return out
}

func buildConsumer(c *kinesisstore.Consumer) map[string]any {
	return map[string]any{
		"ConsumerName":             c.Name,
		"ConsumerARN":              c.ARN,
		"ConsumerStatus":           c.Status,
		"ConsumerCreationTimestamp": c.CreatedAt.Unix(),
	}
}

func sortStreamsByName(streams []kinesisstore.Stream) {
	for i := 1; i < len(streams); i++ {
		for j := i; j > 0 && streams[j].Name < streams[j-1].Name; j-- {
			streams[j], streams[j-1] = streams[j-1], streams[j]
		}
	}
}

func kErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func providerErr(err error) error {
	if ke, ok := err.(*kinesisstore.KinesisError); ok {
		return model.NewProviderError(ke.Code, ke.Message, ke.Status)
	}
	return model.NewProviderError("InternalFailure", err.Error(), 500)
}
