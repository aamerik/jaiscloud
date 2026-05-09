// Package esm implements Lambda Event Source Mappings (ESM).
// It provides CRUD operations for ESMs and background pollers for SQS and
// DynamoDB Streams that automatically invoke Lambda functions when events arrive.
package esm

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	"log/slog"

	"github.com/google/uuid"
)

const esmResourceType = "lambda_esm"

// FunctionInvoker is the narrow interface ESM uses to invoke Lambda functions.
type FunctionInvoker interface {
	InvokeInternal(ctx context.Context, functionName string, payload []byte) ([]byte, error)
}

// esmPoller tracks a running poller goroutine for a single ESM.
type esmPoller struct {
	uuid   string
	ctx    context.Context
	cancel context.CancelFunc
}

// Provider handles ESM CRUD operations and manages background pollers.
type Provider struct {
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	logger       *slog.Logger
	resources    store.ResourceStore
	invoker      FunctionInvoker
	queueAPI     QueueInternalAPI
	streamStore  StreamStoreAPI
	esmMu        sync.Mutex
	esmPollers   map[string]*esmPoller
}

// New constructs an ESM Provider.
func New(
	ctx context.Context,
	resources store.ResourceStore,
	invoker FunctionInvoker,
	qp QueueInternalAPI,
	ss StreamStoreAPI,
	logger *slog.Logger,
) *Provider {
	pCtx, cancel := context.WithCancel(ctx)
	if logger == nil {
		logger = slog.Default()
	}
	return &Provider{
		ctx:         pCtx,
		cancel:      cancel,
		logger:      logger,
		resources:   resources,
		invoker:     invoker,
		queueAPI:    qp,
		streamStore: ss,
		esmPollers:  make(map[string]*esmPoller),
	}
}

// Routes returns all ESM handler registrations under the "Function" prefix.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Function.CreateEventSourceMapping": p.handleCreateESM,
		"Function.GetEventSourceMapping":    p.handleGetESM,
		"Function.ListEventSourceMappings":  p.handleListESMs,
		"Function.UpdateEventSourceMapping": p.handleUpdateESM,
		"Function.DeleteEventSourceMapping": p.handleDeleteESM,
	}
}

// Shutdown stops all pollers and waits for them to finish.
func (p *Provider) Shutdown(ctx context.Context) {
	p.cancel()
	p.wg.Wait()
}

// RehydratePollers starts pollers for all enabled ESMs on startup.
func (p *Provider) RehydratePollers(ctx context.Context) {
	entries, err := p.resources.List(ctx, esmResourceType, "")
	if err != nil {
		p.logger.Warn("esm: failed to list ESMs for rehydration", "err", err)
		return
	}
	for _, entry := range entries {
		var esm EventSourceMapping
		if err := json.Unmarshal(entry.Data, &esm); err != nil {
			continue
		}
		// Restore non-serialized fields from ESM data
		esm = p.restoreTransientFields(esm)
		if esm.State == ESMStateEnabled {
			p.startPollerForMapping(esm)
		}
	}
}

// ─── CRUD handlers ────────────────────────────────────────────────────────────

func (p *Provider) handleCreateESM(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	functionName := strParam(nr.Params, "FunctionName")
	if functionName == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "FunctionName is required", 400)
	}
	eventSourceArn := strParam(nr.Params, "EventSourceArn")
	if eventSourceArn == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "EventSourceArn is required", 400)
	}

	// Detect source type and resolve names from ARN
	sourceType, queueName, tableName, err := resolveEventSource(eventSourceArn)
	if err != nil {
		return nil, model.NewProviderError("InvalidParameterValueException", err.Error(), 400)
	}

	// Validate function exists
	if _, err := p.resources.Get(ctx, "lambda_functions", functionName); err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException",
			"Function not found: "+functionName, 404)
	}

	// Check for duplicate: same function + same event source
	if p.esmDuplicateExists(ctx, functionName, eventSourceArn) {
		return nil, model.NewProviderError("ResourceConflictException",
			"An event source mapping with the same event source and function is already in progress or exists", 409)
	}

	batchSize := intParamOrDefault(nr.Params, "BatchSize", defaultBatchSize(sourceType))
	maxBatchingWindow := intParamOrDefault(nr.Params, "MaximumBatchingWindowInSeconds", 0)
	maxRetry := intParamOrDefault(nr.Params, "MaximumRetryAttempts", -1)
	bisect := boolParamOrDefault(nr.Params, "BisectBatchOnFunctionError", false)
	enabled := true
	if v, ok := nr.Params["Enabled"]; ok {
		if b, ok := v.(bool); ok {
			enabled = b
		}
	}

	// Resolve function ARN
	functionArn := functionName
	if nr.ResourceID != nil {
		functionArn = nr.ResourceID(model.RTLambdaFunction, functionName)
	}

	now := time.Now()
	if nr.Clock != nil {
		now = nr.Clock.Now()
	}

	id := generateUUID()
	state := ESMStateEnabled
	if !enabled {
		state = ESMStateDisabled
	}

	region := nr.Region
	cloud := string(nr.Cloud)
	if cloud == "" {
		cloud = "aws"
	}

	esm := EventSourceMapping{
		UUID:                           id,
		FunctionName:                   functionName,
		FunctionArn:                    functionArn,
		EventSourceArn:                 eventSourceArn,
		BatchSize:                      batchSize,
		MaximumBatchingWindowInSeconds: maxBatchingWindow,
		Enabled:                        enabled,
		State:                          state,
		StateTransitionReason:          "USER_INITIATED",
		LastModified:                   now,
		LastProcessingResult:           "No records processed",
		SourceType:                     sourceType,
		MaximumRetryAttempts:           maxRetry,
		BisectBatchOnFunctionError:     bisect,
		Region:                         region,
		Cloud:                          cloud,
		QueueName:                      queueName,
		TableName:                      tableName,
	}

	if err := p.persistESM(ctx, esm); err != nil {
		return nil, model.NewProviderError("ServiceException", "failed to save ESM: "+err.Error(), 500)
	}

	if enabled {
		p.startPollerForMapping(esm)
	}

	return &model.ProviderResponse{HTTPStatus: 202, Data: p.esmToWireJSON(esm)}, nil
}

func (p *Provider) handleGetESM(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "_esm_uuid")
	if id == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "ESM UUID is required", 400)
	}
	esm, err := p.loadESM(ctx, id)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "event source mapping not found: "+id)
	}
	return provider.OK(p.esmToWireJSON(esm)), nil
}

func (p *Provider) handleListESMs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, esmResourceType, "")
	if err != nil {
		return nil, err
	}

	filterFunctionName := strParam(nr.Params, "FunctionName")
	filterEventSourceArn := strParam(nr.Params, "EventSourceArn")

	var mappings []map[string]any
	for _, entry := range entries {
		var esm EventSourceMapping
		if err := json.Unmarshal(entry.Data, &esm); err != nil {
			continue
		}
		if filterFunctionName != "" && esm.FunctionName != filterFunctionName {
			continue
		}
		if filterEventSourceArn != "" && esm.EventSourceArn != filterEventSourceArn {
			continue
		}
		mappings = append(mappings, p.esmToWireJSON(esm))
	}

	if mappings == nil {
		mappings = []map[string]any{}
	}
	return provider.OK(map[string]any{"EventSourceMappings": mappings}), nil
}

func (p *Provider) handleUpdateESM(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "_esm_uuid")
	if id == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "ESM UUID is required", 400)
	}
	esm, err := p.loadESM(ctx, id)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "event source mapping not found: "+id)
	}

	// Apply updates
	if v, ok := nr.Params["BatchSize"]; ok {
		esm.BatchSize = intFromAny(v, esm.BatchSize)
	}
	if v, ok := nr.Params["MaximumBatchingWindowInSeconds"]; ok {
		esm.MaximumBatchingWindowInSeconds = intFromAny(v, esm.MaximumBatchingWindowInSeconds)
	}
	if v, ok := nr.Params["MaximumRetryAttempts"]; ok {
		esm.MaximumRetryAttempts = intFromAny(v, esm.MaximumRetryAttempts)
	}
	if v, ok := nr.Params["BisectBatchOnFunctionError"]; ok {
		if b, ok := v.(bool); ok {
			esm.BisectBatchOnFunctionError = b
		}
	}

	wasEnabled := esm.Enabled
	if v, ok := nr.Params["Enabled"]; ok {
		if b, ok := v.(bool); ok {
			esm.Enabled = b
		}
	}

	now := time.Now()
	if nr.Clock != nil {
		now = nr.Clock.Now()
	}
	esm.LastModified = now

	// Update state based on enabled flag
	if esm.Enabled && !wasEnabled {
		esm.State = ESMStateEnabled
		esm.StateTransitionReason = "USER_INITIATED"
	} else if !esm.Enabled && wasEnabled {
		esm.State = ESMStateDisabled
		esm.StateTransitionReason = "USER_INITIATED"
	}

	if err := p.persistESM(ctx, esm); err != nil {
		return nil, model.NewProviderError("ServiceException", "failed to update ESM: "+err.Error(), 500)
	}

	// Manage poller lifecycle
	if esm.Enabled && !wasEnabled {
		p.startPollerForMapping(esm)
	} else if !esm.Enabled && wasEnabled {
		p.stopPollerForMapping(id)
	}

	return provider.OK(p.esmToWireJSON(esm)), nil
}

func (p *Provider) handleDeleteESM(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "_esm_uuid")
	if id == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "ESM UUID is required", 400)
	}
	esm, err := p.loadESM(ctx, id)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "event source mapping not found: "+id)
	}

	// Stop poller before deletion
	p.stopPollerForMapping(id)

	if err := p.resources.Delete(ctx, esmResourceType, id); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "event source mapping not found: "+id)
	}

	esm.State = ESMStateDeleting
	return provider.OK(p.esmToWireJSON(esm)), nil
}

// ─── Helper methods ───────────────────────────────────────────────────────────

func (p *Provider) loadESM(ctx context.Context, id string) (EventSourceMapping, error) {
	entry, err := p.resources.Get(ctx, esmResourceType, id)
	if err != nil {
		return EventSourceMapping{}, err
	}
	var esm EventSourceMapping
	if err := json.Unmarshal(entry.Data, &esm); err != nil {
		return EventSourceMapping{}, err
	}
	return p.restoreTransientFields(esm), nil
}

func (p *Provider) persistESM(ctx context.Context, esm EventSourceMapping) error {
	data, err := json.Marshal(esm)
	if err != nil {
		return err
	}
	entry := store.ResourceEntry{
		Type:      esmResourceType,
		ID:        esm.UUID,
		Data:      data,
		UpdatedAt: time.Now(),
	}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			return p.resources.Update(ctx, entry)
		}
		return err
	}
	return nil
}

func (p *Provider) esmDuplicateExists(ctx context.Context, functionName, eventSourceArn string) bool {
	entries, err := p.resources.List(ctx, esmResourceType, "")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		var esm EventSourceMapping
		if json.Unmarshal(entry.Data, &esm) != nil {
			continue
		}
		if esm.FunctionName == functionName && esm.EventSourceArn == eventSourceArn {
			return true
		}
	}
	return false
}

func (p *Provider) esmToWireJSON(esm EventSourceMapping) map[string]any {
	return map[string]any{
		"UUID":                           esm.UUID,
		"FunctionArn":                    esm.FunctionArn,
		"EventSourceArn":                 esm.EventSourceArn,
		"BatchSize":                      esm.BatchSize,
		"MaximumBatchingWindowInSeconds": esm.MaximumBatchingWindowInSeconds,
		"State":                          esm.State,
		"StateTransitionReason":          esm.StateTransitionReason,
		"LastModified":                   esm.LastModified.Unix(),
		"LastProcessingResult":           esm.LastProcessingResult,
		"MaximumRetryAttempts":           esm.MaximumRetryAttempts,
		"BisectBatchOnFunctionError":     esm.BisectBatchOnFunctionError,
	}
}

// restoreTransientFields repopulates the non-serialized fields (QueueName, TableName)
// from the serialized EventSourceArn and SourceType.
func (p *Provider) restoreTransientFields(esm EventSourceMapping) EventSourceMapping {
	if esm.SourceType == "" {
		sourceType, queueName, tableName, err := resolveEventSource(esm.EventSourceArn)
		if err == nil {
			esm.SourceType = sourceType
			esm.QueueName = queueName
			esm.TableName = tableName
		}
	} else {
		switch esm.SourceType {
		case ESMSourceSQS:
			esm.QueueName = queueNameFromArn(esm.EventSourceArn)
		case ESMSourceDynamoDBStreams:
			esm.TableName = tableNameFromStreamArn(esm.EventSourceArn)
		}
	}
	return esm
}

func (p *Provider) startPollerForMapping(esm EventSourceMapping) {
	p.esmMu.Lock()
	defer p.esmMu.Unlock()

	// Cancel any existing poller for this UUID
	if existing, ok := p.esmPollers[esm.UUID]; ok {
		existing.cancel()
		delete(p.esmPollers, esm.UUID)
	}

	pollCtx, pollCancel := context.WithCancel(p.ctx)
	poller := &esmPoller{
		uuid:   esm.UUID,
		ctx:    pollCtx,
		cancel: pollCancel,
	}
	p.esmPollers[esm.UUID] = poller

	p.wg.Add(1)
	switch esm.SourceType {
	case ESMSourceSQS:
		go func() {
			defer p.wg.Done()
			p.runSQSPoller(poller, esm)
		}()
	case ESMSourceDynamoDBStreams:
		go func() {
			defer p.wg.Done()
			p.runDynamoDBStreamsPoller(poller, esm)
		}()
	default:
		p.wg.Done()
		p.logger.Warn("esm: unknown source type, poller not started", "uuid", esm.UUID, "sourceType", esm.SourceType)
	}
}

func (p *Provider) stopPollerForMapping(uuid string) {
	p.cancelPollerForMapping(uuid)
}

func (p *Provider) cancelPollerForMapping(uuid string) {
	p.esmMu.Lock()
	defer p.esmMu.Unlock()
	if poller, ok := p.esmPollers[uuid]; ok {
		poller.cancel()
		delete(p.esmPollers, uuid)
	}
}

// ─── Helper functions ─────────────────────────────────────────────────────────

func generateUUID() string {
	return uuid.New().String()
}

func resolveEventSource(eventSourceArn string) (sourceType, queueName, tableName string, err error) {
	// arn:aws:sqs:region:account:queue-name
	// arn:aws:dynamodb:region:account:table/TableName/stream/timestamp
	parts := strings.Split(eventSourceArn, ":")
	if len(parts) < 6 {
		return "", "", "", model.NewProviderError("InvalidParameterValueException",
			"invalid EventSourceArn: "+eventSourceArn, 400)
	}

	service := parts[2] // "sqs" or "dynamodb"

	switch service {
	case "sqs":
		return ESMSourceSQS, parts[5], "", nil
	case "dynamodb":
		// parts[5] = "table/MyTable/stream/2021-01-01T00:00:00.000"
		resourcePath := parts[5]
		tableName = tableNameFromResourcePath(resourcePath)
		if tableName == "" {
			return "", "", "", model.NewProviderError("InvalidParameterValueException",
				"cannot parse table name from DynamoDB stream ARN: "+eventSourceArn, 400)
		}
		return ESMSourceDynamoDBStreams, "", tableName, nil
	default:
		return "", "", "", model.NewProviderError("InvalidParameterValueException",
			"unsupported event source service: "+service, 400)
	}
}

func tableNameFromResourcePath(resourcePath string) string {
	// resourcePath: "table/MyTable/stream/timestamp" or "table/MyTable"
	segments := strings.Split(resourcePath, "/")
	if len(segments) >= 2 && segments[0] == "table" {
		return segments[1]
	}
	return ""
}

func tableNameFromStreamArn(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return tableNameFromResourcePath(parts[5])
}

func queueNameFromArn(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return parts[5]
}

func defaultBatchSize(sourceType string) int {
	switch sourceType {
	case ESMSourceDynamoDBStreams:
		return 100
	default:
		return 10
	}
}

func intParamOrDefault(params map[string]any, key string, def int) int {
	v, ok := params[key]
	if !ok {
		return def
	}
	return intFromAny(v, def)
}

func intFromAny(v any, def int) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return def
}

func boolParamOrDefault(params map[string]any, key string, def bool) bool {
	v, ok := params[key]
	if !ok {
		return def
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return def
}

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
