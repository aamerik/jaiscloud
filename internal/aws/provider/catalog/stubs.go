// Package catalog — Glue stub implementations for Connections, Resource Policy,
// and Partition Indexes. Full in-memory CRUD, no enforcement.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ─── Resource types ───────────────────────────────────────────────────────────

const (
	rtConnection     = "glue_connection"
	rtResourcePolicy = "glue_resource_policy"
	rtPartitionIndex = "glue_partition_index"
)

// ─── Connections ──────────────────────────────────────────────────────────────

func connectionID(name string) string { return "conn/" + strings.ToLower(name) }

func (p *GlueProvider) CreateConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	inp, _ := nr.Params["ConnectionInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "ConnectionInput is required", HTTPStatus: http.StatusBadRequest}
	}
	name, _ := inp["Name"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "ConnectionInput.Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtConnection, connectionID(name)); err == nil {
		return nil, &model.ProviderError{Code: "AlreadyExists", Message: fmt.Sprintf("Connection %s already exists", name), HTTPStatus: http.StatusBadRequest}
	}
	data, _ := json.Marshal(inp)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtConnection, ID: connectionID(name), Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtConnection, connectionID(name))
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFound", Message: fmt.Sprintf("Connection %s not found", name), HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return nil, err
	}
	var conn map[string]any
	json.Unmarshal(e.Data, &conn)
	return provider.OK(map[string]any{"Connection": conn}), nil
}

func (p *GlueProvider) GetConnections(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtConnection, "conn/")
	if err != nil {
		return nil, err
	}
	conns := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var conn map[string]any
		if json.Unmarshal(e.Data, &conn) == nil {
			conns = append(conns, conn)
		}
	}
	return provider.OK(map[string]any{"ConnectionList": conns}), nil
}

func (p *GlueProvider) UpdateConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtConnection, connectionID(name)); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFound", Message: fmt.Sprintf("Connection %s not found", name), HTTPStatus: http.StatusBadRequest}
	}
	inp, _ := nr.Params["ConnectionInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "ConnectionInput is required", HTTPStatus: http.StatusBadRequest}
	}
	data, _ := json.Marshal(inp)
	if err := p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtConnection, ID: connectionID(name), Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) DeleteConnection(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "ConnectionName")
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtConnection, connectionID(name)); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFound", Message: fmt.Sprintf("Connection %s not found", name), HTTPStatus: http.StatusBadRequest}
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtConnection, connectionID(name)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Resource Policy ──────────────────────────────────────────────────────────

const resourcePolicyID = "singleton"

func (p *GlueProvider) PutResourcePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	policyJSON := strParam(nr.Params, "PolicyInJson")
	data, _ := json.Marshal(map[string]any{"PolicyInJson": policyJSON})
	entry := store.ResourceEntry{Type: rtResourcePolicy, ID: resourcePolicyID, Data: data}
	if err := p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetResourcePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtResourcePolicy, resourcePolicyID)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NotFound", Message: "No resource policy found", HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return nil, err
	}
	var stored map[string]any
	json.Unmarshal(e.Data, &stored)
	return provider.OK(stored), nil
}

func (p *GlueProvider) DeleteResourcePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtResourcePolicy, resourcePolicyID); err != nil && err != store.ErrNotFound {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Partition Indexes ────────────────────────────────────────────────────────

func partitionIndexID(db, table, indexName string) string {
	return strings.ToLower(db) + "/" + strings.ToLower(table) + "/" + strings.ToLower(indexName)
}

func partitionIndexPrefix(db, table string) string {
	return strings.ToLower(db) + "/" + strings.ToLower(table) + "/"
}

func (p *GlueProvider) CreatePartitionIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db := strParam(nr.Params, "DatabaseName")
	table := strParam(nr.Params, "TableName")
	inp, _ := nr.Params["PartitionIndex"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "PartitionIndex is required", HTTPStatus: http.StatusBadRequest}
	}
	indexName, _ := inp["IndexName"].(string)
	if indexName == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "PartitionIndex.IndexName is required", HTTPStatus: http.StatusBadRequest}
	}
	id := partitionIndexID(db, table, indexName)
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtPartitionIndex, id); err == nil {
		return nil, &model.ProviderError{Code: "AlreadyExists", Message: fmt.Sprintf("Partition index %s already exists", indexName), HTTPStatus: http.StatusBadRequest}
	}
	payload := map[string]any{
		"DatabaseName":   db,
		"TableName":      table,
		"PartitionIndex": inp,
		"IndexStatus":    "ACTIVE",
	}
	data, _ := json.Marshal(payload)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtPartitionIndex, ID: id, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetPartitionIndexes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db := strParam(nr.Params, "DatabaseName")
	table := strParam(nr.Params, "TableName")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtPartitionIndex, partitionIndexPrefix(db, table))
	if err != nil {
		return nil, err
	}
	indexes := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var idx map[string]any
		if json.Unmarshal(e.Data, &idx) == nil {
			indexes = append(indexes, idx)
		}
	}
	return provider.OK(map[string]any{"PartitionIndexDescriptorList": indexes}), nil
}

func (p *GlueProvider) DeletePartitionIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db := strParam(nr.Params, "DatabaseName")
	table := strParam(nr.Params, "TableName")
	indexName := strParam(nr.Params, "IndexName")
	id := partitionIndexID(db, table, indexName)
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPartitionIndex, id); err != nil && err != store.ErrNotFound {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}
