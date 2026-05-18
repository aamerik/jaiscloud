// Package catalog implements the AWS Glue Data Catalog provider.
package catalog

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// GlueProvider handles all Glue Data Catalog operations.
type GlueProvider struct {
	resources      store.ResourceStore
	objectProvider ObjectProviderAPI
	sparkExecutor  SparkExecutorAPI
}

func New(resources store.ResourceStore) *GlueProvider {
	return &GlueProvider{resources: resources}
}

func (p *GlueProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Databases
		"Glue.CreateDatabase": p.CreateDatabase,
		"Glue.GetDatabase":    p.GetDatabase,
		"Glue.GetDatabases":   p.GetDatabases,
		"Glue.UpdateDatabase": p.UpdateDatabase,
		"Glue.DeleteDatabase": p.DeleteDatabase,
		// Tables
		"Glue.CreateTable": p.CreateTable,
		"Glue.GetTable":    p.GetTable,
		"Glue.GetTables":   p.GetTables,
		"Glue.UpdateTable": p.UpdateTable,
		"Glue.DeleteTable": p.DeleteTable,
		// Partitions
		"Glue.CreatePartition":      p.CreatePartition,
		"Glue.GetPartition":         p.GetPartition,
		"Glue.GetPartitions":        p.GetPartitions,
		"Glue.BatchCreatePartition": p.BatchCreatePartition,
		"Glue.BatchDeletePartition": p.BatchDeletePartition,
		"Glue.UpdatePartition":      p.UpdatePartition,
		// Tagging (13.12)
		"Glue.TagResource":   p.TagResource,
		"Glue.UntagResource": p.UntagResource,
		"Glue.GetTags":       p.GetTags,
		// Jobs (§3.9)
		"Glue.CreateJob":       p.CreateJob,
		"Glue.UpdateJob":       p.UpdateJob,
		"Glue.DeleteJob":       p.DeleteJob,
		"Glue.GetJob":          p.GetJob,
		"Glue.GetJobs":         p.GetJobs,
		"Glue.StartJobRun":     p.StartJobRun,
		"Glue.GetJobRun":       p.GetJobRun,
		"Glue.GetJobRuns":      p.GetJobRuns,
		"Glue.BatchStopJobRun": p.BatchStopJobRun,
		// Crawlers (§3.9)
		"Glue.CreateCrawler":      p.CreateCrawler,
		"Glue.UpdateCrawler":      p.UpdateCrawler,
		"Glue.DeleteCrawler":      p.DeleteCrawler,
		"Glue.GetCrawler":         p.GetCrawler,
		"Glue.GetCrawlers":        p.GetCrawlers,
		"Glue.StartCrawler":       p.StartCrawler,
		"Glue.StopCrawler":        p.StopCrawler,
		"Glue.GetCrawlerMetrics":  p.GetCrawlerMetrics,
		// Table Versions (§3.9)
		"Glue.GetTableVersion":        p.GetTableVersion,
		"Glue.GetTableVersions":       p.GetTableVersions,
		"Glue.DeleteTableVersions":    p.DeleteTableVersions,
		"Glue.BatchDeleteTableVersion": p.BatchDeleteTableVersion,
		// Connections (stub)
		"Glue.CreateConnection": p.CreateConnection,
		"Glue.GetConnection":    p.GetConnection,
		"Glue.GetConnections":   p.GetConnections,
		"Glue.UpdateConnection": p.UpdateConnection,
		"Glue.DeleteConnection": p.DeleteConnection,
		// Resource Policy (stub)
		"Glue.PutResourcePolicy":    p.PutResourcePolicy,
		"Glue.GetResourcePolicy":    p.GetResourcePolicy,
		"Glue.DeleteResourcePolicy": p.DeleteResourcePolicy,
		// Partition Indexes (stub)
		"Glue.CreatePartitionIndex":  p.CreatePartitionIndex,
		"Glue.GetPartitionIndexes":   p.GetPartitionIndexes,
		"Glue.DeletePartitionIndex":  p.DeletePartitionIndex,
	}
}

// ─── Resource types ───────────────────────────────────────────────────────────

const (
	rtDatabase  = "glue_database"
	rtTable     = "glue_table"
	rtPartition = "glue_partition"
	rtGlueTags  = "glue_tags"
)

// ─── Tagging (13.12) ──────────────────────────────────────────────────────────

func (p *GlueProvider) loadGlueTags(ctx context.Context, account, region, arn string) map[string]string {
	tags := map[string]string{}
	if e, err := p.resources.Get(ctx, account, region, rtGlueTags, arn); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	return tags
}

func (p *GlueProvider) saveGlueTags(ctx context.Context, account, region, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtGlueTags, ID: arn, Data: data}
	if err := p.resources.Create(ctx, account, region, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, account, region, entry)
		}
	}
}

func (p *GlueProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	tags := p.loadGlueTags(ctx, nr.AccountID, nr.Region, arn)
	if rawTags, ok := nr.Params["TagsToAdd"].(map[string]any); ok {
		for k, v := range rawTags {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
	}
	p.saveGlueTags(ctx, nr.AccountID, nr.Region, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	tags := p.loadGlueTags(ctx, nr.AccountID, nr.Region, arn)
	if keys, ok := nr.Params["TagsToRemove"].([]any); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				delete(tags, s)
			}
		}
	}
	p.saveGlueTags(ctx, nr.AccountID, nr.Region, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	if arn == "" {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	tags := p.loadGlueTags(ctx, nr.AccountID, nr.Region, arn)
	return provider.OK(map[string]any{"Tags": tags}), nil
}

func dbID(name string) string        { return strings.ToLower(name) }
func tableID(db, table string) string { return strings.ToLower(db) + "/" + strings.ToLower(table) }
func partitionID(db, table string, values []string) string {
	h := md5.New()
	fmt.Fprintf(h, "%s/%s/%s", strings.ToLower(db), strings.ToLower(table), strings.Join(values, "#"))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ─── Database metadata ────────────────────────────────────────────────────────

type glueDatabase struct {
	Name         string            `json:"Name"`
	OriginalName string            `json:"OriginalName,omitempty"` // preserves caller's casing
	Description  string            `json:"Description"`
	LocationUri  string            `json:"LocationUri,omitempty"`
	Parameters   map[string]string `json:"Parameters,omitempty"`
	CreateTime   time.Time         `json:"CreateTime"`
}

func (p *GlueProvider) saveDB(ctx context.Context, account, region string, db glueDatabase) error {
	data, _ := json.Marshal(db)
	entry := store.ResourceEntry{Type: rtDatabase, ID: dbID(db.Name), Data: data}
	err := p.resources.Create(ctx, account, region, entry)
	if err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, account, region, entry)
	}
	return err
}

func (p *GlueProvider) loadDB(ctx context.Context, account, region, name string) (glueDatabase, error) {
	e, err := p.resources.Get(ctx, account, region, rtDatabase, dbID(name))
	if err == store.ErrNotFound {
		return glueDatabase{}, &model.ProviderError{Code: "NotFound", Message: fmt.Sprintf("Database %s not found", name), HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return glueDatabase{}, err
	}
	var db glueDatabase
	json.Unmarshal(e.Data, &db)
	return db, nil
}

// ─── Table metadata ───────────────────────────────────────────────────────────

type glueTable struct {
	DatabaseName                   string            `json:"DatabaseName"`
	OriginalDatabaseName           string            `json:"OriginalDatabaseName,omitempty"` // preserves caller's casing
	Name                           string            `json:"Name"`
	OriginalName                   string            `json:"OriginalName,omitempty"` // preserves caller's casing
	Description                    string            `json:"Description,omitempty"`
	Owner                          string            `json:"Owner,omitempty"`
	TableType                      string            `json:"TableType,omitempty"`
	Parameters                     map[string]string `json:"Parameters,omitempty"`
	StorageDescriptor              map[string]any    `json:"StorageDescriptor,omitempty"`
	PartitionKeys                  []map[string]any  `json:"PartitionKeys,omitempty"`
	CreateTime                     time.Time         `json:"CreateTime"`
	UpdateTime                     time.Time         `json:"UpdateTime"`
	IsRegisteredWithLakeFormation  bool              `json:"IsRegisteredWithLakeFormation,omitempty"`
}

func (p *GlueProvider) saveTable(ctx context.Context, account, region string, t glueTable) error {
	data, _ := json.Marshal(t)
	entry := store.ResourceEntry{Type: rtTable, ID: tableID(t.DatabaseName, t.Name), Data: data}
	err := p.resources.Create(ctx, account, region, entry)
	if err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, account, region, entry)
	}
	return err
}

func (p *GlueProvider) loadTable(ctx context.Context, account, region, db, name string) (glueTable, error) {
	e, err := p.resources.Get(ctx, account, region, rtTable, tableID(db, name))
	if err == store.ErrNotFound {
		return glueTable{}, &model.ProviderError{Code: "NotFound", Message: fmt.Sprintf("Table %s not found in database %s", name, db), HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return glueTable{}, err
	}
	var t glueTable
	json.Unmarshal(e.Data, &t)
	return t, nil
}

// ─── Partition metadata ───────────────────────────────────────────────────────

type gluePartition struct {
	DatabaseName      string            `json:"DatabaseName"`
	TableName         string            `json:"TableName"`
	Values            []string          `json:"Values"`
	Parameters        map[string]string `json:"Parameters,omitempty"`
	StorageDescriptor map[string]any    `json:"StorageDescriptor,omitempty"`
	CreationTime      time.Time         `json:"CreationTime"`
	LastAccessTime    time.Time         `json:"LastAccessTime,omitempty"`
}

func (p *GlueProvider) savePartition(ctx context.Context, account, region string, part gluePartition) error {
	data, _ := json.Marshal(part)
	id := partitionID(part.DatabaseName, part.TableName, part.Values)
	entry := store.ResourceEntry{Type: rtPartition, ID: id, Data: data}
	err := p.resources.Create(ctx, account, region, entry)
	if err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, account, region, entry)
	}
	return err
}

func (p *GlueProvider) loadPartition(ctx context.Context, account, region, db, table string, values []string) (gluePartition, error) {
	id := partitionID(db, table, values)
	e, err := p.resources.Get(ctx, account, region, rtPartition, id)
	if err == store.ErrNotFound {
		return gluePartition{}, &model.ProviderError{Code: "NotFound", Message: fmt.Sprintf("Partition not found"), HTTPStatus: http.StatusBadRequest}
	}
	if err != nil {
		return gluePartition{}, err
	}
	var part gluePartition
	json.Unmarshal(e.Data, &part)
	return part, nil
}

// ─── Database operations ──────────────────────────────────────────────────────

func (p *GlueProvider) CreateDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	inp, _ := nr.Params["DatabaseInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "DatabaseInput is required", HTTPStatus: http.StatusBadRequest}
	}
	name, _ := inp["Name"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "DatabaseInput.Name is required", HTTPStatus: http.StatusBadRequest}
	}

	// Check duplicate
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtDatabase, dbID(name)); err == nil {
		return nil, &model.ProviderError{Code: "AlreadyExists", Message: fmt.Sprintf("Database %s already exists", name), HTTPStatus: http.StatusBadRequest}
	}

	db := glueDatabase{
		Name:         strings.ToLower(name), // canonical lookup key
		OriginalName: name,
		Description:  strParam(inp, "Description"),
		LocationUri:  strParam(inp, "LocationUri"),
		Parameters:   strMapParam(inp, "Parameters"),
		CreateTime:   time.Now(),
	}
	if err := p.saveDB(ctx, nr.AccountID, nr.Region, db); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	db, err := p.loadDB(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Database": dbToWire(db)}), nil
}

func (p *GlueProvider) GetDatabases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtDatabase, "")
	if err != nil {
		return nil, err
	}
	dbs := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var db glueDatabase
		json.Unmarshal(e.Data, &db)
		dbs = append(dbs, dbToWire(db))
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(dbs, maxResults, token, "GetDatabases")
	if pgErr != nil {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: pgErr.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data := map[string]any{"DatabaseList": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *GlueProvider) UpdateDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	inp, _ := nr.Params["DatabaseInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "DatabaseInput is required", HTTPStatus: http.StatusBadRequest}
	}
	db, err := p.loadDB(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	if d, ok := inp["Description"].(string); ok {
		db.Description = d
	}
	if l, ok := inp["LocationUri"].(string); ok {
		db.LocationUri = l
	}
	if params, ok := inp["Parameters"].(map[string]any); ok {
		if db.Parameters == nil {
			db.Parameters = map[string]string{}
		}
		for k, v := range params {
			if s, ok := v.(string); ok {
				db.Parameters[k] = s
			}
		}
	}
	if err := p.saveDB(ctx, nr.AccountID, nr.Region, db); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) DeleteDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if _, err := p.loadDB(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, err
	}
	// Cascade: delete all partitions then tables belonging to this database
	// Partitions use MD5 IDs — must full-scan and filter by DatabaseName
	partEntries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtPartition, "")
	for _, pe := range partEntries {
		var part gluePartition
		if json.Unmarshal(pe.Data, &part) == nil && strings.EqualFold(part.DatabaseName, name) {
			_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPartition, pe.ID)
		}
	}
	tablePrefix := tableID(name, "")
	tableEntries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtTable, tablePrefix)
	for _, te := range tableEntries {
		var t glueTable
		if json.Unmarshal(te.Data, &t) == nil && strings.EqualFold(t.DatabaseName, name) {
			_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTable, te.ID)
		}
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtDatabase, dbID(name)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Table operations ─────────────────────────────────────────────────────────

func (p *GlueProvider) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	inp, _ := nr.Params["TableInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "TableInput is required", HTTPStatus: http.StatusBadRequest}
	}
	name, _ := inp["Name"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "TableInput.Name is required", HTTPStatus: http.StatusBadRequest}
	}
	// Database must exist
	if _, err := p.loadDB(ctx, nr.AccountID, nr.Region, dbName); err != nil {
		return nil, err
	}
	// Check duplicate
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTable, tableID(dbName, name)); err == nil {
		return nil, &model.ProviderError{Code: "AlreadyExists", Message: fmt.Sprintf("Table %s already exists in %s", name, dbName), HTTPStatus: http.StatusBadRequest}
	}

	now := time.Now()
	t := glueTable{
		DatabaseName:         strings.ToLower(dbName), // canonical lookup key
		OriginalDatabaseName: dbName,
		Name:                 strings.ToLower(name), // canonical lookup key
		OriginalName:         name,
		Description:          strParam(inp, "Description"),
		Owner:                strParam(inp, "Owner"),
		TableType:            strParam(inp, "TableType"),
		Parameters:           strMapParam(inp, "Parameters"),
		StorageDescriptor:    anyMapParam(inp, "StorageDescriptor"),
		PartitionKeys:        anySliceParam(inp, "PartitionKeys"),
		CreateTime:           now,
		UpdateTime:           now,
	}
	if err := p.saveTable(ctx, nr.AccountID, nr.Region, t); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	name := strParam(nr.Params, "Name")
	t, err := p.loadTable(ctx, nr.AccountID, nr.Region, dbName, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Table": tableToWire(t)}), nil
}

func (p *GlueProvider) GetTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	prefix := tableID(dbName, "")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtTable, prefix)
	if err != nil {
		return nil, err
	}
	tables := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var t glueTable
		json.Unmarshal(e.Data, &t)
		if strings.EqualFold(t.DatabaseName, dbName) {
			tables = append(tables, tableToWire(t))
		}
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(tables, maxResults, token, "GetTables")
	if pgErr != nil {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: pgErr.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data := map[string]any{"TableList": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

// UpdateTable supports Iceberg metadata_location CAS:
// If the request includes ExpectedMetadataLocation, the current Parameters.metadata_location
// must match before the update is applied.
func (p *GlueProvider) UpdateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	inp, _ := nr.Params["TableInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "TableInput is required", HTTPStatus: http.StatusBadRequest}
	}
	name, _ := inp["Name"].(string)
	t, err := p.loadTable(ctx, nr.AccountID, nr.Region, dbName, name)
	if err != nil {
		return nil, err
	}

	// Write a version snapshot before applying changes
	p.WriteTableVersionWithAccount(ctx, nr.AccountID, nr.Region, t)

	// Iceberg CAS: check ExpectedMetadataLocation if provided
	if expected, ok := nr.Params["VersionId"].(string); ok && expected != "" {
		current := ""
		if t.Parameters != nil {
			current = t.Parameters["metadata_location"]
		}
		if current != expected {
			return nil, &model.ProviderError{
				Code:       "ConcurrentModificationException",
				Message:    fmt.Sprintf("metadata_location mismatch: expected %q, got %q", expected, current),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}

	// Apply updates
	if d, ok := inp["Description"].(string); ok {
		t.Description = d
	}
	if tt, ok := inp["TableType"].(string); ok {
		t.TableType = tt
	}
	if params, ok := inp["Parameters"].(map[string]any); ok {
		if t.Parameters == nil {
			t.Parameters = map[string]string{}
		}
		for k, v := range params {
			if s, ok := v.(string); ok {
				t.Parameters[k] = s
			}
		}
	}
	if sd, ok := inp["StorageDescriptor"].(map[string]any); ok {
		t.StorageDescriptor = sd
	}
	if pk, ok := inp["PartitionKeys"].([]any); ok {
		t.PartitionKeys = toAnyMapSlice(pk)
	}
	t.UpdateTime = time.Now()

	if err := p.saveTable(ctx, nr.AccountID, nr.Region, t); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	name := strParam(nr.Params, "Name")
	if _, err := p.loadTable(ctx, nr.AccountID, nr.Region, dbName, name); err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtTable, tableID(dbName, name)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Partition operations ─────────────────────────────────────────────────────

func (p *GlueProvider) CreatePartition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	tableName := strParam(nr.Params, "TableName")
	inp, _ := nr.Params["PartitionInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "PartitionInput is required", HTTPStatus: http.StatusBadRequest}
	}
	// Validate parent table exists
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtTable, tableID(dbName, tableName)); err != nil {
		return nil, &model.ProviderError{Code: "EntityNotFoundException", Message: fmt.Sprintf("Table %s not found in database %s", tableName, dbName), HTTPStatus: http.StatusBadRequest}
	}
	values := strSliceParam(inp, "Values")

	now := time.Now()
	part := gluePartition{
		DatabaseName:      dbName,
		TableName:         tableName,
		Values:            values,
		Parameters:        strMapParam(inp, "Parameters"),
		StorageDescriptor: anyMapParam(inp, "StorageDescriptor"),
		CreationTime:      now,
		LastAccessTime:    now,
	}
	if err := p.savePartition(ctx, nr.AccountID, nr.Region, part); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetPartition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	tableName := strParam(nr.Params, "TableName")
	values := strSliceParam(nr.Params, "PartitionValues")
	part, err := p.loadPartition(ctx, nr.AccountID, nr.Region, dbName, tableName, values)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Partition": partitionToWire(part)}), nil
}

func (p *GlueProvider) GetPartitions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	tableName := strParam(nr.Params, "TableName")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtPartition, "")
	if err != nil {
		return nil, err
	}
	parts := make([]map[string]any, 0)
	for _, e := range entries {
		var part gluePartition
		json.Unmarshal(e.Data, &part)
		if strings.EqualFold(part.DatabaseName, dbName) && strings.EqualFold(part.TableName, tableName) {
			parts = append(parts, partitionToWire(part))
		}
	}
	// Sort by values for determinism
	sort.Slice(parts, func(i, j int) bool {
		vi, _ := parts[i]["Values"].([]string)
		vj, _ := parts[j]["Values"].([]string)
		return strings.Join(vi, "#") < strings.Join(vj, "#")
	})
	return provider.OK(map[string]any{"Partitions": parts}), nil
}

func (p *GlueProvider) BatchCreatePartition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	tableName := strParam(nr.Params, "TableName")
	inputs, _ := nr.Params["PartitionInputList"].([]any)

	errors := []map[string]any{}
	for _, raw := range inputs {
		inp, _ := raw.(map[string]any)
		if inp == nil {
			continue
		}
		values := strSliceParam(inp, "Values")
		now := time.Now()
		part := gluePartition{
			DatabaseName:      dbName,
			TableName:         tableName,
			Values:            values,
			Parameters:        strMapParam(inp, "Parameters"),
			StorageDescriptor: anyMapParam(inp, "StorageDescriptor"),
			CreationTime:      now,
			LastAccessTime:    now,
		}
		if err := p.savePartition(ctx, nr.AccountID, nr.Region, part); err != nil {
			errors = append(errors, map[string]any{
				"PartitionValues": values,
				"ErrorDetail":     map[string]any{"ErrorCode": "AlreadyExistsException", "ErrorMessage": err.Error()},
			})
		}
	}
	return provider.OK(map[string]any{"Errors": errors}), nil
}

func (p *GlueProvider) BatchDeletePartition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	tableName := strParam(nr.Params, "TableName")
	toDelete, _ := nr.Params["PartitionsToDelete"].([]any)

	errors := []map[string]any{}
	for _, raw := range toDelete {
		inp, _ := raw.(map[string]any)
		if inp == nil {
			continue
		}
		values := strSliceParam(inp, "Values")
		id := partitionID(dbName, tableName, values)
		if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPartition, id); err != nil {
			errors = append(errors, map[string]any{
				"PartitionValues": values,
				"ErrorDetail":     map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": "Partition not found"},
			})
		}
	}
	return provider.OK(map[string]any{"Errors": errors}), nil
}

func (p *GlueProvider) UpdatePartition(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dbName := strParam(nr.Params, "DatabaseName")
	tableName := strParam(nr.Params, "TableName")
	oldValues := strSliceParam(nr.Params, "PartitionValueList")
	inp, _ := nr.Params["PartitionInput"].(map[string]any)
	if inp == nil {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "PartitionInput is required", HTTPStatus: http.StatusBadRequest}
	}
	// Load existing
	part, err := p.loadPartition(ctx, nr.AccountID, nr.Region, dbName, tableName, oldValues)
	if err != nil {
		return nil, err
	}
	// Apply updates
	if vals := strSliceParam(inp, "Values"); len(vals) > 0 {
		// Delete old and re-create with new values
		p.resources.Delete(ctx, nr.AccountID, nr.Region, rtPartition, partitionID(dbName, tableName, oldValues))
		part.Values = vals
	}
	if params, ok := inp["Parameters"].(map[string]any); ok {
		if part.Parameters == nil {
			part.Parameters = map[string]string{}
		}
		for k, v := range params {
			if s, ok := v.(string); ok {
				part.Parameters[k] = s
			}
		}
	}
	if sd, ok := inp["StorageDescriptor"].(map[string]any); ok {
		part.StorageDescriptor = sd
	}
	if err := p.savePartition(ctx, nr.AccountID, nr.Region, part); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Wire serialisation helpers ───────────────────────────────────────────────

func dbToWire(db glueDatabase) map[string]any {
	// Return the original casing if available; fall back to stored Name.
	displayName := db.OriginalName
	if displayName == "" {
		displayName = db.Name
	}
	return map[string]any{
		"Name":        displayName,
		"Description": db.Description,
		"LocationUri": db.LocationUri,
		"Parameters":  db.Parameters,
		"CreateTime":  float64(db.CreateTime.UnixNano()) / 1e9,
	}
}

func tableToWire(t glueTable) map[string]any {
	// Return the original casing if available; fall back to stored names.
	displayDB := t.OriginalDatabaseName
	if displayDB == "" {
		displayDB = t.DatabaseName
	}
	displayName := t.OriginalName
	if displayName == "" {
		displayName = t.Name
	}
	return map[string]any{
		"DatabaseName":      displayDB,
		"Name":              displayName,
		"Description":       t.Description,
		"Owner":             t.Owner,
		"TableType":         t.TableType,
		"Parameters":        t.Parameters,
		"StorageDescriptor": t.StorageDescriptor,
		"PartitionKeys":     t.PartitionKeys,
		"CreateTime":        float64(t.CreateTime.UnixNano()) / 1e9,
		"UpdateTime":        float64(t.UpdateTime.UnixNano()) / 1e9,
	}
}

func partitionToWire(part gluePartition) map[string]any {
	return map[string]any{
		"DatabaseName":      part.DatabaseName,
		"TableName":         part.TableName,
		"Values":            part.Values,
		"Parameters":        part.Parameters,
		"StorageDescriptor": part.StorageDescriptor,
		"CreationTime":      float64(part.CreationTime.UnixNano()) / 1e9,
		"LastAccessTime":    float64(part.LastAccessTime.UnixNano()) / 1e9,
	}
}

// ─── Parameter helpers ────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func strMapParam(params map[string]any, key string) map[string]string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			result[k] = s
		}
	}
	return result
}

func anyMapParam(params map[string]any, key string) map[string]any {
	v, ok := params[key]
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return m
}

func anySliceParam(params map[string]any, key string) []map[string]any {
	v, ok := params[key]
	if !ok {
		return nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	return toAnyMapSlice(raw)
}

func toAnyMapSlice(raw []any) []map[string]any {
	result := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			result = append(result, m)
		}
	}
	return result
}

func strSliceParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}
