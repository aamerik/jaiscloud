// Package athena implements the Athena query provider.
package athena

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtQueryExec  = "athena_query_execution"
	rtWorkGroup  = "athena_workgroup"
	rtAthenaTags = "athena_tags"
)

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Query execution
		"Athena.StartQueryExecution":     p.StartQueryExecution,
		"Athena.GetQueryExecution":       p.GetQueryExecution,
		"Athena.GetQueryResults":         p.GetQueryResults,
		"Athena.StopQueryExecution":      p.StopQueryExecution,
		"Athena.ListQueryExecutions":     p.ListQueryExecutions,
		"Athena.BatchGetQueryExecution":  p.BatchGetQueryExecution,
		// WorkGroups
		"Athena.CreateWorkGroup":         p.CreateWorkGroup,
		"Athena.GetWorkGroup":            p.GetWorkGroup,
		"Athena.UpdateWorkGroup":         p.UpdateWorkGroup,
		"Athena.DeleteWorkGroup":         p.DeleteWorkGroup,
		"Athena.ListWorkGroups":          p.ListWorkGroups,
		// Tagging
		"Athena.TagResource":             p.TagResource,
		"Athena.UntagResource":           p.UntagResource,
		"Athena.ListTagsForResource":     p.ListTagsForResource,
	}
}

type queryExecution struct {
	QueryExecutionID string    `json:"QueryExecutionId"`
	Query            string    `json:"Query"`
	WorkGroup        string    `json:"WorkGroup"`
	State            string    `json:"State"`
	SubmissionTime   time.Time `json:"SubmissionDateTime"`
	CompletionTime   time.Time `json:"CompletionDateTime"`
}

type workGroup struct {
	Name         string    `json:"Name"`
	State        string    `json:"State"`
	Description  string    `json:"Description"`
	CreationTime time.Time `json:"CreationTime"`
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newQueryID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func athErr(code, msg string) error {
	return model.NewProviderError(code, msg, http.StatusBadRequest)
}

func execToWire(q queryExecution) map[string]any {
	return map[string]any{
		"QueryExecutionId": q.QueryExecutionID,
		"Query":            q.Query,
		"WorkGroup":        q.WorkGroup,
		"Status": map[string]any{
			"State":              q.State,
			"SubmissionDateTime": q.SubmissionTime.Unix(),
			"CompletionDateTime": q.CompletionTime.Unix(),
		},
		"Statistics": map[string]any{
			"EngineExecutionTimeInMillis": 42,
			"DataScannedInBytes":          0,
			"TotalExecutionTimeInMillis":  45,
		},
	}
}

func wgToWire(wg workGroup) map[string]any {
	return map[string]any{
		"Name":         wg.Name,
		"State":        wg.State,
		"Description":  wg.Description,
		"CreationTime": wg.CreationTime.Unix(),
	}
}

func (p *Provider) StartQueryExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	query := str(nr.Params, "QueryString")
	if query == "" {
		return nil, athErr("InvalidRequestException", "QueryString is required")
	}
	wg := str(nr.Params, "WorkGroup")
	if wg == "" {
		wg = "primary"
	}
	now := time.Now().UTC()
	qid := newQueryID()
	q := queryExecution{
		QueryExecutionID: qid,
		Query:            query,
		WorkGroup:        wg,
		State:            "SUCCEEDED",
		SubmissionTime:   now,
		CompletionTime:   now,
	}
	data, _ := json.Marshal(q)
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtQueryExec, ID: qid, Data: data})
	return provider.OK(map[string]any{"QueryExecutionId": qid}), nil
}

func (p *Provider) GetQueryExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	qid := str(nr.Params, "QueryExecutionId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtQueryExec, qid)
	if err != nil {
		return nil, athErr("InvalidRequestException", "Query execution not found: "+qid)
	}
	var q queryExecution
	_ = json.Unmarshal(e.Data, &q)
	return provider.OK(map[string]any{"QueryExecution": execToWire(q)}), nil
}

func (p *Provider) GetQueryResults(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	qid := str(nr.Params, "QueryExecutionId")
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtQueryExec, qid); err != nil {
		return nil, athErr("InvalidRequestException", "Query execution not found: "+qid)
	}
	return provider.OK(map[string]any{
		"ResultSet": map[string]any{
			"Rows":             []any{},
			"ResultSetMetadata": map[string]any{"ColumnInfo": []any{}},
		},
	}), nil
}

func (p *Provider) StopQueryExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	qid := str(nr.Params, "QueryExecutionId")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtQueryExec, qid)
	if err != nil {
		return nil, athErr("InvalidRequestException", "Query execution not found: "+qid)
	}
	var q queryExecution
	_ = json.Unmarshal(e.Data, &q)
	if q.State != "SUCCEEDED" && q.State != "FAILED" && q.State != "CANCELLED" {
		q.State = "CANCELLED"
		data, _ := json.Marshal(q)
		_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtQueryExec, ID: qid, Data: data})
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListQueryExecutions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	wg := str(nr.Params, "WorkGroup")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtQueryExec, "")
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		var q queryExecution
		if json.Unmarshal(e.Data, &q) == nil {
			if wg == "" || q.WorkGroup == wg {
				ids = append(ids, q.QueryExecutionID)
			}
		}
	}
	return provider.OK(map[string]any{"QueryExecutionIds": ids}), nil
}

func (p *Provider) BatchGetQueryExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ids, _ := nr.Params["QueryExecutionIds"].([]any)
	var execs []map[string]any
	var unprocessed []string
	for _, id := range ids {
		qid, ok := id.(string)
		if !ok {
			continue
		}
		e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtQueryExec, qid)
		if err != nil {
			unprocessed = append(unprocessed, qid)
			continue
		}
		var q queryExecution
		if json.Unmarshal(e.Data, &q) == nil {
			execs = append(execs, execToWire(q))
		}
	}
	if execs == nil {
		execs = []map[string]any{}
	}
	if unprocessed == nil {
		unprocessed = []string{}
	}
	return provider.OK(map[string]any{
		"QueryExecutions":              execs,
		"UnprocessedQueryExecutionIds": unprocessed,
	}), nil
}

func (p *Provider) CreateWorkGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "Name")
	if name == "" {
		return nil, athErr("InvalidRequestException", "Name is required")
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtWorkGroup, name); err == nil {
		return nil, athErr("InvalidRequestException", "WorkGroup "+name+" already exists")
	}
	wg := workGroup{
		Name:         name,
		State:        "ENABLED",
		Description:  str(nr.Params, "Description"),
		CreationTime: time.Now().UTC(),
	}
	data, _ := json.Marshal(wg)
	_ = p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtWorkGroup, ID: name, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) GetWorkGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "WorkGroup")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtWorkGroup, name)
	if err != nil {
		return nil, athErr("InvalidRequestException", "WorkGroup not found: "+name)
	}
	var wg workGroup
	_ = json.Unmarshal(e.Data, &wg)
	return provider.OK(map[string]any{"WorkGroup": wgToWire(wg)}), nil
}

func (p *Provider) UpdateWorkGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "WorkGroup")
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtWorkGroup, name)
	if err != nil {
		return nil, athErr("InvalidRequestException", "WorkGroup not found: "+name)
	}
	var wg workGroup
	_ = json.Unmarshal(e.Data, &wg)
	if v := str(nr.Params, "Description"); v != "" {
		wg.Description = v
	}
	if v := str(nr.Params, "State"); v != "" {
		wg.State = v
	}
	data, _ := json.Marshal(wg)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: rtWorkGroup, ID: name, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DeleteWorkGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := str(nr.Params, "WorkGroup")
	if name == "primary" {
		return nil, athErr("InvalidRequestException", "Cannot delete the primary workgroup")
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtWorkGroup, name); err != nil {
		return nil, athErr("InvalidRequestException", "WorkGroup not found: "+name)
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, rtWorkGroup, name)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListWorkGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, rtWorkGroup, "")
	wgs := make([]map[string]any, 0, len(entries)+1)
	// Always include primary
	wgs = append(wgs, map[string]any{"Name": "primary", "State": "ENABLED"})
	for _, e := range entries {
		var wg workGroup
		if json.Unmarshal(e.Data, &wg) == nil && wg.Name != "primary" {
			wgs = append(wgs, wgToWire(wg))
		}
	}
	return provider.OK(map[string]any{"WorkGroups": wgs}), nil
}

func (p *Provider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "ResourceARN")
	tags := loadAthenaTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	if raw, ok := nr.Params["Tags"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				v, _ := m["Value"].(string)
				tags[k] = v
			}
		}
	}
	saveAthenaTags(ctx, p.resources, nr.AccountID, nr.Region, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "ResourceARN")
	tags := loadAthenaTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				delete(tags, s)
			}
		}
	}
	saveAthenaTags(ctx, p.resources, nr.AccountID, nr.Region, arn, tags)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "ResourceARN")
	tags := loadAthenaTags(ctx, p.resources, nr.AccountID, nr.Region, arn)
	list := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": list}), nil
}

func loadAthenaTags(ctx context.Context, res store.ResourceStore, account, region, arn string) map[string]string {
	e, err := res.Get(ctx, account, region, rtAthenaTags, arn)
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	_ = json.Unmarshal(e.Data, &m)
	return m
}

func saveAthenaTags(ctx context.Context, res store.ResourceStore, account, region, arn string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: rtAthenaTags, ID: arn, Data: data}
	if err := res.Create(ctx, account, region, entry); err == store.ErrAlreadyExists {
		res.Update(ctx, account, region, entry)
	}
}

// Silence unused import warning
var _ = fmt.Sprintf
var _ = strings.HasPrefix
var _ = randHex
