// Package catalog — Glue Crawlers implementation.
package catalog

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ─── Resource types ───────────────────────────────────────────────────────────

const rtCrawler = "glue_crawler"

// ─── ObjectProviderAPI ────────────────────────────────────────────────────────

// ObjectProviderAPI is the minimal S3 surface needed by the crawler.
// Only wired when InternalListObjects/InternalGetObject exist on the object provider.
type ObjectProviderAPI interface {
	InternalListObjects(ctx context.Context, bucket, prefix string) ([]string, error)
	InternalGetObject(ctx context.Context, bucket, key string) ([]byte, error)
}

// ─── Types ────────────────────────────────────────────────────────────────────

type crawlerEntry struct {
	Name         string         `json:"Name"`
	Role         string         `json:"Role"`
	Targets      map[string]any `json:"Targets,omitempty"` // {S3Targets:[{Path}], ...}
	DatabaseName string         `json:"DatabaseName"`
	Schedule     string         `json:"Schedule,omitempty"`
	State        string         `json:"State"` // READY | RUNNING | STOPPING
	LastCrawl    *crawlStatus   `json:"LastCrawl,omitempty"`
	CreatedOn    time.Time      `json:"CreatedOn"`
	LastUpdated  time.Time      `json:"LastUpdated"`
}

type crawlStatus struct {
	Status       string    `json:"Status"` // SUCCEEDED | FAILED
	StartTime    time.Time `json:"StartTime"`
	EndTime      time.Time `json:"EndTime"`
	ErrorMessage string    `json:"ErrorMessage,omitempty"`
}

// ─── Provider field ───────────────────────────────────────────────────────────

// objectProviderMu guards reads/writes to objectProvider.
var objectProviderMu sync.RWMutex

// SetObjectProvider injects an optional S3 provider for crawler schema inference.
func (p *GlueProvider) SetObjectProvider(op ObjectProviderAPI) {
	objectProviderMu.Lock()
	defer objectProviderMu.Unlock()
	p.objectProvider = op
}

func (p *GlueProvider) getObjectProvider() ObjectProviderAPI {
	objectProviderMu.RLock()
	defer objectProviderMu.RUnlock()
	return p.objectProvider
}

// ─── ID helpers ───────────────────────────────────────────────────────────────

func crawlerID(name string) string { return "crawler/" + name }

// ─── Persistence helpers ──────────────────────────────────────────────────────

func (p *GlueProvider) saveCrawler(ctx context.Context, account, region string, c crawlerEntry) error {
	data, _ := json.Marshal(c)
	entry := store.ResourceEntry{Type: rtCrawler, ID: crawlerID(c.Name), Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *GlueProvider) loadCrawler(ctx context.Context, account, region, name string) (crawlerEntry, error) {
	e, err := p.resources.Get(ctx, account, region, rtCrawler, crawlerID(name))
	if err == store.ErrNotFound {
		return crawlerEntry{}, &model.ProviderError{
			Code:       "NotFound",
			Message:    fmt.Sprintf("Crawler %s not found", name),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err != nil {
		return crawlerEntry{}, err
	}
	var c crawlerEntry
	json.Unmarshal(e.Data, &c)
	return c, nil
}

// ─── Crawler operations ───────────────────────────────────────────────────────

func (p *GlueProvider) CreateCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, rtCrawler, crawlerID(name)); err == nil {
		return nil, &model.ProviderError{Code: "AlreadyExists", Message: fmt.Sprintf("Crawler %s already exists", name), HTTPStatus: http.StatusBadRequest}
	}

	now := clock.Now()
	c := crawlerEntry{
		Name:         name,
		Role:         strParam(nr.Params, "Role"),
		Targets:      anyMapParam(nr.Params, "Targets"),
		DatabaseName: strParam(nr.Params, "DatabaseName"),
		Schedule:     strParam(nr.Params, "Schedule"),
		State:        "READY",
		CreatedOn:    now,
		LastUpdated:  now,
	}
	if err := p.saveCrawler(ctx, nr.AccountID, nr.Region, c); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) UpdateCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	c, err := p.loadCrawler(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	if v, ok := nr.Params["Role"].(string); ok {
		c.Role = v
	}
	if v, ok := nr.Params["Targets"].(map[string]any); ok {
		c.Targets = v
	}
	if v, ok := nr.Params["DatabaseName"].(string); ok {
		c.DatabaseName = v
	}
	if v, ok := nr.Params["Schedule"].(string); ok {
		c.Schedule = v
	}
	c.LastUpdated = clock.Now()
	if err := p.saveCrawler(ctx, nr.AccountID, nr.Region, c); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) DeleteCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if _, err := p.loadCrawler(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, rtCrawler, crawlerID(name)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	c, err := p.loadCrawler(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Crawler": crawlerToWire(c)}), nil
}

func (p *GlueProvider) GetCrawlers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, rtCrawler, "crawler/")
	if err != nil {
		return nil, err
	}
	crawlers := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var c crawlerEntry
		if json.Unmarshal(e.Data, &c) == nil {
			crawlers = append(crawlers, crawlerToWire(c))
		}
	}
	maxResults := 100
	if v, ok := nr.Params["MaxResults"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(crawlers, maxResults, token, "GetCrawlers")
	if pgErr != nil {
		return nil, &model.ProviderError{Code: "InvalidInputException", Message: pgErr.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data := map[string]any{"Crawlers": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *GlueProvider) StartCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	c, err := p.loadCrawler(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	if c.State == "RUNNING" {
		return nil, &model.ProviderError{Code: "CrawlerRunningException", Message: "Crawler is already running", HTTPStatus: http.StatusBadRequest}
	}

	c.State = "RUNNING"
	if err := p.saveCrawler(ctx, nr.AccountID, nr.Region, c); err != nil {
		return nil, err
	}

	// Run async crawl in mock mode
	go p.runCrawlAsync(nr.AccountID, nr.Region, c.Name)

	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) StopCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	c, err := p.loadCrawler(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	if c.State != "RUNNING" {
		return nil, &model.ProviderError{Code: "CrawlerNotRunningException", Message: "Crawler is not running", HTTPStatus: http.StatusBadRequest}
	}
	c.State = "STOPPING"
	p.saveCrawler(ctx, nr.AccountID, nr.Region, c) //nolint:errcheck
	return provider.OK(map[string]any{}), nil
}

func (p *GlueProvider) GetCrawlerMetrics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Stub: return empty list
	return provider.OK(map[string]any{"CrawlerMetricsList": []map[string]any{}}), nil
}

// ─── Async crawl ──────────────────────────────────────────────────────────────

func (p *GlueProvider) runCrawlAsync(account, region, crawlerName string) {
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	c, err := p.loadCrawler(ctx, account, region, crawlerName)
	if err != nil {
		return
	}

	startTime := clock.Now()

	// If crawler was stopped before we woke up, honour it
	if c.State == "STOPPING" {
		c.State = "READY"
		c.LastCrawl = &crawlStatus{
			Status:    "SUCCEEDED",
			StartTime: startTime,
			EndTime:   clock.Now(),
		}
		p.saveCrawler(ctx, account, region, c) //nolint:errcheck
		return
	}

	// Walk S3 targets and infer schema if object provider is available
	op := p.getObjectProvider()
	if op != nil && c.Targets != nil {
		if s3Targets, ok := c.Targets["S3Targets"].([]any); ok {
			for _, raw := range s3Targets {
				target, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				path, _ := target["Path"].(string)
				if path == "" {
					continue
				}
				p.crawlS3Path(ctx, account, region, c, path, op)
			}
		}
	}

	c.State = "READY"
	c.LastCrawl = &crawlStatus{
		Status:    "SUCCEEDED",
		StartTime: startTime,
		EndTime:   clock.Now(),
	}
	p.saveCrawler(ctx, account, region, c) //nolint:errcheck
}

// crawlS3Path walks one S3 path, infers CSV schema from the first .csv file found,
// and upserts a catalog table.
func (p *GlueProvider) crawlS3Path(ctx context.Context, account, region string, c crawlerEntry, s3Path string, op ObjectProviderAPI) {
	// Parse s3://bucket/prefix
	path := strings.TrimPrefix(s3Path, "s3://")
	idx := strings.Index(path, "/")
	var bucket, prefix string
	if idx == -1 {
		bucket = path
	} else {
		bucket = path[:idx]
		prefix = path[idx+1:]
	}

	keys, err := op.InternalListObjects(ctx, bucket, prefix)
	if err != nil {
		return
	}

	// Find first CSV and infer schema
	var columns []map[string]any
	for _, key := range keys {
		if !strings.HasSuffix(strings.ToLower(key), ".csv") {
			continue
		}
		data, err := op.InternalGetObject(ctx, bucket, key)
		if err != nil {
			continue
		}
		columns = inferCSVColumns(data)
		break
	}

	if c.DatabaseName == "" {
		return
	}

	// Derive table name from the last non-empty segment of the path
	tableName := deriveTableName(s3Path)
	if tableName == "" {
		tableName = "crawled_table"
	}

	now := clock.Now()
	t := glueTable{
		DatabaseName: c.DatabaseName,
		Name:         tableName,
		TableType:    "EXTERNAL_TABLE",
		StorageDescriptor: map[string]any{
			"Location": s3Path,
			"Columns":  columns,
		},
		CreateTime: now,
		UpdateTime: now,
	}
	// saveTable uses upsert semantics
	p.saveTable(ctx, account, region, t) //nolint:errcheck
}

func inferCSVColumns(data []byte) []map[string]any {
	r := csv.NewReader(strings.NewReader(string(data)))
	headers, err := r.Read()
	if err != nil {
		return nil
	}
	cols := make([]map[string]any, 0, len(headers))
	for _, h := range headers {
		cols = append(cols, map[string]any{
			"Name": strings.TrimSpace(h),
			"Type": "string",
		})
	}
	return cols
}

func deriveTableName(s3Path string) string {
	// Strip trailing slash, then take last path segment
	path := strings.TrimSuffix(s3Path, "/")
	idx := strings.LastIndex(path, "/")
	if idx >= 0 {
		path = path[idx+1:]
	}
	// Strip s3:// prefix if path had no slash
	path = strings.TrimPrefix(path, "s3://")
	// Replace non-alphanumeric with underscore
	var b strings.Builder
	for _, r := range path {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return strings.ToLower(b.String())
}

// ─── Wire serialisation ───────────────────────────────────────────────────────

func crawlerToWire(c crawlerEntry) map[string]any {
	m := map[string]any{
		"Name":         c.Name,
		"Role":         c.Role,
		"Targets":      c.Targets,
		"DatabaseName": c.DatabaseName,
		"State":        c.State,
		"CreatedOn":    float64(c.CreatedOn.UnixMilli()) / 1000.0,
		"LastUpdated":  float64(c.LastUpdated.UnixMilli()) / 1000.0,
	}
	if c.Schedule != "" {
		m["Schedule"] = map[string]any{
			"ScheduleExpression": c.Schedule,
			"State":              "SCHEDULED",
		}
	}
	if c.LastCrawl != nil {
		lc := map[string]any{
			"Status":    c.LastCrawl.Status,
			"StartTime": float64(c.LastCrawl.StartTime.UnixMilli()) / 1000.0,
			"EndTime":   float64(c.LastCrawl.EndTime.UnixMilli()) / 1000.0,
		}
		if c.LastCrawl.ErrorMessage != "" {
			lc["ErrorMessage"] = c.LastCrawl.ErrorMessage
		}
		m["LastCrawl"] = lc
	}
	return m
}
