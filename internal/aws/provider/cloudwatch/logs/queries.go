package logs

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ─── Query CRUD (13.10) ───────────────────────────────────────────────────────

func newQueryID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b) + "-" + fmt.Sprintf("%d", clock.Now().UnixNano()%1e9)
}

func (p *Provider) StartQuery(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queryStr := paramStr(nr.Params, "queryString")
	if queryStr == "" {
		return nil, logsErr("MalformedQueryException", "queryString is required", http.StatusBadRequest)
	}
	startTime := paramInt(nr.Params, "startTime", 0)
	endTime := paramInt(nr.Params, "endTime", 0)
	if startTime > 0 && endTime > 0 && startTime >= endTime {
		return nil, logsErr("InvalidParameterException", "startTime must be before endTime", http.StatusBadRequest)
	}
	var logGroupNames []string
	if v, ok := nr.Params["logGroupNames"].([]any); ok {
		for _, n := range v {
			if s, ok := n.(string); ok {
				logGroupNames = append(logGroupNames, s)
			}
		}
	} else if s := paramStr(nr.Params, "logGroupName"); s != "" {
		logGroupNames = []string{s}
	}

	qid := newQueryID()
	q := &LogQuery{
		QueryID:       qid,
		QueryString:   queryStr,
		LogGroupNames: logGroupNames,
		StartTime:     int64(startTime),
		EndTime:       int64(endTime),
		Status:        "Complete",
		CreatedAt:     clock.Now(),
		Results:       [][]map[string]string{},
		Statistics:    map[string]float64{"recordsMatched": 0, "recordsScanned": 0, "bytesScanned": 0},
	}
	p.store.mu.Lock()
	p.store.queries[qid] = q
	p.store.mu.Unlock()
	return provider.OK(map[string]any{"queryId": qid}), nil
}

func (p *Provider) GetQueryResults(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	qid := paramStr(nr.Params, "queryId")
	p.store.mu.RLock()
	q, ok := p.store.queries[qid]
	p.store.mu.RUnlock()
	if !ok {
		return nil, logsErr("ResourceNotFoundException", "Query not found: "+qid, http.StatusBadRequest)
	}
	return provider.OK(map[string]any{
		"queryId": q.QueryID,
		"status":  q.Status,
		"results": q.Results,
		"statistics": map[string]any{
			"recordsMatched": q.Statistics["recordsMatched"],
			"recordsScanned": q.Statistics["recordsScanned"],
			"bytesScanned":   q.Statistics["bytesScanned"],
		},
	}), nil
}

func (p *Provider) StopQuery(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	qid := paramStr(nr.Params, "queryId")
	p.store.mu.Lock()
	q, ok := p.store.queries[qid]
	if ok {
		q.Status = "Cancelled"
	}
	p.store.mu.Unlock()
	if !ok {
		return nil, logsErr("ResourceNotFoundException", "Query not found: "+qid, http.StatusBadRequest)
	}
	return provider.OK(map[string]any{"success": true}), nil
}

func (p *Provider) PutQueryDefinition(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "name")
	queryStr := paramStr(nr.Params, "queryString")
	if name == "" || queryStr == "" {
		return nil, logsErr("InvalidParameterException", "name and queryString are required", http.StatusBadRequest)
	}
	var logGroupNames []string
	if v, ok := nr.Params["logGroupNames"].([]any); ok {
		for _, n := range v {
			if s, ok := n.(string); ok {
				logGroupNames = append(logGroupNames, s)
			}
		}
	}
	// Look for existing definition with same name to update
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	for _, qd := range p.store.queryDefinitions {
		if qd.Name == name {
			qd.QueryString = queryStr
			qd.LogGroupNames = logGroupNames
			qd.LastModified = clock.Now()
			return provider.OK(map[string]any{"queryDefinitionId": qd.QueryDefinitionID}), nil
		}
	}
	qdid := newQueryID()
	p.store.queryDefinitions[qdid] = &QueryDefinition{
		QueryDefinitionID: qdid,
		Name:              name,
		QueryString:       queryStr,
		LogGroupNames:     logGroupNames,
		LastModified:      clock.Now(),
	}
	return provider.OK(map[string]any{"queryDefinitionId": qdid}), nil
}

func (p *Provider) DescribeQueryDefinitions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	namePrefix := paramStr(nr.Params, "queryDefinitionNamePrefix")
	p.store.mu.RLock()
	defer p.store.mu.RUnlock()
	out := []map[string]any{}
	for _, qd := range p.store.queryDefinitions {
		if namePrefix != "" && !strings.HasPrefix(qd.Name, namePrefix) {
			continue
		}
		out = append(out, map[string]any{
			"queryDefinitionId": qd.QueryDefinitionID,
			"name":              qd.Name,
			"queryString":       qd.QueryString,
			"logGroupNames":     qd.LogGroupNames,
			"lastModified":      qd.LastModified.UnixMilli(),
		})
	}
	return provider.OK(map[string]any{"queryDefinitions": out}), nil
}

func (p *Provider) DeleteQueryDefinition(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	qdid := paramStr(nr.Params, "queryDefinitionId")
	p.store.mu.Lock()
	_, ok := p.store.queryDefinitions[qdid]
	if ok {
		delete(p.store.queryDefinitions, qdid)
	}
	p.store.mu.Unlock()
	if !ok {
		return nil, logsErr("ResourceNotFoundException", "Query definition not found: "+qdid, http.StatusBadRequest)
	}
	return provider.OK(map[string]any{"success": true}), nil
}

// ─── Export Tasks (13.11) ─────────────────────────────────────────────────────

func newTaskID() string {
	return fmt.Sprintf("export-%d", clock.Now().UnixNano())
}

func (p *Provider) CreateExportTask(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	if groupName == "" {
		return nil, logsErr("InvalidParameterException", "logGroupName is required", http.StatusBadRequest)
	}
	p.store.mu.RLock()
	_, groupExists := p.store.groups[groupName]
	p.store.mu.RUnlock()
	if !groupExists {
		return nil, logsErr("ResourceNotFoundException", "The specified log group does not exist: "+groupName, http.StatusBadRequest)
	}
	now := clock.Now().UnixMilli()
	taskID := newTaskID()
	task := &ExportTask{
		TaskID:            taskID,
		TaskName:          paramStr(nr.Params, "taskName"),
		LogGroupName:      groupName,
		From:              int64(paramInt(nr.Params, "from", 0)),
		To:                int64(paramInt(nr.Params, "to", 0)),
		Destination:       paramStr(nr.Params, "destination"),
		DestinationPrefix: paramStr(nr.Params, "destinationPrefix"),
		StatusCode:        "COMPLETED",
		StatusMessage:     "Completed successfully (stub)",
		CreationTime:      now,
		CompletionTime:    now,
	}
	p.store.mu.Lock()
	p.store.exportTasks[taskID] = task
	p.store.mu.Unlock()
	return provider.OK(map[string]any{"taskId": taskID}), nil
}

func (p *Provider) DescribeExportTasks(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	taskIDFilter := paramStr(nr.Params, "taskId")
	statusFilter := paramStr(nr.Params, "statusCode")
	p.store.mu.RLock()
	defer p.store.mu.RUnlock()
	out := []map[string]any{}
	for _, t := range p.store.exportTasks {
		if taskIDFilter != "" && t.TaskID != taskIDFilter {
			continue
		}
		if statusFilter != "" && t.StatusCode != statusFilter {
			continue
		}
		entry := map[string]any{
			"taskId":            t.TaskID,
			"taskName":          t.TaskName,
			"logGroupName":      t.LogGroupName,
			"from":              t.From,
			"to":                t.To,
			"destination":       t.Destination,
			"destinationPrefix": t.DestinationPrefix,
			"status": map[string]any{
				"code":    t.StatusCode,
				"message": t.StatusMessage,
			},
			"executionInfo": map[string]any{
				"creationTime":   t.CreationTime,
				"completionTime": t.CompletionTime,
			},
		}
		out = append(out, entry)
	}
	return provider.OK(map[string]any{"exportTasks": out}), nil
}

func (p *Provider) CancelExportTask(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	taskID := paramStr(nr.Params, "taskId")
	p.store.mu.Lock()
	t, ok := p.store.exportTasks[taskID]
	if ok {
		if t.StatusCode == "PENDING" || t.StatusCode == "RUNNING" {
			t.StatusCode = "CANCELLED"
			t.StatusMessage = "Cancelled by user"
		}
	}
	p.store.mu.Unlock()
	if !ok {
		return nil, logsErr("ResourceNotFoundException", "Export task not found: "+taskID, http.StatusBadRequest)
	}
	return provider.OK(map[string]any{}), nil
}
