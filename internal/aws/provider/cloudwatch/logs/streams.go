package logs

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// CreateLogStream creates a new log stream within a log group.
func (p *Provider) CreateLogStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	streamName := paramStr(nr.Params, "logStreamName")

	if streamName == "" {
		return nil, logsErr("InvalidParameterException", "logStreamName is required", 400)
	}
	if len(streamName) > 512 {
		return nil, logsErr("InvalidParameterException", "logStreamName must not exceed 512 characters", 400)
	}

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, err := p.verifyGroupExists(groupName); err != nil {
		return nil, err
	}

	streams := p.store.streams[groupName]
	if streams == nil {
		streams = make(map[string]*LogStream)
		p.store.streams[groupName] = streams
	}
	if _, exists := streams[streamName]; exists {
		return nil, logsErr("ResourceAlreadyExistsException", "The specified log stream already exists: "+streamName, 400)
	}

	arn := nr.ResourceID("logs-stream", groupName+":log-stream:"+streamName)
	streams[streamName] = &LogStream{
		LogStreamName:       streamName,
		CreationTime:        clock.Now().UnixMilli(),
		UploadSequenceToken: "0",
		Arn:                 arn,
	}

	// Initialise event ring
	events := p.store.events[groupName]
	if events == nil {
		events = make(map[string]*eventRing)
		p.store.events[groupName] = events
	}
	events[streamName] = newEventRing()

	// Initialise seq token counter
	seqTokens := p.store.seqToken[groupName]
	if seqTokens == nil {
		seqTokens = make(map[string]int64)
		p.store.seqToken[groupName] = seqTokens
	}
	seqTokens[streamName] = 0

	return provider.OK(map[string]any{}), nil
}

// DeleteLogStream deletes a log stream from a log group.
func (p *Provider) DeleteLogStream(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	streamName := paramStr(nr.Params, "logStreamName")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, err := p.verifyGroupExists(groupName); err != nil {
		return nil, err
	}

	streams := p.store.streams[groupName]
	if streams == nil || streams[streamName] == nil {
		return nil, logsErr("ResourceNotFoundException", "The specified log stream does not exist: "+streamName, 400)
	}

	delete(streams, streamName)
	if events := p.store.events[groupName]; events != nil {
		delete(events, streamName)
	}
	if seqTokens := p.store.seqToken[groupName]; seqTokens != nil {
		delete(seqTokens, streamName)
	}

	return provider.OK(map[string]any{}), nil
}

// DescribeLogStreams lists log streams in a log group with optional filtering and pagination.
func (p *Provider) DescribeLogStreams(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	streamPrefix := paramStr(nr.Params, "logStreamNamePrefix")
	orderBy := paramStr(nr.Params, "orderBy")
	if orderBy == "" {
		orderBy = "LogStreamName"
	}
	descending, _ := nr.Params["descending"].(bool)
	nextTokenIn := paramStr(nr.Params, "nextToken")
	limit := paramInt(nr.Params, "limit", 50)
	if limit <= 0 || limit > 50 {
		limit = 50
	}

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	if _, err := p.verifyGroupExists(groupName); err != nil {
		return nil, err
	}

	streams := p.store.streams[groupName]
	var list []*LogStream
	for _, s := range streams {
		if streamPrefix != "" && !strings.HasPrefix(s.LogStreamName, streamPrefix) {
			continue
		}
		list = append(list, s)
	}

	// Sort
	if orderBy == "LastEventTime" {
		sort.Slice(list, func(i, j int) bool {
			return list[i].LastEventTimestamp < list[j].LastEventTimestamp
		})
	} else {
		sort.Slice(list, func(i, j int) bool {
			return list[i].LogStreamName < list[j].LogStreamName
		})
	}
	if descending {
		for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
			list[i], list[j] = list[j], list[i]
		}
	}

	// Apply nextToken cursor (skip streams where name <= nextToken when orderBy=LogStreamName)
	if nextTokenIn != "" && orderBy == "LogStreamName" {
		start := 0
		for start < len(list) && list[start].LogStreamName <= nextTokenIn {
			start++
		}
		list = list[start:]
	}

	// Paginate
	var nextToken string
	if len(list) > limit {
		nextToken = list[limit-1].LogStreamName
		list = list[:limit]
	}

	out := make([]any, 0, len(list))
	for _, s := range list {
		out = append(out, streamToMap(s))
	}

	result := map[string]any{"logStreams": out}
	if nextToken != "" {
		result["nextToken"] = nextToken
	}
	return provider.OK(result), nil
}

// streamToMap converts a LogStream to a response map.
func streamToMap(s *LogStream) map[string]any {
	item := map[string]any{
		"logStreamName":       s.LogStreamName,
		"creationTime":        s.CreationTime,
		"storedBytes":         s.StoredBytes,
		"arn":                 s.Arn,
		"uploadSequenceToken": fmt.Sprintf("%056d", 0),
	}
	if s.FirstEventTimestamp != 0 {
		item["firstEventTimestamp"] = s.FirstEventTimestamp
	}
	if s.LastEventTimestamp != 0 {
		item["lastEventTimestamp"] = s.LastEventTimestamp
	}
	if s.LastIngestionTime != 0 {
		item["lastIngestionTime"] = s.LastIngestionTime
	}
	return item
}
