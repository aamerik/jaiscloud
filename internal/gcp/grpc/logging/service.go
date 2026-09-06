// Package logging implements the Cloud Logging (v2) gRPC service
// (LoggingServiceV2) over the shared loggingstore.Store, so write/list/delete
// share state across transports.
package logging

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"

	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements loggingpb.LoggingServiceV2Server over the shared store.
type Service struct {
	loggingpb.UnimplementedLoggingServiceV2Server

	store       loggingstore.Store
	defaultProj string
}

// NewService returns a Logging gRPC service backed by the shared store.
// defaultProj is the config-default project used when a request carries none.
func NewService(store loggingstore.Store, defaultProj string) *Service {
	return &Service{store: store, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── resource-name parsing ────────────────────────────────────────────────────

func logName(project, id string) string {
	return "projects/" + project + "/logs/" + id
}

// splitLogName parses "projects/{p}/logs/{l}".
func splitLogName(name string) (project, logID string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "logs" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// projectFromResourceName extracts the project from "projects/{p}" (or a
// longer "projects/{p}/logs/{l}" name) or a bare project id.
func projectFromResourceName(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	if !strings.Contains(name, "/") {
		return name
	}
	return ""
}

// ─── LoggingServiceV2 ─────────────────────────────────────────────────────────

func (s *Service) WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) (*loggingpb.WriteLogEntriesResponse, error) {
	if req.GetPartialSuccess() {
		return nil, mapError(model.NewProviderError("UnsupportedOperation", "partial_success is not supported", 501))
	}

	defaultLogName := req.GetLogName()
	defaultResource := ""
	var defaultResourceLabels map[string]string
	if req.GetResource() != nil {
		defaultResource = req.GetResource().GetType()
		defaultResourceLabels = req.GetResource().GetLabels()
	}
	defaultLabels := req.GetLabels()

	// Pass 1: transcode + apply defaults + validate every entry BEFORE writing
	// any, so a permanently-invalid entry rejects the whole batch atomically.
	entries := make([]loggingstore.LogEntry, 0, len(req.GetEntries()))
	for _, proto := range req.GetEntries() {
		e := entryFromProto(proto)
		if e.LogName == "" {
			e.LogName = defaultLogName
		}
		if e.ResourceType == "" {
			e.ResourceType = defaultResource
		}
		if len(e.ResourceLabels) == 0 && len(defaultResourceLabels) > 0 {
			e.ResourceLabels = defaultResourceLabels
		}
		if len(defaultLabels) > 0 {
			if e.Labels == nil {
				e.Labels = make(map[string]string, len(defaultLabels))
			}
			for k, v := range defaultLabels {
				if _, ok := e.Labels[k]; !ok {
					e.Labels[k] = v
				}
			}
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = clock.Now()
		}
		_, _, ok := splitLogName(e.LogName)
		if !ok {
			return nil, mapError(model.NewProviderError("InvalidArgument", "invalid log name: "+e.LogName, 400))
		}
		entries = append(entries, e)
	}

	if req.GetDryRun() {
		return &loggingpb.WriteLogEntriesResponse{}, nil
	}

	// Pass 2: write every validated entry.
	for _, e := range entries {
		project, _, _ := splitLogName(e.LogName)
		if err := s.store.Write(ctx, project, e); err != nil {
			return nil, mapError(err)
		}
	}
	return &loggingpb.WriteLogEntriesResponse{}, nil
}

func (s *Service) ListLogEntries(ctx context.Context, req *loggingpb.ListLogEntriesRequest) (*loggingpb.ListLogEntriesResponse, error) {
	pred, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid filter: "+err.Error(), 400))
	}

	project := ""
	for _, rn := range req.GetResourceNames() {
		if p := projectFromResourceName(rn); p != "" {
			project = p
			break
		}
	}
	if project == "" {
		project = grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	}

	entries, err := s.store.List(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	filtered := entries[:0]
	for _, e := range entries {
		if pred.match(e) {
			filtered = append(filtered, e)
		}
	}

	if err := orderEntries(filtered, req.GetOrderBy()); err != nil {
		return nil, mapError(err)
	}
	page, next, err := paginateEntries(filtered, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}

	out := make([]*loggingpb.LogEntry, 0, len(page))
	for _, e := range page {
		out = append(out, entryToProto(e))
	}
	return &loggingpb.ListLogEntriesResponse{Entries: out, NextPageToken: next}, nil
}

func (s *Service) ListLogs(ctx context.Context, req *loggingpb.ListLogsRequest) (*loggingpb.ListLogsResponse, error) {
	project := projectFromResourceName(req.GetParent())
	if project == "" {
		project = grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	}
	names, err := s.store.ListLogs(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := paging.Page(names, func(n string) string { return n },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	return &loggingpb.ListLogsResponse{LogNames: page, NextPageToken: next}, nil
}

func (s *Service) DeleteLog(ctx context.Context, req *loggingpb.DeleteLogRequest) (*emptypb.Empty, error) {
	project, _, ok := splitLogName(req.GetLogName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid log name: "+req.GetLogName(), 400))
	}
	if err := s.store.DeleteLog(ctx, project, req.GetLogName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── ordering / pagination ────────────────────────────────────────────────────

// orderEntries orders the filtered entries by timestamp. The default is
// "timestamp asc" (the store's natural order); "timestamp desc" reverses it.
// Any other value is invalid and returns an InvalidArgument error.
func orderEntries(entries []loggingstore.LogEntry, orderBy string) error {
	switch strings.TrimSpace(orderBy) {
	case "", "timestamp asc":
		return nil // already ascending from the store
	case "timestamp desc":
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
		return nil
	default:
		return model.NewProviderError("InvalidArgument", "invalid order_by: "+orderBy, 400)
	}
}

func paginateEntries(entries []loggingstore.LogEntry, pageSize int, token string) ([]loggingstore.LogEntry, string, error) {
	if pageSize < 0 || pageSize > 1000 {
		return nil, "", model.NewProviderError("InvalidArgument", "page_size must be between 0 and 1000", 400)
	}
	if pageSize == 0 {
		pageSize = 50
	}
	start := decodeOffset(token)
	if start > len(entries) {
		start = len(entries)
	}
	end := start + pageSize
	if end > len(entries) {
		end = len(entries)
	}
	next := ""
	if end < len(entries) {
		next = encodeOffset(end)
	}
	return entries[start:end], next, nil
}

func encodeOffset(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeOffset(token string) int {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(string(b))
	return n
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func entryFromProto(p *loggingpb.LogEntry) loggingstore.LogEntry {
	e := loggingstore.LogEntry{
		LogName:  p.GetLogName(),
		Severity: int(p.GetSeverity()),
		InsertID: p.GetInsertId(),
		Labels:   p.GetLabels(),
	}
	if p.GetResource() != nil {
		e.ResourceType = p.GetResource().GetType()
		e.ResourceLabels = p.GetResource().GetLabels()
	}
	if p.GetTimestamp() != nil {
		e.Timestamp = p.GetTimestamp().AsTime()
	}
	switch payload := p.GetPayload().(type) {
	case *loggingpb.LogEntry_TextPayload:
		e.PayloadType = "text"
		e.TextPayload = payload.TextPayload
	case *loggingpb.LogEntry_JsonPayload:
		e.PayloadType = "json"
		if payload.JsonPayload != nil {
			e.JsonPayload = payload.JsonPayload.AsMap()
		}
	}
	return e
}

func entryToProto(e loggingstore.LogEntry) *loggingpb.LogEntry {
	out := &loggingpb.LogEntry{
		LogName:  e.LogName,
		Severity: ltype.LogSeverity(e.Severity),
		InsertId: e.InsertID,
		Labels:   e.Labels,
	}
	if e.ResourceType != "" || len(e.ResourceLabels) > 0 {
		out.Resource = &monitoredres.MonitoredResource{Type: e.ResourceType, Labels: e.ResourceLabels}
	}
	if !e.Timestamp.IsZero() {
		out.Timestamp = timestamppb.New(e.Timestamp)
	}
	switch e.PayloadType {
	case "json":
		if st, err := structpb.NewStruct(e.JsonPayload); err == nil {
			out.Payload = &loggingpb.LogEntry_JsonPayload{JsonPayload: st}
		}
	default:
		out.Payload = &loggingpb.LogEntry_TextPayload{TextPayload: e.TextPayload}
	}
	return out
}
