// Package logging implements the Cloud Logging (v2) gRPC service
// (LoggingServiceV2) over the shared loggingstore.Store, so write/list/delete
// share state across transports.
//
// Implemented RPCs: WriteLogEntries, ListLogEntries, ListLogs, DeleteLog,
// ListMonitoredResourceDescriptors, and TailLogEntries. Monitored resource
// descriptors are served from the canonical catalog shared with the Cloud
// Monitoring service (internal/gcp/rescatalog); the Logging descriptors carry
// type/display_name/description/labels but no resource name. This proto
// revision's ListMonitoredResourceDescriptorsRequest has only
// page_size/page_token (no parent/filter), so the catalog is global and
// unfiltered.
//
// TailLogEntries is a bounded, store-polling approximation of the real
// streaming read. It accepts the initial resource_names/filter/buffer_window
// (and later requests that change the filter/project), then re-reads the store
// on a timer derived from buffer_window and streams entries written after the
// stream began whose fields satisfy the list filter engine. Documented
// approximation: real Logging provides at-least-once delivery, server-side
// buffer_window reordering of late-arriving entries, and per-response timestamp
// ordering. The emulator records the store's monotonic write id at stream start
// and emits every entry with a larger id in (timestamp, id) order — i.e.
// at-most-once delivery against the read snapshot, no server-side retention,
// and latency bounded below by the poll interval (2 s by default, 50 ms
// minimum). The session terminates cleanly when the client half-closes or
// cancels.
package logging

import (
	"context"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/resource"
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
	return resource.ResourceID(project)("log", id)
}

// defaultScope resolves the scope parent used when a request omits its
// resource name, from gRPC routing metadata or the configured default project.
// It returns "" when nothing resolves (an empty store key lists nothing).
func (s *Service) defaultScope(ctx context.Context) string {
	scope, err := parseScopeParent(grpcutil.ProjectFromMetadata(ctx, s.defaultProj))
	if err != nil {
		return ""
	}
	return scope
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
		scope, logID, perr := parseLogName(e.LogName)
		if perr != nil {
			return nil, mapError(perr)
		}
		e.LogName = canonicalLogName(scope, logID)
		entries = append(entries, e)
	}

	if req.GetDryRun() {
		return &loggingpb.WriteLogEntriesResponse{}, nil
	}

	// Pass 2: write every validated entry.
	for _, e := range entries {
		scope, _, _ := parseLogName(e.LogName)
		if err := s.store.Write(ctx, scope, e); err != nil {
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

	var scopes []string
	seenScope := make(map[string]struct{}, len(req.GetResourceNames()))
	for _, rn := range req.GetResourceNames() {
		scope, perr := parseScopeParent(rn)
		if perr != nil {
			return nil, mapError(perr)
		}
		if _, seen := seenScope[scope]; !seen {
			seenScope[scope] = struct{}{}
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 {
		if scope := s.defaultScope(ctx); scope != "" {
			scopes = append(scopes, scope)
		}
	}

	var entries []loggingstore.LogEntry
	for _, scope := range scopes {
		list, lerr := s.store.List(ctx, scope)
		if lerr != nil {
			return nil, mapError(lerr)
		}
		entries = append(entries, list...)
	}
	if len(scopes) > 1 {
		sort.SliceStable(entries, func(i, j int) bool {
			if entries[i].Timestamp.Equal(entries[j].Timestamp) {
				return entries[i].ID < entries[j].ID
			}
			return entries[i].Timestamp.Before(entries[j].Timestamp)
		})
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
	scope := ""
	if parent := req.GetParent(); parent == "" {
		scope = s.defaultScope(ctx)
	} else {
		parsed, perr := parseScopeParent(parent)
		if perr != nil {
			return nil, mapError(perr)
		}
		scope = parsed
	}
	names, err := s.store.ListLogs(ctx, scope)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := paging.Page(names, func(n string) string { return n },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	return &loggingpb.ListLogsResponse{LogNames: page, NextPageToken: next}, nil
}

func (s *Service) DeleteLog(ctx context.Context, req *loggingpb.DeleteLogRequest) (*emptypb.Empty, error) {
	scope, logID, perr := parseLogName(req.GetLogName())
	if perr != nil {
		return nil, mapError(perr)
	}
	if err := s.store.DeleteLog(ctx, scope, canonicalLogName(scope, logID)); err != nil {
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
