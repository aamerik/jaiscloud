// Package monitoring implements the Cloud Monitoring (v3) gRPC service
// (MetricService + AlertPolicyService) over the shared monitoringstore.Store —
// the Amazon CloudWatch metrics+alarms analogue. It manages the metric
// descriptor catalog, the time-series data plane, and the alert-policy (alarm)
// registry. Alert policies are stored verbatim; a background evaluator
// (evaluator.go) evaluates condition_threshold conditions, opens/closes
// incidents, and delivers notifications to the referenced notification
// channels.
//
// Monitored resource descriptors are served from a canonical catalog of
// well-known types (gce_instance, gcs_bucket, pubsub_topic, ...); Get returns
// NotFound for a type outside it.
//
// The descriptor list methods honor a discovery subset of the Monitoring
// filter grammar: equality (`type = "x"`, `metric.type = "x"`,
// `resource.type = "x"`) and `starts_with("prefix")` clauses joined by `AND`,
// over the descriptor type (and its `name`). Any other key, operator, or
// malformed clause is rejected with InvalidArgument rather than silently
// matching everything.
//
// Documented limitations:
//
//   - Only condition_threshold alert conditions are evaluated;
//     condition_absent, condition_matched_log,
//     condition_monitoring_query_language, condition_prometheus_query_language,
//     and condition_sql are stored but never evaluated.
//   - ListTimeSeries supports only the metric.type / resource.type equality
//     filter subset.
//   - NotificationChannelService supports CRUD only;
//     ListNotificationChannelDescriptors, GetNotificationChannelDescriptor,
//     and the verification-code RPCs are Unimplemented.
package monitoring

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/resource"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	"jaiscloud/internal/model"

	"github.com/google/uuid"
)

// Service implements monitoringpb.MetricServiceServer,
// monitoringpb.AlertPolicyServiceServer, and
// monitoringpb.NotificationChannelServiceServer over the shared store.
type Service struct {
	monitoringpb.UnimplementedMetricServiceServer
	monitoringpb.UnimplementedAlertPolicyServiceServer
	monitoringpb.UnimplementedNotificationChannelServiceServer

	store       monitoringstore.Store
	defaultProj string
}

// NewService returns a Monitoring gRPC service backed by the shared store.
// defaultProj is the config-default project used when a request carries none.
func NewService(store monitoringstore.Store, defaultProj string) *Service {
	return &Service{store: store, defaultProj: defaultProj}
}

func mapError(err error) error {
	switch {
	case errors.Is(err, monitoringstore.ErrMetricDescriptorNotFound),
		errors.Is(err, monitoringstore.ErrAlertPolicyNotFound),
		errors.Is(err, monitoringstore.ErrNotificationChannelNotFound):
		return grpcutil.GRPCStatus(model.NewProviderError("NotFound", "resource not found", 404))
	case errors.Is(err, monitoringstore.ErrAlertPolicyExists):
		return grpcutil.GRPCStatus(model.NewProviderError("AlreadyExists", "alert policy already exists", 409))
	case errors.Is(err, monitoringstore.ErrNotificationChannelExists):
		return grpcutil.GRPCStatus(model.NewProviderError("AlreadyExists", "notification channel already exists", 409))
	}
	return grpcutil.GRPCStatus(err)
}

// project resolves the project from a "projects/{p}" (or longer) resource name,
// falling back to routing metadata and then the configured default.
func (s *Service) project(ctx context.Context, name string) string {
	if p := projectFromResourceName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// projectFromResourceName extracts the project id from "projects/{p}" or a
// longer "projects/{p}/..." name.
func projectFromResourceName(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

// ─── resource-name helpers ────────────────────────────────────────────────────

func metricDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("metric-descriptor", typ)
}

// splitMetricDescriptorName parses "projects/{p}/metricDescriptors/{type}",
// where {type} may itself contain slashes (e.g.
// "custom.googleapis.com/invoice/paid/amount").
func splitMetricDescriptorName(name string) (project, typ string, ok bool) {
	const prefix = "projects/"
	const marker = "metricDescriptors/"
	if !strings.HasPrefix(name, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(name, prefix)
	project, after, found := strings.Cut(rest, "/")
	if !found || !strings.HasPrefix(after, marker) {
		return "", "", false
	}
	typ = strings.TrimPrefix(after, marker)
	if typ == "" {
		return "", "", false
	}
	return project, typ, true
}

func alertPolicyName(project, id string) string {
	return resource.ResourceID(project)("alert-policy", id)
}

// splitMonitoredResourceDescriptorName parses
// "projects/{p}/monitoredResourceDescriptors/{type}". Monitored resource types
// contain no slashes, so the name has exactly four segments.
func splitMonitoredResourceDescriptorName(name string) (project, typ string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "monitoredResourceDescriptors" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// splitAlertPolicyName parses "projects/{p}/alertPolicies/{id}".
func splitAlertPolicyName(name string) (project, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "alertPolicies" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

func notificationChannelName(project, id string) string {
	return resource.ResourceID(project)("notification-channel", id)
}

// splitNotificationChannelName parses
// "projects/{p}/notificationChannels/{id}".
func splitNotificationChannelName(name string) (project, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "notificationChannels" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// ─── pagination ───────────────────────────────────────────────────────────────

func pageSlice[T any](items []T, pageSize int32, token string) ([]T, string) {
	size := int(pageSize)
	if size <= 0 {
		size = 10000
	}
	start := decodeOffset(token)
	if start > len(items) {
		start = len(items)
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeOffset(end)
	}
	return items[start:end], next
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

// ─── MetricService ────────────────────────────────────────────────────────────

func (s *Service) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest) (*monitoringpb.ListMetricDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())
	filter, err := compileDescriptorFilter(req.GetFilter(), metricDescriptorFilterKeys)
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid filter: "+err.Error(), 400))
	}
	descriptors, err := s.store.ListMetricDescriptors(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	matching := make([]monitoringstore.MetricDescriptor, 0, len(descriptors))
	for _, d := range descriptors {
		if filter.match(d.Type, metricDescriptorName(project, d.Type)) {
			matching = append(matching, d)
		}
	}
	page, next := pageSlice(matching, req.GetPageSize(), req.GetPageToken())
	out := make([]*metricpb.MetricDescriptor, 0, len(page))
	for _, d := range page {
		out = append(out, descriptorToProto(d, project))
	}
	return &monitoringpb.ListMetricDescriptorsResponse{MetricDescriptors: out, NextPageToken: next}, nil
}

func (s *Service) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	project, typ, ok := splitMetricDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid metric descriptor name: "+req.GetName(), 400))
	}
	d, err := s.store.GetMetricDescriptor(ctx, project, typ)
	if err != nil {
		return nil, mapError(err)
	}
	return descriptorToProto(d, project), nil
}

func (s *Service) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	project := s.project(ctx, req.GetName())
	d := descriptorFromProto(req.GetMetricDescriptor())
	if d.Type == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "metric descriptor type is required", 400))
	}
	stored, err := s.store.CreateMetricDescriptor(ctx, project, d)
	if err != nil {
		return nil, mapError(err)
	}
	return descriptorToProto(stored, project), nil
}

func (s *Service) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest) (*emptypb.Empty, error) {
	project, typ, ok := splitMetricDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid metric descriptor name: "+req.GetName(), 400))
	}
	if err := s.store.DeleteMetricDescriptor(ctx, project, typ); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	project := s.project(ctx, req.GetName())
	filter, err := compileTSFilter(req.GetFilter())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid filter: "+err.Error(), 400))
	}

	series, err := s.store.ListTimeSeries(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}

	matching := make([]monitoringstore.TimeSeries, 0, len(series))
	for _, ts := range series {
		if !filter.match(ts) {
			continue
		}
		ts.Points = filterPoints(ts.Points, req.GetInterval())
		if req.GetView() == monitoringpb.ListTimeSeriesRequest_HEADERS {
			ts.Points = nil
		} else if len(ts.Points) == 0 {
			continue
		}
		matching = append(matching, ts)
	}

	page, next := pageSlice(matching, req.GetPageSize(), req.GetPageToken())
	out := make([]*monitoringpb.TimeSeries, 0, len(page))
	for _, ts := range page {
		out = append(out, timeSeriesToProto(ts))
	}
	return &monitoringpb.ListTimeSeriesResponse{TimeSeries: out, NextPageToken: next}, nil
}

func (s *Service) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	if err := s.writeTimeSeries(ctx, req.GetName(), req.GetTimeSeries()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// CreateServiceTimeSeries is the service-scoped counterpart to
// CreateTimeSeries. In real Cloud Monitoring the two differ only in the
// identity/permission used to authorize the write (the request message type is
// literally reused). The emulator has no authz plane, so this mirrors the
// CreateTimeSeries write path and validates each series identically.
func (s *Service) CreateServiceTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	if err := s.writeTimeSeries(ctx, req.GetName(), req.GetTimeSeries()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// writeTimeSeries transcodes and persists each supplied series, rejecting a
// series with no metric type as InvalidArgument.
func (s *Service) writeTimeSeries(ctx context.Context, name string, series []*monitoringpb.TimeSeries) error {
	project := s.project(ctx, name)
	for _, p := range series {
		ts, err := timeSeriesFromProto(p)
		if err != nil {
			return mapError(err)
		}
		if ts.MetricType == "" {
			return mapError(model.NewProviderError("InvalidArgument", "time series metric type is required", 400))
		}
		if err := s.store.CreateTimeSeries(ctx, project, ts); err != nil {
			return mapError(err)
		}
	}
	return nil
}

// ListMonitoredResourceDescriptors returns the canonical catalog of well-known
// monitored resource descriptors, filtered by the discovery subset of the
// Monitoring filter grammar.
func (s *Service) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest) (*monitoringpb.ListMonitoredResourceDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())
	filter, err := compileDescriptorFilter(req.GetFilter(), monitoredResourceFilterKeys)
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid filter: "+err.Error(), 400))
	}

	all := monitoredResourceDescriptors(project)
	matching := make([]*monitoredrespb.MonitoredResourceDescriptor, 0, len(all))
	for _, d := range all {
		if filter.match(d.GetType(), d.GetName()) {
			matching = append(matching, d)
		}
	}
	page, next := pageSlice(matching, req.GetPageSize(), req.GetPageToken())
	return &monitoringpb.ListMonitoredResourceDescriptorsResponse{ResourceDescriptors: page, NextPageToken: next}, nil
}

// GetMonitoredResourceDescriptor returns the canonical descriptor for a
// well-known monitored resource type. An unknown type is NotFound, matching
// real Cloud Monitoring.
func (s *Service) GetMonitoredResourceDescriptor(ctx context.Context, req *monitoringpb.GetMonitoredResourceDescriptorRequest) (*monitoredrespb.MonitoredResourceDescriptor, error) {
	project, typ, ok := splitMonitoredResourceDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid monitored resource descriptor name: "+req.GetName(), 400))
	}
	d, ok := lookupMonitoredResourceDescriptor(project, typ)
	if !ok {
		return nil, mapError(model.NewProviderError("NotFound", "monitored resource descriptor not found: "+typ, 404))
	}
	return d, nil
}

// ─── AlertPolicyService ───────────────────────────────────────────────────────

func (s *Service) ListAlertPolicies(ctx context.Context, req *monitoringpb.ListAlertPoliciesRequest) (*monitoringpb.ListAlertPoliciesResponse, error) {
	project := s.project(ctx, req.GetName())
	policies, err := s.store.ListAlertPolicies(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := pageSlice(policies, req.GetPageSize(), req.GetPageToken())
	out := make([]*monitoringpb.AlertPolicy, 0, len(page))
	for _, p := range page {
		out = append(out, alertPolicyToProto(p, project))
	}
	return &monitoringpb.ListAlertPoliciesResponse{
		AlertPolicies: out,
		NextPageToken: next,
		TotalSize:     int32(len(policies)),
	}, nil
}

func (s *Service) GetAlertPolicy(ctx context.Context, req *monitoringpb.GetAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, id, ok := splitAlertPolicyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid alert policy name: "+req.GetName(), 400))
	}
	p, err := s.store.GetAlertPolicy(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) CreateAlertPolicy(ctx context.Context, req *monitoringpb.CreateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project := s.project(ctx, req.GetName())
	p, err := alertPolicyFromProto(req.GetAlertPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	if p.DisplayName == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "alert policy display_name is required", 400))
	}
	if p.Enabled == nil {
		enabled := true
		p.Enabled = &enabled
	}
	p.ID = uuid.NewString()
	if err := s.store.CreateAlertPolicy(ctx, project, p); err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) UpdateAlertPolicy(ctx context.Context, req *monitoringpb.UpdateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, id, ok := splitAlertPolicyName(req.GetAlertPolicy().GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid alert policy name: "+req.GetAlertPolicy().GetName(), 400))
	}
	incoming, err := alertPolicyFromProto(req.GetAlertPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	incoming.ID = id

	// An empty/nil update mask is a full replace (the historical behavior).
	// A non-empty mask merges field-by-field: masked paths take the incoming
	// value, unmasked paths retain the stored value. The get-check-merge
	// happens inside UpdateAlertPolicyAtomic's locked section so a concurrent
	// masked update touching different fields can't read the same stale
	// snapshot and silently overwrite this one's change.
	p, err := s.store.UpdateAlertPolicyAtomic(ctx, project, id, func(stored monitoringstore.AlertPolicy) (monitoringstore.AlertPolicy, error) {
		paths := req.GetUpdateMask().GetPaths()
		if len(paths) == 0 {
			return incoming, nil
		}
		return applyAlertPolicyMask(stored, incoming, paths)
	})
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) DeleteAlertPolicy(ctx context.Context, req *monitoringpb.DeleteAlertPolicyRequest) (*emptypb.Empty, error) {
	project, id, ok := splitAlertPolicyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid alert policy name: "+req.GetName(), 400))
	}
	if err := s.store.DeleteAlertPolicy(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── NotificationChannelService ──────────────────────────────────────────────

func (s *Service) ListNotificationChannels(ctx context.Context, req *monitoringpb.ListNotificationChannelsRequest) (*monitoringpb.ListNotificationChannelsResponse, error) {
	project := s.project(ctx, req.GetName())
	channels, err := s.store.ListNotificationChannels(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := pageSlice(channels, req.GetPageSize(), req.GetPageToken())
	out := make([]*monitoringpb.NotificationChannel, 0, len(page))
	for _, c := range page {
		out = append(out, notificationChannelToProto(c, project))
	}
	return &monitoringpb.ListNotificationChannelsResponse{
		NotificationChannels: out,
		NextPageToken:        next,
		TotalSize:            int32(len(channels)),
	}, nil
}

func (s *Service) GetNotificationChannel(ctx context.Context, req *monitoringpb.GetNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project, id, ok := splitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid notification channel name: "+req.GetName(), 400))
	}
	c, err := s.store.GetNotificationChannel(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

func (s *Service) CreateNotificationChannel(ctx context.Context, req *monitoringpb.CreateNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project := s.project(ctx, req.GetName())
	c := notificationChannelFromProto(req.GetNotificationChannel())
	if c.Type == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "notification channel type is required", 400))
	}
	if c.Enabled == nil {
		enabled := true
		c.Enabled = &enabled
	}
	now := clock.Now()
	c.ID = uuid.NewString()
	c.CreateTime = now
	c.UpdateTime = now
	c.VerificationStatus = int32(monitoringpb.NotificationChannel_VERIFIED)
	if err := s.store.CreateNotificationChannel(ctx, project, c); err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

func (s *Service) UpdateNotificationChannel(ctx context.Context, req *monitoringpb.UpdateNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project, id, ok := splitNotificationChannelName(req.GetNotificationChannel().GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid notification channel name: "+req.GetNotificationChannel().GetName(), 400))
	}
	incoming := notificationChannelFromProto(req.GetNotificationChannel())
	incoming.ID = id
	incoming.UpdateTime = clock.Now()

	c, err := s.store.UpdateNotificationChannelAtomic(ctx, project, id, func(stored monitoringstore.NotificationChannel) (monitoringstore.NotificationChannel, error) {
		paths := req.GetUpdateMask().GetPaths()
		if len(paths) == 0 {
			// Full replacement, but preserve the immutable create time and
			// verification status unless explicitly masked.
			incoming.CreateTime = stored.CreateTime
			if incoming.VerificationStatus == 0 {
				incoming.VerificationStatus = stored.VerificationStatus
			}
			return incoming, nil
		}
		return applyNotificationChannelMask(stored, incoming, paths)
	})
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

func (s *Service) DeleteNotificationChannel(ctx context.Context, req *monitoringpb.DeleteNotificationChannelRequest) (*emptypb.Empty, error) {
	project, id, ok := splitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid notification channel name: "+req.GetName(), 400))
	}
	if err := s.store.DeleteNotificationChannel(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func descriptorToProto(d monitoringstore.MetricDescriptor, project string) *metricpb.MetricDescriptor {
	out := &metricpb.MetricDescriptor{
		Name:                   metricDescriptorName(project, d.Type),
		Type:                   d.Type,
		MetricKind:             metricpb.MetricDescriptor_MetricKind(d.MetricKind),
		ValueType:              metricpb.MetricDescriptor_ValueType(d.ValueType),
		Unit:                   d.Unit,
		Description:            d.Description,
		DisplayName:            d.DisplayName,
		MonitoredResourceTypes: d.MonitoredResourceTypes,
	}
	for _, l := range d.Labels {
		out.Labels = append(out.Labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelpb.LabelDescriptor_ValueType(l.ValueType),
			Description: l.Description,
		})
	}
	return out
}

func descriptorFromProto(p *metricpb.MetricDescriptor) monitoringstore.MetricDescriptor {
	if p == nil {
		return monitoringstore.MetricDescriptor{}
	}
	d := monitoringstore.MetricDescriptor{
		Type:                   p.GetType(),
		MetricKind:             int32(p.GetMetricKind()),
		ValueType:              int32(p.GetValueType()),
		Unit:                   p.GetUnit(),
		Description:            p.GetDescription(),
		DisplayName:            p.GetDisplayName(),
		MonitoredResourceTypes: p.GetMonitoredResourceTypes(),
	}
	for _, l := range p.GetLabels() {
		d.Labels = append(d.Labels, monitoringstore.LabelDescriptor{
			Key:         l.GetKey(),
			ValueType:   int32(l.GetValueType()),
			Description: l.GetDescription(),
		})
	}
	return d
}

func timeSeriesToProto(ts monitoringstore.TimeSeries) *monitoringpb.TimeSeries {
	out := &monitoringpb.TimeSeries{
		Metric:     &metricpb.Metric{Type: ts.MetricType, Labels: ts.MetricLabels},
		Resource:   &monitoredrespb.MonitoredResource{Type: ts.ResourceType, Labels: ts.ResourceLabels},
		MetricKind: metricpb.MetricDescriptor_MetricKind(ts.MetricKind),
		ValueType:  metricpb.MetricDescriptor_ValueType(ts.ValueType),
		Unit:       ts.Unit,
	}
	// Points are returned reverse-chronologically (most recent first).
	points := make([]monitoringstore.Point, len(ts.Points))
	copy(points, ts.Points)
	sort.Slice(points, func(i, j int) bool { return points[i].EndTime.After(points[j].EndTime) })
	for _, pt := range points {
		out.Points = append(out.Points, pointToProto(pt))
	}
	return out
}

func timeSeriesFromProto(p *monitoringpb.TimeSeries) (monitoringstore.TimeSeries, error) {
	ts := monitoringstore.TimeSeries{}
	if p.GetMetric() != nil {
		ts.MetricType = p.GetMetric().GetType()
		ts.MetricLabels = p.GetMetric().GetLabels()
	}
	if p.GetResource() != nil {
		ts.ResourceType = p.GetResource().GetType()
		ts.ResourceLabels = p.GetResource().GetLabels()
	}
	ts.MetricKind = int32(p.GetMetricKind())
	ts.ValueType = int32(p.GetValueType())
	ts.Unit = p.GetUnit()
	for _, pt := range p.GetPoints() {
		sp, err := pointFromProto(pt)
		if err != nil {
			return ts, err
		}
		ts.Points = append(ts.Points, sp)
	}
	return ts, nil
}

func pointToProto(p monitoringstore.Point) *monitoringpb.Point {
	iv := &monitoringpb.TimeInterval{}
	if !p.EndTime.IsZero() {
		iv.EndTime = timestamppb.New(p.EndTime)
	}
	if !p.StartTime.IsZero() {
		iv.StartTime = timestamppb.New(p.StartTime)
	}
	return &monitoringpb.Point{Interval: iv, Value: typedValueToProto(p.Value)}
}

func pointFromProto(p *monitoringpb.Point) (monitoringstore.Point, error) {
	pt := monitoringstore.Point{}
	if p == nil {
		return pt, nil
	}
	if iv := p.GetInterval(); iv != nil {
		if iv.GetStartTime() != nil {
			pt.StartTime = iv.GetStartTime().AsTime()
		}
		if iv.GetEndTime() != nil {
			pt.EndTime = iv.GetEndTime().AsTime()
		}
	}
	if pt.EndTime.IsZero() {
		pt.EndTime = clock.Now()
	}
	v, err := typedValueFromProto(p.GetValue())
	if err != nil {
		return pt, err
	}
	pt.Value = v
	return pt, nil
}

func typedValueToProto(v monitoringstore.TypedValue) *monitoringpb.TypedValue {
	switch {
	case v.BoolValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: *v.BoolValue}}
	case v.Int64Value != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: *v.Int64Value}}
	case v.DoubleValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.StringValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_StringValue{StringValue: *v.StringValue}}
	case v.DistributionValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: distributionToProto(*v.DistributionValue)}}
	}
	return &monitoringpb.TypedValue{}
}

func typedValueFromProto(p *monitoringpb.TypedValue) (monitoringstore.TypedValue, error) {
	if p == nil {
		return monitoringstore.TypedValue{}, nil
	}
	switch v := p.GetValue().(type) {
	case *monitoringpb.TypedValue_BoolValue:
		return monitoringstore.TypedValue{BoolValue: &v.BoolValue}, nil
	case *monitoringpb.TypedValue_Int64Value:
		return monitoringstore.TypedValue{Int64Value: &v.Int64Value}, nil
	case *monitoringpb.TypedValue_DoubleValue:
		return monitoringstore.TypedValue{DoubleValue: &v.DoubleValue}, nil
	case *monitoringpb.TypedValue_StringValue:
		return monitoringstore.TypedValue{StringValue: &v.StringValue}, nil
	case *monitoringpb.TypedValue_DistributionValue:
		d := distributionFromProto(v.DistributionValue)
		return monitoringstore.TypedValue{DistributionValue: &d}, nil
	}
	return monitoringstore.TypedValue{}, nil
}

// distributionToProto transcodes a stored Distribution back to its
// google.api.Distribution wire form, including bucket options and exemplars.
func distributionToProto(d monitoringstore.Distribution) *distributionpb.Distribution {
	out := &distributionpb.Distribution{
		Count:                 d.Count,
		Mean:                  d.Mean,
		SumOfSquaredDeviation: d.SumOfSquaredDeviation,
		BucketCounts:          append([]int64(nil), d.BucketCounts...),
	}
	if d.Range != nil {
		out.Range = &distributionpb.Distribution_Range{Min: d.Range.Min, Max: d.Range.Max}
	}
	if d.BucketOptions != nil {
		bo := &distributionpb.Distribution_BucketOptions{}
		switch {
		case d.BucketOptions.Linear != nil:
			bo.Options = &distributionpb.Distribution_BucketOptions_LinearBuckets{
				LinearBuckets: &distributionpb.Distribution_BucketOptions_Linear{
					NumFiniteBuckets: d.BucketOptions.Linear.NumFiniteBuckets,
					Width:            d.BucketOptions.Linear.Width,
					Offset:           d.BucketOptions.Linear.Offset,
				},
			}
		case d.BucketOptions.Exponential != nil:
			bo.Options = &distributionpb.Distribution_BucketOptions_ExponentialBuckets{
				ExponentialBuckets: &distributionpb.Distribution_BucketOptions_Exponential{
					NumFiniteBuckets: d.BucketOptions.Exponential.NumFiniteBuckets,
					GrowthFactor:     d.BucketOptions.Exponential.GrowthFactor,
					Scale:            d.BucketOptions.Exponential.Scale,
				},
			}
		case d.BucketOptions.Explicit != nil:
			bo.Options = &distributionpb.Distribution_BucketOptions_ExplicitBuckets{
				ExplicitBuckets: &distributionpb.Distribution_BucketOptions_Explicit{
					Bounds: append([]float64(nil), d.BucketOptions.Explicit.Bounds...),
				},
			}
		}
		out.BucketOptions = bo
	}
	for _, ex := range d.Exemplars {
		pe := &distributionpb.Distribution_Exemplar{Value: ex.Value}
		if !ex.Timestamp.IsZero() {
			pe.Timestamp = timestamppb.New(ex.Timestamp)
		}
		for _, raw := range ex.Attachments {
			var a anypb.Any
			if proto.Unmarshal(raw, &a) == nil {
				pe.Attachments = append(pe.Attachments, &a)
			}
		}
		out.Exemplars = append(out.Exemplars, pe)
	}
	return out
}

// distributionFromProto transcodes a google.api.Distribution into the store
// representation.
func distributionFromProto(p *distributionpb.Distribution) monitoringstore.Distribution {
	d := monitoringstore.Distribution{
		Count:                 p.GetCount(),
		Mean:                  p.GetMean(),
		SumOfSquaredDeviation: p.GetSumOfSquaredDeviation(),
		BucketCounts:          append([]int64(nil), p.GetBucketCounts()...),
	}
	if r := p.GetRange(); r != nil {
		d.Range = &monitoringstore.DistributionRange{Min: r.GetMin(), Max: r.GetMax()}
	}
	if bo := p.GetBucketOptions(); bo != nil {
		out := &monitoringstore.BucketOptions{}
		switch o := bo.GetOptions().(type) {
		case *distributionpb.Distribution_BucketOptions_LinearBuckets:
			out.Linear = &monitoringstore.LinearBuckets{
				NumFiniteBuckets: o.LinearBuckets.GetNumFiniteBuckets(),
				Width:            o.LinearBuckets.GetWidth(),
				Offset:           o.LinearBuckets.GetOffset(),
			}
		case *distributionpb.Distribution_BucketOptions_ExponentialBuckets:
			out.Exponential = &monitoringstore.ExponentialBuckets{
				NumFiniteBuckets: o.ExponentialBuckets.GetNumFiniteBuckets(),
				GrowthFactor:     o.ExponentialBuckets.GetGrowthFactor(),
				Scale:            o.ExponentialBuckets.GetScale(),
			}
		case *distributionpb.Distribution_BucketOptions_ExplicitBuckets:
			out.Explicit = &monitoringstore.ExplicitBuckets{
				Bounds: append([]float64(nil), o.ExplicitBuckets.GetBounds()...),
			}
		}
		d.BucketOptions = out
	}
	for _, ex := range p.GetExemplars() {
		me := monitoringstore.Exemplar{Value: ex.GetValue()}
		if ex.GetTimestamp() != nil {
			me.Timestamp = ex.GetTimestamp().AsTime()
		}
		for _, a := range ex.GetAttachments() {
			if b, err := proto.Marshal(a); err == nil {
				me.Attachments = append(me.Attachments, b)
			}
		}
		d.Exemplars = append(d.Exemplars, me)
	}
	return d
}

func alertPolicyToProto(p monitoringstore.AlertPolicy, project string) *monitoringpb.AlertPolicy {
	out := &monitoringpb.AlertPolicy{
		Name:                 alertPolicyName(project, p.ID),
		DisplayName:          p.DisplayName,
		Combiner:             monitoringpb.AlertPolicy_ConditionCombinerType(p.Combiner),
		NotificationChannels: p.NotificationChannels,
		UserLabels:           p.UserLabels,
	}
	if p.Enabled != nil {
		out.Enabled = wrapperspb.Bool(*p.Enabled)
	}
	if len(p.Documentation) > 0 && string(p.Documentation) != "null" {
		doc := &monitoringpb.AlertPolicy_Documentation{}
		if protojson.Unmarshal(p.Documentation, doc) == nil {
			out.Documentation = doc
		}
	}
	for _, c := range p.Conditions {
		cond := &monitoringpb.AlertPolicy_Condition{}
		if protojson.Unmarshal(c, cond) == nil {
			out.Conditions = append(out.Conditions, cond)
		}
	}
	return out
}

func alertPolicyFromProto(p *monitoringpb.AlertPolicy) (monitoringstore.AlertPolicy, error) {
	ap := monitoringstore.AlertPolicy{
		DisplayName:          p.GetDisplayName(),
		Combiner:             int32(p.GetCombiner()),
		NotificationChannels: p.GetNotificationChannels(),
		UserLabels:           p.GetUserLabels(),
	}
	if p.GetEnabled() != nil {
		v := p.GetEnabled().GetValue()
		ap.Enabled = &v
	}
	if p.GetDocumentation() != nil {
		b, err := protojson.Marshal(p.GetDocumentation())
		if err != nil {
			return ap, err
		}
		ap.Documentation = json.RawMessage(b)
	}
	for _, c := range p.GetConditions() {
		b, err := protojson.Marshal(c)
		if err != nil {
			return ap, err
		}
		ap.Conditions = append(ap.Conditions, json.RawMessage(b))
	}
	return ap, nil
}

// applyAlertPolicyMask merges an incoming alert policy into the stored policy
// according to the field paths in updateMask. For each masked path the incoming
// value wins; every other field retains the stored value. Supported paths are
// display_name, documentation, conditions, combiner, enabled,
// notification_channels, and user_labels. Any other path is unsupported and
// returns an error mapped to Unimplemented.
func applyAlertPolicyMask(stored, incoming monitoringstore.AlertPolicy, updateMask []string) (monitoringstore.AlertPolicy, error) {
	for _, path := range updateMask {
		switch path {
		case "display_name":
			stored.DisplayName = incoming.DisplayName
		case "documentation":
			stored.Documentation = incoming.Documentation
		case "conditions":
			stored.Conditions = incoming.Conditions
		case "combiner":
			stored.Combiner = incoming.Combiner
		case "enabled":
			stored.Enabled = incoming.Enabled
		case "notification_channels":
			stored.NotificationChannels = incoming.NotificationChannels
		case "user_labels":
			stored.UserLabels = incoming.UserLabels
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
	}
	return stored, nil
}

func notificationChannelToProto(c monitoringstore.NotificationChannel, project string) *monitoringpb.NotificationChannel {
	out := &monitoringpb.NotificationChannel{
		Name:               notificationChannelName(project, c.ID),
		Type:               c.Type,
		DisplayName:        c.DisplayName,
		Description:        c.Description,
		Labels:             c.Labels,
		UserLabels:         c.UserLabels,
		VerificationStatus: monitoringpb.NotificationChannel_VerificationStatus(c.VerificationStatus),
	}
	if c.Enabled != nil {
		out.Enabled = wrapperspb.Bool(*c.Enabled)
	}
	if !c.CreateTime.IsZero() {
		out.CreationRecord = &monitoringpb.MutationRecord{
			MutateTime: timestamppb.New(c.CreateTime),
		}
	}
	if !c.UpdateTime.IsZero() {
		out.MutationRecords = []*monitoringpb.MutationRecord{{
			MutateTime: timestamppb.New(c.UpdateTime),
		}}
	}
	return out
}

func notificationChannelFromProto(p *monitoringpb.NotificationChannel) monitoringstore.NotificationChannel {
	if p == nil {
		return monitoringstore.NotificationChannel{}
	}
	c := monitoringstore.NotificationChannel{
		Type:               p.GetType(),
		DisplayName:        p.GetDisplayName(),
		Description:        p.GetDescription(),
		Labels:             p.GetLabels(),
		UserLabels:         p.GetUserLabels(),
		VerificationStatus: int32(p.GetVerificationStatus()),
	}
	if p.GetEnabled() != nil {
		v := p.GetEnabled().GetValue()
		c.Enabled = &v
	}
	return c
}

// applyNotificationChannelMask merges an incoming channel into the stored
// channel according to the field paths in updateMask. Supported paths are
// type, display_name, description, labels, user_labels, and enabled. Any other
// path returns an error mapped to Unimplemented.
func applyNotificationChannelMask(stored, incoming monitoringstore.NotificationChannel, updateMask []string) (monitoringstore.NotificationChannel, error) {
	for _, path := range updateMask {
		switch path {
		case "type":
			stored.Type = incoming.Type
		case "display_name":
			stored.DisplayName = incoming.DisplayName
		case "description":
			stored.Description = incoming.Description
		case "labels":
			stored.Labels = incoming.Labels
		case "user_labels":
			stored.UserLabels = incoming.UserLabels
		case "enabled":
			stored.Enabled = incoming.Enabled
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
	}
	stored.UpdateTime = incoming.UpdateTime
	return stored, nil
}

// filterPoints restricts points to those whose end time falls within the
// half-open interval (start, end]. A nil interval keeps every point.
func filterPoints(points []monitoringstore.Point, iv *monitoringpb.TimeInterval) []monitoringstore.Point {
	if iv == nil || len(points) == 0 {
		return points
	}
	var start, end time.Time
	if iv.GetStartTime() != nil {
		start = iv.GetStartTime().AsTime()
	}
	if iv.GetEndTime() != nil {
		end = iv.GetEndTime().AsTime()
	}
	out := make([]monitoringstore.Point, 0, len(points))
	for _, p := range points {
		if !end.IsZero() && !p.EndTime.IsZero() && p.EndTime.After(end) {
			continue
		}
		if !start.IsZero() && !p.EndTime.IsZero() && !p.EndTime.After(start) {
			continue
		}
		out = append(out, p)
	}
	return out
}
