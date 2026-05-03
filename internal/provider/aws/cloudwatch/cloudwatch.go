package cloudwatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Provider handles CloudWatch metric + alarm RPCs.
// Metric data is stored in an in-memory ring (bounded) per (namespace, metric, dims).
// Alarms are persisted via store.ResourceStore so they survive restarts in full mode.
type Provider struct {
	resources store.ResourceStore
	bus       *events.EventBus

	mu      sync.Mutex
	metrics map[string]*metricRing // key: ringKey(ns, name, dims)
}

const ringSize = 256

type metricRing struct {
	namespace string
	name      string
	points    []datapoint
	idx       int
}

type datapoint struct {
	Timestamp time.Time
	Value     float64
	Unit      string
	Dims      map[string]string
}

func New(resources store.ResourceStore, bus *events.EventBus) *Provider {
	return &Provider{
		resources: resources,
		bus:       bus,
		metrics:   make(map[string]*metricRing),
	}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudWatch.PutMetricData":           p.PutMetricData,
		"CloudWatch.GetMetricStatistics":     p.GetMetricStatistics,
		"CloudWatch.GetMetricData":           p.GetMetricData,
		"CloudWatch.ListMetrics":             p.ListMetrics,
		"CloudWatch.PutMetricAlarm":          p.PutMetricAlarm,
		"CloudWatch.DescribeAlarms":          p.DescribeAlarms,
		"CloudWatch.DescribeAlarmsForMetric": p.DescribeAlarmsForMetric,
		"CloudWatch.DeleteAlarms":            p.DeleteAlarms,
		"CloudWatch.SetAlarmState":           p.SetAlarmState,
		"CloudWatch.GetDashboard":            p.GetDashboard,
		"CloudWatch.ListDashboards":          p.ListDashboards,
		"CloudWatch.PutDashboard":            p.PutDashboard,
		"CloudWatch.DeleteDashboards":        p.DeleteDashboards,
		"CloudWatch.TagResource":             p.TagResource,
		"CloudWatch.UntagResource":           p.UntagResource,
		"CloudWatch.ListTagsForResource":     p.ListTagsForResource,
	}
}

// PutMetricData stores datapoints in the in-memory ring keyed by
// (namespace, metricName, sorted-dimensions) so different dimension sets for
// the same metric name get separate rings.
func (p *Provider) PutMetricData(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns, _ := nr.Params["Namespace"].(string)
	if ns == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "Namespace is required", HTTPStatus: 400}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 1; ; i++ {
		prefix := "MetricData.member." + strconv.Itoa(i) + "."
		name, ok := nr.Params[prefix+"MetricName"].(string)
		if !ok || name == "" {
			break
		}
		value, _ := toFloat(nr.Params[prefix+"Value"])
		unit, _ := nr.Params[prefix+"Unit"].(string)
		ts := parseTimestamp(nr.Params[prefix+"Timestamp"])
		dims := extractDimensions(nr.Params, prefix)

		key := ringKey(ns, name, dims)
		ring, exists := p.metrics[key]
		if !exists {
			ring = &metricRing{namespace: ns, name: name, points: make([]datapoint, ringSize)}
			p.metrics[key] = ring
		}
		ring.points[ring.idx] = datapoint{Timestamp: ts, Value: value, Unit: unit, Dims: dims}
		ring.idx = (ring.idx + 1) % ringSize
	}
	return provider.OK(map[string]any{"__action__": "PutMetricData"}), nil
}

// GetMetricStatistics returns an empty Datapoints array.
// Real devbox callers (health-check pollers) treat empty as "no recent data".
func (p *Provider) GetMetricStatistics(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	label, _ := nr.Params["MetricName"].(string)
	return provider.OK(map[string]any{
		"__action__": "GetMetricStatistics",
		"Label":      label,
		"Datapoints": []any{},
	}), nil
}

func (p *Provider) GetMetricData(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "GetMetricData", "MetricDataResults": []any{}}), nil
}

func (p *Provider) ListMetrics(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]any, 0, len(p.metrics))
	for _, r := range p.metrics {
		out = append(out, map[string]any{"Namespace": r.namespace, "MetricName": r.name})
	}
	return provider.OK(map[string]any{"__action__": "ListMetrics", "Metrics": out}), nil
}

func (p *Provider) PutMetricAlarm(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["AlarmName"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "AlarmName is required", HTTPStatus: 400}
	}
	data, err := json.Marshal(nr.Params)
	if err != nil {
		return nil, err
	}
	entry := store.ResourceEntry{Type: "cloudwatch_alarm", ID: name, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			if err := p.resources.Update(ctx, entry); err != nil {
				slog.Error("cloudwatch: failed to update alarm",
					"alarm", name, "err", err)
			}
		} else {
			slog.Error("cloudwatch: failed to persist alarm",
				"alarm", name, "err", err)
		}
	}
	return provider.OK(map[string]any{"__action__": "PutMetricAlarm"}), nil
}

func (p *Provider) DescribeAlarms(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, "cloudwatch_alarm", "")
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		var params map[string]any
		if err := json.Unmarshal(e.Data, &params); err != nil {
			slog.Warn("cloudwatch: failed to unmarshal alarm",
				"pkg", "cloudwatch", "op", "DescribeAlarms", "alarm", e.ID, "err", err)
			continue
		}
		out = append(out, params)
	}
	return provider.OK(map[string]any{"__action__": "DescribeAlarms", "MetricAlarms": out}), nil
}

func (p *Provider) DescribeAlarmsForMetric(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "DescribeAlarmsForMetric", "MetricAlarms": []any{}}), nil
}

func (p *Provider) DeleteAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	for i := 1; ; i++ {
		key := "AlarmNames.member." + strconv.Itoa(i)
		name, ok := nr.Params[key].(string)
		if !ok || name == "" {
			break
		}
		if err := p.resources.Delete(ctx, "cloudwatch_alarm", name); err != nil {
			slog.Error("cloudwatch: failed to delete alarm",
				"pkg", "cloudwatch", "op", "DeleteAlarms", "alarm", name, "err", err)
		}
	}
	return provider.OK(map[string]any{"__action__": "DeleteAlarms"}), nil
}

func (p *Provider) SetAlarmState(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["AlarmName"].(string)
	stateValue, _ := nr.Params["StateValue"].(string)
	stateReason, _ := nr.Params["StateReason"].(string)
	if name == "" || stateValue == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "AlarmName and StateValue are required", HTTPStatus: 400}
	}
	e, err := p.resources.Get(ctx, "cloudwatch_alarm", name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Alarm not found: " + name, HTTPStatus: 404}
	}
	var params map[string]any
	if err := json.Unmarshal(e.Data, &params); err != nil {
		return nil, err
	}
	params["StateValue"] = stateValue
	params["StateReason"] = stateReason
	data, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Update(ctx, store.ResourceEntry{Type: "cloudwatch_alarm", ID: name, Data: data}); err != nil {
		slog.Error("cloudwatch: failed to persist alarm state", "alarm", name, "err", err)
	}
	return provider.OK(map[string]any{"__action__": "SetAlarmState"}), nil
}

func (p *Provider) GetDashboard(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "GetDashboard", "DashboardBody": "{}"}), nil
}

func (p *Provider) ListDashboards(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "ListDashboards", "DashboardEntries": []any{}}), nil
}

func (p *Provider) PutDashboard(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "PutDashboard"}), nil
}

func (p *Provider) DeleteDashboards(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "DeleteDashboards"}), nil
}

func (p *Provider) TagResource(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "TagResource"}), nil
}

func (p *Provider) UntagResource(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "UntagResource"}), nil
}

func (p *Provider) ListTagsForResource(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "ListTagsForResource", "Tags": []any{}}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func ringKey(ns, name string, dims map[string]string) string {
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	h.Write([]byte(ns + "\x00" + name + "\x00"))
	for _, k := range keys {
		h.Write([]byte(k + "=" + dims[k] + "\x00"))
	}
	return ns + "/" + name + "/" + hex.EncodeToString(h.Sum(nil))[:16]
}

func extractDimensions(params map[string]any, prefix string) map[string]string {
	out := map[string]string{}
	for i := 1; ; i++ {
		base := prefix + "Dimensions.member." + strconv.Itoa(i) + "."
		n, nok := params[base+"Name"].(string)
		v, vok := params[base+"Value"].(string)
		if !nok || !vok || n == "" {
			break
		}
		out[n] = v
	}
	return out
}

func parseTimestamp(raw any) time.Time {
	switch v := raw.(type) {
	case string:
		if v == "" {
			return time.Now().UTC()
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UTC()
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return time.Unix(int64(f), 0).UTC()
		}
	case float64:
		return time.Unix(int64(v), 0).UTC()
	}
	return time.Now().UTC()
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	}
	return 0, false
}
