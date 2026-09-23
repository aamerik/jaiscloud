package monitoring

import (
	"context"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// Provider handles the Cloud Monitoring v3 REST data plane. It is a thin
// adapter: every handler decodes the NormalizedRequest body/params into the
// core's typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Monitoring REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Monitoring.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Monitoring.ListMetricDescriptors":                   p.ListMetricDescriptors,
		"Monitoring.GetMetricDescriptor":                     p.GetMetricDescriptor,
		"Monitoring.CreateMetricDescriptor":                  p.CreateMetricDescriptor,
		"Monitoring.DeleteMetricDescriptor":                  p.DeleteMetricDescriptor,
		"Monitoring.ListTimeSeries":                          p.ListTimeSeries,
		"Monitoring.CreateTimeSeries":                        p.CreateTimeSeries,
		"Monitoring.CreateServiceTimeSeries":                 p.CreateServiceTimeSeries,
		"Monitoring.ListMonitoredResourceDescriptors":        p.ListMonitoredResourceDescriptors,
		"Monitoring.GetMonitoredResourceDescriptor":          p.GetMonitoredResourceDescriptor,
		"Monitoring.ListAlertPolicies":                       p.ListAlertPolicies,
		"Monitoring.GetAlertPolicy":                          p.GetAlertPolicy,
		"Monitoring.CreateAlertPolicy":                       p.CreateAlertPolicy,
		"Monitoring.UpdateAlertPolicy":                       p.UpdateAlertPolicy,
		"Monitoring.DeleteAlertPolicy":                       p.DeleteAlertPolicy,
		"Monitoring.ListNotificationChannels":                p.ListNotificationChannels,
		"Monitoring.GetNotificationChannel":                  p.GetNotificationChannel,
		"Monitoring.CreateNotificationChannel":               p.CreateNotificationChannel,
		"Monitoring.UpdateNotificationChannel":               p.UpdateNotificationChannel,
		"Monitoring.DeleteNotificationChannel":               p.DeleteNotificationChannel,
		"Monitoring.ListNotificationChannelDescriptors":      p.ListNotificationChannelDescriptors,
		"Monitoring.GetNotificationChannelDescriptor":        p.GetNotificationChannelDescriptor,
		"Monitoring.SendNotificationChannelVerificationCode": p.SendNotificationChannelVerificationCode,
		"Monitoring.GetNotificationChannelVerificationCode":  p.GetNotificationChannelVerificationCode,
		"Monitoring.VerifyNotificationChannel":               p.VerifyNotificationChannel,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// ─── metric descriptors ───────────────────────────────────────────────────────

func (p *Provider) ListMetricDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListMetricDescriptors(ctx, project,
		strParam(nr, "filter"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(page) > 0 {
		arr := make([]any, 0, len(page))
		for _, d := range page {
			arr = append(arr, metricDescriptorToJSON(d, project))
		}
		out["metricDescriptors"] = arr
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) GetMetricDescriptor(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, typ, ok := core.SplitMetricDescriptorName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid metric descriptor name: " + strParam(nr, "name"))
	}
	d, err := p.core.GetMetricDescriptor(ctx, project, typ)
	if err != nil {
		return nil, err
	}
	return provider.OK(metricDescriptorToJSON(d, project)), nil
}

func (p *Provider) CreateMetricDescriptor(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	stored, err := p.core.CreateMetricDescriptor(ctx, project, metricDescriptorFromJSON(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(metricDescriptorToJSON(stored, project)), nil
}

func (p *Provider) DeleteMetricDescriptor(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, typ, ok := core.SplitMetricDescriptorName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid metric descriptor name: " + strParam(nr, "name"))
	}
	if err := p.core.DeleteMetricDescriptor(ctx, project, typ); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── time series ──────────────────────────────────────────────────────────────

func (p *Provider) ListTimeSeries(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	var interval *core.TimeInterval
	start := parseTime(strParam(nr, "interval.startTime"))
	end := parseTime(strParam(nr, "interval.endTime"))
	if !start.IsZero() || !end.IsZero() {
		interval = &core.TimeInterval{Start: start, End: end}
	}
	headersOnly := strings.EqualFold(strParam(nr, "view"), "HEADERS")
	page, next, err := p.core.ListTimeSeries(ctx, project, strParam(nr, "filter"), interval, headersOnly,
		intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(page) > 0 {
		arr := make([]any, 0, len(page))
		for _, ts := range page {
			arr = append(arr, timeSeriesToJSON(ts))
		}
		out["timeSeries"] = arr
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) CreateTimeSeries(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.writeTimeSeries(ctx, nr); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CreateServiceTimeSeries(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.writeTimeSeries(ctx, nr); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) writeTimeSeries(ctx context.Context, nr *model.NormalizedRequest) error {
	project := p.project(nr)
	var series []monitoringstore.TimeSeries
	for _, s := range listFrom(bodyOf(nr)["timeSeries"]) {
		series = append(series, timeSeriesFromJSON(s))
	}
	return p.core.CreateTimeSeries(ctx, project, series)
}

// ─── monitored resource descriptors ───────────────────────────────────────────

func (p *Provider) ListMonitoredResourceDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListMonitoredResourceDescriptors(ctx, project,
		strParam(nr, "filter"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(page) > 0 {
		arr := make([]any, 0, len(page))
		for _, d := range page {
			arr = append(arr, monitoredResourceDescriptorToJSON(d))
		}
		out["resourceDescriptors"] = arr
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) GetMonitoredResourceDescriptor(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, typ, ok := core.SplitMonitoredResourceDescriptorName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid monitored resource descriptor name: " + strParam(nr, "name"))
	}
	d, err := p.core.GetMonitoredResourceDescriptor(ctx, project, typ)
	if err != nil {
		return nil, err
	}
	return provider.OK(monitoredResourceDescriptorToJSON(d)), nil
}

// ─── alert policies ───────────────────────────────────────────────────────────

func (p *Provider) ListAlertPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, total, next, err := p.core.ListAlertPolicies(ctx, project, intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(page) > 0 {
		arr := make([]any, 0, len(page))
		for _, pol := range page {
			arr = append(arr, alertPolicyToJSON(pol, project))
		}
		out["alertPolicies"] = arr
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	if total > 0 {
		out["totalSize"] = total
	}
	return provider.OK(out), nil
}

func (p *Provider) GetAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitAlertPolicyName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid alert policy name: " + strParam(nr, "name"))
	}
	pol, err := p.core.GetAlertPolicy(ctx, project, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(alertPolicyToJSON(pol, project)), nil
}

func (p *Provider) CreateAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pol, err := p.core.CreateAlertPolicy(ctx, project, alertPolicyFromJSON(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(alertPolicyToJSON(pol, project)), nil
}

func (p *Provider) UpdateAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitAlertPolicyName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid alert policy name: " + strParam(nr, "name"))
	}
	pol, err := p.core.UpdateAlertPolicy(ctx, project, id, alertPolicyFromJSON(bodyOf(nr)), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(alertPolicyToJSON(pol, project)), nil
}

func (p *Provider) DeleteAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitAlertPolicyName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid alert policy name: " + strParam(nr, "name"))
	}
	if err := p.core.DeleteAlertPolicy(ctx, project, id); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── notification channels ────────────────────────────────────────────────────

func (p *Provider) ListNotificationChannels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, total, next, err := p.core.ListNotificationChannels(ctx, project, intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(page) > 0 {
		arr := make([]any, 0, len(page))
		for _, c := range page {
			arr = append(arr, notificationChannelToJSON(c, project))
		}
		out["notificationChannels"] = arr
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	if total > 0 {
		out["totalSize"] = total
	}
	return provider.OK(out), nil
}

func (p *Provider) GetNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel name: " + strParam(nr, "name"))
	}
	c, err := p.core.GetNotificationChannel(ctx, project, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(notificationChannelToJSON(c, project)), nil
}

func (p *Provider) CreateNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	c, err := p.core.CreateNotificationChannel(ctx, project, notificationChannelFromJSON(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(notificationChannelToJSON(c, project)), nil
}

func (p *Provider) UpdateNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel name: " + strParam(nr, "name"))
	}
	c, err := p.core.UpdateNotificationChannel(ctx, project, id, notificationChannelFromJSON(bodyOf(nr)), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(notificationChannelToJSON(c, project)), nil
}

func (p *Provider) DeleteNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel name: " + strParam(nr, "name"))
	}
	if err := p.core.DeleteNotificationChannel(ctx, project, id); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListNotificationChannelDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListNotificationChannelDescriptors(ctx, project, intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(page) > 0 {
		arr := make([]any, 0, len(page))
		for _, d := range page {
			arr = append(arr, notificationChannelDescriptorToJSON(d))
		}
		out["channelDescriptors"] = arr
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) GetNotificationChannelDescriptor(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, typ, ok := core.SplitNotificationChannelDescriptorName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel descriptor name: " + strParam(nr, "name"))
	}
	d, err := p.core.GetNotificationChannelDescriptor(ctx, project, typ)
	if err != nil {
		return nil, err
	}
	return provider.OK(notificationChannelDescriptorToJSON(d)), nil
}

func (p *Provider) SendNotificationChannelVerificationCode(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel name: " + strParam(nr, "name"))
	}
	if err := p.core.SendNotificationChannelVerificationCode(ctx, project, id); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) GetNotificationChannelVerificationCode(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel name: " + strParam(nr, "name"))
	}
	// The Discovery method declares no request body, but a client may supply an
	// expireTime; honor it when present, else the core defaults to now+1h.
	var expire *time.Time
	if et := parseTime(strFrom(bodyOf(nr)["expireTime"])); !et.IsZero() {
		expire = &et
	}
	code, expireTime, err := p.core.GetNotificationChannelVerificationCode(ctx, project, id, expire)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"code":       code,
		"expireTime": expireTime.UTC().Format(time.RFC3339Nano),
	}), nil
}

func (p *Provider) VerifyNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(strParam(nr, "name"))
	if !ok {
		return nil, invalidArgument("invalid notification channel name: " + strParam(nr, "name"))
	}
	c, err := p.core.VerifyNotificationChannel(ctx, project, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(notificationChannelToJSON(c, project)), nil
}
