package logging

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// Provider handles the Cloud Logging v2 REST data plane. It is a thin adapter:
// every handler decodes the NormalizedRequest body/params into the core's typed
// API, calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Logging REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Logging.<Action>" keys to their handlers. The action names are
// chosen so the fidelity resolver derives the Discovery method ids
// (entries.write, entries.list, logs.list, logs.delete,
// monitoredResourceDescriptors.list).
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Logging.EntryWrite":                      p.EntryWrite,
		"Logging.EntryList":                       p.EntryList,
		"Logging.LogList":                         p.LogList,
		"Logging.LogDelete":                       p.LogDelete,
		"Logging.MonitoredResourceDescriptorList": p.MonitoredResourceDescriptorList,
	}
}

// project resolves the owning project: the request's account (project) scope,
// else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// defaultScope resolves the fallback scope parent ("projects/{p}") from the
// request project, or "" when no project resolves.
func (p *Provider) defaultScope(nr *model.NormalizedRequest) string {
	project := p.project(nr)
	if project == "" {
		return ""
	}
	scope, err := core.ParseScopeParent(project)
	if err != nil {
		return ""
	}
	return scope
}

// ─── handlers ─────────────────────────────────────────────────────────────────

func (p *Provider) EntryWrite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	req := &core.WriteRequest{
		PartialSuccess: boolFrom(body["partialSuccess"]),
		DryRun:         boolFrom(body["dryRun"]),
		LogName:        strFrom(body["logName"]),
		Labels:         stringMapFrom(body["labels"]),
	}
	if res, ok := body["resource"].(map[string]any); ok {
		req.ResourceType = strFrom(res["type"])
		req.ResourceLabels = stringMapFrom(res["labels"])
	}
	if arr, ok := body["entries"].([]any); ok {
		req.Entries = make([]loggingstore.LogEntry, 0, len(arr))
		for _, e := range arr {
			req.Entries = append(req.Entries, entryFromWire(e))
		}
	}
	if err := p.core.WriteEntries(ctx, req); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) EntryList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)

	resourceNames := strListFrom(body["resourceNames"])
	// projectIds is the legacy request field; the proto specifies it is added to
	// resourceNames, so both spellings resolve to the same scope set.
	for _, pid := range strListFrom(body["projectIds"]) {
		resourceNames = append(resourceNames, "projects/"+pid)
	}

	res, err := p.core.ListEntries(ctx, &core.ListEntriesRequest{
		ResourceNames: resourceNames,
		Filter:        strFrom(body["filter"]),
		OrderBy:       strFrom(body["orderBy"]),
		PageSize:      intFrom(body["pageSize"]),
		PageToken:     strFrom(body["pageToken"]),
		Scope:         p.defaultScope(nr),
	})
	if err != nil {
		return nil, err
	}

	out := map[string]any{}
	if len(res.Entries) > 0 {
		entries := make([]any, 0, len(res.Entries))
		for _, e := range res.Entries {
			entries = append(entries, entryToWire(e))
		}
		out["entries"] = entries
	}
	if res.NextPageToken != "" {
		out["nextPageToken"] = res.NextPageToken
	}
	return provider.OK(out), nil
}

func (p *Provider) LogList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names, next, err := p.core.ListLogs(ctx,
		strParam(nr, "parent"),
		p.defaultScope(nr),
		intFrom(nr.Params["pageSize"]),
		strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(names) > 0 {
		out["logNames"] = names
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) LogDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteLog(ctx, strParam(nr, "logName")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) MonitoredResourceDescriptorList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	page, next := p.core.ListMonitoredResourceDescriptors(
		intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	out := map[string]any{}
	if len(page) > 0 {
		descs := make([]any, 0, len(page))
		for _, d := range page {
			descs = append(descs, descriptorToWire(d))
		}
		out["resourceDescriptors"] = descs
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}
