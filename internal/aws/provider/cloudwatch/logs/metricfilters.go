package logs

import (
	"context"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

func (p *Provider) PutMetricFilter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	logGroup := paramStr(nr.Params, "logGroupName")
	filterName := paramStr(nr.Params, "filterName")
	pattern := paramStr(nr.Params, "filterPattern")

	var xforms []map[string]any
	if v, ok := nr.Params["metricTransformations"].([]any); ok {
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				xforms = append(xforms, m)
			}
		}
	}

	mf := &MetricFilter{
		FilterName:            filterName,
		LogGroupName:          logGroup,
		FilterPattern:         pattern,
		MetricTransformations: xforms,
	}

	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	if p.store.metricFilters[logGroup] == nil {
		p.store.metricFilters[logGroup] = make(map[string]*MetricFilter)
	}
	p.store.metricFilters[logGroup][filterName] = mf
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeMetricFilters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	logGroup := paramStr(nr.Params, "logGroupName")
	prefix := paramStr(nr.Params, "filterNamePrefix")

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	var filters []map[string]any
	for grp, byName := range p.store.metricFilters {
		if logGroup != "" && grp != logGroup {
			continue
		}
		for _, mf := range byName {
			if prefix != "" && !strings.HasPrefix(mf.FilterName, prefix) {
				continue
			}
			filters = append(filters, map[string]any{
				"filterName":            mf.FilterName,
				"logGroupName":          mf.LogGroupName,
				"filterPattern":         mf.FilterPattern,
				"metricTransformations": mf.MetricTransformations,
			})
		}
	}
	if filters == nil {
		filters = []map[string]any{}
	}
	return provider.OK(map[string]any{"metricFilters": filters}), nil
}

func (p *Provider) DeleteMetricFilter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	logGroup := paramStr(nr.Params, "logGroupName")
	filterName := paramStr(nr.Params, "filterName")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	if byName, ok := p.store.metricFilters[logGroup]; ok {
		delete(byName, filterName)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) TestMetricFilter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"matches": []any{}}), nil
}
