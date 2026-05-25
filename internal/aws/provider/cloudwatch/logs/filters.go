package logs

import (
	"context"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// PutSubscriptionFilter creates or replaces a subscription filter on a log group.
func (p *Provider) PutSubscriptionFilter(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	filterName := paramStr(nr.Params, "filterName")
	filterPattern := paramStr(nr.Params, "filterPattern")
	destinationArn := paramStr(nr.Params, "destinationArn")
	distribution := paramStr(nr.Params, "distribution")
	if distribution == "" {
		distribution = "ByLogStream"
	}

	if groupName == "" || filterName == "" {
		return nil, logsErr("InvalidParameterException", "logGroupName and filterName are required", 400)
	}

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, ok := p.store.groups[groupName]; !ok {
		return nil, logsErr("ResourceNotFoundException", "The specified log group does not exist: "+groupName, 400)
	}

	if p.store.subscriptionFilters[groupName] == nil {
		p.store.subscriptionFilters[groupName] = make(map[string]*SubscriptionFilter)
	}
	// AWS allows at most 2 subscription filters per log group; we skip that limit here.
	p.store.subscriptionFilters[groupName][filterName] = &SubscriptionFilter{
		LogGroupName:   groupName,
		FilterName:     filterName,
		FilterPattern:  filterPattern,
		DestinationArn: destinationArn,
		Distribution:   distribution,
		CreationTime:   clock.Now().UnixMilli(),
	}
	return provider.OK(map[string]any{}), nil
}

// DescribeSubscriptionFilters lists subscription filters for a log group.
func (p *Provider) DescribeSubscriptionFilters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	prefix := paramStr(nr.Params, "filterNamePrefix")

	if groupName == "" {
		return nil, logsErr("InvalidParameterException", "logGroupName is required", 400)
	}

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	if _, ok := p.store.groups[groupName]; !ok {
		return nil, logsErr("ResourceNotFoundException", "The specified log group does not exist: "+groupName, 400)
	}

	out := make([]map[string]any, 0)
	for name, f := range p.store.subscriptionFilters[groupName] {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		out = append(out, map[string]any{
			"logGroupName":   f.LogGroupName,
			"filterName":     f.FilterName,
			"filterPattern":  f.FilterPattern,
			"destinationArn": f.DestinationArn,
			"distribution":   f.Distribution,
			"creationTime":   f.CreationTime,
		})
	}
	return provider.OK(map[string]any{"subscriptionFilters": out}), nil
}

// DeleteSubscriptionFilter removes a subscription filter from a log group.
func (p *Provider) DeleteSubscriptionFilter(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	filterName := paramStr(nr.Params, "filterName")

	if groupName == "" || filterName == "" {
		return nil, logsErr("InvalidParameterException", "logGroupName and filterName are required", 400)
	}

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, ok := p.store.groups[groupName]; !ok {
		return nil, logsErr("ResourceNotFoundException", "The specified log group does not exist: "+groupName, 400)
	}
	if _, ok := p.store.subscriptionFilters[groupName][filterName]; !ok {
		return nil, logsErr("ResourceNotFoundException", "The specified subscription filter does not exist: "+filterName, 400)
	}
	delete(p.store.subscriptionFilters[groupName], filterName)
	return provider.OK(map[string]any{}), nil
}
