package dns

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ─── Resource type ────────────────────────────────────────────────────────────

const rtDelegationSet = "route53_delegation_set"

// ─── Delegation sets ──────────────────────────────────────────────────────────

type delegationSet struct {
	Id          string   `json:"Id"`
	CallerRef   string   `json:"CallerReference"`
	NameServers []string `json:"NameServers"`
}

func (d delegationSet) toWire() map[string]any {
	return map[string]any{
		"Id":              "/delegationset/" + d.Id,
		"CallerReference": d.CallerRef,
		"NameServers":     d.NameServers,
	}
}

func defaultNameServers() []string {
	return []string{
		"ns-1.awsdns-01.com",
		"ns-2.awsdns-02.net",
		"ns-3.awsdns-03.org",
		"ns-4.awsdns-04.co.uk",
	}
}

func (p *DNSProvider) CreateReusableDelegationSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	callerRef := strParam(nr.Params, "CallerReference")
	if callerRef == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "CallerReference is required", HTTPStatus: http.StatusBadRequest}
	}
	id := newShortID()
	ds := delegationSet{
		Id:          id,
		CallerRef:   callerRef,
		NameServers: defaultNameServers(),
	}
	data, _ := json.Marshal(ds)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtDelegationSet, ID: id, Data: data}); err != nil {
		if err == store.ErrAlreadyExists {
			return nil, &model.ProviderError{Code: "DelegationSetAlreadyReusable", Message: "Delegation set already exists", HTTPStatus: http.StatusBadRequest}
		}
		return nil, err
	}
	return &model.ProviderResponse{
		HTTPStatus: http.StatusCreated,
		Data:       map[string]any{"DelegationSet": ds.toWire()},
	}, nil
}

func (p *DNSProvider) GetReusableDelegationSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := cleanDelegationSetID(strParam(nr.Params, "Id"))
	e, err := p.resources.Get(ctx, rtDelegationSet, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchDelegationSet", Message: "No delegation set found with ID: " + id, HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var ds delegationSet
	json.Unmarshal(e.Data, &ds)
	return provider.OK(map[string]any{"DelegationSet": ds.toWire()}), nil
}

func (p *DNSProvider) ListReusableDelegationSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtDelegationSet, "")
	if err != nil {
		return nil, err
	}
	sets := []map[string]any{}
	for _, e := range entries {
		var ds delegationSet
		if json.Unmarshal(e.Data, &ds) == nil {
			sets = append(sets, ds.toWire())
		}
	}
	return provider.OK(map[string]any{"DelegationSets": sets}), nil
}

func (p *DNSProvider) DeleteReusableDelegationSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := cleanDelegationSetID(strParam(nr.Params, "Id"))
	if err := p.resources.Delete(ctx, rtDelegationSet, id); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchDelegationSet", Message: "No delegation set found with ID: " + id, HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(nil), nil
}

// ─── Health Check update ──────────────────────────────────────────────────────

func (p *DNSProvider) UpdateHealthCheck(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "Id")
	id = strings.TrimPrefix(id, "/healthcheck/")
	e, err := p.resources.Get(ctx, rtHealthCheck, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchHealthCheck", Message: "Health check not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var hc healthCheck
	json.Unmarshal(e.Data, &hc)
	// Apply optional updates
	if v := strParam(nr.Params, "FullyQualifiedDomainName"); v != "" {
		hc.FullyQualifiedDomainName = v
	}
	if v := strParam(nr.Params, "ResourcePath"); v != "" {
		hc.ResourcePath = v
	}
	if v := strParam(nr.Params, "Type"); v != "" {
		hc.Type = v
	}
	data, _ := json.Marshal(hc)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: rtHealthCheck, ID: id, Data: data})
	return provider.OK(map[string]any{"HealthCheck": hcToWire(hc)}), nil
}

// ─── VPC associations ─────────────────────────────────────────────────────────

func (p *DNSProvider) AssociateVPCWithHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Metadata-only: validate zone exists and return success
	zoneID := cleanZoneID(strParam(nr.Params, "HostedZoneId"))
	if _, err := p.resources.Get(ctx, rtHostedZone, zoneID); err != nil {
		return nil, &model.ProviderError{Code: "NoSuchHostedZone", Message: "Hosted zone not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{
		"ChangeInfo": map[string]any{
			"Status": "INSYNC",
		},
	}), nil
}

func (p *DNSProvider) DisassociateVPCFromHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Metadata-only: validate zone exists and return success
	zoneID := cleanZoneID(strParam(nr.Params, "HostedZoneId"))
	if _, err := p.resources.Get(ctx, rtHostedZone, zoneID); err != nil {
		return nil, &model.ProviderError{Code: "NoSuchHostedZone", Message: "Hosted zone not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{
		"ChangeInfo": map[string]any{
			"Status": "INSYNC",
		},
	}), nil
}

// ─── Hosted zone comment update ───────────────────────────────────────────────

func (p *DNSProvider) UpdateHostedZoneComment(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := cleanZoneID(strParam(nr.Params, "Id"))
	e, err := p.resources.Get(ctx, rtHostedZone, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchHostedZone", Message: "Hosted zone not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var hz hostedZone
	json.Unmarshal(e.Data, &hz)
	if v := strParam(nr.Params, "Comment"); v != "" {
		hz.Comment = v
	}
	data, _ := json.Marshal(hz)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: rtHostedZone, ID: id, Data: data})
	return provider.OK(map[string]any{"HostedZone": zoneToWire(hz)}), nil
}

// ─── Hosted zones by name ─────────────────────────────────────────────────────

func (p *DNSProvider) ListHostedZonesByName(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dnsName := strParam(nr.Params, "DNSName")
	entries, err := p.resources.List(ctx, rtHostedZone, "")
	if err != nil {
		return nil, err
	}
	zones := []map[string]any{}
	for _, e := range entries {
		var hz hostedZone
		if json.Unmarshal(e.Data, &hz) != nil {
			continue
		}
		// Filter by DNS name prefix if provided
		if dnsName != "" {
			normalised := dnsName
			if !strings.HasSuffix(normalised, ".") {
				normalised += "."
			}
			if !strings.HasPrefix(hz.Name, normalised) {
				continue
			}
		}
		zones = append(zones, zoneToWire(hz))
	}
	return provider.OK(map[string]any{"HostedZones": zones}), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func cleanDelegationSetID(id string) string {
	return strings.TrimPrefix(id, "/delegationset/")
}
