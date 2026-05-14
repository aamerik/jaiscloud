// Package dns implements the Route53 provider (DNSProvider).
package dns

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// DNSProvider handles Route53 hosted zones, record sets, and health checks.
type DNSProvider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *DNSProvider {
	return &DNSProvider{resources: resources}
}

func (p *DNSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"DNS.CreateHostedZone":          p.CreateHostedZone,
		"DNS.GetHostedZone":             p.GetHostedZone,
		"DNS.ListHostedZones":           p.ListHostedZones,
		"DNS.DeleteHostedZone":          p.DeleteHostedZone,
		"DNS.ChangeResourceRecordSets":  p.ChangeResourceRecordSets,
		"DNS.ListResourceRecordSets":    p.ListResourceRecordSets,
		"DNS.CreateHealthCheck":         p.CreateHealthCheck,
		"DNS.GetHealthCheck":            p.GetHealthCheck,
		"DNS.ListHealthChecks":          p.ListHealthChecks,
		"DNS.DeleteHealthCheck":         p.DeleteHealthCheck,
		"DNS.GetChange":                 p.GetChange,
		// Tagging (13.13)
		"DNS.ChangeTagsForResource":    p.ChangeTagsForResource,
		"DNS.ListTagsForResource":      p.ListTagsForResource,
		"DNS.ListTagsForResources":     p.ListTagsForResources,
		// Extras
		"DNS.ListHostedZonesByName":           p.ListHostedZonesByName,
		"DNS.UpdateHealthCheck":               p.UpdateHealthCheck,
		"DNS.AssociateVPCWithHostedZone":      p.AssociateVPCWithHostedZone,
		"DNS.DisassociateVPCFromHostedZone":   p.DisassociateVPCFromHostedZone,
		"DNS.CreateReusableDelegationSet":     p.CreateReusableDelegationSet,
		"DNS.GetReusableDelegationSet":        p.GetReusableDelegationSet,
		"DNS.ListReusableDelegationSets":      p.ListReusableDelegationSets,
		"DNS.DeleteReusableDelegationSet":     p.DeleteReusableDelegationSet,
		"DNS.UpdateHostedZoneComment":         p.UpdateHostedZoneComment,
	}
}

// ─── Resource types ───────────────────────────────────────────────────────────

const (
	rtHostedZone  = "route53_hosted_zone"
	rtRRSet       = "route53_rrset"
	rtHealthCheck = "route53_health_check"
	rtR53Tags     = "route53_tags"
)

func newShortID() string {
	b := make([]byte, 7)
	rand.Read(b)
	return "Z" + strings.ToUpper(hex.EncodeToString(b))[:14]
}

// newChangeID generates a unique Route53 change ID: "C" + 14 uppercase hex chars.
func newChangeID() string {
	b := make([]byte, 7)
	rand.Read(b)
	return "C" + strings.ToUpper(hex.EncodeToString(b))[:14]
}

// ─── Hosted Zone metadata ─────────────────────────────────────────────────────

type hostedZone struct {
	Id                     string    `json:"Id"`
	Name                   string    `json:"Name"`
	Comment                string    `json:"Comment"`
	PrivateZone            bool      `json:"PrivateZone"`
	ResourceRecordSetCount int       `json:"ResourceRecordSetCount"`
	CreatedAt              time.Time `json:"CreatedAt"`
}

func zoneToWire(hz hostedZone) map[string]any {
	private := "false"
	if hz.PrivateZone {
		private = "true"
	}
	return map[string]any{
		"Id":                     "/hostedzone/" + hz.Id,
		"Name":                   hz.Name,
		"Comment":                hz.Comment,
		"PrivateZone":            private,
		"ResourceRecordSetCount": fmt.Sprintf("%d", hz.ResourceRecordSetCount),
	}
}

// ─── Hosted Zone operations ───────────────────────────────────────────────────

func (p *DNSProvider) CreateHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	// Normalise: ensure trailing dot
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	id := newShortID()
	hz := hostedZone{
		Id:          id,
		Name:        name,
		Comment:     strParam(nr.Params, "Comment"),
		PrivateZone: false,
		CreatedAt:   time.Now(),
		ResourceRecordSetCount: 2, // default SOA + NS
	}
	data, _ := json.Marshal(hz)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtHostedZone, ID: id, Data: data}); err != nil {
		return nil, err
	}
	loc := fmt.Sprintf("https://route53.amazonaws.com/2013-04-01/hostedzone/%s", id)
	return &model.ProviderResponse{
		HTTPStatus: http.StatusCreated,
		Data: map[string]any{
			"CreateHostedZoneResponse": true,
			"HostedZoneCreated":        zoneToWire(hz),
			"Location":                 loc,
			"ChangeId":                 newChangeID(),
		},
	}, nil
}

func (p *DNSProvider) GetHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := cleanZoneID(strParam(nr.Params, "Id"))
	e, err := p.resources.Get(ctx, rtHostedZone, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchHostedZone", Message: fmt.Sprintf("No hosted zone found with ID: %s", id), HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var hz hostedZone
	json.Unmarshal(e.Data, &hz)
	return provider.OK(map[string]any{"HostedZone": zoneToWire(hz)}), nil
}

func (p *DNSProvider) ListHostedZones(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtHostedZone, "")
	if err != nil {
		return nil, err
	}
	zones := []map[string]any{}
	for _, e := range entries {
		var hz hostedZone
		json.Unmarshal(e.Data, &hz)
		zones = append(zones, zoneToWire(hz))
	}
	if maxStr, _ := nr.Params["MaxItems"].(string); maxStr != "" {
		if max, err := strconv.Atoi(maxStr); err == nil && max > 0 && max < len(zones) {
			zones = zones[:max]
		}
	}
	return provider.OK(map[string]any{"HostedZones": zones}), nil
}

func (p *DNSProvider) DeleteHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := cleanZoneID(strParam(nr.Params, "Id"))
	if err := p.resources.Delete(ctx, rtHostedZone, id); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchHostedZone", Message: "Hosted zone not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(map[string]any{
		"DeleteHostedZoneResponse": true,
		"Status":                   "INSYNC",
		"SubmittedAt":              time.Now().UTC().Format(time.RFC3339),
		"ChangeId":                 newChangeID(),
	}), nil
}

// ─── Resource Record Sets ─────────────────────────────────────────────────────

type rrSet struct {
	ZoneId  string   `json:"ZoneId"`
	Name    string   `json:"Name"`
	Type    string   `json:"Type"`
	TTL     int      `json:"TTL"`
	Records []string `json:"Records"`
}

func rrID(zoneId, name, rrtype string) string {
	return fmt.Sprintf("%s/%s/%s", zoneId, strings.TrimSuffix(name, "."), rrtype)
}

func (p *DNSProvider) ChangeResourceRecordSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zoneId := cleanZoneID(strParam(nr.Params, "HostedZoneId"))

	changeID := newChangeID()

	// Typed batch path: codec sets params["Changes"] = []map[string]any{...}
	if rawChanges, ok := nr.Params["Changes"]; ok {
		if changes, ok := rawChanges.([]map[string]any); ok {
			for _, c := range changes {
				p.applyRRSetChange(ctx, zoneId, c)
			}
			p.updateZoneRRSetCount(ctx, zoneId)
			return provider.OK(map[string]any{
				"ChangeInfo":  true,
				"Status":      "INSYNC",
				"SubmittedAt": time.Now().UTC().Format(time.RFC3339),
				"ChangeId":    changeID,
			}), nil
		}
	}

	// Legacy flat-parse fallback (single change body without batch wrapper).
	action := strParam(nr.Params, "Action")
	name := strParam(nr.Params, "Name")
	rrtype := strParam(nr.Params, "Type")
	if name != "" && rrtype != "" {
		p.applyRRSetChange(ctx, zoneId, map[string]any{
			"Action":  action,
			"Name":    name,
			"Type":    rrtype,
			"TTL":     strParam(nr.Params, "TTL"),
			"Records": []string{strParam(nr.Params, "Value")},
		})
	}
	p.updateZoneRRSetCount(ctx, zoneId)
	return provider.OK(map[string]any{
		"ChangeInfo":  true,
		"Status":      "INSYNC",
		"SubmittedAt": time.Now().UTC().Format(time.RFC3339),
		"ChangeId":    changeID,
	}), nil
}

func (p *DNSProvider) applyRRSetChange(ctx context.Context, zoneId string, c map[string]any) {
	name := strParam(c, "Name")
	rrtype := strParam(c, "Type")
	action := strParam(c, "Action")
	if name == "" || rrtype == "" {
		return
	}
	ttl := 300
	if tv := strParam(c, "TTL"); tv != "" {
		if n, err := strconv.Atoi(tv); err == nil {
			ttl = n
		}
	}
	var records []string
	if rv, ok := c["Records"]; ok {
		switch v := rv.(type) {
		case []string:
			records = v
		case []any:
			for _, r := range v {
				if s, ok := r.(string); ok {
					records = append(records, s)
				}
			}
		}
	}
	id := rrID(zoneId, name, rrtype)
	if action == "DELETE" {
		p.resources.Delete(ctx, rtRRSet, id)
		return
	}
	rr := rrSet{ZoneId: zoneId, Name: name, Type: rrtype, TTL: ttl, Records: records}
	data, _ := json.Marshal(rr)
	entry := store.ResourceEntry{Type: rtRRSet, ID: id, Data: data}
	if err := p.resources.Create(ctx, entry); err == store.ErrAlreadyExists {
		p.resources.Update(ctx, entry)
	}
}

func (p *DNSProvider) ListResourceRecordSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zoneId := cleanZoneID(strParam(nr.Params, "HostedZoneId"))
	entries, err := p.resources.List(ctx, rtRRSet, zoneId)
	if err != nil {
		return nil, err
	}
	sets := []map[string]any{}
	for _, e := range entries {
		var rr rrSet
		json.Unmarshal(e.Data, &rr)
		if rr.ZoneId != zoneId {
			continue
		}
		sets = append(sets, map[string]any{
			"Name":            rr.Name,
			"Type":            rr.Type,
			"TTL":             fmt.Sprintf("%d", rr.TTL),
			"ResourceRecords": rr.Records,
		})
	}
	return provider.OK(map[string]any{"ResourceRecordSets": sets}), nil
}

// ─── Health Checks ────────────────────────────────────────────────────────────

type healthCheck struct {
	Id                       string `json:"Id"`
	Type                     string `json:"Type"`
	FullyQualifiedDomainName string `json:"FullyQualifiedDomainName"`
	Port                     int    `json:"Port"`
	ResourcePath             string `json:"ResourcePath"`
}

func hcToWire(hc healthCheck) map[string]any {
	return map[string]any{
		"Id":                       hc.Id,
		"Type":                     hc.Type,
		"FullyQualifiedDomainName": hc.FullyQualifiedDomainName,
		"Port":                     fmt.Sprintf("%d", hc.Port),
	}
}

func (p *DNSProvider) CreateHealthCheck(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := newShortID()
	hc := healthCheck{
		Id:                       id,
		Type:                     strParam(nr.Params, "Type"),
		FullyQualifiedDomainName: strParam(nr.Params, "FullyQualifiedDomainName"),
		Port:                     80,
	}
	data, _ := json.Marshal(hc)
	if err := p.resources.Create(ctx, store.ResourceEntry{Type: rtHealthCheck, ID: id, Data: data}); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{
		HTTPStatus: http.StatusCreated,
		Data:       map[string]any{"HealthCheck": hcToWire(hc)},
	}, nil
}

func (p *DNSProvider) GetHealthCheck(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "Id")
	e, err := p.resources.Get(ctx, rtHealthCheck, id)
	if err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchHealthCheck", Message: "Health check not found", HTTPStatus: http.StatusNotFound}
	}
	if err != nil {
		return nil, err
	}
	var hc healthCheck
	json.Unmarshal(e.Data, &hc)
	return provider.OK(map[string]any{"HealthCheck": hcToWire(hc)}), nil
}

func (p *DNSProvider) ListHealthChecks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, rtHealthCheck, "")
	if err != nil {
		return nil, err
	}
	hcs := []map[string]any{}
	for _, e := range entries {
		var hc healthCheck
		json.Unmarshal(e.Data, &hc)
		hcs = append(hcs, hcToWire(hc))
	}
	return provider.OK(map[string]any{"HealthChecks": hcs}), nil
}

func (p *DNSProvider) DeleteHealthCheck(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr.Params, "Id")
	if err := p.resources.Delete(ctx, rtHealthCheck, id); err == store.ErrNotFound {
		return nil, &model.ProviderError{Code: "NoSuchHealthCheck", Message: "Health check not found", HTTPStatus: http.StatusNotFound}
	}
	return provider.OK(nil), nil
}

func (p *DNSProvider) GetChange(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"Status": "INSYNC"}), nil
}

// updateZoneRRSetCount recounts RRSets for a zone and persists the updated count.
func (p *DNSProvider) updateZoneRRSetCount(ctx context.Context, zoneId string) {
	entries, err := p.resources.List(ctx, rtRRSet, zoneId)
	if err != nil {
		return
	}
	count := 0
	for _, e := range entries {
		var rr rrSet
		if json.Unmarshal(e.Data, &rr) == nil && rr.ZoneId == zoneId {
			count++
		}
	}
	e, err := p.resources.Get(ctx, rtHostedZone, zoneId)
	if err != nil {
		return
	}
	var hz hostedZone
	if json.Unmarshal(e.Data, &hz) != nil {
		return
	}
	hz.ResourceRecordSetCount = count
	data, _ := json.Marshal(hz)
	_ = p.resources.Update(ctx, store.ResourceEntry{Type: rtHostedZone, ID: zoneId, Data: data})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// cleanZoneID strips the /hostedzone/ prefix Route53 SDK sends in path segments.
func cleanZoneID(id string) string {
	id = strings.TrimPrefix(id, "/hostedzone/")
	id = strings.TrimPrefix(id, "/healthcheck/")
	return id
}

// ─── Tagging (13.13) ──────────────────────────────────────────────────────────

// r53TagKey returns the store key for a resource's tags.
// resourceType is "hostedzone" or "healthcheck", resourceID is the bare ID.
func r53TagKey(resourceType, resourceID string) string {
	return resourceType + "/" + resourceID
}

func (p *DNSProvider) loadR53Tags(ctx context.Context, resourceType, resourceID string) map[string]string {
	tags := map[string]string{}
	if e, err := p.resources.Get(ctx, rtR53Tags, r53TagKey(resourceType, resourceID)); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	return tags
}

func (p *DNSProvider) saveR53Tags(ctx context.Context, resourceType, resourceID string, tags map[string]string) {
	data, _ := json.Marshal(tags)
	key := r53TagKey(resourceType, resourceID)
	entry := store.ResourceEntry{Type: rtR53Tags, ID: key, Data: data}
	if err := p.resources.Create(ctx, entry); err != nil {
		if err == store.ErrAlreadyExists {
			_ = p.resources.Update(ctx, entry)
		}
	}
}

func (p *DNSProvider) ChangeTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceType := strParam(nr.Params, "ResourceType")
	resourceID := cleanZoneID(strParam(nr.Params, "ResourceId"))
	if resourceType == "" || resourceID == "" {
		return nil, &model.ProviderError{Code: "InvalidInput", Message: "ResourceType and ResourceId are required", HTTPStatus: http.StatusBadRequest}
	}
	// Validate resource exists
	switch resourceType {
	case "hostedzone":
		if _, err := p.resources.Get(ctx, rtHostedZone, resourceID); err != nil {
			return nil, &model.ProviderError{Code: "NoSuchHostedZone", Message: fmt.Sprintf("No hosted zone found with ID: %s", resourceID), HTTPStatus: http.StatusNotFound}
		}
	case "healthcheck":
		if _, err := p.resources.Get(ctx, rtHealthCheck, resourceID); err != nil {
			return nil, &model.ProviderError{Code: "NoSuchHealthCheck", Message: "Health check not found", HTTPStatus: http.StatusNotFound}
		}
	}
	tags := p.loadR53Tags(ctx, resourceType, resourceID)
	// AddTags
	if rawAdd, ok := nr.Params["AddTags"].([]any); ok {
		for _, t := range rawAdd {
			if m, ok := t.(map[string]any); ok {
				k, _ := m["Key"].(string)
				v, _ := m["Value"].(string)
				if k != "" {
					tags[k] = v
				}
			}
		}
	}
	// RemoveTagKeys
	if rawRemove, ok := nr.Params["RemoveTagKeys"].([]any); ok {
		for _, k := range rawRemove {
			if s, ok := k.(string); ok {
				delete(tags, s)
			}
		}
	}
	p.saveR53Tags(ctx, resourceType, resourceID, tags)
	return provider.OK(map[string]any{"ChangeTagsForResourceResponse": true}), nil
}

func (p *DNSProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceType := strParam(nr.Params, "ResourceType")
	resourceID := cleanZoneID(strParam(nr.Params, "ResourceId"))
	tags := p.loadR53Tags(ctx, resourceType, resourceID)
	tagList := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{
		"ListTagsForResourceResponse": true,
		"ResourceTagSet": map[string]any{
			"ResourceType": resourceType,
			"ResourceId":   resourceID,
			"Tags":         tagList,
		},
	}), nil
}

func (p *DNSProvider) ListTagsForResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resourceType := strParam(nr.Params, "ResourceType")
	var resourceIDs []string
	if raw, ok := nr.Params["ResourceIds"].([]any); ok {
		for _, id := range raw {
			if s, ok := id.(string); ok {
				resourceIDs = append(resourceIDs, cleanZoneID(s))
			}
		}
	}
	sets := make([]map[string]any, 0, len(resourceIDs))
	for _, resourceID := range resourceIDs {
		tags := p.loadR53Tags(ctx, resourceType, resourceID)
		tagList := make([]map[string]any, 0, len(tags))
		for k, v := range tags {
			tagList = append(tagList, map[string]any{"Key": k, "Value": v})
		}
		sets = append(sets, map[string]any{
			"ResourceType": resourceType,
			"ResourceId":   resourceID,
			"Tags":         tagList,
		})
	}
	return provider.OK(map[string]any{
		"ListTagsForResourcesResponse": true,
		"ResourceTagSets":              sets,
	}), nil
}

// Ensure time import is used (Route53 hostedZone uses time.Time).
var _ = time.Now
