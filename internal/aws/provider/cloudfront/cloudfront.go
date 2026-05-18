// Package cloudfront implements the CloudFront distribution provider.
package cloudfront

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtDistribution = "cloudfront_distribution"

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudFront.CreateDistribution":    p.CreateDistribution,
		"CloudFront.GetDistribution":       p.GetDistribution,
		"CloudFront.UpdateDistribution":    p.UpdateDistribution,
		"CloudFront.DeleteDistribution":    p.DeleteDistribution,
		"CloudFront.ListDistributions":     p.ListDistributions,
		"CloudFront.TagResource":           p.TagResource,
		"CloudFront.UntagResource":         p.UntagResource,
		"CloudFront.ListTagsForResource":   p.ListTagsForResource,
	}
}

type distribution struct {
	ID              string            `json:"Id"`
	ARN             string            `json:"ARN"`
	Status          string            `json:"Status"`
	DomainName      string            `json:"DomainName"`
	Enabled         bool              `json:"Enabled"`
	Comment         string            `json:"Comment"`
	PriceClass      string            `json:"PriceClass"`
	HttpVersion     string            `json:"HttpVersion"`
	CallerReference string            `json:"CallerReference"`
	ETag            string            `json:"ETag"`
	Tags            map[string]string `json:"Tags"`
	LastModified    time.Time         `json:"LastModifiedTime"`
	RawConfig       string            `json:"RawConfig"`
}

func randAlphaNum(n int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	rand.Read(b)
	for i, v := range b {
		b[i] = chars[int(v)%len(chars)]
	}
	return string(b)
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newDistributionID() string  { return "D" + randAlphaNum(13) }
func newETag() string            { return randHex(16) }

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func cfErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func distToWire(d distribution) map[string]any {
	return map[string]any{
		"Id":           d.ID,
		"ARN":          d.ARN,
		"Status":       d.Status,
		"DomainName":   d.DomainName,
		"LastModifiedTime": d.LastModified.UTC().Format(time.RFC3339),
		"_etag":        d.ETag,
		"DistributionConfig": map[string]any{
			"CallerReference": d.CallerReference,
			"Comment":         d.Comment,
			"Enabled":         d.Enabled,
			"PriceClass":      d.PriceClass,
			"HttpVersion":     d.HttpVersion,
		},
	}
}

func (p *Provider) loadDist(ctx context.Context, account, region, id string) (distribution, error) {
	e, err := p.resources.Get(ctx, account, region, rtDistribution, id)
	if err != nil {
		return distribution{}, cfErr("NoSuchDistribution", "Distribution not found: "+id, http.StatusNotFound)
	}
	var d distribution
	_ = json.Unmarshal(e.Data, &d)
	return d, nil
}

func (p *Provider) saveDist(ctx context.Context, account, region string, d distribution) {
	data, _ := json.Marshal(d)
	entry := store.ResourceEntry{Type: rtDistribution, ID: d.ID, Data: data}
	if err := p.resources.Create(ctx, account, region, entry); err == store.ErrAlreadyExists {
		p.resources.Update(ctx, account, region, entry)
	}
}

// findByCallerRef checks for duplicate CallerReference.
func (p *Provider) findByCallerRef(ctx context.Context, account, region, ref string) bool {
	entries, _ := p.resources.List(ctx, account, region, rtDistribution, "")
	for _, e := range entries {
		var d distribution
		if json.Unmarshal(e.Data, &d) == nil && d.CallerReference == ref {
			return true
		}
	}
	return false
}

func (p *Provider) CreateDistribution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	callerRef := str(nr.Params, "CallerReference")
	if callerRef != "" && p.findByCallerRef(ctx, nr.AccountID, "", callerRef) {
		return nil, cfErr("DistributionAlreadyExists", "A distribution with this CallerReference already exists", http.StatusConflict)
	}
	enabled := str(nr.Params, "Enabled") != "false"
	id := newDistributionID()
	d := distribution{
		ID:              id,
		ARN:             nr.ResourceID("cloudfront-distribution", id),
		Status:          "Deployed",
		DomainName:      strings.ToLower(id) + ".cloudfront.net",
		Enabled:         enabled,
		Comment:         str(nr.Params, "Comment"),
		PriceClass:      str(nr.Params, "PriceClass"),
		HttpVersion:     str(nr.Params, "HttpVersion"),
		CallerReference: callerRef,
		ETag:            newETag(),
		Tags:            extractTags(nr.Params),
		LastModified:    time.Now().UTC(),
	}
	if d.HttpVersion == "" {
		d.HttpVersion = "http2"
	}
	if d.PriceClass == "" {
		d.PriceClass = "PriceClass_All"
	}
	p.saveDist(ctx, nr.AccountID, "", d)
	w := distToWire(d)
	w["Location"] = "/2020-05-31/distribution/" + id
	return provider.OK(w), nil
}

func (p *Provider) GetDistribution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "Id")
	d, err := p.loadDist(ctx, nr.AccountID, "", id)
	if err != nil {
		return nil, err
	}
	return provider.OK(distToWire(d)), nil
}

func (p *Provider) UpdateDistribution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "Id")
	ifMatch := str(nr.Params, "IfMatch")
	d, err := p.loadDist(ctx, nr.AccountID, "", id)
	if err != nil {
		return nil, err
	}
	if ifMatch != "" && ifMatch != d.ETag {
		return nil, cfErr("PreconditionFailed", "The If-Match value does not match the ETag", http.StatusPreconditionFailed)
	}
	if v := str(nr.Params, "Comment"); v != "" {
		d.Comment = v
	}
	if v := str(nr.Params, "Enabled"); v != "" {
		d.Enabled = v != "false"
	}
	d.ETag = newETag()
	d.LastModified = time.Now().UTC()
	p.saveDist(ctx, nr.AccountID, "", d)
	return provider.OK(distToWire(d)), nil
}

func (p *Provider) DeleteDistribution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := str(nr.Params, "Id")
	d, err := p.loadDist(ctx, nr.AccountID, "", id)
	if err != nil {
		return nil, err
	}
	if d.Enabled {
		return nil, cfErr("DistributionNotDisabled", "Distribution must be disabled before deletion", http.StatusConflict)
	}
	_ = p.resources.Delete(ctx, nr.AccountID, "", rtDistribution, id)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListDistributions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, "", rtDistribution, "")
	items := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var d distribution
		if json.Unmarshal(e.Data, &d) == nil {
			items = append(items, distToWire(d))
		}
	}
	return provider.OK(map[string]any{
		"DistributionList": map[string]any{
			"Items":       items,
			"Quantity":    len(items),
			"IsTruncated": false,
		},
	}), nil
}

func (p *Provider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "Resource")
	// Find distribution by ARN
	entries, _ := p.resources.List(ctx, nr.AccountID, "", rtDistribution, "")
	for _, e := range entries {
		var d distribution
		if json.Unmarshal(e.Data, &d) != nil || d.ARN != arn {
			continue
		}
		newTags := extractTags(nr.Params)
		for k, v := range newTags {
			d.Tags[k] = v
		}
		p.saveDist(ctx, nr.AccountID, "", d)
		return provider.OK(map[string]any{}), nil
	}
	return nil, cfErr("NoSuchDistribution", "Resource not found: "+arn, http.StatusNotFound)
}

func (p *Provider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "Resource")
	entries, _ := p.resources.List(ctx, nr.AccountID, "", rtDistribution, "")
	for _, e := range entries {
		var d distribution
		if json.Unmarshal(e.Data, &d) != nil || d.ARN != arn {
			continue
		}
		if keys, ok := nr.Params["Tags"].(map[string]string); ok {
			for k := range keys {
				delete(d.Tags, k)
			}
		}
		p.saveDist(ctx, nr.AccountID, "", d)
		return provider.OK(map[string]any{}), nil
	}
	return nil, cfErr("NoSuchDistribution", "Resource not found: "+arn, http.StatusNotFound)
}

func (p *Provider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "Resource")
	entries, _ := p.resources.List(ctx, nr.AccountID, "", rtDistribution, "")
	for _, e := range entries {
		var d distribution
		if json.Unmarshal(e.Data, &d) != nil || d.ARN != arn {
			continue
		}
		items := make([]map[string]any, 0, len(d.Tags))
		for k, v := range d.Tags {
			items = append(items, map[string]any{"Key": k, "Value": v})
		}
		return provider.OK(map[string]any{"Tags": map[string]any{"Items": items}}), nil
	}
	return nil, cfErr("NoSuchDistribution", "Resource not found: "+arn, http.StatusNotFound)
}

func extractTags(params map[string]any) map[string]string {
	tags := map[string]string{}
	if raw, ok := params["Tags"].(map[string]string); ok {
		return raw
	}
	if raw, ok := params["Tags"].(map[string]any); ok {
		for k, v := range raw {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
	}
	return tags
}
