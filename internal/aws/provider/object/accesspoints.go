package object

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtAccessPoint = "s3_access_point"

var validAPName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,48}[a-z0-9]$`)

type accessPoint struct {
	Name          string    `json:"Name"`
	Bucket        string    `json:"Bucket"`
	ARN           string    `json:"AccessPointArn"`
	Alias         string    `json:"Alias"`
	NetworkOrigin string    `json:"NetworkOrigin"`
	Policy        string    `json:"Policy"`
	CreationDate  time.Time `json:"CreationDate"`
}

func apAlias(name, account string) string {
	h := sha256.Sum256([]byte(name + account))
	return fmt.Sprintf("%s-%x-s3alias", name, h[:8])
}

func apNameFromKey(key string) string {
	// key = "accesspoint/{name}" or "accesspoint/{name}/policy" etc.
	parts := strings.SplitN(key, "/", 3)
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

func apErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func (p *ObjectProvider) loadAP(ctx context.Context, name string) (accessPoint, error) {
	e, err := p.resources.Get(ctx, rtAccessPoint, name)
	if err != nil {
		return accessPoint{}, apErr("NoSuchAccessPoint", "The specified access point does not exist: "+name, http.StatusNotFound)
	}
	var ap accessPoint
	_ = json.Unmarshal(e.Data, &ap)
	return ap, nil
}

func (p *ObjectProvider) saveAP(ctx context.Context, ap accessPoint) error {
	data, _ := json.Marshal(ap)
	entry := store.ResourceEntry{Type: rtAccessPoint, ID: ap.Name, Data: data}
	if err := p.resources.Create(ctx, entry); err == store.ErrAlreadyExists {
		return p.resources.Update(ctx, entry)
	}
	return nil
}

func (p *ObjectProvider) CreateAccessPoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	key := strParam(nr.Params, "_key")
	name := apNameFromKey(key)
	if name == "" || !validAPName.MatchString(name) {
		return nil, apErr("InvalidRequest", "Invalid access point name: "+name, http.StatusBadRequest)
	}
	if _, err := p.resources.Get(ctx, rtAccessPoint, name); err == nil {
		return nil, apErr("AccessPointAlreadyOwnedByYou", "An access point with the name "+name+" already exists", http.StatusConflict)
	}

	// Parse XML body for Bucket.
	bucket := ""
	body, _ := nr.Params["_body"].([]byte)
	if len(body) > 0 {
		var req struct {
			Bucket string `xml:"Bucket"`
		}
		_ = xml.Unmarshal(body, &req)
		bucket = req.Bucket
	}
	if bucket == "" {
		bucket = strParam(nr.Params, "Bucket")
	}
	if bucket != "" {
		if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
			return nil, apErr("NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		}
	}

	account := nr.AccountID
	if v := strParam(nr.Params, "_account_id"); v != "" {
		account = v
	}
	ap := accessPoint{
		Name:          name,
		Bucket:        bucket,
		ARN:           nr.ResourceID("s3-accesspoint", name),
		Alias:         apAlias(name, account),
		NetworkOrigin: "Internet",
		CreationDate:  time.Now().UTC(),
	}
	_ = p.saveAP(ctx, ap)
	return provider.OK(map[string]any{
		"AccessPointArn": ap.ARN,
		"Alias":          ap.Alias,
	}), nil
}

func (p *ObjectProvider) GetAccessPoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := apNameFromKey(strParam(nr.Params, "_key"))
	ap, err := p.loadAP(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"Name":                    ap.Name,
		"Bucket":                  ap.Bucket,
		"AccessPointArn":          ap.ARN,
		"Alias":                   ap.Alias,
		"NetworkOrigin":           ap.NetworkOrigin,
		"CreationDate":            ap.CreationDate.UTC().Format(time.RFC3339),
		"PublicAccessBlockConfiguration": map[string]any{
			"BlockPublicAcls":       true,
			"IgnorePublicAcls":      true,
			"BlockPublicPolicy":     true,
			"RestrictPublicBuckets": true,
		},
	}), nil
}

func (p *ObjectProvider) ListAccessPoints(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "bucket")
	entries, _ := p.resources.List(ctx, rtAccessPoint, "")
	var items []map[string]any
	for _, e := range entries {
		var ap accessPoint
		if json.Unmarshal(e.Data, &ap) != nil {
			continue
		}
		if bucket != "" && ap.Bucket != bucket {
			continue
		}
		items = append(items, map[string]any{
			"Name":           ap.Name,
			"Bucket":         ap.Bucket,
			"AccessPointArn": ap.ARN,
			"Alias":          ap.Alias,
			"NetworkOrigin":  ap.NetworkOrigin,
		})
	}
	if items == nil {
		items = []map[string]any{}
	}
	return provider.OK(map[string]any{
		"AccessPointList": items,
	}), nil
}

func (p *ObjectProvider) DeleteAccessPoint(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := apNameFromKey(strParam(nr.Params, "_key"))
	if _, err := p.loadAP(ctx, name); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, rtAccessPoint, name)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) PutAccessPointPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// key = "accesspoint/{name}/policy"
	parts := strings.SplitN(strParam(nr.Params, "_key"), "/", 3)
	if len(parts) < 2 {
		return nil, apErr("InvalidRequest", "Invalid path", http.StatusBadRequest)
	}
	name := parts[1]
	ap, err := p.loadAP(ctx, name)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["_body"].([]byte)
	ap.Policy = string(body)
	_ = p.saveAP(ctx, ap)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetAccessPointPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parts := strings.SplitN(strParam(nr.Params, "_key"), "/", 3)
	if len(parts) < 2 {
		return nil, apErr("InvalidRequest", "Invalid path", http.StatusBadRequest)
	}
	name := parts[1]
	ap, err := p.loadAP(ctx, name)
	if err != nil {
		return nil, err
	}
	if ap.Policy == "" {
		return nil, apErr("NoSuchAccessPointPolicy", "The specified access point does not have a policy", http.StatusNotFound)
	}
	return provider.OK(map[string]any{"Policy": ap.Policy}), nil
}

func (p *ObjectProvider) DeleteAccessPointPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parts := strings.SplitN(strParam(nr.Params, "_key"), "/", 3)
	if len(parts) < 2 {
		return nil, apErr("InvalidRequest", "Invalid path", http.StatusBadRequest)
	}
	name := parts[1]
	ap, err := p.loadAP(ctx, name)
	if err != nil {
		return nil, err
	}
	ap.Policy = ""
	_ = p.saveAP(ctx, ap)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetAccessPointPolicyStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parts := strings.SplitN(strParam(nr.Params, "_key"), "/", 3)
	if len(parts) < 2 {
		return nil, apErr("InvalidRequest", "Invalid path", http.StatusBadRequest)
	}
	name := parts[1]
	if _, err := p.loadAP(ctx, name); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"PolicyStatus": map[string]any{"IsPublic": false},
	}), nil
}
