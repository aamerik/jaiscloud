// Package acm implements the ACM (Certificate Manager) provider.
package acm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const rtCert = "acm_certificate"

type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ACM.RequestCertificate":      p.RequestCertificate,
		"ACM.DescribeCertificate":     p.DescribeCertificate,
		"ACM.ListCertificates":        p.ListCertificates,
		"ACM.DeleteCertificate":       p.DeleteCertificate,
		"ACM.ImportCertificate":       p.ImportCertificate,
		"ACM.ListTagsForCertificate":  p.ListTagsForCertificate,
		"ACM.AddTagsToCertificate":    p.AddTagsToCertificate,
		"ACM.RemoveTagsFromCertificate": p.RemoveTagsFromCertificate,
		"ACM.RenewCertificate":        p.RenewCertificate,
	}
}

type domainValidationOption struct {
	DomainName       string         `json:"DomainName"`
	ValidationMethod string         `json:"ValidationMethod"`
	ValidationStatus string         `json:"ValidationStatus"`
	ResourceRecord   map[string]any `json:"ResourceRecord"`
}

type certificate struct {
	CertificateARN          string                   `json:"CertificateArn"`
	DomainName              string                   `json:"DomainName"`
	SubjectAlternativeNames []string                 `json:"SubjectAlternativeNames"`
	SerialNumber            string                   `json:"SerialNumber"`
	Status                  string                   `json:"Status"`
	Type                    string                   `json:"Type"`
	KeyAlgorithm            string                   `json:"KeyAlgorithm"`
	InUseBy                 []string                 `json:"InUseBy"`
	Tags                    map[string]string        `json:"Tags"`
	CreatedAt               time.Time                `json:"CreatedAt"`
	IssuedAt                time.Time                `json:"IssuedAt"`
	NotBefore               time.Time                `json:"NotBefore"`
	NotAfter                time.Time                `json:"NotAfter"`
	ValidationMethod        string                   `json:"ValidationMethod"`
	DomainValidationOptions []domainValidationOption `json:"DomainValidationOptions"`
	RenewalEligibility      string                   `json:"RenewalEligibility"`
	KeyUsages               []map[string]any         `json:"KeyUsages"`
	ExtendedKeyUsages       []map[string]any         `json:"ExtendedKeyUsages"`
	Options                 map[string]any           `json:"Options"`
}

func randUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func str(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func acmErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

func certToWire(c certificate) map[string]any {
	dvo := make([]map[string]any, 0, len(c.DomainValidationOptions))
	for _, d := range c.DomainValidationOptions {
		dvo = append(dvo, map[string]any{
			"DomainName":       d.DomainName,
			"ValidationMethod": d.ValidationMethod,
			"ValidationStatus": d.ValidationStatus,
			"ResourceRecord":   d.ResourceRecord,
		})
	}
	keyUsages := c.KeyUsages
	if keyUsages == nil {
		keyUsages = []map[string]any{}
	}
	extKeyUsages := c.ExtendedKeyUsages
	if extKeyUsages == nil {
		extKeyUsages = []map[string]any{}
	}
	options := c.Options
	if options == nil {
		options = map[string]any{}
	}
	return map[string]any{
		"CertificateArn":          c.CertificateARN,
		"DomainName":              c.DomainName,
		"SubjectAlternativeNames": c.SubjectAlternativeNames,
		"SerialNumber":            c.SerialNumber,
		"Status":                  c.Status,
		"Type":                    c.Type,
		"KeyAlgorithm":            c.KeyAlgorithm,
		"InUseBy":                 c.InUseBy,
		"CreatedAt":               c.CreatedAt.Unix(),
		"IssuedAt":                c.IssuedAt.Unix(),
		"NotBefore":               c.NotBefore.Unix(),
		"NotAfter":                c.NotAfter.Unix(),
		"DomainValidationOptions": dvo,
		"RenewalEligibility":      c.RenewalEligibility,
		"KeyUsages":               keyUsages,
		"ExtendedKeyUsages":       extKeyUsages,
		"Options":                 options,
	}
}

func (p *Provider) loadCert(ctx context.Context, arn string) (certificate, error) {
	e, err := p.resources.Get(ctx, rtCert, arn)
	if err != nil {
		return certificate{}, acmErr("ResourceNotFoundException", "Certificate not found: "+arn, http.StatusBadRequest)
	}
	var c certificate
	_ = json.Unmarshal(e.Data, &c)
	return c, nil
}

func (p *Provider) saveCert(ctx context.Context, c certificate) {
	data, _ := json.Marshal(c)
	entry := store.ResourceEntry{Type: rtCert, ID: c.CertificateARN, Data: data}
	if err := p.resources.Create(ctx, entry); err == store.ErrAlreadyExists {
		p.resources.Update(ctx, entry)
	}
}

func (p *Provider) RequestCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	domain := str(nr.Params, "DomainName")
	if domain == "" {
		return nil, acmErr("InvalidParameterException", "DomainName is required", http.StatusBadRequest)
	}
	method := str(nr.Params, "ValidationMethod")
	if method == "" {
		method = "DNS"
	}
	var sans []string
	if raw, ok := nr.Params["SubjectAlternativeNames"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				sans = append(sans, s)
			}
		}
	}
	now := time.Now().UTC()
	arn := nr.ResourceID("acm-certificate", randUUID())
	c := certificate{
		CertificateARN:          arn,
		DomainName:              domain,
		SubjectAlternativeNames: sans,
		SerialNumber:            randHex(16),
		Status:                  "ISSUED",
		Type:                    "AMAZON_ISSUED",
		KeyAlgorithm:            "RSA_2048",
		InUseBy:                 []string{},
		Tags:                    map[string]string{},
		CreatedAt:               now,
		IssuedAt:                now,
		NotBefore:               now,
		NotAfter:                now.Add(365 * 24 * time.Hour),
		ValidationMethod:        method,
		DomainValidationOptions: []domainValidationOption{
			{
				DomainName:       domain,
				ValidationMethod: method,
				ValidationStatus: "SUCCESS",
				ResourceRecord: map[string]any{
					"Name":  "_acme-challenge." + domain,
					"Type":  "CNAME",
					"Value": "mock-validation-value",
				},
			},
		},
		RenewalEligibility: "INELIGIBLE",
		KeyUsages:          []map[string]any{{"Name": "DIGITAL_SIGNATURE"}},
		ExtendedKeyUsages:  []map[string]any{{"Name": "TLS_WEB_SERVER_AUTHENTICATION"}},
		Options:            map[string]any{"CertificateTransparencyLoggingPreference": "ENABLED"},
	}
	p.saveCert(ctx, c)
	return provider.OK(map[string]any{"CertificateArn": arn}), nil
}

func (p *Provider) DescribeCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "CertificateArn")
	c, err := p.loadCert(ctx, arn)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Certificate": certToWire(c)}), nil
}

func (p *Provider) ListCertificates(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, rtCert, "")
	var certs []map[string]any
	for _, e := range entries {
		var c certificate
		if json.Unmarshal(e.Data, &c) == nil {
			certs = append(certs, map[string]any{
				"CertificateArn": c.CertificateARN,
				"DomainName":     c.DomainName,
			})
		}
	}
	if certs == nil {
		certs = []map[string]any{}
	}
	return provider.OK(map[string]any{"CertificateSummaryList": certs}), nil
}

func (p *Provider) DeleteCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "CertificateArn")
	c, err := p.loadCert(ctx, arn)
	if err != nil {
		return nil, err
	}
	if len(c.InUseBy) > 0 {
		return nil, acmErr("ResourceInUseException", "Certificate is in use by: "+c.InUseBy[0], http.StatusBadRequest)
	}
	_ = p.resources.Delete(ctx, rtCert, arn)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ImportCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	existingARN := str(nr.Params, "CertificateArn")
	domain := str(nr.Params, "DomainName")
	if domain == "" {
		domain = "imported.example.com"
	}
	now := time.Now().UTC()
	var arn string
	if existingARN != "" {
		arn = existingARN
	} else {
		arn = nr.ResourceID("acm-certificate", randUUID())
	}
	c := certificate{
		CertificateARN:          arn,
		DomainName:              domain,
		SubjectAlternativeNames: []string{},
		SerialNumber:            randHex(16),
		Status:                  "ISSUED",
		Type:                    "IMPORTED",
		KeyAlgorithm:            "RSA_2048",
		InUseBy:                 []string{},
		Tags:                    map[string]string{},
		CreatedAt:               now,
		IssuedAt:                now,
		NotBefore:               now,
		NotAfter:                now.Add(365 * 24 * time.Hour),
	}
	p.saveCert(ctx, c)
	return provider.OK(map[string]any{"CertificateArn": arn}), nil
}

func (p *Provider) AddTagsToCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "CertificateArn")
	c, err := p.loadCert(ctx, arn)
	if err != nil {
		return nil, err
	}
	if raw, ok := nr.Params["Tags"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				v, _ := m["Value"].(string)
				if k != "" {
					c.Tags[k] = v
				}
			}
		}
	}
	p.saveCert(ctx, c)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) RemoveTagsFromCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "CertificateArn")
	c, err := p.loadCert(ctx, arn)
	if err != nil {
		return nil, err
	}
	if raw, ok := nr.Params["Tags"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				delete(c.Tags, k)
			}
		}
	}
	p.saveCert(ctx, c)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "CertificateArn")
	c, err := p.loadCert(ctx, arn)
	if err != nil {
		return nil, err
	}
	tags := make([]map[string]any, 0, len(c.Tags))
	for k, v := range c.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}

func (p *Provider) RenewCertificate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := str(nr.Params, "CertificateArn")
	if _, err := p.loadCert(ctx, arn); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}
