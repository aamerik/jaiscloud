package object

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	objectstore "jaiscloud/internal/aws/store/object"
)

// ─── P2-4: ACLs ──────────────────────────────────────────────────────────────

type s3ACL struct {
	Owner  s3ACLOwner `json:"owner"`
	Grants []s3Grant  `json:"grants"`
}
type s3ACLOwner struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}
type s3Grant struct {
	Permission string    `json:"permission"`
	Grantee    s3Grantee `json:"grantee"`
}
type s3Grantee struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
	URI  string `json:"uri,omitempty"`
}

func resolveACL(cannedACL, ownerID string) string {
	if ownerID == "" {
		ownerID = "owner"
	}
	acl := s3ACL{Owner: s3ACLOwner{ID: ownerID, DisplayName: ownerID}}
	switch cannedACL {
	case "public-read":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
			{Permission: "READ", Grantee: s3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/global/AllUsers"}},
		}
	case "public-read-write":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
			{Permission: "READ", Grantee: s3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/global/AllUsers"}},
			{Permission: "WRITE", Grantee: s3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/global/AllUsers"}},
		}
	case "authenticated-read":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
			{Permission: "READ", Grantee: s3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"}},
		}
	case "bucket-owner-read":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
			{Permission: "READ", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
		}
	case "bucket-owner-full-control":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
		}
	case "aws-exec-read":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
			{Permission: "READ", Grantee: s3Grantee{Type: "CanonicalUser", ID: "ec2"}},
		}
	case "log-delivery-write":
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
			{Permission: "WRITE", Grantee: s3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/s3/LogDelivery"}},
			{Permission: "READ_ACP", Grantee: s3Grantee{Type: "Group", URI: "http://acs.amazonaws.com/groups/s3/LogDelivery"}},
		}
	default: // "private" or ""
		acl.Grants = []s3Grant{
			{Permission: "FULL_CONTROL", Grantee: s3Grantee{Type: "CanonicalUser", ID: ownerID}},
		}
	}
	raw, _ := json.Marshal(acl)
	return string(raw)
}

func aclToResponseData(aclJSON, ownerID string) map[string]any {
	var acl s3ACL
	if err := json.Unmarshal([]byte(aclJSON), &acl); err != nil || len(acl.Grants) == 0 {
		// Default: full control for owner
		id := ownerID
		if id == "" {
			id = "owner"
		}
		return map[string]any{
			"Owner":  map[string]any{"ID": id, "DisplayName": id},
			"Grants": []map[string]any{{"GranteeType": "CanonicalUser", "GranteeID": id, "Permission": "FULL_CONTROL"}},
		}
	}
	grants := make([]map[string]any, 0, len(acl.Grants))
	for _, g := range acl.Grants {
		grants = append(grants, map[string]any{
			"GranteeType": g.Grantee.Type,
			"GranteeID":   g.Grantee.ID,
			"GranteeURI":  g.Grantee.URI,
			"Permission":  g.Permission,
		})
	}
	return map[string]any{
		"Owner":  map[string]any{"ID": acl.Owner.ID, "DisplayName": acl.Owner.DisplayName},
		"Grants": grants,
	}
}

func (p *ObjectProvider) GetBucketAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	aclJSON, _ := meta["acl"].(string)
	return provider.OK(aclToResponseData(aclJSON, nr.AccountID)), nil
}

func (p *ObjectProvider) PutBucketAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	cannedACL := strParam(nr.Params, "_acl")

	// P4.3: Check BucketOwnerEnforced — disallow ACL modifications
	if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
		if ownership, _ := bucketMeta["ownership_controls"].(string); ownership == "BucketOwnerEnforced" {
			if cannedACL != "" && cannedACL != "bucket-owner-full-control" {
				return nil, model.NewProviderError("AccessControlListNotSupported",
					"The bucket does not allow ACLs", 400)
			}
		}
	}

	var aclJSON string
	if cannedACL != "" {
		aclJSON = resolveACL(cannedACL, nr.AccountID)
	} else {
		body, _ := nr.Params["_body"].([]byte)
		if len(body) > 0 {
			var req struct {
				XMLName xml.Name `xml:"AccessControlPolicy"`
				Owner   struct {
					ID          string `xml:"ID"`
					DisplayName string `xml:"DisplayName"`
				} `xml:"Owner"`
				AccessControlList struct {
					Grants []struct {
						Grantee struct {
							Type string `xml:"type,attr"`
							ID   string `xml:"ID"`
							URI  string `xml:"URI"`
						} `xml:"Grantee"`
						Permission string `xml:"Permission"`
					} `xml:"Grant"`
				} `xml:"AccessControlList"`
			}
			if err := xml.Unmarshal(body, &req); err != nil {
				return nil, model.NewProviderError("MalformedACLError",
					"The XML you provided was not well-formed or did not validate against our published schema", 400)
			}
			acl := s3ACL{Owner: s3ACLOwner{ID: req.Owner.ID, DisplayName: req.Owner.DisplayName}}
			for _, g := range req.AccessControlList.Grants {
				acl.Grants = append(acl.Grants, s3Grant{
					Permission: g.Permission,
					Grantee: s3Grantee{
						Type: g.Grantee.Type,
						ID:   g.Grantee.ID,
						URI:  g.Grantee.URI,
					},
				})
			}
			raw, _ := json.Marshal(acl)
			aclJSON = string(raw)
		} else {
			aclJSON = resolveACL("private", nr.AccountID)
		}
	}

	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["acl"] = aclJSON
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetObjectAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	return provider.OK(aclToResponseData(m.ACL, nr.AccountID)), nil
}

func (p *ObjectProvider) PutObjectAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	cannedACL := strParam(nr.Params, "_acl")

	// P4.3: Check BucketOwnerEnforced — disallow all object ACL modifications
	if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
		if ownership, _ := bucketMeta["ownership_controls"].(string); ownership == "BucketOwnerEnforced" {
			body, _ := nr.Params["_body"].([]byte)
			if cannedACL != "" || len(body) > 0 {
				return nil, model.NewProviderError("AccessControlListNotSupported",
					"The bucket does not allow ACLs", 400)
			}
		}
	}

	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	m.ACL = resolveACL(cannedACL, nr.AccountID)
	if err := p.meta.PutObjectMeta(ctx, bucket, key, m); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// ─── P4.3: Ownership Controls ─────────────────────────────────────────────────

func (p *ObjectProvider) PutBucketOwnershipControls(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"OwnershipControls"`
		Rules   []struct {
			ObjectOwnership string `xml:"ObjectOwnership"`
		} `xml:"Rule"`
	}
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Rules) == 0 {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	ownership := req.Rules[0].ObjectOwnership
	if ownership != "BucketOwnerEnforced" && ownership != "BucketOwnerPreferred" && ownership != "ObjectWriter" {
		return nil, model.NewProviderError("InvalidArgument", "Invalid ObjectOwnership value", 400)
	}
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["ownership_controls"] = ownership
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketOwnershipControls(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	ownership, _ := meta["ownership_controls"].(string)
	if ownership == "" {
		return nil, model.NewProviderError("OwnershipControlsNotFoundError",
			"The bucket does not have OwnershipControls", 404)
	}
	return provider.OK(map[string]any{"ObjectOwnership": ownership}), nil
}

func (p *ObjectProvider) DeleteBucketOwnershipControls(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "ownership_controls")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── P2-5: Lifecycle ──────────────────────────────────────────────────────────

func (p *ObjectProvider) PutBucketLifecycleConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"LifecycleConfiguration"`
		Rules   []struct {
			ID     string `xml:"ID"`
			Status string `xml:"Status"`
			Filter struct {
				Prefix string `xml:"Prefix"`
			} `xml:"Filter"`
			Expiration struct {
				Days int    `xml:"Days"`
				Date string `xml:"Date"`
			} `xml:"Expiration"`
		} `xml:"Rule"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	rules := make([]any, 0, len(req.Rules))
	for _, r := range req.Rules {
		rules = append(rules, map[string]any{
			"ID": r.ID, "Status": r.Status,
			"Prefix":         r.Filter.Prefix,
			"ExpirationDays": r.Expiration.Days,
			"ExpirationDate": r.Expiration.Date,
		})
	}
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["lifecycle_rules"] = rules
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketLifecycleConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	rules, _ := meta["lifecycle_rules"].([]any)
	if rules == nil {
		return nil, model.NewProviderError("NoSuchLifecycleConfiguration",
			"The lifecycle configuration does not exist", 404)
	}
	return provider.OK(map[string]any{"LifecycleRules": rules}), nil
}

func (p *ObjectProvider) DeleteBucketLifecycle(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "lifecycle_rules")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// computeLifecycleExpiration returns the x-amz-expiration header value if a matching
// lifecycle rule is found. Returns "" if no rule matches.
func computeLifecycleExpiration(bucketMeta map[string]any, key string, lastModified time.Time) string {
	rulesRaw, ok := bucketMeta["lifecycle_rules"]
	if !ok {
		return ""
	}
	var rules []map[string]any
	switch v := rulesRaw.(type) {
	case []map[string]any:
		rules = v
	case []any:
		for _, r := range v {
			if m, ok := r.(map[string]any); ok {
				rules = append(rules, m)
			}
		}
	}
	for _, rule := range rules {
		status, _ := rule["Status"].(string)
		if status != objectstore.VersioningEnabled {
			continue
		}
		prefix, _ := rule["Prefix"].(string)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		days := 0
		switch d := rule["ExpirationDays"].(type) {
		case int:
			days = d
		case float64:
			days = int(d)
		}
		if days > 0 {
			expiry := lastModified.Add(time.Duration(days) * 24 * time.Hour)
			return fmt.Sprintf(`expiry-date="%s", rule-id="%s"`,
				expiry.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
				fmt.Sprint(rule["ID"]))
		}
	}
	return ""
}

// ─── P2-6: CORS ───────────────────────────────────────────────────────────────

func (p *ObjectProvider) PutBucketCors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"CORSConfiguration"`
		Rules   []struct {
			AllowedOrigin []string `xml:"AllowedOrigin"`
			AllowedMethod []string `xml:"AllowedMethod"`
			AllowedHeader []string `xml:"AllowedHeader"`
			ExposeHeader  []string `xml:"ExposeHeader"`
			MaxAgeSeconds int      `xml:"MaxAgeSeconds"`
		} `xml:"CORSRule"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	rules := make([]any, 0, len(req.Rules))
	for _, r := range req.Rules {
		rules = append(rules, map[string]any{
			"AllowedOrigins": r.AllowedOrigin,
			"AllowedMethods": r.AllowedMethod,
			"AllowedHeaders": r.AllowedHeader,
			"ExposeHeaders":  r.ExposeHeader,
			"MaxAgeSeconds":  r.MaxAgeSeconds,
		})
	}
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["cors_rules"] = rules
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketCors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	rules, _ := meta["cors_rules"].([]any)
	if rules == nil {
		return nil, model.NewProviderError("NoSuchCORSConfiguration",
			"The CORS configuration does not exist", 404)
	}
	return provider.OK(map[string]any{"CORSRules": rules}), nil
}

func (p *ObjectProvider) DeleteBucketCors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "cors_rules")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// GetBucketCORSRules returns the CORS rules for a bucket (used by gateway CORS interceptor).
func (p *ObjectProvider) GetBucketCORSRules(bucket string) []map[string]any {
	meta, err := p.meta.GetBucket(context.Background(), bucket)
	if err != nil {
		return nil
	}
	switch v := meta["cors_rules"].(type) {
	case []map[string]any:
		return v
	case []any:
		var rules []map[string]any
		for _, r := range v {
			if m, ok := r.(map[string]any); ok {
				rules = append(rules, m)
			}
		}
		return rules
	}
	return nil
}

// ─── P4.12: GetObjectAttributes ───────────────────────────────────────────────

func (p *ObjectProvider) GetObjectAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	attrs := strParam(nr.Params, "_object_attributes")

	requestedVersionID := strParam(nr.Params, "versionId")
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)

	var m objectstore.ObjectMeta
	if (vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended) && requestedVersionID != "" {
		var err error
		m, err = p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
		if err != nil {
			return nil, model.NewProviderError("NoSuchVersion", "The specified version does not exist", 404)
		}
	} else {
		var err error
		m, err = p.meta.GetObjectMeta(ctx, bucket, key)
		if err != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
	}

	attrSet := map[string]bool{}
	for _, a := range strings.Split(attrs, ",") {
		attrSet[strings.TrimSpace(a)] = true
	}

	data := map[string]any{
		"LastModified": m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}
	if attrSet["ETag"] {
		data["ETag"] = m.ETag
	}
	if attrSet["ObjectSize"] {
		data["ObjectSize"] = m.Size
	}
	if attrSet["StorageClass"] {
		data["StorageClass"] = m.StorageClass
	}
	if attrSet["Checksum"] {
		cksum := map[string]any{}
		if m.ChecksumAlgorithm != "" && m.ChecksumValue != "" {
			switch m.ChecksumAlgorithm {
			case "CRC32":
				cksum["ChecksumCRC32"] = m.ChecksumValue
			case "CRC32C":
				cksum["ChecksumCRC32C"] = m.ChecksumValue
			case "SHA1":
				cksum["ChecksumSHA1"] = m.ChecksumValue
			case "SHA256":
				cksum["ChecksumSHA256"] = m.ChecksumValue
			}
		} else if m.CRC32 != "" {
			cksum["ChecksumCRC32"] = m.CRC32
		}
		if len(cksum) > 0 {
			data["Checksum"] = cksum
		}
	}
	if m.VersionID != "" {
		data["_version_id"] = m.VersionID
	}
	return provider.OK(data), nil
}

// ─── Bucket Policy / Website / Logging / Replication (P1.16-1.19) ────────────

func (p *ObjectProvider) PutBucketPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["policy"] = string(body)
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	policy, _ := meta["policy"].(string)
	if policy == "" {
		return nil, model.NewProviderError("NoSuchBucketPolicy", "The bucket policy does not exist", 404)
	}
	return provider.OK(map[string]any{"_raw_json": policy}), nil
}

func (p *ObjectProvider) DeleteBucketPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "policy")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) PutBucketWebsite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["website_config"] = string(body)
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketWebsite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	cfg, _ := meta["website_config"].(string)
	if cfg == "" {
		return nil, model.NewProviderError("NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration", 404)
	}
	return provider.OK(map[string]any{"_raw_xml": cfg}), nil
}

func (p *ObjectProvider) DeleteBucketWebsite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "website_config")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) PutBucketLogging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["logging_config"] = string(body)
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketLogging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	cfg, _ := meta["logging_config"].(string)
	if cfg == "" {
		cfg = "<BucketLoggingStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"
	}
	return provider.OK(map[string]any{"_raw_xml": cfg}), nil
}

func (p *ObjectProvider) PutBucketReplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["replication_config"] = string(body)
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketReplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	cfg, _ := meta["replication_config"].(string)
	if cfg == "" {
		return nil, model.NewProviderError("ReplicationConfigurationNotFoundError", "The replication configuration was not found", 404)
	}
	return provider.OK(map[string]any{"_raw_xml": cfg}), nil
}

func (p *ObjectProvider) DeleteBucketReplication(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "replication_config")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── P15.9: SelectObjectContent ──────────────────────────────────────────────

// selectRequest is the XML body of a SelectObjectContent request.
type selectRequest struct {
	Expression     string `xml:"Expression"`
	ExpressionType string `xml:"ExpressionType"`
	InputSerialization struct {
		CSV *struct {
			FileHeaderInfo             string `xml:"FileHeaderInfo"`
			RecordDelimiter            string `xml:"RecordDelimiter"`
			FieldDelimiter             string `xml:"FieldDelimiter"`
			QuoteCharacter             string `xml:"QuoteCharacter"`
			AllowQuotedRecordDelimiter string `xml:"AllowQuotedRecordDelimiter"`
		} `xml:"CSV"`
		JSON *struct {
			Type string `xml:"Type"` // DOCUMENT | LINES
		} `xml:"JSON"`
	} `xml:"InputSerialization"`
	OutputSerialization struct {
		CSV *struct {
			FieldDelimiter  string `xml:"FieldDelimiter"`
			RecordDelimiter string `xml:"RecordDelimiter"`
		} `xml:"CSV"`
		JSON *struct {
			RecordDelimiter string `xml:"RecordDelimiter"`
		} `xml:"JSON"`
	} `xml:"OutputSerialization"`
}

func (p *ObjectProvider) SelectObjectContent(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")

	if _, err := p.meta.GetObjectMeta(ctx, bucket, key); err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}

	rc, err := p.blobs.GetStream(ctx, bucket, key, 0, -1)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	defer rc.Close()

	objectBytes, err := io.ReadAll(rc)
	if err != nil {
		return nil, model.NewProviderError("InternalError", "Failed to read object", 500)
	}

	// Parse the select request XML from the request body.
	var req selectRequest
	reqBody, _ := nr.Params["_body"].([]byte)
	if len(reqBody) > 0 {
		if xmlErr := xml.Unmarshal(reqBody, &req); xmlErr != nil {
			return nil, model.NewProviderError("InvalidRequest", "Failed to parse SelectObjectContent request: "+xmlErr.Error(), 400)
		}
	}

	payload, processErr := s3SelectProcess(objectBytes, &req)
	if processErr != nil {
		return nil, model.NewProviderError("InternalError", "Failed to process select query: "+processErr.Error(), 500)
	}

	return provider.OK(map[string]any{"_select_payload": payload}), nil
}

// s3SelectProcess applies the SELECT expression to the object bytes and returns
// the result rows formatted according to OutputSerialization.
// For SELECT * (and any SELECT query), all rows are returned.
// Supports CSV and JSON-lines input.
func s3SelectProcess(data []byte, req *selectRequest) ([]byte, error) {
	isCSVInput := req.InputSerialization.CSV != nil
	isJSONInput := req.InputSerialization.JSON != nil

	var rows [][]string
	var headers []string

	switch {
	case isCSVInput:
		csvSpec := req.InputSerialization.CSV
		fieldDelim := ","
		if csvSpec.FieldDelimiter != "" {
			fieldDelim = csvSpec.FieldDelimiter
		}
		r := csv.NewReader(bytes.NewReader(data))
		r.Comma = rune(fieldDelim[0])
		r.LazyQuotes = true
		r.TrimLeadingSpace = true

		allRecords, err := r.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("CSV parse error: %w", err)
		}

		headerInfo := strings.ToUpper(csvSpec.FileHeaderInfo)
		switch headerInfo {
		case "USE":
			if len(allRecords) > 0 {
				headers = allRecords[0]
				rows = allRecords[1:]
			}
		case "IGNORE":
			if len(allRecords) > 0 {
				rows = allRecords[1:]
			}
		default: // NONE or empty
			rows = allRecords
		}

	case isJSONInput:
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var obj map[string]any
			if err := json.Unmarshal([]byte(line), &obj); err != nil {
				// Try as JSON array
				var arr []any
				if err2 := json.Unmarshal([]byte(line), &arr); err2 != nil {
					continue
				}
				row := make([]string, len(arr))
				for i, v := range arr {
					row[i] = fmt.Sprintf("%v", v)
				}
				rows = append(rows, row)
				continue
			}
			if len(headers) == 0 {
				for k := range obj {
					headers = append(headers, k)
				}
			}
			row := make([]string, len(headers))
			for i, h := range headers {
				if v, ok := obj[h]; ok {
					switch vt := v.(type) {
					case string:
						row[i] = vt
					default:
						b, _ := json.Marshal(v)
						row[i] = string(b)
					}
				}
			}
			rows = append(rows, row)
		}

	default:
		// Unknown or unspecified input format — return raw bytes as-is.
		return data, nil
	}

	// Format output according to OutputSerialization.
	isJSONOutput := req.OutputSerialization.JSON != nil
	var outBuf bytes.Buffer

	if isJSONOutput {
		recDelim := "\n"
		if req.OutputSerialization.JSON.RecordDelimiter != "" {
			recDelim = req.OutputSerialization.JSON.RecordDelimiter
		}
		for _, row := range rows {
			if len(headers) > 0 && len(row) == len(headers) {
				obj := make(map[string]any, len(headers))
				for i, h := range headers {
					obj[h] = row[i]
				}
				b, _ := json.Marshal(obj)
				outBuf.Write(b)
			} else {
				b, _ := json.Marshal(row)
				outBuf.Write(b)
			}
			outBuf.WriteString(recDelim)
		}
	} else {
		// Default: CSV output
		fieldDelim := ","
		if req.OutputSerialization.CSV != nil && req.OutputSerialization.CSV.FieldDelimiter != "" {
			fieldDelim = req.OutputSerialization.CSV.FieldDelimiter
		}
		w := csv.NewWriter(&outBuf)
		w.Comma = rune(fieldDelim[0])
		for _, row := range rows {
			_ = w.Write(row)
		}
		w.Flush()
	}

	return outBuf.Bytes(), nil
}
