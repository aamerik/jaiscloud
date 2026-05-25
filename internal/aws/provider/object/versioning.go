package object

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	objectstore "jaiscloud/internal/aws/store/object"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ─── Tagging helpers ──────────────────────────────────────────────────────────

func validateTags(tags map[string]string, maxCount int) error {
	if len(tags) > maxCount {
		return fmt.Errorf("Object tags cannot be greater than %d", maxCount)
	}
	for k, v := range tags {
		if len(k) < 1 || len(k) > 128 {
			return fmt.Errorf("The TagKey you have provided is invalid")
		}
		if len(v) > 256 {
			return fmt.Errorf("The TagValue you have provided is invalid")
		}
	}
	return nil
}

// bucketTagsFromMeta extracts tags from bucket metadata, handling both
// map[string]string (memory store) and map[string]any (postgres JSONB round-trip).
func bucketTagsFromMeta(meta map[string]any) map[string]string {
	tags := map[string]string{}
	switch t := meta["tags"].(type) {
	case map[string]string:
		for k, v := range t {
			tags[k] = v
		}
	case map[string]any:
		for k, v := range t {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
	}
	return tags
}

func parseTaggingHeader(header string) (map[string]string, error) {
	parsed, err := url.ParseQuery(header)
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(parsed))
	for k, vs := range parsed {
		if len(vs) > 0 {
			tags[k] = vs[0]
		}
	}
	return tags, nil
}

func parseTaggingXML(body []byte) (map[string]string, error) {
	var req struct {
		XMLName xml.Name `xml:"Tagging"`
		TagSet  struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tag"`
		} `xml:"TagSet"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(req.TagSet.Tags))
	for _, t := range req.TagSet.Tags {
		tags[t.Key] = t.Value
	}
	return tags, nil
}

// ─── P2-7: Tagging handlers ───────────────────────────────────────────────────

func (p *ObjectProvider) PutObjectTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	body, _ := nr.Params["_body"].([]byte)
	tags, err := parseTaggingXML(body)
	if err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	if err := validateTags(tags, 10); err != nil {
		return nil, model.NewProviderError("InvalidTag", err.Error(), 400)
	}
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	m.Tags = tags
	if err := p.meta.PutObjectMeta(ctx, bucket, key, m); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetObjectTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	tags := m.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}

func (p *ObjectProvider) DeleteObjectTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	m.Tags = nil
	if err := p.meta.PutObjectMeta(ctx, bucket, key, m); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) PutBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	tags, err := parseTaggingXML(body)
	if err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	if err := validateTags(tags, 50); err != nil {
		return nil, model.NewProviderError("InvalidTag", err.Error(), 400)
	}
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["tags"] = tags
	}); err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	tags := bucketTagsFromMeta(meta)
	return provider.OK(map[string]any{"Tags": tags}), nil
}

func (p *ObjectProvider) DeleteBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "tags")
	}); err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── P2-1: SSE ────────────────────────────────────────────────────────────────

func (p *ObjectProvider) resolveSSE(ctx context.Context, nr *model.NormalizedRequest, bucket string) (encryption, kmsKeyID, ssecKeyMD5 string, err error) {
	sseAlgo := strParam(nr.Params, "_server_side_encryption")
	ssecAlgo := strParam(nr.Params, "_server_side_encryption_customer_algorithm")
	if sseAlgo != "" && ssecAlgo != "" {
		return "", "", "", model.NewProviderError("InvalidArgument",
			"x-amz-server-side-encryption and SSE-C are mutually exclusive", 400)
	}
	if ssecAlgo != "" {
		if ssecAlgo != "AES256" {
			return "", "", "", model.NewProviderError("InvalidEncryptionAlgorithmError",
				"The encryption request you specified is not valid. Supported value: AES256", 400)
		}
		keyB64 := strParam(nr.Params, "_server_side_encryption_customer_key")
		keyMD5 := strParam(nr.Params, "_server_side_encryption_customer_key_md5")
		keyBytes, decErr := base64.StdEncoding.DecodeString(keyB64)
		if decErr != nil || len(keyBytes) != 32 {
			return "", "", "", model.NewProviderError("InvalidArgument",
				"The secret key was invalid for the specified algorithm", 400)
		}
		h := md5.Sum(keyBytes)
		computedMD5 := base64.StdEncoding.EncodeToString(h[:])
		if keyMD5 != computedMD5 {
			return "", "", "", model.NewProviderError("InvalidArgument",
				"The calculated MD5 hash of the key did not match the hash that was provided", 400)
		}
		return "AES256", "", computedMD5, nil
	}
	if sseAlgo != "" {
		kmsKey := strParam(nr.Params, "_server_side_encryption_aws_kms_key_id")
		return sseAlgo, kmsKey, "", nil
	}
	// No explicit SSE — apply bucket default.
	bucketMeta, _ := p.meta.GetBucket(ctx, bucket)
	if rule, ok := bucketMeta["encryption_rule"].(map[string]any); ok {
		if algo, ok := rule["Algorithm"].(string); ok {
			kmsKey, _ := rule["KMSKeyID"].(string)
			return algo, kmsKey, "", nil
		}
	}
	return "AES256", "", "", nil // AWS default since Jan 2023
}

func sseResponseData(data map[string]any, enc, kmsKey, ssecMD5 string) {
	if enc != "" {
		data["_sse"] = enc
	}
	if kmsKey != "" {
		data["_sse_kms_key_id"] = kmsKey
	}
	if ssecMD5 != "" {
		data["_ssec_algo"] = "AES256"
		data["_ssec_key_md5"] = ssecMD5
	}
}

func (p *ObjectProvider) PutBucketEncryption(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"ServerSideEncryptionConfiguration"`
		Rules   []struct {
			Apply struct {
				SSEAlgorithm   string `xml:"SSEAlgorithm"`
				KMSMasterKeyID string `xml:"KMSMasterKeyID"`
			} `xml:"ApplyServerSideEncryptionByDefault"`
			BucketKeyEnabled bool `xml:"BucketKeyEnabled"`
		} `xml:"Rule"`
	}
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Rules) == 0 {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	rule := map[string]any{"Algorithm": req.Rules[0].Apply.SSEAlgorithm}
	if req.Rules[0].Apply.KMSMasterKeyID != "" {
		rule["KMSKeyID"] = req.Rules[0].Apply.KMSMasterKeyID
	}
	if req.Rules[0].BucketKeyEnabled {
		rule["BucketKeyEnabled"] = true
	}
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["encryption_rule"] = rule
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketEncryption(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	rule, _ := meta["encryption_rule"].(map[string]any)
	if rule == nil {
		rule = map[string]any{"Algorithm": "AES256"}
	}
	return provider.OK(map[string]any{"EncryptionRule": rule}), nil
}

func (p *ObjectProvider) DeleteBucketEncryption(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		delete(meta, "encryption_rule")
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── P2-2: Versioning ─────────────────────────────────────────────────────────

func (p *ObjectProvider) PutBucketVersioning(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"VersioningConfiguration"`
		Status  string   `xml:"Status"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	if req.Status != objectstore.VersioningEnabled && req.Status != objectstore.VersioningSuspended {
		return nil, model.NewProviderError("MalformedXML", "Status must be Enabled or Suspended", 400)
	}
	if err := p.meta.SetBucketVersioning(ctx, bucket, req.Status); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetBucketVersioning(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	status, err := p.meta.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return provider.OK(map[string]any{"VersioningStatus": status}), nil
}

func (p *ObjectProvider) ListObjectVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	prefix := strParam(nr.Params, "prefix")
	keyMarker := strParam(nr.Params, "key-marker")
	versionIDMarker := strParam(nr.Params, "version-id-marker")
	maxKeys := intParam(nr.Params, "max-keys", 1000)

	if versionIDMarker != "" && keyMarker == "" {
		return nil, model.NewProviderError("InvalidArgument",
			"A version-id marker cannot be specified without a key marker.", 400)
	}

	versions, truncated, err := p.meta.ListObjectVersions(ctx, bucket, prefix, keyMarker, versionIDMarker, maxKeys)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	// The codec iterates data["Versions"] and uses IsDeleteMarker to emit
	// <DeleteMarker> vs <Version> elements — merge everything into one slice.
	allVersions := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		entry := map[string]any{
			"Key":            v.Key,
			"VersionId":      v.VersionID,
			"IsLatest":       fmt.Sprintf("%v", v.IsLatest),
			"LastModified":   v.LastModified.UTC().Format(time.RFC3339),
			"IsDeleteMarker": v.IsDeleteMarker,
		}
		if !v.IsDeleteMarker {
			entry["ETag"] = v.ETag
			entry["Size"] = fmt.Sprintf("%d", v.Size)
			entry["StorageClass"] = v.StorageClass
		}
		allVersions = append(allVersions, entry)
	}
	return provider.OK(map[string]any{
		"Name":            bucket,
		"Prefix":          prefix,
		"KeyMarker":       keyMarker,
		"VersionIdMarker": versionIDMarker,
		"MaxKeys":         fmt.Sprintf("%d", maxKeys),
		"IsTruncated":     fmt.Sprintf("%v", truncated),
		"Versions":        allVersions,
	}), nil
}

// ─── P2-3: Object Lock ────────────────────────────────────────────────────────

func (p *ObjectProvider) PutObjectLockConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"ObjectLockConfiguration"`
		Enabled string   `xml:"ObjectLockEnabled"`
		Rule    struct {
			DefaultRetention struct {
				Mode string `xml:"Mode"`
				Days int    `xml:"Days"`
			} `xml:"DefaultRetention"`
		} `xml:"Rule"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	// P4.1: ObjectLockEnabled must be "Enabled"
	if req.Enabled != "Enabled" {
		return nil, model.NewProviderError("InvalidArgument",
			"x-amz-bucket-object-lock-enabled must be set to 'Enabled' when using PutObjectLockConfiguration", 400)
	}
	lockConfig := map[string]any{
		"ObjectLockEnabled": req.Enabled,
		"DefaultMode":       req.Rule.DefaultRetention.Mode,
		"DefaultDays":       req.Rule.DefaultRetention.Days,
	}
	if err := p.updateBucketConfig(ctx, bucket, func(meta map[string]any) {
		meta["object_lock_config"] = lockConfig
	}); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetObjectLockConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	cfg, _ := meta["object_lock_config"].(map[string]any)
	if cfg == nil {
		cfg = map[string]any{"ObjectLockEnabled": "Disabled"}
	}
	return provider.OK(map[string]any{"ObjectLockConfig": cfg}), nil
}

func (p *ObjectProvider) PutObjectRetention(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName         xml.Name `xml:"Retention"`
		Mode            string   `xml:"Mode"`
		RetainUntilDate string   `xml:"RetainUntilDate"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}

	// P4.1: Parse new retention
	var newMode string
	var newUntil *time.Time
	if req.Mode != "" {
		newMode = req.Mode
		if req.RetainUntilDate != "" {
			t, _ := time.Parse(time.RFC3339, req.RetainUntilDate)
			newUntil = &t
		}
		if newUntil != nil && clock.Now().After(*newUntil) {
			return nil, model.NewProviderError("InvalidArgument",
				"The retain until date must be in the future!", 400)
		}
	}

	// P4.1: Validate lock reduction (cannot shorten COMPLIANCE; GOVERNANCE requires bypass header)
	bypassGovernance := strParam(nr.Params, "_bypass_governance_retention") == "true"
	isReducing := newMode == "" ||
		(m.LockRetainUntil != nil && newUntil != nil && m.LockRetainUntil.After(*newUntil)) ||
		(newMode == "GOVERNANCE" && m.LockMode == "COMPLIANCE")
	if isReducing {
		if m.LockMode == "COMPLIANCE" {
			return nil, model.NewProviderError("AccessDenied",
				"Access Denied because object protected by object lock.", 403)
		}
		if m.LockMode == "GOVERNANCE" && !bypassGovernance {
			return nil, model.NewProviderError("AccessDenied",
				"Access Denied because object protected by object lock.", 403)
		}
	}

	m.LockMode = newMode
	m.LockRetainUntil = newUntil
	if err := p.meta.PutObjectMeta(ctx, bucket, key, m); err != nil {
		return nil, err
	}
	// Mirror the lock change into the version record so DeleteObject with a
	// versionId sees the updated lock state.
	if m.VersionID != "" {
		_ = p.meta.UpdateObjectVersion(ctx, bucket, key, m)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetObjectRetention(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	data := map[string]any{"LockMode": m.LockMode}
	if m.LockRetainUntil != nil {
		data["RetainUntilDate"] = m.LockRetainUntil.UTC().Format(time.RFC3339)
	}
	return provider.OK(data), nil
}

func (p *ObjectProvider) PutObjectLegalHold(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	body, _ := nr.Params["_body"].([]byte)
	var req struct {
		XMLName xml.Name `xml:"LegalHold"`
		Status  string   `xml:"Status"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return nil, model.NewProviderError("MalformedXML", "Invalid XML", 400)
	}
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	m.LegalHoldStatus = req.Status
	if err := p.meta.PutObjectMeta(ctx, bucket, key, m); err != nil {
		return nil, err
	}
	// Mirror the hold change into the version record so DeleteObject with a
	// versionId sees the updated hold state.
	if m.VersionID != "" {
		_ = p.meta.UpdateObjectVersion(ctx, bucket, key, m)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) GetObjectLegalHold(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	status := m.LegalHoldStatus
	if status == "" {
		status = "OFF"
	}
	return provider.OK(map[string]any{"LegalHoldStatus": status}), nil
}
