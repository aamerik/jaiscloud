// Package object implements the S3 provider (ObjectProvider).
package object

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	s3store "jaiscloud/internal/store/aws/s3"
)

// ObjectProvider handles all S3 operations.
type ObjectProvider struct {
	meta  s3store.S3ObjectMetaStore
	blobs blobfs.BlobStore
}

func New(meta s3store.S3ObjectMetaStore, blobs blobfs.BlobStore) *ObjectProvider {
	return &ObjectProvider{meta: meta, blobs: blobs}
}

func (p *ObjectProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Bucket operations
		"Object.CreateBucket":          p.CreateBucket,
		"Object.DeleteBucket":          p.DeleteBucket,
		"Object.ListBuckets":           p.ListBuckets,
		"Object.GetBucketLocation":     p.GetBucketLocation,
		"Object.HeadBucket":            p.HeadBucket,
		// Object operations
		"Object.PutObject":             p.PutObject,
		"Object.GetObject":             p.GetObject,
		"Object.HeadObject":            p.HeadObject,
		"Object.DeleteObject":          p.DeleteObject,
		"Object.CopyObject":            p.CopyObject,
		// Listing
		"Object.ListObjectsV1":         p.ListObjectsV1,
		"Object.ListObjectsV2":         p.ListObjectsV2,
		"Object.DeleteObjects":         p.DeleteObjects,
		// Multipart
		"Object.CreateMultipartUpload": p.CreateMultipartUpload,
		"Object.UploadPart":            p.UploadPart,
		"Object.CompleteMultipartUpload": p.CompleteMultipartUpload,
		"Object.AbortMultipartUpload":  p.AbortMultipartUpload,
		"Object.ListMultipartUploads":  p.ListMultipartUploads,
		"Object.ListParts":             p.ListParts,
		// Tagging/versioning (stub)
		"Object.PutObjectTagging":      p.stub,
		"Object.GetObjectTagging":      p.stub,
		"Object.DeleteObjectTagging":   p.stub,
		"Object.PutBucketTagging":      p.stub,
		"Object.GetBucketTagging":      p.stub,
		"Object.DeleteBucketTagging":   p.stub,
		"Object.PutBucketVersioning":   p.stub,
		"Object.GetBucketVersioning":   p.stub,
		"Object.GetBucketAcl":          p.stub,
		"Object.PutBucketAcl":          p.stub,
		"Object.GetObjectAcl":          p.stub,
		"Object.PutObjectAcl":          p.stub,
	}
}

func (p *ObjectProvider) stub(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case int64:
			return int(n)
		case string:
			if i, err := strconv.Atoi(n); err == nil {
				return i
			}
		}
	}
	return def
}

func newUploadID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func etag(data []byte) string {
	return fmt.Sprintf(`"%x"`, md5.Sum(data))
}

// ─── Buckets ──────────────────────────────────────────────────────────────────

func (p *ObjectProvider) CreateBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if bucket == "" {
		return nil, model.NewProviderError("InvalidBucketName", "Bucket name is required", 400)
	}
	if err := p.meta.CreateBucket(ctx, bucket, nil); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// S3: CreateBucket is idempotent for same owner
			return provider.OK(map[string]any{"Location": "/" + bucket}), nil
		}
		return nil, model.NewProviderError("InternalError", err.Error(), 500)
	}
	return provider.OK(map[string]any{"Location": "/" + bucket}), nil
}

func (p *ObjectProvider) DeleteBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if err := p.meta.DeleteBucket(ctx, bucket); err != nil {
		if strings.Contains(err.Error(), "NoSuchBucket") {
			return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
		}
		if strings.Contains(err.Error(), "BucketNotEmpty") {
			return nil, model.NewProviderError("BucketNotEmpty", "The bucket is not empty", 409)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) ListBuckets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	buckets, err := p.meta.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	if buckets == nil {
		buckets = []map[string]any{}
	}
	return provider.OK(map[string]any{
		"Buckets": buckets,
		"Owner":   map[string]any{"ID": nr.AccountID, "DisplayName": nr.AccountID},
	}), nil
}

func (p *ObjectProvider) GetBucketLocation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return provider.OK(map[string]any{"LocationConstraint": nr.Region}), nil
}

func (p *ObjectProvider) HeadBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// ─── Objects ──────────────────────────────────────────────────────────────────

func (p *ObjectProvider) PutObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	body, _ := nr.Params["_body"].([]byte)
	contentType := strParam(nr.Params, "_content_type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := p.blobs.Put(ctx, bucket, key, body); err != nil {
		return nil, err
	}

	etagVal := etag(body)
	meta := s3store.ObjectMeta{
		Key:          key,
		ETag:         etagVal,
		Size:         int64(len(body)),
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
		StorageClass: "STANDARD",
		Metadata:     extractUserMetadata(nr.Params),
	}
	if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"ETag": etagVal}}, nil
}

// extractUserMetadata collects x-amz-meta-* headers stored under "_meta_*" params.
func extractUserMetadata(params map[string]any) map[string]string {
	var m map[string]string
	for k, v := range params {
		if strings.HasPrefix(k, "_meta_") {
			if s, ok := v.(string); ok {
				if m == nil {
					m = make(map[string]string)
				}
				m[strings.TrimPrefix(k, "_meta_")] = s
			}
		}
	}
	return m
}

func (p *ObjectProvider) GetObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")

	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}

	body, err := p.blobs.Get(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}

	status := 200
	rangeHdr := strParam(nr.Params, "_range")
	if rangeHdr != "" {
		start, end, ok := parseByteRange(rangeHdr, int64(len(body)))
		if !ok {
			return nil, model.NewProviderError("InvalidRange", "The requested range is not satisfiable", 416)
		}
		body = body[start : end+1]
		status = 206
	}

	data := map[string]any{
		"_raw_body":     body,
		"_passthrough":  true,
		"_content_type": m.ContentType,
		"ETag":          m.ETag,
		"ContentLength": int64(len(body)),
		"LastModified":  m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}
	if status == 206 {
		data["_status"] = status
	}
	// Return user metadata so the codec can emit x-amz-meta-* headers.
	if len(m.Metadata) > 0 {
		data["_metadata"] = m.Metadata
	}

	return &model.ProviderResponse{
		HTTPStatus: status,
		Data:       data,
	}, nil
}

// parseByteRange parses a "bytes=<start>-<end>" Range header.
// Returns inclusive [start, end] indices and true on success.
func parseByteRange(hdr string, size int64) (int64, int64, bool) {
	hdr = strings.TrimSpace(hdr)
	if !strings.HasPrefix(hdr, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(hdr, "bytes=")
	// Only handle single range (no multi-range)
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	startStr, endStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	var start, end int64
	if startStr == "" {
		// Suffix range: bytes=-N (last N bytes)
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		start = size - n
		if start < 0 {
			start = 0
		}
		end = size - 1
	} else {
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		if endStr == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(endStr, 10, 64)
			if err != nil {
				return 0, 0, false
			}
		}
	}
	if start < 0 || end >= size || start > end {
		return 0, 0, false
	}
	return start, end, true
}

func (p *ObjectProvider) HeadObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")

	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"ETag":          m.ETag,
		"ContentLength": m.Size,
		"ContentType":   m.ContentType,
		"LastModified":  m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}}, nil
}

func (p *ObjectProvider) DeleteObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	_ = p.blobs.Delete(ctx, bucket, key)
	_ = p.meta.DeleteObjectMeta(ctx, bucket, key)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) CopyObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dstBucket := strParam(nr.Params, "_bucket")
	dstKey := strParam(nr.Params, "_key")
	src := strings.TrimPrefix(strParam(nr.Params, "_copy_source"), "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		return nil, model.NewProviderError("InvalidArgument", "Invalid copy source", 400)
	}
	srcBucket, srcKey := parts[0], parts[1]

	data, err := p.blobs.Get(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}
	srcMeta, err := p.meta.GetObjectMeta(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}

	_ = p.blobs.Put(ctx, dstBucket, dstKey, data)
	etagVal := etag(data)
	now := time.Now().UTC()
	_ = p.meta.PutObjectMeta(ctx, dstBucket, dstKey, s3store.ObjectMeta{
		Key: dstKey, ETag: etagVal, Size: srcMeta.Size,
		ContentType: srcMeta.ContentType, LastModified: now, StorageClass: "STANDARD",
	})
	return provider.OK(map[string]any{
		"CopyObjectResult": map[string]any{
			"ETag":         etagVal,
			"LastModified": now.Format(time.RFC3339),
		},
	}), nil
}

// ─── List ─────────────────────────────────────────────────────────────────────

func (p *ObjectProvider) ListObjectsV1(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.listObjects(ctx, nr, false)
}

func (p *ObjectProvider) ListObjectsV2(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.listObjects(ctx, nr, true)
}

func (p *ObjectProvider) listObjects(ctx context.Context, nr *model.NormalizedRequest, v2 bool) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	prefix := strParam(nr.Params, "prefix")
	if prefix == "" {
		prefix = strParam(nr.Params, "Prefix")
	}
	delimiter := strParam(nr.Params, "delimiter")
	if delimiter == "" {
		delimiter = strParam(nr.Params, "Delimiter")
	}
	marker := strParam(nr.Params, "marker")
	if marker == "" {
		marker = strParam(nr.Params, "continuation-token")
	}
	maxKeys := intParam(nr.Params, "max-keys", 1000)

	objects, commonPrefixes, truncated, err := p.meta.ListObjectMeta(ctx, bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}

	var contents []map[string]any
	for _, obj := range objects {
		contents = append(contents, map[string]any{
			"Key":          obj.Key,
			"ETag":         obj.ETag,
			"Size":         obj.Size,
			"LastModified": obj.LastModified.Format(time.RFC3339),
			"StorageClass": obj.StorageClass,
		})
	}
	if contents == nil {
		contents = []map[string]any{}
	}

	result := map[string]any{
		"Name":           bucket,
		"Prefix":         prefix,
		"Delimiter":      delimiter,
		"MaxKeys":        maxKeys,
		"IsTruncated":    truncated,
		"Contents":       contents,
		"CommonPrefixes": commonPrefixes,
	}
	if v2 {
		result["KeyCount"] = len(contents)
	}
	return provider.OK(result), nil
}

func (p *ObjectProvider) DeleteObjects(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	objects, _ := nr.Params["Delete"].(map[string]any)
	if objects == nil {
		return provider.OK(map[string]any{"Deleted": []any{}}), nil
	}
	keys, _ := objects["Object"].([]any)
	var deleted []map[string]any
	for _, k := range keys {
		km, _ := k.(map[string]any)
		key, _ := km["Key"].(string)
		_ = p.blobs.Delete(ctx, bucket, key)
		_ = p.meta.DeleteObjectMeta(ctx, bucket, key)
		deleted = append(deleted, map[string]any{"Key": key})
	}
	if deleted == nil {
		deleted = []map[string]any{}
	}
	return provider.OK(map[string]any{"Deleted": deleted}), nil
}

// ─── Multipart ────────────────────────────────────────────────────────────────

func (p *ObjectProvider) CreateMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	uploadID := newUploadID()
	if err := p.meta.InitMultipart(ctx, bucket, key, uploadID, nil); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"Bucket":   bucket,
		"Key":      key,
		"UploadId": uploadID,
	}), nil
}

func (p *ObjectProvider) UploadPart(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	uploadID := strParam(nr.Params, "uploadId")
	partNumber := intParam(nr.Params, "partNumber", 0)
	body, _ := nr.Params["_body"].([]byte)

	partKey := fmt.Sprintf("%s/part%d", uploadID, partNumber)
	if err := p.blobs.Put(ctx, bucket+"/__parts__", partKey, body); err != nil {
		return nil, err
	}
	etagVal := etag(body)
	if err := p.meta.PutPart(ctx, uploadID, partNumber, s3store.PartMeta{
		PartNumber: partNumber, ETag: etagVal, Size: int64(len(body)),
	}); err != nil {
		_ = key // suppress unused
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"ETag": etagVal}}, nil
}

func (p *ObjectProvider) CompleteMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	uploadID := strParam(nr.Params, "uploadId")

	parts, err := p.meta.CompleteMultipart(ctx, bucket, key, uploadID)
	if err != nil {
		return nil, model.NewProviderError("NoSuchUpload", "The specified upload does not exist", 404)
	}

	// Concatenate all part blobs.
	var combined []byte
	for _, part := range parts {
		partKey := fmt.Sprintf("%s/part%d", uploadID, part.PartNumber)
		data, _ := p.blobs.Get(ctx, bucket+"/__parts__", partKey)
		combined = append(combined, data...)
		_ = p.blobs.Delete(ctx, bucket+"/__parts__", partKey)
	}

	if err := p.blobs.Put(ctx, bucket, key, combined); err != nil {
		return nil, err
	}
	etagVal := etag(combined)
	_ = p.meta.PutObjectMeta(ctx, bucket, key, s3store.ObjectMeta{
		Key: key, ETag: etagVal, Size: int64(len(combined)),
		ContentType: "application/octet-stream", LastModified: time.Now().UTC(),
		StorageClass: "STANDARD",
	})

	return provider.OK(map[string]any{
		"Location": fmt.Sprintf("http://s3.amazonaws.com/%s/%s", bucket, key),
		"Bucket":   bucket,
		"Key":      key,
		"ETag":     etagVal,
	}), nil
}

func (p *ObjectProvider) AbortMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	uploadID := strParam(nr.Params, "uploadId")
	_ = p.meta.AbortMultipart(ctx, uploadID)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) ListMultipartUploads(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"Uploads": []any{}}), nil
}

func (p *ObjectProvider) ListParts(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"Parts": []any{}}), nil
}
