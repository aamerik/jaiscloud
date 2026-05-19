package object

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	objectstore "jaiscloud/internal/aws/store/object"
)

// ─── Multipart ────────────────────────────────────────────────────────────────

func (p *ObjectProvider) CreateMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	uploadID := newUploadID()
	uploadMeta := map[string]any{}
	if ct := strParam(nr.Params, "_content_type"); ct != "" {
		uploadMeta["content-type"] = ct
	}
	if um := extractUserMetadata(nr.Params); len(um) > 0 {
		uploadMeta["user-metadata"] = um
	}
	if err := p.meta.InitMultipart(ctx, bucket, key, uploadID, uploadMeta); err != nil {
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
	uploadID := strParam(nr.Params, "uploadId")
	partNumber := intParam(nr.Params, "partNumber", 0)
	if partNumber < 1 || partNumber > 10000 {
		return nil, model.NewProviderError("InvalidArgument",
			"Part number must be an integer between 1 and 10000, inclusive", 400)
	}
	partKey := fmt.Sprintf("%s/part%d", uploadID, partNumber)

	var etagVal string
	var size int64

	if _, streaming := nr.Params["_streaming"]; streaming {
		var err error
		var crc32Val string
		etagVal, crc32Val, _, size, err = p.writeChecksums(ctx, bucket+"/__parts__", partKey, bodyReader(nr), "")
		_ = crc32Val
		if err != nil {
			return nil, err
		}
	} else {
		body, _ := nr.Params["_body"].([]byte)
		if err := p.blobs.Put(ctx, bucket+"/__parts__", partKey, body); err != nil {
			return nil, err
		}
		etagVal = etag(body)
		size = int64(len(body))
	}

	if err := p.meta.PutPart(ctx, uploadID, partNumber, objectstore.PartMeta{
		PartNumber: partNumber, ETag: etagVal, Size: size,
	}); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"ETag": etagVal}}, nil
}

func (p *ObjectProvider) CompleteMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	uploadID := strParam(nr.Params, "uploadId")

	_, _, uploadMeta, _ := p.meta.GetMultipartMeta(ctx, uploadID)

	// Validate part order from caller's XML body (parsed by codec into _requested_parts).
	if callerParts, ok := nr.Params["_requested_parts"].([]map[string]any); ok && len(callerParts) > 0 {
		for i := 1; i < len(callerParts); i++ {
			prev := intParam(callerParts[i-1], "PartNumber", 0)
			cur := intParam(callerParts[i], "PartNumber", 0)
			if cur <= prev {
				return nil, model.NewProviderError("InvalidPartOrder",
					"The list of parts was not in ascending order.", 400)
			}
		}
	}

	parts, err := p.meta.CompleteMultipart(ctx, bucket, key, uploadID)
	if err != nil {
		return nil, model.NewProviderError("NoSuchUpload", "The specified upload does not exist", 404)
	}

	// AWS requires all parts except the last to be at least 5 MB.
	const minPartSize = 5 * 1024 * 1024
	for i, part := range parts {
		if i < len(parts)-1 && part.Size < minPartSize {
			return nil, model.NewProviderError("EntityTooSmall",
				fmt.Sprintf("Your proposed upload is smaller than the minimum allowed size. "+
					"Part number %d is smaller than the minimum allowed size.", part.PartNumber), 400)
		}
	}

	// Stream parts sequentially into the final object, computing ETag+CRC32
	// in a single pass with no in-memory accumulation.
	seq := &seqPartReader{
		ctx:      ctx,
		blobs:    p.blobs,
		bucket:   bucket,
		uploadID: uploadID,
		parts:    parts,
	}
	defer seq.Close()

	partETags := make([]string, len(parts))
	for i, p := range parts {
		partETags[i] = p.ETag
	}
	multipartETag := computeMultipartETag(partETags)
	_, crc32Val, _, totalSize, err := p.writeChecksums(ctx, bucket, key, seq, "")
	if err != nil {
		return nil, err
	}

	// Remove part blobs after successful assembly.
	for _, part := range parts {
		partKey := fmt.Sprintf("%s/part%d", uploadID, part.PartNumber)
		_ = p.blobs.Delete(ctx, bucket+"/__parts__", partKey)
	}

	ct, userMeta := extractUploadMeta(uploadMeta)
	// P2-1: SSE for completed multipart
	enc, kmsKey, ssecMD5, _ := p.resolveSSE(ctx, nr, bucket)
	finalMeta := objectstore.ObjectMeta{
		Key: key, ETag: multipartETag, CRC32: crc32Val, Size: totalSize,
		ContentType: ct, Metadata: userMeta, LastModified: time.Now().UTC(),
		StorageClass: "STANDARD", Encryption: enc, KMSKeyID: kmsKey, SSECKeyMD5: ssecMD5,
	}
	if err := p.meta.PutObjectMeta(ctx, bucket, key, finalMeta); err != nil {
		slog.Warn("CompleteMultipartUpload: failed to put object meta", "bucket", bucket, "key", key, "err", err)
	}

	scheme := "http"
	if nr.Raw.TLS != nil {
		scheme = "https"
	}
	respData := map[string]any{
		"Location": fmt.Sprintf("%s://%s/%s/%s", scheme, nr.Raw.Host, bucket, key),
		"Bucket":   bucket,
		"Key":      key,
		"ETag":     multipartETag,
	}
	sseResponseData(respData, enc, kmsKey, ssecMD5)
	return provider.OK(respData), nil
}

func (p *ObjectProvider) AbortMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	uploadID := strParam(nr.Params, "uploadId")
	_ = p.meta.AbortMultipart(ctx, uploadID)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *ObjectProvider) ListMultipartUploads(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	prefix := strParam(nr.Params, "prefix")
	uploads, err := p.meta.ListActiveUploads(ctx, bucket)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(uploads))
	for _, u := range uploads {
		if prefix != "" && !strings.HasPrefix(u.Key, prefix) {
			continue
		}
		items = append(items, map[string]any{
			"Key":       u.Key,
			"UploadId":  u.UploadID,
			"Initiated": u.Initiated.UTC().Format(time.RFC3339),
		})
	}
	return provider.OK(map[string]any{
		"Bucket":  bucket,
		"Uploads": items,
	}), nil
}

func (p *ObjectProvider) ListParts(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	uploadID := strParam(nr.Params, "uploadId")
	parts, err := p.meta.ListParts(ctx, uploadID)
	if err != nil {
		return nil, model.NewProviderError("NoSuchUpload", "The specified upload does not exist", 404)
	}
	items := make([]map[string]any, 0, len(parts))
	for _, pt := range parts {
		items = append(items, map[string]any{
			"PartNumber": pt.PartNumber,
			"ETag":       pt.ETag,
			"Size":       pt.Size,
		})
	}
	return provider.OK(map[string]any{"Parts": items}), nil
}

func (p *ObjectProvider) UploadPartCopy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	uploadID := strParam(nr.Params, "uploadId")
	partNumber := intParam(nr.Params, "partNumber", 0)

	src := strings.TrimPrefix(strParam(nr.Params, "_copy_source"), "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		return nil, model.NewProviderError("InvalidArgument", "Invalid copy source", 400)
	}
	srcBucket, srcKey := parts[0], parts[1]

	var offset, length int64 = 0, -1
	if rng := strParam(nr.Params, "_copy_source_range"); rng != "" {
		srcMeta, err := p.meta.GetObjectMeta(ctx, srcBucket, srcKey)
		if err != nil {
			return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
		}
		if s, e, ok := parseByteRange(rng, srcMeta.Size); ok {
			offset, length = s, e-s+1
		}
	}

	rc, err := p.blobs.GetStream(ctx, srcBucket, srcKey, offset, length)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}
	partKey := fmt.Sprintf("%s/part%d", uploadID, partNumber)
	etagVal, _, _, size, err := p.writeChecksums(ctx, bucket+"/__parts__", partKey, rc, "")
	rc.Close()
	if err != nil {
		return nil, err
	}

	if err := p.meta.PutPart(ctx, uploadID, partNumber, objectstore.PartMeta{
		PartNumber: partNumber, ETag: etagVal, Size: size,
	}); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return provider.OK(map[string]any{
		"CopyPartResult": map[string]any{
			"ETag":         etagVal,
			"LastModified": now.UTC().Format(time.RFC3339),
		},
	}), nil
}

