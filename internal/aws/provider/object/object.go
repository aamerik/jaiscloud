// Package object implements the S3 provider (ObjectProvider).
package object

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
	objectstore "jaiscloud/internal/store/object"
)

// ObjectProvider handles all S3 operations.
type ObjectProvider struct {
	meta       objectstore.ObjectMetaStore
	blobs      blobfs.BlobStore
	resources  store.ResourceStore
	notifStore sync.Map
	bus        *events.EventBus
	fanout     S3FanoutConfig
}

func New(meta objectstore.ObjectMetaStore, blobs blobfs.BlobStore) *ObjectProvider {
	return &ObjectProvider{meta: meta, blobs: blobs}
}

// NewWithBus constructs an ObjectProvider with an event bus for S3 notification dispatch.
func NewWithBus(meta objectstore.ObjectMetaStore, blobs blobfs.BlobStore, bus *events.EventBus) *ObjectProvider {
	return &ObjectProvider{meta: meta, blobs: blobs, bus: bus}
}

// WithResourceStore attaches a generic ResourceStore for access points and other metadata.
func (p *ObjectProvider) WithResourceStore(res store.ResourceStore) *ObjectProvider {
	p.resources = res
	return p
}

func (p *ObjectProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Bucket operations
		"Object.CreateBucket":      p.CreateBucket,
		"Object.DeleteBucket":      p.DeleteBucket,
		"Object.ListBuckets":       p.ListBuckets,
		"Object.GetBucketLocation": p.GetBucketLocation,
		"Object.HeadBucket":        p.HeadBucket,
		// Object operations
		"Object.PutObject":    p.PutObject,
		"Object.GetObject":    p.GetObject,
		"Object.HeadObject":   p.HeadObject,
		"Object.DeleteObject": p.DeleteObject,
		"Object.CopyObject":   p.CopyObject,
		// Listing
		"Object.ListObjectsV1": p.ListObjectsV1,
		"Object.ListObjectsV2": p.ListObjectsV2,
		"Object.DeleteObjects": p.DeleteObjects,
		// Multipart
		"Object.CreateMultipartUpload":   p.CreateMultipartUpload,
		"Object.UploadPart":              p.UploadPart,
		"Object.UploadPartCopy":          p.UploadPartCopy,
		"Object.CompleteMultipartUpload": p.CompleteMultipartUpload,
		"Object.AbortMultipartUpload":    p.AbortMultipartUpload,
		"Object.ListMultipartUploads":    p.ListMultipartUploads,
		"Object.ListParts":               p.ListParts,
		// Tagging (P2-7)
		"Object.PutObjectTagging":    p.PutObjectTagging,
		"Object.GetObjectTagging":    p.GetObjectTagging,
		"Object.DeleteObjectTagging": p.DeleteObjectTagging,
		"Object.PutBucketTagging":    p.PutBucketTagging,
		"Object.GetBucketTagging":    p.GetBucketTagging,
		"Object.DeleteBucketTagging": p.DeleteBucketTagging,
		// Encryption (P2-1)
		"Object.PutBucketEncryption":    p.PutBucketEncryption,
		"Object.GetBucketEncryption":    p.GetBucketEncryption,
		"Object.DeleteBucketEncryption": p.DeleteBucketEncryption,
		// Versioning (P2-2)
		"Object.PutBucketVersioning": p.PutBucketVersioning,
		"Object.GetBucketVersioning": p.GetBucketVersioning,
		"Object.ListObjectVersions":  p.ListObjectVersions,
		// Object Lock (P2-3)
		"Object.PutObjectLockConfiguration": p.PutObjectLockConfiguration,
		"Object.GetObjectLockConfiguration": p.GetObjectLockConfiguration,
		"Object.PutObjectRetention":         p.PutObjectRetention,
		"Object.GetObjectRetention":         p.GetObjectRetention,
		"Object.PutObjectLegalHold":         p.PutObjectLegalHold,
		"Object.GetObjectLegalHold":         p.GetObjectLegalHold,
		// ACLs (P2-4)
		"Object.GetBucketAcl":  p.GetBucketAcl,
		"Object.PutBucketAcl":  p.PutBucketAcl,
		"Object.GetObjectAcl":  p.GetObjectAcl,
		"Object.PutObjectAcl":  p.PutObjectAcl,
		// Ownership Controls (P4.3)
		"Object.PutBucketOwnershipControls":    p.PutBucketOwnershipControls,
		"Object.GetBucketOwnershipControls":    p.GetBucketOwnershipControls,
		"Object.DeleteBucketOwnershipControls": p.DeleteBucketOwnershipControls,
		// GetObjectAttributes (P4.12)
		"Object.GetObjectAttributes": p.GetObjectAttributes,
		// Lifecycle (P2-5)
		"Object.PutBucketLifecycleConfiguration": p.PutBucketLifecycleConfiguration,
		"Object.GetBucketLifecycleConfiguration": p.GetBucketLifecycleConfiguration,
		"Object.DeleteBucketLifecycle":            p.DeleteBucketLifecycle,
		// CORS (P2-6)
		"Object.PutBucketCors":    p.PutBucketCors,
		"Object.GetBucketCors":    p.GetBucketCors,
		"Object.DeleteBucketCors": p.DeleteBucketCors,
		// Notifications (P5.1)
		"Object.PutBucketNotificationConfiguration": p.PutBucketNotificationConfiguration,
		"Object.GetBucketNotificationConfiguration": p.GetBucketNotificationConfiguration,
		// S3 Select (P15.9)
		"Object.SelectObjectContent": p.SelectObjectContent,
		// S3 Access Points (P15.10)
		"Object.CreateAccessPoint":          p.CreateAccessPoint,
		"Object.GetAccessPoint":             p.GetAccessPoint,
		"Object.ListAccessPoints":           p.ListAccessPoints,
		"Object.DeleteAccessPoint":          p.DeleteAccessPoint,
		"Object.PutAccessPointPolicy":       p.PutAccessPointPolicy,
		"Object.GetAccessPointPolicy":       p.GetAccessPointPolicy,
		"Object.DeleteAccessPointPolicy":    p.DeleteAccessPointPolicy,
		"Object.GetAccessPointPolicyStatus": p.GetAccessPointPolicyStatus,
		// Policy / Website / Logging / Replication (P1.16-1.19)
		"Object.PutBucketPolicy":       p.PutBucketPolicy,
		"Object.GetBucketPolicy":       p.GetBucketPolicy,
		"Object.DeleteBucketPolicy":    p.DeleteBucketPolicy,
		"Object.PutBucketWebsite":      p.PutBucketWebsite,
		"Object.GetBucketWebsite":      p.GetBucketWebsite,
		"Object.DeleteBucketWebsite":   p.DeleteBucketWebsite,
		"Object.PutBucketLogging":      p.PutBucketLogging,
		"Object.GetBucketLogging":      p.GetBucketLogging,
		"Object.PutBucketReplication":  p.PutBucketReplication,
		"Object.GetBucketReplication":  p.GetBucketReplication,
		"Object.DeleteBucketReplication": p.DeleteBucketReplication,
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

// crc32Base64 returns the base64-encoded IEEE CRC32 of data, matching the
// x-amz-checksum-crc32 format expected by AWS SDK v2.
func crc32Base64(data []byte) string {
	sum := crc32.ChecksumIEEE(data)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)
	return base64.StdEncoding.EncodeToString(b)
}

// crc32Base64FromHash encodes the running CRC32 hash state as base64.
func crc32Base64FromHash(h *crc32State) string {
	sum := h.Sum32()
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)
	return base64.StdEncoding.EncodeToString(b)
}

// crc32State wraps hash/crc32 as an io.Writer so it can feed a TeeReader.
type crc32State struct{ h hash.Hash32 }

func newCRC32() *crc32State                       { return &crc32State{h: crc32.NewIEEE()} }
func (c *crc32State) Write(p []byte) (int, error) { return c.h.Write(p) }
func (c *crc32State) Sum32() uint32               { return c.h.Sum32() }

// computeChecksumValue computes and base64-encodes a checksum for data using algo.
// algo is one of: "CRC32", "CRC32C", "SHA1", "SHA256".
func computeChecksumValue(data []byte, algo string) string {
	switch algo {
	case "CRC32":
		sum := crc32.ChecksumIEEE(data)
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, sum)
		return base64.StdEncoding.EncodeToString(b)
	case "CRC32C":
		sum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, sum)
		return base64.StdEncoding.EncodeToString(b)
	case "SHA1":
		h := sha1.Sum(data)
		return base64.StdEncoding.EncodeToString(h[:])
	case "SHA256":
		h := sha256.Sum256(data)
		return base64.StdEncoding.EncodeToString(h[:])
	}
	return ""
}

// writeChecksums writes from r to the BlobStore and returns the MD5 ETag,
// CRC32 checksum (base64), and byte count — all computed in a single pass.
// If extraAlgo is non-empty, also computes and returns that checksum.
func (p *ObjectProvider) writeChecksums(ctx context.Context, bucket, key string, r io.Reader, extraAlgo string) (etagVal, crc32Val, extraChecksum string, n int64, err error) {
	md5h := md5.New()
	crc32h := newCRC32()
	writers := []io.Writer{md5h, crc32h}

	var extraH hash.Hash
	var crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	switch extraAlgo {
	case "CRC32C":
		extraH = crc32.New(crc32cTable)
	case "SHA1":
		extraH = sha1.New()
	case "SHA256":
		extraH = sha256.New()
	}
	if extraH != nil {
		writers = append(writers, extraH)
	}

	tee := io.TeeReader(r, io.MultiWriter(writers...))
	n, err = p.blobs.PutStream(ctx, bucket, key, tee)
	if err != nil {
		return "", "", "", 0, err
	}
	etagVal = fmt.Sprintf(`"%x"`, md5h.Sum(nil))
	crc32Val = crc32Base64FromHash(crc32h)
	if extraH != nil {
		switch extraAlgo {
		case "CRC32C":
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, extraH.(hash.Hash32).Sum32())
			extraChecksum = base64.StdEncoding.EncodeToString(b)
		default:
			extraChecksum = base64.StdEncoding.EncodeToString(extraH.Sum(nil))
		}
	}
	return etagVal, crc32Val, extraChecksum, n, nil
}

// bodyReader returns an io.Reader for the request body, decoding aws-chunked
// framing when present. Used by the streaming upload path.
func bodyReader(nr *model.NormalizedRequest) io.Reader {
	r := nr.Raw.Body
	if strings.Contains(strings.ToLower(nr.Raw.Header.Get("Content-Encoding")), "aws-chunked") ||
		strings.HasPrefix(nr.Raw.Header.Get("x-amz-content-sha256"), "STREAMING-") ||
		nr.Raw.Header.Get("x-amz-decoded-content-length") != "" {
		return newAWSChunkedReader(r)
	}
	return r
}

// ─── aws-chunked streaming decoder ───────────────────────────────────────────
//
// AWS SDK v2 sends large uploads with Content-Encoding: aws-chunked.
// Format: "<hex-size>[;chunk-signature=...]\r\n<data>\r\n" … "0[;...]\r\n\r\n"

type awsChunkedReader struct {
	r         *bufio.Reader
	chunkLeft int64
	done      bool
}

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{r: bufio.NewReaderSize(r, 32*1024)}
}

func (a *awsChunkedReader) Read(p []byte) (int, error) {
	if a.done {
		return 0, io.EOF
	}
	for a.chunkLeft == 0 {
		line, err := a.r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		if semi := strings.IndexByte(line, ';'); semi >= 0 {
			line = line[:semi]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		size, err := strconv.ParseInt(line, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("aws-chunked: invalid chunk size %q: %w", line, err)
		}
		if size == 0 {
			a.done = true
			return 0, io.EOF
		}
		a.chunkLeft = size
	}
	if int64(len(p)) > a.chunkLeft {
		p = p[:a.chunkLeft]
	}
	n, err := a.r.Read(p)
	a.chunkLeft -= int64(n)
	if a.chunkLeft == 0 && err == nil {
		// Consume the trailing \r\n after the chunk data.
		a.r.ReadString('\n')
	}
	return n, err
}

// ─── seqPartReader ────────────────────────────────────────────────────────────
//
// Reads multipart upload parts sequentially, opening one at a time to keep
// the number of concurrent open file descriptors to O(1).

type seqPartReader struct {
	ctx      context.Context
	blobs    blobfs.BlobStore
	bucket   string
	uploadID string
	parts    []objectstore.PartMeta
	idx      int
	current  io.ReadCloser
}

func (s *seqPartReader) Read(p []byte) (int, error) {
	for {
		if s.current == nil {
			if s.idx >= len(s.parts) {
				return 0, io.EOF
			}
			part := s.parts[s.idx]
			s.idx++
			partKey := fmt.Sprintf("%s/part%d", s.uploadID, part.PartNumber)
			rc, err := s.blobs.GetStream(s.ctx, s.bucket+"/__parts__", partKey, 0, -1)
			if err != nil {
				return 0, err
			}
			s.current = rc
		}
		n, err := s.current.Read(p)
		if err == io.EOF {
			s.current.Close()
			s.current = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (s *seqPartReader) Close() error {
	if s.current != nil {
		return s.current.Close()
	}
	return nil
}

// ─── Buckets ──────────────────────────────────────────────────────────────────

func (p *ObjectProvider) CreateBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	if bucket == "" {
		return nil, model.NewProviderError("InvalidBucketName", "The specified bucket is not valid", 400)
	}
	// Determine requested region (LocationConstraint in body, or nr.Region)
	locationConstraint := strParam(nr.Params, "LocationConstraint")
	if locationConstraint == "" {
		locationConstraint = nr.Region
	}
	meta := map[string]any{
		"AccountID": nr.AccountID,
		"Region":    locationConstraint,
	}
	if err := p.meta.CreateBucket(ctx, bucket, meta); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// S3 idempotency: load existing bucket metadata
			existing, getErr := p.meta.GetBucket(ctx, bucket)
			if getErr != nil {
				// Can't read existing — treat as success (idempotent)
				return provider.OK(map[string]any{"Location": "/" + bucket}), nil
			}
			existingOwner, _ := existing["AccountID"].(string)
			existingRegion, _ := existing["Region"].(string)
			if existingOwner != nr.AccountID {
				// Different account owns this bucket
				return nil, model.NewProviderError("BucketAlreadyExists", "The requested bucket name is not available", 409)
			}
			// Same account owns the bucket
			if existingRegion == "us-east-1" && locationConstraint == "us-east-1" {
				// Same account, us-east-1: idempotent success
				return provider.OK(map[string]any{"Location": "/" + bucket}), nil
			}
			if existingRegion != locationConstraint {
				// Same account but different region
				return nil, model.NewProviderError("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it", 409)
			}
			// Same account, same region: idempotent success
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
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"_region": nr.Region}}, nil
}

// ─── Objects ──────────────────────────────────────────────────────────────────

func (p *ObjectProvider) PutObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	contentType := strParam(nr.Params, "_content_type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// P2-2: Determine blob storage key before writing so versioned blobs land at
	// the right path and GetObject can find them via blobKeyForVersion.
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)
	var preVersionID string
	blobKey := key
	switch vStatus {
	case objectstore.VersioningEnabled:
		preVersionID = newVersionID()
		blobKey = blobKeyForVersion(key, preVersionID)
	case objectstore.VersioningSuspended:
		preVersionID = "null"
		blobKey = blobKeyForVersion(key, "null")
	}

	var etagVal, crc32Val string
	var size int64

	// Determine client-supplied checksum algo (if any) for P4.4 validation.
	checksumAlgo := ""
	if checksumHdr := strParam(nr.Params, "_checksum_header"); checksumHdr != "" {
		checksumAlgo = strings.ToUpper(strings.TrimPrefix(checksumHdr, "x-amz-checksum-"))
		checksumAlgo = strings.ReplaceAll(checksumAlgo, "-", "")
	}
	expectedChecksum := strParam(nr.Params, "_checksum_value")

	var computedChecksum string
	if _, streaming := nr.Params["_streaming"]; streaming {
		// Streaming path: body arrives via nr.Raw.Body (gateway skipped io.ReadAll).
		var err error
		etagVal, crc32Val, computedChecksum, size, err = p.writeChecksums(ctx, bucket, blobKey, bodyReader(nr), checksumAlgo)
		if err != nil {
			return nil, err
		}
		// writeChecksums returns extraChecksum only for CRC32C/SHA1/SHA256.
		// CRC32 is always computed as crc32Val; use it here to match the non-streaming path.
		if checksumAlgo == "CRC32" {
			computedChecksum = crc32Val
		}
	} else {
		body, _ := nr.Params["_body"].([]byte)
		if err := p.blobs.Put(ctx, bucket, blobKey, body); err != nil {
			return nil, err
		}
		etagVal = etag(body)
		crc32Val = crc32Base64(body)
		size = int64(len(body))
		if checksumAlgo != "" {
			if checksumAlgo == "CRC32" {
				computedChecksum = crc32Val
			} else {
				computedChecksum = computeChecksumValue(body, checksumAlgo)
			}
		}
	}

	// P4.4: Validate client-supplied checksum; BadDigest on mismatch.
	if checksumAlgo != "" && expectedChecksum != "" && computedChecksum != expectedChecksum {
		_ = p.blobs.Delete(ctx, bucket, blobKey)
		return nil, model.NewProviderError("BadDigest",
			"The Content-MD5 or checksum you specified did not match what we received.", 400)
	}

	meta := objectstore.ObjectMeta{
		Key:          key,
		ETag:         etagVal,
		CRC32:        crc32Val,
		Size:         size,
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
		StorageClass: "STANDARD",
		Metadata:     extractUserMetadata(nr.Params),
	}
	if checksumAlgo != "" {
		meta.ChecksumAlgorithm = checksumAlgo
		if computedChecksum != "" {
			meta.ChecksumValue = computedChecksum
		} else {
			meta.ChecksumValue = expectedChecksum
		}
	}
	// P2-7: Tagging
	if tagging := strParam(nr.Params, "_tagging"); tagging != "" {
		if tags, err := parseTaggingHeader(tagging); err == nil {
			if err := validateTags(tags, 10); err != nil {
				return nil, model.NewProviderError("InvalidTag", err.Error(), 400)
			}
			meta.Tags = tags
		}
	}
	// P2-1: SSE
	enc, kmsKey, ssecMD5, sseErr := p.resolveSSE(ctx, nr, bucket)
	if sseErr != nil {
		return nil, sseErr
	}
	meta.Encryption = enc
	meta.KMSKeyID = kmsKey
	meta.SSECKeyMD5 = ssecMD5
	// P2-4: ACL
	meta.ACL = resolveACL(strParam(nr.Params, "_acl"), nr.AccountID)

	// P4.1: Object lock headers
	lockMode := strParam(nr.Params, "_lock_mode")
	lockUntilStr := strParam(nr.Params, "_lock_retain_until_date")
	legalHold := strParam(nr.Params, "_lock_legal_hold")
	if lockMode != "" && lockUntilStr == "" {
		return nil, model.NewProviderError("InvalidArgument",
			"x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied", 400)
	}
	if lockMode == "" && lockUntilStr != "" {
		return nil, model.NewProviderError("InvalidArgument",
			"x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied", 400)
	}
	if lockMode != "" && lockMode != "GOVERNANCE" && lockMode != "COMPLIANCE" {
		return nil, model.NewProviderError("InvalidArgument", "Unknown wormMode directive.", 400)
	}
	var lockRetainUntil *time.Time
	if lockUntilStr != "" {
		t, _ := time.Parse(time.RFC3339, lockUntilStr)
		lockRetainUntil = &t
	}
	if lockMode == "" {
		if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
			if lockCfg, ok := bucketMeta["object_lock_config"].(map[string]any); ok {
				if defaultMode, _ := lockCfg["DefaultMode"].(string); defaultMode != "" {
					lockMode = defaultMode
					days := 0
					switch d := lockCfg["DefaultDays"].(type) {
					case float64:
						days = int(d)
					case int:
						days = d
					}
					t := time.Now().Add(time.Duration(days) * 24 * time.Hour)
					lockRetainUntil = &t
				}
			}
		}
	}
	meta.LockMode = lockMode
	meta.LockRetainUntil = lockRetainUntil
	meta.LegalHoldStatus = legalHold

	// P2-2: Versioning — use pre-generated versionID so metadata and blob agree.
	var versionID string
	if vStatus == objectstore.VersioningEnabled {
		meta.VersionID = preVersionID
		var verr error
		versionID, verr = p.meta.PutObjectVersion(ctx, bucket, key, meta)
		if verr != nil {
			return nil, model.NewProviderError("InternalError", verr.Error(), 500)
		}
		if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
			slog.Warn("versioned PutObject: failed to update current-object pointer", "bucket", bucket, "key", key, "err", err)
		}
	} else if vStatus == objectstore.VersioningSuspended {
		meta.VersionID = "null"
		versionID, _ = p.meta.PutObjectVersion(ctx, bucket, key, meta)
		if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
			slog.Warn("suspended PutObject: failed to update current-object pointer", "bucket", bucket, "key", key, "err", err)
		}
	} else {
		if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
			if strings.Contains(err.Error(), "NoSuchBucket") {
				return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
			}
			return nil, model.NewProviderError("InternalError", err.Error(), 500)
		}
	}
	respData := map[string]any{
		"ETag":          etagVal,
		"_server_crc32": crc32Val,
	}
	if versionID != "" {
		respData["_version_id"] = versionID
	}
	sseResponseData(respData, enc, kmsKey, ssecMD5)
	p.dispatchNotification(ctx, bucket, key, "s3:ObjectCreated:Put")
	return &model.ProviderResponse{HTTPStatus: 200, Data: respData}, nil
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

	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)
	requestedVersionID := strParam(nr.Params, "versionId")
	blobKey := key

	var m objectstore.ObjectMeta
	if (vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended) {
		if requestedVersionID != "" {
			var err error
			m, err = p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
			if err != nil {
				return nil, model.NewProviderError("NoSuchVersion", "The specified version does not exist", 404)
			}
			if m.IsDeleteMarker {
				return nil, model.NewProviderError("MethodNotAllowed", "The specified method is not allowed against this resource", 405)
			}
			blobKey = blobKeyForVersion(key, m.VersionID)
		} else {
			versions, _, _ := p.meta.ListObjectVersions(ctx, bucket, key, "", "", 100)
			var found *objectstore.ObjectMeta
			for i := range versions {
				if versions[i].Key == key {
					found = &versions[i]
					break
				}
			}
			if found == nil {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
			}
			if found.IsDeleteMarker {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404).
					WithData(map[string]any{"_delete_marker": true, "_version_id": found.VersionID})
			}
			m = *found
			blobKey = blobKeyForVersion(key, m.VersionID)
		}
	} else {
		var err error
		m, err = p.meta.GetObjectMeta(ctx, bucket, key)
		if err != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
	}
	// P4.2: SSE-C re-validation — the caller must supply the matching key
	if m.SSECKeyMD5 != "" {
		requestedKeyMD5 := strParam(nr.Params, "_server_side_encryption_customer_key_md5")
		if requestedKeyMD5 == "" {
			return nil, model.NewProviderError("InvalidRequest",
				"The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.", 400)
		}
		if requestedKeyMD5 != m.SSECKeyMD5 {
			return nil, model.NewProviderError("AccessDenied",
				"Requests specifying Server Side Encryption with Customer provided keys must provide the correct secret key.", 403)
		}
	}

	if resp304, pe := checkConditions(nr, objectCondMeta{ETag: m.ETag, LastModified: m.LastModified, ContentType: m.ContentType}); pe != nil {
		return nil, pe
	} else if resp304 != nil {
		return resp304, nil
	}

	status := 200
	var offset, length int64 = 0, -1
	contentLength := m.Size

	rangeHdr := strParam(nr.Params, "_range")
	if rangeHdr != "" {
		start, end, ok := parseByteRange(rangeHdr, m.Size)
		if !ok {
			return nil, model.NewProviderError("InvalidRange", "The requested range is not satisfiable", 416)
		}
		offset = start
		length = end - start + 1
		contentLength = length
		status = 206
	}

	rc, err := p.blobs.GetStream(ctx, bucket, blobKey, offset, length)
	if err != nil {
		// Blob miss — may be a concurrent delete racing with our metadata read.
		// Recheck metadata: if it's also gone, this is a clean concurrent delete → 404.
		// If metadata is still present the blob is missing without a delete → 500.
		if _, recheckErr := p.meta.GetObjectMeta(ctx, bucket, key); recheckErr != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
		slog.Error("object: blob missing but metadata present — possible storage corruption",
			"bucket", bucket, "key", key, "err", err)
		return nil, model.NewProviderError("InternalError", "Internal server error", 500)
	}

	ct := m.ContentType
	if rct := strParam(nr.Params, "response-content-type"); rct != "" {
		ct = rct
	}
	data := map[string]any{
		"_stream":       rc,
		"_passthrough":  true,
		"_content_type": ct,
		"ETag":          m.ETag,
		"_crc32":        m.CRC32,
		"ContentLength": contentLength,
		"LastModified":  m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}
	// P4.4: expose stored checksum algo+value for codec to emit correct header
	if m.ChecksumAlgorithm != "" {
		data["_checksum_algo"] = m.ChecksumAlgorithm
		data["_checksum_value"] = m.ChecksumValue
	}
	if status == 206 {
		data["_status"] = status
		data["_range_start"] = offset
		data["_range_end"] = offset + length - 1
		data["_range_total"] = m.Size
	}
	if len(m.Metadata) > 0 {
		data["_metadata"] = m.Metadata
	}
	// P2-7: tagging count
	if len(m.Tags) > 0 {
		data["_tagging_count"] = len(m.Tags)
	}
	// P2-1: SSE headers
	sseResponseData(data, m.Encryption, m.KMSKeyID, m.SSECKeyMD5)
	// P2-2: version-id
	if m.VersionID != "" {
		data["_version_id"] = m.VersionID
	}
	// P2-5: lifecycle expiration
	if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
		if exp := computeLifecycleExpiration(bucketMeta, key, m.LastModified); exp != "" {
			data["_expiration"] = exp
		}
	}
	overrides := map[string]string{}
	for _, pair := range [][2]string{
		{"response-content-disposition", "Content-Disposition"},
		{"response-content-language", "Content-Language"},
		{"response-content-encoding", "Content-Encoding"},
		{"response-cache-control", "Cache-Control"},
		{"response-expires", "Expires"},
	} {
		if v := strParam(nr.Params, pair[0]); v != "" {
			overrides[pair[1]] = v
		}
	}
	if len(overrides) > 0 {
		data["_response_overrides"] = overrides
	}
	return &model.ProviderResponse{HTTPStatus: status, Data: data}, nil
}

// parseByteRange parses a "bytes=<start>-<end>" Range header.
// Returns inclusive [start, end] indices and true on success.
func parseByteRange(hdr string, size int64) (int64, int64, bool) {
	hdr = strings.TrimSpace(hdr)
	if !strings.HasPrefix(hdr, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(hdr, "bytes=")
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
	requestedVersionID := strParam(nr.Params, "versionId")

	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)

	var m objectstore.ObjectMeta
	if vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended {
		if requestedVersionID != "" {
			vm, err := p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
			if err != nil {
				return nil, model.NewProviderError("NoSuchVersion", "The specified version does not exist", 404)
			}
			if vm.IsDeleteMarker {
				return nil, model.NewProviderError("MethodNotAllowed", "The specified method is not allowed against this resource", 405).
					WithData(map[string]any{"_delete_marker": true, "_version_id": requestedVersionID})
			}
			m = vm
		} else {
			versions, _, _ := p.meta.ListObjectVersions(ctx, bucket, key, "", "", 1)
			if len(versions) == 0 {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
			}
			if versions[0].IsDeleteMarker {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
			}
			m = versions[0]
		}
	} else {
		var err error
		m, err = p.meta.GetObjectMeta(ctx, bucket, key)
		if err != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
	}

	if resp304, pe := checkConditions(nr, objectCondMeta{ETag: m.ETag, LastModified: m.LastModified, ContentType: m.ContentType}); pe != nil {
		return nil, pe
	} else if resp304 != nil {
		return resp304, nil
	}
	data := map[string]any{
		"ETag":          m.ETag,
		"ContentLength": m.Size,
		"ContentType":   m.ContentType,
		"LastModified":  m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}
	if len(m.Metadata) > 0 {
		data["_metadata"] = m.Metadata
	}
	if len(m.Tags) > 0 {
		data["_tagging_count"] = len(m.Tags)
	}
	sseResponseData(data, m.Encryption, m.KMSKeyID, m.SSECKeyMD5)
	if m.VersionID != "" {
		data["_version_id"] = m.VersionID
	}
	if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
		if exp := computeLifecycleExpiration(bucketMeta, key, m.LastModified); exp != "" {
			data["_expiration"] = exp
		}
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: data}, nil
}

func (p *ObjectProvider) DeleteObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	requestedVersionID := strParam(nr.Params, "versionId")

	// P2-2: Versioning path
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)
	if vStatus == objectstore.VersioningEnabled {
		if requestedVersionID != "" {
			// Delete a specific version — check lock before deleting.
			m, err := p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
			if err == nil {
				if lockErr := checkObjectLock(nr, m); lockErr != nil {
					return nil, lockErr
				}
			}
			_ = p.meta.DeleteObjectVersion(ctx, bucket, key, requestedVersionID)
			_ = p.blobs.Delete(ctx, bucket, blobKeyForVersion(key, requestedVersionID))
			return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{
				"_version_id": requestedVersionID,
			}}, nil
		}
		// No versionId: insert a delete marker.
		marker := objectstore.ObjectMeta{
			Key:            key,
			IsDeleteMarker: true,
			LastModified:   time.Now().UTC(),
			StorageClass:   "STANDARD",
		}
		markerID, _ := p.meta.PutObjectVersion(ctx, bucket, key, marker)
		return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{
			"_delete_marker": true,
			"_version_id":    markerID,
		}}, nil
	}

	// Non-versioned path — check lock first.
	if m, err := p.meta.GetObjectMeta(ctx, bucket, key); err == nil {
		if lockErr := checkObjectLock(nr, m); lockErr != nil {
			return nil, lockErr
		}
	}

	// Metadata-first: after this succeeds, GetObject returns 404 immediately so
	// no caller ever sees metadata present + blob absent (torn state).
	if err := p.meta.DeleteObjectMeta(ctx, bucket, key); err != nil {
		// Only log; don't delete the blob — that would create the reverse torn state
		// (metadata present, blob gone). Real S3 returns 204 for missing keys so we
		// swallow the error and return success regardless.
		slog.Warn("object: metadata delete failed", "bucket", bucket, "key", key, "err", err)
		return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
	}
	// Best-effort blob delete. Any orphaned blob is invisible via GetObject.
	if err := p.blobs.Delete(ctx, bucket, key); err != nil {
		slog.Warn("object: blob delete failed after metadata delete", "bucket", bucket, "key", key, "err", err)
	}
	p.dispatchNotification(ctx, bucket, key, "s3:ObjectRemoved:Delete")
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// checkObjectLock returns an error if the object is protected by a lock.
func checkObjectLock(nr *model.NormalizedRequest, m objectstore.ObjectMeta) error {
	if m.LegalHoldStatus == "ON" {
		return model.NewProviderError("AccessDenied", "Object protected by legal hold", 403)
	}
	if m.LockRetainUntil != nil && time.Now().Before(*m.LockRetainUntil) {
		if m.LockMode == "COMPLIANCE" {
			return model.NewProviderError("AccessDenied", "Object locked in COMPLIANCE mode", 403)
		}
		if m.LockMode == "GOVERNANCE" && strParam(nr.Params, "_bypass_governance_retention") != "true" {
			return model.NewProviderError("AccessDenied", "Object locked in GOVERNANCE mode", 403)
		}
	}
	return nil
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

	srcMeta, err := p.meta.GetObjectMeta(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}

	// Copy-source conditional checks (params set by S3Codec).
	if _, pe := checkCopySourceConditions(nr, srcMeta); pe != nil {
		return nil, pe
	}

	data, err := p.blobs.Get(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}

	// P2-2: Determine dest blob key before writing.
	dstVStatus, _ := p.meta.GetBucketVersioning(ctx, dstBucket)
	var preDstVersionID string
	dstBlobKey := dstKey
	switch dstVStatus {
	case objectstore.VersioningEnabled:
		preDstVersionID = newVersionID()
		dstBlobKey = blobKeyForVersion(dstKey, preDstVersionID)
	case objectstore.VersioningSuspended:
		preDstVersionID = "null"
		dstBlobKey = blobKeyForVersion(dstKey, "null")
	}

	_ = p.blobs.Put(ctx, dstBucket, dstBlobKey, data)
	etagVal := etag(data)
	now := time.Now().UTC()
	dstMeta := objectstore.ObjectMeta{
		Key: dstKey, ETag: etagVal, CRC32: crc32Base64(data), Size: srcMeta.Size,
		LastModified: now, StorageClass: "STANDARD",
	}
	if strParam(nr.Params, "_metadata_directive") == "REPLACE" {
		dstMeta.ContentType = strParam(nr.Params, "_content_type")
		if dstMeta.ContentType == "" {
			dstMeta.ContentType = "application/octet-stream"
		}
		dstMeta.Metadata = extractUserMetadata(nr.Params)
	} else {
		dstMeta.ContentType = srcMeta.ContentType
		dstMeta.Metadata = srcMeta.Metadata
	}
	// P2-7: Tagging directive
	tagDirective := strParam(nr.Params, "_tagging_directive")
	if strings.ToUpper(tagDirective) == "REPLACE" {
		if tagging := strParam(nr.Params, "_tagging"); tagging != "" {
			tags, _ := parseTaggingHeader(tagging)
			dstMeta.Tags = tags
		}
	} else {
		dstMeta.Tags = srcMeta.Tags
	}
	// P2-1: SSE on destination
	enc, kmsKey, ssecMD5, sseErr := p.resolveSSE(ctx, nr, dstBucket)
	if sseErr != nil {
		return nil, sseErr
	}
	dstMeta.Encryption = enc
	dstMeta.KMSKeyID = kmsKey
	dstMeta.SSECKeyMD5 = ssecMD5
	// P2-4: ACL
	dstMeta.ACL = resolveACL(strParam(nr.Params, "_acl"), nr.AccountID)

	// P2-2: Versioning — use pre-generated versionID.
	var versionID string
	if dstVStatus == objectstore.VersioningEnabled {
		dstMeta.VersionID = preDstVersionID
		versionID, _ = p.meta.PutObjectVersion(ctx, dstBucket, dstKey, dstMeta)
		if err := p.meta.PutObjectMeta(ctx, dstBucket, dstKey, dstMeta); err != nil {
			slog.Warn("versioned CopyObject: failed to update current-object pointer", "bucket", dstBucket, "key", dstKey, "err", err)
		}
	} else if dstVStatus == objectstore.VersioningSuspended {
		dstMeta.VersionID = "null"
		versionID, _ = p.meta.PutObjectVersion(ctx, dstBucket, dstKey, dstMeta)
		if err := p.meta.PutObjectMeta(ctx, dstBucket, dstKey, dstMeta); err != nil {
			slog.Warn("suspended CopyObject: failed to update current-object pointer", "bucket", dstBucket, "key", dstKey, "err", err)
		}
	} else {
		if err := p.meta.PutObjectMeta(ctx, dstBucket, dstKey, dstMeta); err != nil {
			slog.Warn("CopyObject: failed to put object meta", "bucket", dstBucket, "key", dstKey, "err", err)
		}
	}
	respData := map[string]any{
		"CopyObjectResult": map[string]any{
			"ETag":         etagVal,
			"LastModified": now.UTC().Format(time.RFC3339),
		},
	}
	if versionID != "" {
		respData["_version_id"] = versionID
	}
	sseResponseData(respData, enc, kmsKey, ssecMD5)
	return provider.OK(respData), nil
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
	marker := strParam(nr.Params, "marker") // ListObjectsV1
	if marker == "" {
		marker = strParam(nr.Params, "continuation-token") // ListObjectsV2 subsequent pages
	}
	if marker == "" {
		marker = strParam(nr.Params, "start-after") // ListObjectsV2 first page with StartAfter
	}
	maxKeys := intParam(nr.Params, "max-keys", 1000)
	encodingType := strParam(nr.Params, "encoding-type")

	objects, commonPrefixes, truncated, nextMarker, err := p.meta.ListObjectMeta(ctx, bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}

	var contents []map[string]any
	for _, obj := range objects {
		k := obj.Key
		if encodingType == "url" {
			k = urlEncode(k)
		}
		contents = append(contents, map[string]any{
			"Key":          k,
			"ETag":         obj.ETag,
			"Size":         obj.Size,
			"LastModified": obj.LastModified.UTC().Format(time.RFC3339),
			"StorageClass": obj.StorageClass,
		})
	}
	if contents == nil {
		contents = []map[string]any{}
	}

	result := map[string]any{
		"Name":        bucket,
		"Prefix":      prefix,
		"Delimiter":   delimiter,
		"MaxKeys":     maxKeys,
		"IsTruncated": truncated,
		"Contents":    contents,
		"Marker":      marker,
	}
	if encodingType == "url" {
		result["EncodingType"] = "url"
		result["Prefix"] = urlEncode(prefix)
		result["Delimiter"] = urlEncode(delimiter)
		for i, cp := range commonPrefixes {
			commonPrefixes[i] = urlEncode(cp)
		}
	}
	result["CommonPrefixes"] = commonPrefixes
	if v2 {
		result["KeyCount"] = len(contents)
	}
	// Pass the opaque next-page token to the codec using a cloud-neutral key.
	// The codec translates it to the cloud-specific field name (e.g. AWS
	// NextContinuationToken for V2, NextMarker for V1).
	if truncated && nextMarker != "" {
		result["_nextPageToken"] = nextMarker
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
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)

	var deleted []map[string]any
	var errored []map[string]any

	for _, k := range keys {
		km, _ := k.(map[string]any)
		key, _ := km["Key"].(string)
		versionID, _ := km["VersionId"].(string)

		if vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended {
			if versionID != "" {
				// Delete a specific version.
				m, err := p.meta.GetObjectVersion(ctx, bucket, key, versionID)
				if err != nil {
					errored = append(errored, map[string]any{
						"Key": key, "VersionId": versionID,
						"Code": "NoSuchVersion", "Message": "The specified version does not exist",
					})
					continue
				}
				if lockErr := checkObjectLock(nr, m); lockErr != nil {
					errored = append(errored, map[string]any{
						"Key": key, "VersionId": versionID,
						"Code": "AccessDenied", "Message": lockErr.Error(),
					})
					continue
				}
				wasMarker := m.IsDeleteMarker
				if err := p.meta.DeleteObjectVersion(ctx, bucket, key, versionID); err != nil {
					errored = append(errored, map[string]any{
						"Key": key, "VersionId": versionID,
						"Code": "InternalError", "Message": err.Error(),
					})
					continue
				}
				if !wasMarker {
					_ = p.blobs.Delete(ctx, bucket, key)
				}
				entry := map[string]any{"Key": key, "VersionId": versionID}
				if wasMarker {
					entry["DeleteMarker"] = true
					entry["DeleteMarkerVersionId"] = versionID
				}
				deleted = append(deleted, entry)
			} else {
				// No versionId: create a delete marker.
				marker := objectstore.ObjectMeta{
					Key:            key,
					IsDeleteMarker: true,
				}
				markerID, err := p.meta.PutObjectVersion(ctx, bucket, key, marker)
				if err != nil {
					errored = append(errored, map[string]any{
						"Key": key, "Code": "InternalError", "Message": err.Error(),
					})
					continue
				}
				deleted = append(deleted, map[string]any{
					"Key":                  key,
					"DeleteMarker":         true,
					"DeleteMarkerVersionId": markerID,
				})
			}
		} else {
			// Non-versioned path.
			if err := p.meta.DeleteObjectMeta(ctx, bucket, key); err != nil {
				slog.Warn("object: meta delete failed in DeleteObjects", "bucket", bucket, "key", key, "err", err)
				continue
			}
			if err := p.blobs.Delete(ctx, bucket, key); err != nil {
				slog.Warn("object: blob delete failed in DeleteObjects", "bucket", bucket, "key", key, "err", err)
			}
			deleted = append(deleted, map[string]any{"Key": key})
		}
	}
	if deleted == nil {
		deleted = []map[string]any{}
	}
	result := map[string]any{"Deleted": deleted}
	if len(errored) > 0 {
		result["Errors"] = errored
	}
	return provider.OK(result), nil
}

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

// ─── helpers ──────────────────────────────────────────────────────────────────

// computeMultipartETag computes the S3 multipart ETag: hex(md5(concat(binary_etags))) + "-N".
func computeMultipartETag(etags []string) string {
	h := md5.New()
	for _, e := range etags {
		raw := strings.Trim(e, `"`)
		if b, err := hex.DecodeString(raw); err == nil {
			h.Write(b)
		} else {
			h.Write([]byte(raw))
		}
	}
	return fmt.Sprintf(`"%x-%d"`, h.Sum(nil), len(etags))
}

// extractUploadMeta recovers ContentType and user metadata from an InitMultipart meta map.
func extractUploadMeta(m map[string]any) (contentType string, metadata map[string]string) {
	contentType = "application/octet-stream"
	if m == nil {
		return
	}
	if ct, ok := m["content-type"].(string); ok && ct != "" {
		contentType = ct
	}
	switch um := m["user-metadata"].(type) {
	case map[string]string:
		metadata = um
	case map[string]interface{}:
		metadata = make(map[string]string, len(um))
		for k, v := range um {
			if s, ok := v.(string); ok {
				metadata[k] = s
			}
		}
	}
	return
}

// objectCondMeta holds the fields needed for conditional-request evaluation.
// Using a local type keeps checkConditions decoupled from the store package.
type objectCondMeta struct {
	ETag         string
	LastModified time.Time
	ContentType  string
}

// checkConditions evaluates conditional request headers per RFC 7232 §6 ordering:
// If-Match → If-Unmodified-Since → If-None-Match → If-Modified-Since.
// Returns (304 response, nil) for Not Modified, (nil, 412 error) for Precondition Failed,
// or (nil, nil) when all conditions are satisfied.
func checkConditions(nr *model.NormalizedRequest, m objectCondMeta) (*model.ProviderResponse, error) {
	etagRaw := strings.Trim(m.ETag, `"`)
	notModifiedResp := func() *model.ProviderResponse {
		return &model.ProviderResponse{HTTPStatus: 304, Data: map[string]any{
			"ETag":         m.ETag,
			"ContentType":  m.ContentType,
			"LastModified": m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
		}}
	}
	if v := strParam(nr.Params, "_cond_if_match"); v != "" {
		if v != "*" && strings.Trim(v, `"`) != etagRaw {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_cond_if_unmodified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && m.LastModified.After(t) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_cond_if_none_match"); v != "" {
		if v == "*" || strings.Trim(v, `"`) == etagRaw {
			return notModifiedResp(), nil
		}
	}
	if v := strParam(nr.Params, "_cond_if_modified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && !m.LastModified.After(t) {
			return notModifiedResp(), nil
		}
	}
	return nil, nil
}

// checkCopySourceConditions evaluates x-amz-copy-source-if-* params (set by S3Codec).
// Returns (nil, PreconditionFailed 412) when a condition fails, (nil, nil) when all pass.
func checkCopySourceConditions(nr *model.NormalizedRequest, m objectstore.ObjectMeta) (*model.ProviderResponse, error) {
	etagRaw := strings.Trim(m.ETag, `"`)
	if v := strParam(nr.Params, "_copy_source_if_match"); v != "" {
		if v != "*" && strings.Trim(v, `"`) != etagRaw {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_copy_source_if_unmodified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && m.LastModified.After(t) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_copy_source_if_none_match"); v != "" {
		if v == "*" || strings.Trim(v, `"`) == etagRaw {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_copy_source_if_modified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && !m.LastModified.After(t) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	return nil, nil
}

func parseHTTPDate(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC1123, "Mon, 02-Jan-2006 15:04:05 MST", time.ANSIC} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid HTTP date: %q", s)
}

func urlEncode(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// blobKeyForVersion returns the blob key for a versioned object.
func newVersionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// blobKeyForVersion returns the blob store key for a versioned object.
// Versioned blobs are stored under the __jc_ver__ prefix so they never
// collide with unversioned blob keys (which use the S3 key directly).
// The null/empty versionID case maps to the same path as the unversioned key
// (Suspended-mode behaviour: only one "null" version slot exists per key).
func blobKeyForVersion(key, versionID string) string {
	if versionID == "" || versionID == "null" {
		return key
	}
	return "__jc_ver__/" + versionID + "/" + key
}

// ─── updateBucketConfig ───────────────────────────────────────────────────────

func (p *ObjectProvider) updateBucketConfig(ctx context.Context, bucket string, mutate func(meta map[string]any)) error {
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return err
	}
	mutate(meta)
	return p.meta.UpdateBucketMeta(ctx, bucket, meta)
}

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
		if newUntil != nil && time.Now().After(*newUntil) {
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

// ─── Internal helpers (cross-provider) ───────────────────────────────────────

// InternalPutObject stores body into bucket/key directly, bypassing the HTTP
// codec.  Used by other providers (EMR log upload, etc.) to write to S3 without
// going through the full request pipeline.  Creates the bucket if it does not
// already exist (idempotent).
func (p *ObjectProvider) InternalPutObject(ctx context.Context, bucket, key, contentType string, body []byte) error {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	// Ensure the bucket exists — create it silently if absent.
	if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
		if createErr := p.meta.CreateBucket(ctx, bucket, map[string]any{"name": bucket}); createErr != nil {
			// Ignore "already exists" race; any other error is fatal.
			if _, checkErr := p.meta.GetBucket(ctx, bucket); checkErr != nil {
				return fmt.Errorf("InternalPutObject: create bucket %s: %w", bucket, createErr)
			}
		}
	}
	if err := p.blobs.Put(ctx, bucket, key, body); err != nil {
		return fmt.Errorf("InternalPutObject: put blob %s/%s: %w", bucket, key, err)
	}
	etagVal := etag(body)
	crc32Val := crc32Base64(body)
	meta := objectstore.ObjectMeta{
		Key:          key,
		ETag:         etagVal,
		CRC32:        crc32Val,
		Size:         int64(len(body)),
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
		StorageClass: "STANDARD",
	}
	return p.meta.PutObjectMeta(ctx, bucket, key, meta)
}

// InternalListObjects returns all object keys in bucket with the given prefix.
// Used by Glue crawlers for schema inference.
func (p *ObjectProvider) InternalListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	objs, _, _, _, err := p.meta.ListObjectMeta(ctx, bucket, prefix, "", "", 10000)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys, nil
}

// InternalGetObject fetches the body of an object.
// Used by Glue crawlers for schema inference.
func (p *ObjectProvider) InternalGetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	return p.blobs.Get(ctx, bucket, key)
}

// ─── P15.9: SelectObjectContent ──────────────────────────────────────────────

func (p *ObjectProvider) SelectObjectContent(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")

	m, err := p.meta.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}

	rc, err := p.blobs.GetStream(ctx, bucket, key, 0, -1)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	defer rc.Close()

	payload, err := io.ReadAll(rc)
	if err != nil {
		return nil, model.NewProviderError("InternalError", "Failed to read object", 500)
	}
	_ = m
	return provider.OK(map[string]any{"_select_payload": payload}), nil
}
