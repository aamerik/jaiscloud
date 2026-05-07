package services

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
)

// decodeAWSChunked strips AWS chunked-upload framing.
// Each chunk: <hex-size>[;chunk-signature=...]\r\n<data>\r\n
// Final chunk: 0[;...]\r\n
// Returns body unchanged if it does not look like aws-chunked format.
func decodeAWSChunked(body []byte) []byte {
	var out []byte
	// parsedChunked tracks whether we have successfully recognised at least one
	// valid chunk header (including the terminal 0-length chunk). If true, we
	// know the body was valid aws-chunked and it is safe to return an empty
	// slice when no data chunks were present (i.e. an empty object upload).
	parsedChunked := false
	remaining := body
	for len(remaining) > 0 {
		idx := strings.Index(string(remaining), "\r\n")
		if idx < 0 {
			break
		}
		sizeLine := string(remaining[:idx])
		if semi := strings.IndexByte(sizeLine, ';'); semi >= 0 {
			sizeLine = sizeLine[:semi]
		}
		sizeLine = strings.TrimSpace(sizeLine)
		if sizeLine == "" {
			return body
		}
		chunkLen, err := strconv.ParseInt(sizeLine, 16, 64)
		if err != nil {
			return body // not aws-chunked, pass through unchanged
		}
		parsedChunked = true
		remaining = remaining[idx+2:]
		if chunkLen == 0 {
			break // final chunk — end of valid chunked data
		}
		if int64(len(remaining)) < chunkLen {
			return body // truncated, pass through
		}
		out = append(out, remaining[:chunkLen]...)
		remaining = remaining[chunkLen:]
		if len(remaining) >= 2 && remaining[0] == '\r' && remaining[1] == '\n' {
			remaining = remaining[2:]
		}
	}
	// Only fall back to the raw body if we never saw a valid chunk header.
	// An empty aws-chunked stream (terminal 0-chunk only) correctly decodes
	// to empty bytes — do not return the raw framing bytes in that case.
	if len(out) == 0 && len(body) > 0 && !parsedChunked {
		return body
	}
	return out
}

// s3ChecksumCRC32 returns the base64-encoded IEEE CRC32 of body.
// The AWS SDK v2 validates this on PutObject / CopyObject / CompleteMultipartUpload responses.
func s3ChecksumCRC32(body []byte) string {
	sum := crc32.ChecksumIEEE(body)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)
	return base64.StdEncoding.EncodeToString(b)
}

// S3Codec handles S3 REST wire format (path-style and virtual-hosted URLs).
// VirtualHostBases lists host suffixes (e.g. jaiscloud.devbox.svc.cluster.local)
// that the codec treats as virtual-hosted: "bucket.<base>" -> bucket extracted
// from the hostname, key from the URL path.
type S3Codec struct {
	VirtualHostBases []string
}

func (c *S3Codec) ServiceName() string { return "s3" }

// extractVirtualHostedBucket returns the bucket name when the request host
// matches one of the configured VirtualHostBases (strips port first).
// Returns "" if no base matches.
func (c *S3Codec) extractVirtualHostedBucket(host string) string {
	// Strip port suffix if present.
	h := host
	if idx := strings.LastIndexByte(h, ':'); idx >= 0 {
		h = h[:idx]
	}
	for _, base := range c.VirtualHostBases {
		suffix := "." + base
		if strings.HasSuffix(h, suffix) {
			return h[:len(h)-len(suffix)]
		}
	}
	return ""
}

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *S3Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	// Decode AWS chunked body so ETag/checksum are computed over actual content.
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") ||
		strings.HasPrefix(r.Header.Get("x-amz-content-sha256"), "STREAMING-") ||
		r.Header.Get("x-amz-decoded-content-length") != "" {
		body = decodeAWSChunked(body)
	}

	var bucket, key string
	if host := r.Host; strings.Contains(host, ".s3.") {
		// Virtual-hosted: "mybucket.s3.us-east-1.amazonaws.com"
		bucket = host[:strings.Index(host, ".s3.")]
		key = strings.TrimPrefix(r.URL.Path, "/")
	} else if b := c.extractVirtualHostedBucket(r.Host); b != "" {
		// Virtual-hosted with custom base: "mybucket.jaiscloud.devbox.svc.cluster.local[:port]"
		bucket = b
		key = strings.TrimPrefix(r.URL.Path, "/")
	} else {
		// Path-style: /{bucket}/{key...}
		path := strings.TrimPrefix(r.URL.Path, "/")
		if idx := strings.IndexByte(path, '/'); idx >= 0 {
			bucket = path[:idx]
			key = path[idx+1:]
		} else {
			bucket = path
		}
	}

	query := r.URL.Query()
	action := s3DetectAction(r.Method, bucket, key, query, r.Header)

	params := map[string]any{
		"_bucket": bucket,
		"_key":    key,
		"_body":   body,
	}
	// body == nil only when the gateway determined this is a streaming upload
	// (PutObject / UploadPart) and intentionally skipped io.ReadAll.
	if body == nil {
		params["_streaming"] = true
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		params["_content_type"] = ct
	}
	if cs := r.Header.Get("X-Amz-Copy-Source"); cs != "" {
		params["_copy_source"] = cs
	}
	if rng := r.Header.Get("Range"); rng != "" {
		params["_range"] = rng
	}
	if md := r.Header.Get("X-Amz-Metadata-Directive"); md != "" {
		params["_metadata_directive"] = md
	}
	if csr := r.Header.Get("X-Amz-Copy-Source-Range"); csr != "" {
		params["_copy_source_range"] = csr
	}
	if v := r.Header.Get("If-Match"); v != "" {
		params["_cond_if_match"] = v
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		params["_cond_if_none_match"] = v
	}
	if v := r.Header.Get("If-Modified-Since"); v != "" {
		params["_cond_if_modified_since"] = v
	}
	if v := r.Header.Get("If-Unmodified-Since"); v != "" {
		params["_cond_if_unmodified_since"] = v
	}
	// Capture x-amz-meta-* user metadata headers.
	for k, vs := range r.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(vs) > 0 {
			params["_meta_"+strings.TrimPrefix(lower, "x-amz-meta-")] = vs[0]
		}
	}
	// Capture inbound flexible checksum to echo back in response.
	for _, algo := range []string{"crc32", "crc32c", "sha256", "sha1"} {
		if v := r.Header.Get("x-amz-checksum-" + algo); v != "" {
			params["_checksum_header"] = "x-amz-checksum-" + algo
			params["_checksum_value"] = v
			break
		}
	}
	// Propagate all URL query params (prefix, delimiter, marker, max-keys, etc.)
	for k, vs := range query {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	// DeleteObjects: parse XML body into params["Delete"]
	if action == "DeleteObjects" && len(body) > 0 {
		var deleteReq struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		if xml.Unmarshal(body, &deleteReq) == nil {
			objs := make([]any, 0, len(deleteReq.Objects))
			for _, o := range deleteReq.Objects {
				objs = append(objs, map[string]any{"Key": o.Key})
			}
			params["Delete"] = map[string]any{"Object": objs}
		}
	}

	if action == "CreateBucket" && !isValidBucketName(bucket) {
		return nil, model.NewProviderError("InvalidBucketName", "The specified bucket is not valid", 400)
	}

	return &model.NormalizedRequest{
		Service: "s3",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

// isValidBucketName enforces AWS S3 bucket naming rules:
// 3–63 chars, lowercase alphanumeric/hyphens/dots, no leading/trailing hyphens or dots, no "..".
func isValidBucketName(name string) bool {
	n := len(name)
	if n < 3 || n > 63 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.') {
			return false
		}
	}
	if name[0] == '-' || name[0] == '.' || name[n-1] == '-' || name[n-1] == '.' {
		return false
	}
	return !strings.Contains(name, "..")
}

func s3DetectAction(method, bucket, key string, query url.Values, headers http.Header) string {
	hasCopySource := headers.Get("X-Amz-Copy-Source") != ""

	if bucket == "" {
		return "ListBuckets"
	}

	if key == "" {
		// Bucket-level operations
		switch method {
		case http.MethodHead:
			return "HeadBucket"
		case http.MethodDelete:
			return "DeleteBucket"
		case http.MethodPut:
			switch {
			case query.Has("acl"):
				return "PutBucketAcl"
			case query.Has("tagging"):
				return "PutBucketTagging"
			case query.Has("versioning"):
				return "PutBucketVersioning"
			default:
				return "CreateBucket"
			}
		case http.MethodGet:
			switch {
			case query.Has("location"):
				return "GetBucketLocation"
			case query.Has("uploads"):
				return "ListMultipartUploads"
			case query.Get("list-type") == "2":
				return "ListObjectsV2"
			case query.Has("acl"):
				return "GetBucketAcl"
			case query.Has("tagging"):
				return "GetBucketTagging"
			case query.Has("versioning"):
				return "GetBucketVersioning"
			default:
				return "ListObjectsV1"
			}
		case http.MethodPost:
			if query.Has("delete") {
				return "DeleteObjects"
			}
		}
		return "ListObjectsV1"
	}

	// Object-level operations
	switch method {
	case http.MethodGet:
		switch {
		case query.Has("uploadId") && !query.Has("partNumber"):
			return "ListParts"
		case query.Has("tagging"):
			return "GetObjectTagging"
		case query.Has("acl"):
			return "GetObjectAcl"
		default:
			return "GetObject"
		}
	case http.MethodHead:
		return "HeadObject"
	case http.MethodPut:
		switch {
		case hasCopySource && query.Has("partNumber"):
			return "UploadPartCopy"
		case hasCopySource:
			return "CopyObject"
		case query.Has("partNumber"):
			return "UploadPart"
		case query.Has("tagging"):
			return "PutObjectTagging"
		case query.Has("acl"):
			return "PutObjectAcl"
		default:
			return "PutObject"
		}
	case http.MethodDelete:
		switch {
		case query.Has("uploadId"):
			return "AbortMultipartUpload"
		case query.Has("tagging"):
			return "DeleteObjectTagging"
		default:
			return "DeleteObject"
		}
	case http.MethodPost:
		switch {
		case query.Has("uploads"):
			return "CreateMultipartUpload"
		case query.Has("uploadId"):
			return "CompleteMultipartUpload"
		}
	}
	return "GetObject"
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *S3Codec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	// Raw body passthrough (GetObject / range reads)
	if pass, _ := resp.Data["_passthrough"].(bool); pass {
		h := http.Header{}
		if ct, ok := resp.Data["_content_type"].(string); ok && ct != "" {
			h.Set("Content-Type", ct)
		} else {
			h.Set("Content-Type", "application/octet-stream")
		}
		if etag, ok := resp.Data["ETag"].(string); ok {
			h.Set("ETag", etag)
		}
		if lm, ok := resp.Data["LastModified"].(string); ok {
			h.Set("Last-Modified", lm)
		}
		if cl, ok := resp.Data["ContentLength"]; ok {
			h.Set("Content-Length", fmt.Sprintf("%v", cl))
		}
		// Emit x-amz-meta-* user metadata headers.
		if md, ok := resp.Data["_metadata"].(map[string]string); ok {
			for k, v := range md {
				h.Set("x-amz-meta-"+k, v)
			}
		}
		// Response header overrides (response-content-disposition etc.)
		if overrides, ok := resp.Data["_response_overrides"].(map[string]string); ok {
			for k, v := range overrides {
				h.Set(k, v)
			}
		}
		// Content-Range for partial content responses.
		if start, ok := resp.Data["_range_start"].(int64); ok {
			end, _ := resp.Data["_range_end"].(int64)
			total, _ := resp.Data["_range_total"].(int64)
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		}
		// CRC32: prefer value stored at upload time; fall back to computing from body.
		if crc32v, ok := resp.Data["_crc32"].(string); ok && crc32v != "" {
			h.Set("x-amz-checksum-crc32", crc32v)
		}
		status := resp.HTTPStatus
		if s, ok := resp.Data["_status"].(int); ok {
			status = s
		}
		// Streaming response: headers are set; gateway will io.Copy the stream.
		if _, ok := resp.Data["_stream"].(io.ReadCloser); ok {
			return status, h, nil
		}
		body, _ := resp.Data["_raw_body"].([]byte)
		if h.Get("x-amz-checksum-crc32") == "" {
			h.Set("x-amz-checksum-crc32", s3ChecksumCRC32(body))
		}
		return status, h, body
	}

	// 304 Not Modified — ETag/Last-Modified headers, no body.
	if resp.HTTPStatus == 304 {
		return 304, s3MetaHeaders(resp.Data), nil
	}

	// HeadObject / HeadBucket — headers only, no body
	if nr.Action == "HeadObject" || nr.Action == "HeadBucket" {
		h := s3MetaHeaders(resp.Data)
		return resp.HTTPStatus, h, nil
	}

	// No-body responses
	if resp.HTTPStatus == 204 || resp.HTTPStatus == 0 {
		h := http.Header{}
		if etag, ok := resp.Data["ETag"].(string); ok {
			h.Set("ETag", etag)
		}
		return 204, h, nil
	}

	// PutObject / UploadPart — ETag header, empty body
	if nr.Action == "PutObject" || nr.Action == "UploadPart" {
		h := http.Header{}
		if etag, ok := resp.Data["ETag"].(string); ok {
			h.Set("ETag", etag)
		}
		if nr.Action == "PutObject" {
			if hdr, ok := nr.Params["_checksum_header"].(string); ok {
				// Client supplied a checksum algorithm — echo the value back.
				if val, ok := nr.Params["_checksum_value"].(string); ok {
					h.Set(hdr, val)
				}
			} else if crc32v, ok := resp.Data["_server_crc32"].(string); ok && crc32v != "" {
				// Provider computed CRC32 during streaming write.
				h.Set("x-amz-checksum-crc32", crc32v)
			} else {
				// Buffered path: compute from the in-memory body.
				uploadedBody, _ := nr.Params["_body"].([]byte)
				h.Set("x-amz-checksum-crc32", s3ChecksumCRC32(uploadedBody))
			}
		}
		return resp.HTTPStatus, h, nil
	}

	// CreateBucket — Location header
	if nr.Action == "CreateBucket" {
		h := http.Header{}
		if loc, ok := resp.Data["Location"].(string); ok {
			h.Set("Location", loc)
		}
		return resp.HTTPStatus, h, nil
	}

	// XML body for all other operations
	body := s3BuildXML(nr.Action, resp.Data)
	h := http.Header{}
	if len(body) > 0 {
		h.Set("Content-Type", "application/xml")
	}
	// AWS SDK v2 validates CRC32 on these write operations
	if nr.Action == "CopyObject" || nr.Action == "CompleteMultipartUpload" {
		h.Set("x-amz-checksum-crc32", s3ChecksumCRC32(body))
	}
	return resp.HTTPStatus, h, body
}

func s3MetaHeaders(data map[string]any) http.Header {
	h := http.Header{}
	if ct, ok := data["ContentType"].(string); ok {
		h.Set("Content-Type", ct)
	}
	if etag, ok := data["ETag"].(string); ok {
		h.Set("ETag", etag)
	}
	if lm, ok := data["LastModified"].(string); ok {
		h.Set("Last-Modified", lm)
	}
	if cl, ok := data["ContentLength"]; ok {
		h.Set("Content-Length", fmt.Sprintf("%v", cl))
	}
	if md, ok := data["_metadata"].(map[string]string); ok {
		for k, v := range md {
			h.Set("x-amz-meta-"+k, v)
		}
	}
	if region, ok := data["_region"].(string); ok && region != "" {
		h.Set("x-amz-bucket-region", region)
	}
	return h
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *S3Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/xml")
	var extra strings.Builder
	for k, v := range perr.Data {
		extra.WriteString(fmt.Sprintf("<%s>%s</%s>", xmlEscape(k), xmlEscape(fmt.Sprint(v)), xmlEscape(k)))
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<Error><Code>%s</Code><Message>%s</Message>%s`+
			`<RequestId>00000000-0000-0000-0000-000000000000</RequestId></Error>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message), extra.String(),
	)
	return perr.HTTPStatus, h, []byte(body)
}

// ─── XML builder ──────────────────────────────────────────────────────────────

func s3BuildXML(action string, data map[string]any) []byte {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)

	switch action {
	case "ListBuckets":
		sb.WriteString(`<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if owner, ok := data["Owner"].(map[string]any); ok {
			sb.WriteString("<Owner>")
			sb.WriteString(xmlTag("ID", str(owner["ID"])))
			sb.WriteString(xmlTag("DisplayName", str(owner["DisplayName"])))
			sb.WriteString("</Owner>")
		}
		sb.WriteString("<Buckets>")
		if buckets, ok := data["Buckets"].([]map[string]any); ok {
			for _, b := range buckets {
				sb.WriteString("<Bucket>")
				sb.WriteString(xmlTag("Name", str(b["Name"])))
				sb.WriteString(xmlTag("CreationDate", str(b["CreationDate"])))
				sb.WriteString("</Bucket>")
			}
		}
		sb.WriteString("</Buckets>")
		sb.WriteString("</ListAllMyBucketsResult>")

	case "ListObjectsV1":
		sb.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Name", str(data["Name"])))
		sb.WriteString(xmlTag("Prefix", str(data["Prefix"])))
		sb.WriteString(xmlTag("MaxKeys", str(data["MaxKeys"])))
		if et := str(data["EncodingType"]); et != "" {
			sb.WriteString(xmlTag("EncodingType", et))
		}
		sb.WriteString(xmlTag("IsTruncated", str(data["IsTruncated"])))
		sb.WriteString(xmlTag("Marker", str(data["Marker"])))
		if nm := str(data["_nextPageToken"]); nm != "" && str(data["Delimiter"]) != "" {
			sb.WriteString(xmlTag("NextMarker", nm))
		}
		if contents, ok := data["Contents"].([]map[string]any); ok {
			for _, obj := range contents {
				sb.WriteString("<Contents>")
				sb.WriteString(xmlTag("Key", str(obj["Key"])))
				sb.WriteString(xmlTag("LastModified", str(obj["LastModified"])))
				sb.WriteString(xmlTag("ETag", str(obj["ETag"])))
				sb.WriteString(xmlTag("Size", str(obj["Size"])))
				sb.WriteString(xmlTag("StorageClass", str(obj["StorageClass"])))
				sb.WriteString("</Contents>")
			}
		}
		if cps, ok := data["CommonPrefixes"].([]string); ok {
			for _, cp := range cps {
				sb.WriteString("<CommonPrefixes>")
				sb.WriteString(xmlTag("Prefix", cp))
				sb.WriteString("</CommonPrefixes>")
			}
		}
		sb.WriteString("</ListBucketResult>")

	case "ListObjectsV2":
		sb.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Name", str(data["Name"])))
		sb.WriteString(xmlTag("Prefix", str(data["Prefix"])))
		sb.WriteString(xmlTag("MaxKeys", str(data["MaxKeys"])))
		sb.WriteString(xmlTag("KeyCount", str(data["KeyCount"])))
		if et := str(data["EncodingType"]); et != "" {
			sb.WriteString(xmlTag("EncodingType", et))
		}
		sb.WriteString(xmlTag("IsTruncated", str(data["IsTruncated"])))
		if nct := str(data["_nextPageToken"]); nct != "" {
			sb.WriteString(xmlTag("NextContinuationToken", nct))
		}
		if contents, ok := data["Contents"].([]map[string]any); ok {
			for _, obj := range contents {
				sb.WriteString("<Contents>")
				sb.WriteString(xmlTag("Key", str(obj["Key"])))
				sb.WriteString(xmlTag("LastModified", str(obj["LastModified"])))
				sb.WriteString(xmlTag("ETag", str(obj["ETag"])))
				sb.WriteString(xmlTag("Size", str(obj["Size"])))
				sb.WriteString(xmlTag("StorageClass", str(obj["StorageClass"])))
				sb.WriteString("</Contents>")
			}
		}
		if cps, ok := data["CommonPrefixes"].([]string); ok {
			for _, cp := range cps {
				sb.WriteString("<CommonPrefixes>")
				sb.WriteString(xmlTag("Prefix", cp))
				sb.WriteString("</CommonPrefixes>")
			}
		}
		sb.WriteString("</ListBucketResult>")

	case "GetBucketLocation":
		lc := str(data["LocationConstraint"])
		// S3 wire protocol represents us-east-1 as an empty LocationConstraint element.
		if lc == "us-east-1" {
			lc = ""
		}
		if lc == "" {
			sb.WriteString(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
		} else {
			sb.WriteString(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
			sb.WriteString(xmlEscape(lc))
			sb.WriteString(`</LocationConstraint>`)
		}

	case "GetBucketVersioning":
		sb.WriteString(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)

	case "GetBucketAcl", "GetObjectAcl":
		sb.WriteString(`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString("<Owner><ID>owner</ID><DisplayName>owner</DisplayName></Owner>")
		sb.WriteString("<AccessControlList>")
		sb.WriteString("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">")
		sb.WriteString("<ID>owner</ID><DisplayName>owner</DisplayName></Grantee>")
		sb.WriteString("<Permission>FULL_CONTROL</Permission></Grant>")
		sb.WriteString("</AccessControlList>")
		sb.WriteString("</AccessControlPolicy>")

	case "GetObjectTagging", "GetBucketTagging":
		sb.WriteString(`<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet/></Tagging>`)

	case "CreateMultipartUpload":
		sb.WriteString(`<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		sb.WriteString(xmlTag("Key", str(data["Key"])))
		sb.WriteString(xmlTag("UploadId", str(data["UploadId"])))
		sb.WriteString("</InitiateMultipartUploadResult>")

	case "CompleteMultipartUpload":
		sb.WriteString(`<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Location", str(data["Location"])))
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		sb.WriteString(xmlTag("Key", str(data["Key"])))
		sb.WriteString(xmlTag("ETag", str(data["ETag"])))
		sb.WriteString("</CompleteMultipartUploadResult>")

	case "ListMultipartUploads":
		sb.WriteString(`<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		sb.WriteString("</ListMultipartUploadsResult>")

	case "ListParts":
		sb.WriteString(`<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString("</ListPartsResult>")

	case "DeleteObjects":
		sb.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if deleted, ok := data["Deleted"].([]map[string]any); ok {
			for _, d := range deleted {
				sb.WriteString("<Deleted>")
				sb.WriteString(xmlTag("Key", str(d["Key"])))
				sb.WriteString("</Deleted>")
			}
		}
		sb.WriteString("</DeleteResult>")

	case "CopyObject":
		if result, ok := data["CopyObjectResult"].(map[string]any); ok {
			sb.WriteString(`<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
			sb.WriteString(xmlTag("ETag", str(result["ETag"])))
			sb.WriteString(xmlTag("LastModified", str(result["LastModified"])))
			sb.WriteString("</CopyObjectResult>")
		}

	case "UploadPartCopy":
		if result, ok := data["CopyPartResult"].(map[string]any); ok {
			sb.WriteString(`<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
			sb.WriteString(xmlTag("ETag", str(result["ETag"])))
			sb.WriteString(xmlTag("LastModified", str(result["LastModified"])))
			sb.WriteString("</CopyPartResult>")
		}
	}

	return []byte(sb.String())
}
