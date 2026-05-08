package services

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── extractVirtualHostedBucket ──────────────────────────────────────────────

func TestExtractVirtualHostedBucket_NoBasesConfigured(t *testing.T) {
	c := &S3Codec{}
	if got := c.extractVirtualHostedBucket("mybucket.example.com"); got != "" {
		t.Errorf("want empty string, got %q", got)
	}
}

func TestExtractVirtualHostedBucket_MatchesSingleBase(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.svc.cluster.local"}}
	got := c.extractVirtualHostedBucket("mybucket.jaiscloud.devbox.svc.cluster.local")
	assert.Equal(t, "mybucket", got)
}

func TestExtractVirtualHostedBucket_StripsPortBeforeMatching(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	got := c.extractVirtualHostedBucket("mybucket.jaiscloud.devbox.local:4566")
	assert.Equal(t, "mybucket", got, "port suffix must be stripped before base comparison")
}

func TestExtractVirtualHostedBucket_HighPortNumber(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	got := c.extractVirtualHostedBucket("data-bucket.devbox.local:9000")
	assert.Equal(t, "data-bucket", got)
}

func TestExtractVirtualHostedBucket_NoMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	got := c.extractVirtualHostedBucket("mybucket.other.local")
	assert.Equal(t, "", got)
}

func TestExtractVirtualHostedBucket_MultipleBasesFirstMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"first.local", "second.local"}}
	assert.Equal(t, "bkt", c.extractVirtualHostedBucket("bkt.first.local"))
}

func TestExtractVirtualHostedBucket_MultipleBasesSecondMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"first.local", "second.local"}}
	assert.Equal(t, "bkt", c.extractVirtualHostedBucket("bkt.second.local"))
}

func TestExtractVirtualHostedBucket_HostEqualsBase_NoBucketPrefix(t *testing.T) {
	// A request to the bare base hostname has no bucket prefix — must return ""
	// so Decode falls through to path-style (ListBuckets, etc.).
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.local"}}
	got := c.extractVirtualHostedBucket("jaiscloud.local")
	assert.Equal(t, "", got)
}

func TestExtractVirtualHostedBucket_BaseAsPartialSubstring(t *testing.T) {
	// base=devbox.local must not match host=mybucket.notdevbox.local
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	got := c.extractVirtualHostedBucket("mybucket.notdevbox.local")
	assert.Equal(t, "", got)
}

func TestExtractVirtualHostedBucket_HyphenatedBucketName(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	got := c.extractVirtualHostedBucket("my-data-bucket.devbox.local")
	assert.Equal(t, "my-data-bucket", got)
}

// ─── Decode: virtual-hosted routing ──────────────────────────────────────────

// newReq builds a minimal *http.Request suitable for S3Codec.Decode tests.
// host overrides r.Host so tests control exactly what the codec sees.
func newS3Req(method, rawURL, host string) *http.Request {
	req, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		panic(err)
	}
	req.Host = host
	return req
}

func TestS3Decode_CustomVirtualHosted_BucketAndKey(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	req := newS3Req(http.MethodGet, "http://mybucket.jaiscloud.devbox.local/prefix/key.txt", "mybucket.jaiscloud.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "prefix/key.txt", nr.Params["_key"])
	assert.Equal(t, "GetObject", nr.Action)
}

func TestS3Decode_CustomVirtualHosted_WithPort(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	req := newS3Req(http.MethodPut, "http://mybucket.jaiscloud.devbox.local:4566/mykey",
		"mybucket.jaiscloud.devbox.local:4566")

	nr, err := c.Decode(req, []byte("data"))
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "mykey", nr.Params["_key"])
}

func TestS3Decode_CustomVirtualHosted_ListObjectsV2(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	req := newS3Req(http.MethodGet, "http://mybucket.devbox.local/?list-type=2&prefix=logs/",
		"mybucket.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "", nr.Params["_key"])
	assert.Equal(t, "ListObjectsV2", nr.Action)
	assert.Equal(t, "logs/", nr.Params["prefix"])
}

func TestS3Decode_CustomVirtualHosted_HeadObject(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	req := newS3Req(http.MethodHead, "http://bkt.devbox.local/some/object", "bkt.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "bkt", nr.Params["_bucket"])
	assert.Equal(t, "some/object", nr.Params["_key"])
	assert.Equal(t, "HeadObject", nr.Action)
}

func TestS3Decode_CustomVirtualHosted_CreateBucket(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	req := newS3Req(http.MethodPut, "http://newbucket.devbox.local/", "newbucket.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "newbucket", nr.Params["_bucket"])
	assert.Equal(t, "CreateBucket", nr.Action)
}

func TestS3Decode_AWSVirtualHostedTakesPriorityOverCustomBase(t *testing.T) {
	// The existing ".s3." check runs before the custom-base branch; a custom base
	// that overlaps with real S3 hostnames must not interfere.
	c := &S3Codec{VirtualHostBases: []string{"s3.us-east-1.amazonaws.com"}}
	req := newS3Req(http.MethodGet, "http://mybucket.s3.us-east-1.amazonaws.com/k",
		"mybucket.s3.us-east-1.amazonaws.com")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "k", nr.Params["_key"])
}

func TestS3Decode_PathStyleFallsThrough_WhenNoBaseMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	req := newS3Req(http.MethodGet, "http://localhost:4566/mybucket/mykey", "localhost:4566")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "mykey", nr.Params["_key"])
}

func TestS3Decode_PathStyle_NoBasesConfigured(t *testing.T) {
	c := &S3Codec{}
	req := newS3Req(http.MethodGet, "http://localhost:4566/bucket/key", "localhost:4566")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "bucket", nr.Params["_bucket"])
	assert.Equal(t, "key", nr.Params["_key"])
}

// ─── decodeAWSChunked ─────────────────────────────────────────────────────────

func TestDecodeAWSChunked(t *testing.T) {
	t.Run("single chunk", func(t *testing.T) {
		body := []byte("5\r\nhello\r\n0\r\n\r\n")
		assert.Equal(t, []byte("hello"), decodeAWSChunked(body))
	})

	t.Run("chunk with signature extension", func(t *testing.T) {
		body := []byte("5;chunk-signature=abc123\r\nhello\r\n0;chunk-signature=def456\r\n\r\n")
		assert.Equal(t, []byte("hello"), decodeAWSChunked(body))
	})

	t.Run("multiple chunks", func(t *testing.T) {
		body := []byte("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n")
		assert.Equal(t, []byte("hello world"), decodeAWSChunked(body))
	})

	t.Run("plain body passthrough", func(t *testing.T) {
		body := []byte("just plain text, not chunked")
		assert.Equal(t, body, decodeAWSChunked(body))
	})

	t.Run("empty body", func(t *testing.T) {
		assert.Equal(t, []byte(nil), decodeAWSChunked([]byte{}))
	})

	t.Run("hex chunk size 0x100 = 256 bytes", func(t *testing.T) {
		data := make([]byte, 256)
		for i := range data {
			data[i] = byte('a' + i%26)
		}
		// Build: "100\r\n<256 bytes>\r\n0\r\n\r\n"
		chunk := append([]byte("100\r\n"), data...)
		chunk = append(chunk, []byte("\r\n0\r\n\r\n")...)
		assert.Equal(t, data, decodeAWSChunked(chunk))
	})
}

// ─── Non-streaming actions with streaming-flagged request ─────────────────────
// The AWS SDK sends Content-Encoding: aws-chunked even for small XML-body
// operations like PutObjectLegalHold. The gateway marks those as "streaming"
// and passes body=nil to the codec. The codec must re-read r.Body for any
// action that is NOT PutObject/UploadPart.

func TestDecode_PutObjectLegalHold_StreamingFlaggedRequest(t *testing.T) {
	xmlBody := `<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>`
	rawURL := "http://localhost:4566/my-bucket/obj.txt?legal-hold"
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader(xmlBody))
	req.Header.Set("Content-Encoding", "aws-chunked") // triggers streaming gate in gateway
	req.Header.Set("Host", "localhost:4566")

	c := &S3Codec{}
	// Gateway skips io.ReadAll when Content-Encoding: aws-chunked → passes nil body
	nr, err := c.Decode(req, nil)
	require.NoError(t, err, "PutObjectLegalHold with nil body must not return MalformedXML")
	assert.Equal(t, "PutObjectLegalHold", nr.Action)
	// Codec must have re-read r.Body and stored it in _body
	body, ok := nr.Params["_body"].([]byte)
	require.True(t, ok, "_body must be a []byte")
	assert.Equal(t, xmlBody, string(body))
}

func TestDecode_PutObjectRetention_StreamingFlaggedRequest(t *testing.T) {
	xmlBody := `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>`
	rawURL := "http://localhost:4566/my-bucket/obj.txt?retention"
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader(xmlBody))
	req.Header.Set("Content-Encoding", "aws-chunked")

	c := &S3Codec{}
	nr, err := c.Decode(req, nil)
	require.NoError(t, err)
	assert.Equal(t, "PutObjectRetention", nr.Action)
	body, ok := nr.Params["_body"].([]byte)
	require.True(t, ok)
	assert.Equal(t, xmlBody, string(body))
}

func TestDecode_PutObject_StreamingFlaggedRequest_BodyNotRead(t *testing.T) {
	// For PutObject, body=nil must NOT be re-read from r.Body — that is the
	// streaming provider's job.
	rawURL := "http://localhost:4566/my-bucket/obj.txt"
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader("blob data"))
	req.Header.Set("Content-Encoding", "aws-chunked")

	c := &S3Codec{}
	nr, err := c.Decode(req, nil)
	require.NoError(t, err)
	assert.Equal(t, "PutObject", nr.Action)
	// _streaming must be true and _body must be nil — provider streams from nr.Raw.Body
	_, streaming := nr.Params["_streaming"]
	assert.True(t, streaming, "_streaming flag must be set for PutObject with nil body")
	body := nr.Params["_body"]
	assert.Nil(t, body, "_body must remain nil for PutObject so the provider can stream it")
}

// ─── P2-8: Presigned URL expiration ──────────────────────────────────────────

func TestPresigned_ExpiredSigV4_403(t *testing.T) {
	// X-Amz-Date in the past, expires in 1 second → already expired.
	past := time.Now().Add(-10 * time.Minute)
	dateStr := past.UTC().Format("20060102T150405Z")
	rawURL := fmt.Sprintf(
		"http://localhost:4566/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=%s&X-Amz-Expires=1&X-Amz-Signature=fake",
		dateStr,
	)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	c := &S3Codec{}
	_, err := c.Decode(req, nil)
	require.Error(t, err, "expired presigned URL must return an error")
}

func TestPresigned_ValidSigV4_Succeeds(t *testing.T) {
	// X-Amz-Date now, expires in 3600 seconds → still valid.
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	rawURL := fmt.Sprintf(
		"http://localhost:4566/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=%s&X-Amz-Expires=3600&X-Amz-Signature=fake",
		dateStr,
	)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	c := &S3Codec{}
	nr, err := c.Decode(req, nil)
	require.NoError(t, err, "valid presigned URL must not return an error")
	assert.Equal(t, "GetObject", nr.Action)
}

func TestPresigned_ExpiredSigV2_403(t *testing.T) {
	// Expires is a Unix timestamp in the past.
	expiredUnix := time.Now().Add(-1 * time.Minute).Unix()
	rawURL := fmt.Sprintf(
		"http://localhost:4566/bucket/key?AWSAccessKeyId=AKID&Signature=fake&Expires=%d",
		expiredUnix,
	)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	c := &S3Codec{}
	_, err := c.Decode(req, nil)
	require.Error(t, err, "expired SigV2 presigned URL must return an error")
}
