package services

import (
	"net/http"
	"testing"

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
