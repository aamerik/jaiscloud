package aws

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// newRouterReq builds a GET request; callers set headers/query as needed.
func newRouterReq(rawURL string) *http.Request {
	return httptest.NewRequest(http.MethodGet, rawURL, nil)
}

// ─── Priority 2: SigV4 Authorization header ──────────────────────────────────

func TestDetectService_SigV4AuthHeader_KnownService(t *testing.T) {
	r := newRouterReq("http://localhost:4566/")
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")
	svc, src := DetectService(r, nil)
	if svc != "s3" || src != SourceSigV4 {
		t.Fatalf("svc=%q src=%v, want s3/SourceSigV4", svc, src)
	}
}

func TestDetectService_SigV4AuthHeader_UnknownService_FallsThrough(t *testing.T) {
	r := newRouterReq("http://localhost:4566/")
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/unknownsvc/aws4_request, SignedHeaders=host, Signature=abc")
	svc, src := DetectService(r, nil)
	if svc != "" || src != SourceUnknown {
		t.Fatalf("svc=%q src=%v, want empty/SourceUnknown", svc, src)
	}
}

// ─── Priority 2.5: SigV4 presigned URL (X-Amz-Credential query param) ───────

func TestDetectService_SigV4Presigned_KnownService(t *testing.T) {
	r := newRouterReq("http://localhost:4566/mybucket/key" +
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKID/20240101/us-east-1/s3/aws4_request" +
		"&X-Amz-Date=20240101T000000Z&X-Amz-Expires=3600&X-Amz-Signature=abc")
	svc, src := DetectService(r, nil)
	if svc != "s3" || src != SourceSigV4 {
		t.Fatalf("svc=%q src=%v, want s3/SourceSigV4", svc, src)
	}
}

func TestDetectService_SigV4Presigned_SQS(t *testing.T) {
	r := newRouterReq("http://localhost:4566/000000000000/my-queue" +
		"?X-Amz-Credential=AKID/20240101/us-east-1/sqs/aws4_request" +
		"&X-Amz-Signature=abc")
	svc, src := DetectService(r, nil)
	if svc != "sqs" || src != SourceSigV4 {
		t.Fatalf("svc=%q src=%v, want sqs/SourceSigV4", svc, src)
	}
}

func TestDetectService_SigV4Presigned_UnknownService_FallsThrough(t *testing.T) {
	r := newRouterReq("http://localhost:4566/?X-Amz-Credential=AKID/20240101/us-east-1/unknownsvc/aws4_request")
	svc, src := DetectService(r, nil)
	if svc != "" || src != SourceUnknown {
		t.Fatalf("svc=%q src=%v, want empty/SourceUnknown", svc, src)
	}
}

func TestDetectService_SigV4Presigned_MalformedCredential_TooFewParts(t *testing.T) {
	// Credential has only 3 slash-separated parts — no service field.
	r := newRouterReq("http://localhost:4566/?X-Amz-Credential=AKID/20240101/us-east-1")
	svc, src := DetectService(r, nil)
	if svc != "" || src != SourceUnknown {
		t.Fatalf("svc=%q src=%v, want empty/SourceUnknown for short credential", svc, src)
	}
}

func TestDetectService_SigV4PresignedBeatsAuthHeader(t *testing.T) {
	// Both Authorization header (SQS) and X-Amz-Credential (S3) present.
	// Authorization is Priority 2 and wins.
	r := newRouterReq("http://localhost:4566/?X-Amz-Credential=AKID/20240101/us-east-1/s3/aws4_request")
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=abc")
	svc, src := DetectService(r, nil)
	if svc != "sqs" || src != SourceSigV4 {
		t.Fatalf("svc=%q src=%v, want sqs/SourceSigV4 (auth header must win)", svc, src)
	}
}

// ─── Priority 5: SigV2 presigned URL (AWSAccessKeyId query param) ────────────

func TestDetectService_SigV2Presigned_ReturnsS3(t *testing.T) {
	r := newRouterReq("http://localhost:4566/mybucket/key" +
		"?AWSAccessKeyId=AKID&Signature=abc&Expires=9999999999")
	svc, src := DetectService(r, nil)
	if svc != "s3" || src != SourceSigV2 {
		t.Fatalf("svc=%q src=%v, want s3/SourceSigV2", svc, src)
	}
}

func TestDetectService_SigV4PresignedBeatsSigV2(t *testing.T) {
	// X-Amz-Credential (Priority 2.5) must win over AWSAccessKeyId (Priority 5).
	r := newRouterReq("http://localhost:4566/mybucket/key" +
		"?X-Amz-Credential=AKID/20240101/us-east-1/s3/aws4_request" +
		"&AWSAccessKeyId=AKID&Signature=abc&Expires=9999999999")
	svc, src := DetectService(r, nil)
	if svc != "s3" || src != SourceSigV4 {
		t.Fatalf("svc=%q src=%v, want s3/SourceSigV4 (presigned SigV4 must beat SigV2)", svc, src)
	}
}

func TestDetectService_SigV2Presigned_VirtualHostedPath(t *testing.T) {
	// SigV2 presigned URL issued for a virtual-hosted bucket URL.
	r := httptest.NewRequest(http.MethodGet,
		"http://mybucket.s3.amazonaws.com/key?AWSAccessKeyId=AKID&Signature=abc&Expires=9999999999", nil)
	r.Host = "mybucket.s3.amazonaws.com"
	svc, src := DetectService(r, nil)
	if svc != "s3" || src != SourceSigV2 {
		t.Fatalf("svc=%q src=%v, want s3/SourceSigV2 for virtual-hosted SigV2 URL", svc, src)
	}
}

// ─── extractSigV4Service ──────────────────────────────────────────────────────

func TestExtractSigV4Service_StandardHeader(t *testing.T) {
	auth := "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=abc"
	if got := extractSigV4Service(auth); got != "dynamodb" {
		t.Fatalf("got %q, want dynamodb", got)
	}
}

func TestExtractSigV4Service_NoCredentialField(t *testing.T) {
	if got := extractSigV4Service("Bearer sometoken"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestExtractSigV4Service_CredentialTooShort(t *testing.T) {
	auth := "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1, SignedHeaders=host, Signature=abc"
	if got := extractSigV4Service(auth); got != "" {
		t.Fatalf("got %q, want empty for short credential", got)
	}
}
