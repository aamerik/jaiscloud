package aws

import (
	"net/http"
	"net/url"
	"strings"
)

// DetectionSource indicates how the service was identified.
type DetectionSource int

const (
	SourceUnknown    DetectionSource = iota
	SourceXAmzTarget                 // X-Amz-Target: <Prefix>.<Action>
	SourceSigV4                      // Authorization: Credential=.../<service>/aws4_request
	SourceAction                     // Action=<value> query/form param
)

// DetectService identifies the AWS service from the HTTP request and body.
// All service metadata is driven by awsServices in services.go — no hardcoded
// prefixes, allow-lists, or action lists here.
func DetectService(r *http.Request, body []byte) (service string, source DetectionSource) {
	// Priority 1: X-Amz-Target header (JSON/Target protocol — DynamoDB, SQS, Glue, ECS, EMR, EventBridge…)
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if svc := detectServiceFromTarget(target); svc != "" {
			return svc, SourceXAmzTarget
		}
	}

	// Priority 2: SigV4 Authorization scope — covers all signed services.
	if auth := r.Header.Get("Authorization"); auth != "" {
		if svc := extractSigV4Service(auth); svc != "" && knownSigV4Services[svc] {
			return svc, SourceSigV4
		}
	}

	// Priority 3: Action in URL query string or POST form body (Query protocol).
	action := r.URL.Query().Get("Action")
	if action == "" && len(body) > 0 {
		if form, err := url.ParseQuery(string(body)); err == nil {
			action = form.Get("Action")
		}
	}
	if action != "" {
		if svc := actionToService[action]; svc != "" {
			return svc, SourceAction
		}
	}

	return "", SourceUnknown
}

// extractSigV4Service parses the service name from an AWS SigV4 Authorization header.
// Format: AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/sqs/aws4_request, ...
func extractSigV4Service(auth string) string {
	idx := strings.Index(auth, "Credential=")
	if idx < 0 {
		return ""
	}
	cred := auth[idx+len("Credential="):]
	if end := strings.IndexByte(cred, ','); end >= 0 {
		cred = cred[:end]
	}
	cred = strings.TrimSpace(cred)
	// Format: AccessKeyId/YYYYMMDD/region/service/aws4_request
	parts := strings.Split(cred, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}
