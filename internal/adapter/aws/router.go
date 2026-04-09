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
	SourceXAmzTarget                 // X-Amz-Target: AmazonSQS.<Action>
	SourceSigV4                      // Authorization: Credential=.../sqs/aws4_request
	SourceAction                     // Action=<SQSAction> query/form param
)

// DetectService identifies the AWS service from the HTTP request and body.
// Phase 0: SQS only. Returns ("sqs", source) or ("", SourceUnknown).
func DetectService(r *http.Request, body []byte) (service string, source DetectionSource) {
	// Priority 1: X-Amz-Target header (JSON protocol)
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if strings.HasPrefix(target, "AmazonSQS.") {
			return "sqs", SourceXAmzTarget
		}
	}

	// Priority 2: SigV4 Authorization scope
	if auth := r.Header.Get("Authorization"); auth != "" {
		if svc := extractSigV4Service(auth); svc == "sqs" {
			return "sqs", SourceSigV4
		}
	}

	// Priority 3: Action in URL query string or POST form body
	action := r.URL.Query().Get("Action")
	if action == "" && len(body) > 0 {
		if form, err := url.ParseQuery(string(body)); err == nil {
			action = form.Get("Action")
		}
	}
	if action != "" && isKnownSQSAction(action) {
		return "sqs", SourceAction
	}

	return "", SourceUnknown
}

// extractSigV4Service parses the service name from an AWS SigV4 Authorization header.
// Format: AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/sqs/aws4_request, ...
func extractSigV4Service(auth string) string {
	// Find "Credential=" part
	idx := strings.Index(auth, "Credential=")
	if idx < 0 {
		return ""
	}
	cred := auth[idx+len("Credential="):]
	// Take up to the first comma or end
	if end := strings.IndexByte(cred, ','); end >= 0 {
		cred = cred[:end]
	}
	cred = strings.TrimSpace(cred)
	// Format: AccessKeyId/YYYYMMDD/region/service/aws4_request
	parts := strings.Split(cred, "/")
	if len(parts) >= 4 {
		return parts[3] // service name
	}
	return ""
}

func isKnownSQSAction(action string) bool {
	switch action {
	case "CreateQueue", "DeleteQueue", "ListQueues", "GetQueueUrl",
		"GetQueueAttributes", "SetQueueAttributes",
		"SendMessage", "ReceiveMessage", "DeleteMessage",
		"ChangeMessageVisibility", "PurgeQueue",
		"SendMessageBatch", "DeleteMessageBatch", "ChangeMessageVisibilityBatch",
		"TagQueue", "UntagQueue", "ListQueueTags":
		return true
	}
	return false
}
