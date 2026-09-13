//go:build gcp_conformance

package gcpconformance

import "strings"

// AllowRule suppresses a divergence that reflects documented, API-specific
// wire-shape variance rather than an emulator defect. A rule matches on the
// divergence Kind and a Path suffix (ValidateTranscripts prefixes Path with the
// entry context, so exact matching would miss), optionally narrowed to a set of
// services.
type AllowRule struct {
	Kind     string
	Path     string
	Services []string // empty means "any service"
	Reason   string
}

func (r AllowRule) matches(d Divergence) bool {
	if r.Kind != d.Kind || !strings.HasSuffix(d.Path, r.Path) {
		return false
	}
	if len(r.Services) == 0 {
		return true
	}
	for _, s := range r.Services {
		if s == d.Service {
			return true
		}
	}
	return false
}

// errorEnvelopeAllowlist records the real, API-specific variance in GCP's JSON
// error envelope. Our validator enforces one canonical shape
// ({code,message,status,errors[]}), but that shape is not universal:
//
//   - Cloud Storage (legacy JSON API) returns errors[] with
//     reason/domain/message but omits google.rpc "status".
//   - Modern APIs (Pub/Sub, Secret Manager, KMS, IAM, Cloud DNS, BigQuery)
//     return "status" (plus "details"/ErrorInfo) and omit the legacy errors[]
//     array entirely.
//
// Demanding both components therefore flags correct responses. Only these two
// "absent optional component" findings are allowed; anything else about the
// envelope (wrong code, missing message, non-object error, malformed errors[])
// is still reported and can gate CI.
var errorEnvelopeAllowlist = []AllowRule{
	{
		Kind:     kindBadErrorEnvelope,
		Path:     "error.status",
		Services: []string{"storage"},
		Reason:   "Cloud Storage JSON errors omit the google.rpc status field",
	},
	{
		Kind: kindBadErrorEnvelope,
		Path: "error.errors",
		Services: []string{
			"pubsub", "secretmanager", "kms", "iam", "clouddns", "bigquery",
		},
		Reason: "modern GCP APIs omit the legacy errors[] array (they carry status + details)",
	},
}

// allowRuleFor returns the first allow rule matching d, or nil.
func allowRuleFor(d Divergence) *AllowRule {
	for i := range errorEnvelopeAllowlist {
		if errorEnvelopeAllowlist[i].matches(d) {
			return &errorEnvelopeAllowlist[i]
		}
	}
	return nil
}

// ApplyAllowlist partitions divergences into those to keep and those suppressed
// by a documented allow rule.
func ApplyAllowlist(divs []Divergence) (kept, suppressed []Divergence) {
	for _, d := range divs {
		if allowRuleFor(d) != nil {
			suppressed = append(suppressed, d)
			continue
		}
		kept = append(kept, d)
	}
	return kept, suppressed
}
