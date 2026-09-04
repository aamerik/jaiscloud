// Package policy provides shared GCP IAM policy helpers used by the Pub/Sub,
// Secret Manager, and IAM providers: getIamPolicy / setIamPolicy (with etag
// optimistic concurrency control) and testIamPermissions.
package policy

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Policy is a GCP IAM policy (version + etag + bindings).
type Policy struct {
	Version  int    `json:"version"`
	Etag     string `json:"etag"`
	Bindings []any  `json:"bindings"`
}

// DefaultEtag is the etag assigned to an empty policy.
const DefaultEtag = "ACAB"

// Etag derives a SHA-1 etag from an arbitrary string.
func Etag(s string) string {
	h := sha1.Sum([]byte(s))
	return base64.StdEncoding.EncodeToString(h[:])
}

// EtagFor derives a fresh etag from a policy's bindings.
func EtagFor(bindings []any) string {
	b, _ := json.Marshal(bindings)
	return Etag(string(b))
}

// Load returns the stored policy for a resource, or an empty default policy.
func Load(ctx context.Context, s store.ResourceStore, account, resourceType, id string) Policy {
	p := Policy{Version: 1, Etag: DefaultEtag, Bindings: []any{}}
	if e, err := s.Get(ctx, account, store.GlobalRegion, resourceType, id); err == nil {
		json.Unmarshal(e.Data, &p)
	}
	return p
}

// Set stores a policy for a resource, enforcing etag OCC. body is the parsed
// request JSON — either the Policy fields directly, or wrapped in a "policy"
// field per SetIamPolicyRequest. Returns the stored policy.
func Set(ctx context.Context, s store.ResourceStore, account, resourceType, id string, body map[string]any) (Policy, error) {
	policyBody := body
	if p, ok := body["policy"].(map[string]any); ok {
		policyBody = p
	}
	bindings := []any{}
	if bs, ok := policyBody["bindings"].([]any); ok {
		bindings = bs
	}
	existing := Load(ctx, s, account, resourceType, id)
	if reqEtag, _ := policyBody["etag"].(string); reqEtag != "" && reqEtag != existing.Etag {
		return Policy{}, model.NewProviderError("Conflict", "etag mismatch: optimistic concurrency control failed", 409)
	}
	pol := Policy{Version: 1, Etag: EtagFor(bindings), Bindings: bindings}
	if v, ok := policyBody["version"].(float64); ok {
		pol.Version = int(v)
	}
	data, _ := json.Marshal(pol)
	_ = s.Upsert(ctx, account, store.GlobalRegion, store.ResourceEntry{Type: resourceType, ID: id, Data: data})
	return pol, nil
}

// ToMap renders a Policy as a response map.
func ToMap(p Policy) map[string]any {
	return map[string]any{"version": p.Version, "etag": p.Etag, "bindings": p.Bindings}
}

// TestPermissions returns the permissions the caller is allowed. JaisCloud does
// not enforce IAM (every request to every project succeeds regardless of
// policy), so the emulator treats the caller as project owner and grants all
// requested permissions — consistent with the no-authz-enforcement posture.
func TestPermissions(permissions []string) []string {
	return permissions
}

// Permissions extracts the "permissions" string array from a
// testIamPermissions request body.
func Permissions(body map[string]any) []string {
	items, _ := body["permissions"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
