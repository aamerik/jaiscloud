// Etag preconditions shared by the Secret Manager REST provider and gRPC
// service.
//
// Real Secret Manager exposes an opaque `etag` on both a Secret and a
// SecretVersion, and accepts it as an optional precondition on `secrets.delete`
// and on the version `disable`/`enable`/`destroy` transitions (the request
// succeeds only if it matches the currently stored object's etag; AIP-154, "an
// etag mismatch fails with ABORTED"). The emulator previously rendered an etag
// but never read one back, so a caller holding a stale etag could still delete
// or state-change the resource.
//
// The etag value is a deterministic hash of the object's mutable content, which
// gives the same observable contract as GCP's opaque token: stable across reads
// of an unchanged object, and different after any mutation. Both transports use
// these helpers to render the etag and to compare a supplied one, so they can
// never disagree about what the current etag is.
package secretmanager

import (
	"encoding/json"

	"jaiscloud/internal/gcp/policy"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// secretEtagContent is the etag-bearing view of a Secret: every mutable
// metadata field, plus the identity (name + createTime). NextVer and the
// version list are deliberately excluded — GCP's Secret etag covers the Secret
// object, not its versions.
type secretEtagContent struct {
	Name           string                       `json:"name"`
	CreateTime     string                       `json:"createTime"`
	Labels         map[string]string            `json:"labels,omitempty"`
	Annotations    map[string]string            `json:"annotations,omitempty"`
	Rotation       *secretmanagerstore.Rotation `json:"rotation,omitempty"`
	VersionAliases map[string]int               `json:"versionAliases,omitempty"`
	KmsKeyName     string                       `json:"kmsKeyName,omitempty"`
}

// SecretEtag returns the current etag for a secret rendered under name.
func SecretEtag(name string, s secretmanagerstore.Secret) string {
	content := secretEtagContent{
		Name:           name,
		CreateTime:     timestamp.Canonical(s.CreateTime),
		Labels:         s.Labels,
		Annotations:    s.Annotations,
		Rotation:       s.Rotation,
		VersionAliases: s.VersionAliases,
		KmsKeyName:     s.KmsKeyName,
	}
	b, _ := json.Marshal(content)
	return policy.Etag(string(b))
}

// VersionEtag returns the current etag for a secret version rendered under
// name. A version's only mutable field is its lifecycle state, so the etag
// changes on every transition (and only then). createTime is the canonical
// rendered timestamp, so a caller holding only the transport's view hashes the
// same content the store-side check does.
func VersionEtag(name, createTime, state string) string {
	b, _ := json.Marshal(struct {
		Name       string `json:"name"`
		CreateTime string `json:"createTime"`
		State      string `json:"state"`
	}{
		Name:       name,
		CreateTime: createTime,
		State:      state,
	})
	return policy.Etag(string(b))
}

// CheckSecretEtag enforces a request-supplied etag against the stored secret.
// An empty supplied etag is a no-op: the caller opted out of the precondition.
func CheckSecretEtag(name string, s secretmanagerstore.Secret, supplied string) error {
	if supplied == "" || supplied == SecretEtag(name, s) {
		return nil
	}
	return etagMismatch(name)
}

// CheckVersionEtag enforces a request-supplied etag against the stored version.
// An empty supplied etag is a no-op.
func CheckVersionEtag(name string, v secretmanagerstore.Version, supplied string) error {
	if supplied == "" || supplied == VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State) {
		return nil
	}
	return etagMismatch(name)
}

// etagMismatch is the failed etag precondition: HTTP 409 with the ABORTED
// google.rpc status (AIP-154, matching the emulator's other OCC surfaces).
func etagMismatch(name string) error {
	return &model.ProviderError{
		Code:       "Aborted",
		Message:    "etag mismatch for " + name + ": the resource changed since the etag was read",
		HTTPStatus: 409,
		Status:     "ABORTED",
	}
}
