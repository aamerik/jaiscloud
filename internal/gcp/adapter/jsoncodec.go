package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// JSONCodec is the generic codec for GCP REST/JSON services routed under
// /v1/projects/{project}/... (Pub/Sub, Secret Manager, KMS, IAM). Detection
// and action derivation are data-driven; per-service providers register the
// resulting "Prefix.Action" keys.
type JSONCodec struct {
	Service string
}

func (c *JSONCodec) ServiceName() string { return c.Service }

// Decode parses a /v1/projects/{project}/... path into a NormalizedRequest.
// Params carry: project, name (full relative resource name), resourceType,
// location (KMS), body, and query parameters. The custom method (":publish",
// ":access", ...) is stripped from the last path segment and folded into the
// action.
func (c *JSONCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+1 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}
	rest := seg[pi+2:]

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	custom := ""
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			custom = last[i+1:]
			rest[len(rest)-1] = last[:i]
		}
	}

	resourceType := detectResourceType(rest)
	name := strings.Join(rest, "/") // full relative resource name

	nr.Params["resourceType"] = resourceType
	nr.Params["name"] = name

	// KMS: surface the location segment for resource-name reconstruction.
	if resourceType == "keyRings" || resourceType == "cryptoKeys" {
		for i, s := range rest {
			if s == "locations" && i+1 < len(rest) {
				nr.Params["location"] = rest[i+1]
				break
			}
		}
	}

	isCollection := len(rest) > 0 && rest[len(rest)-1] == resourceType

	nr.Action = deriveAction(resourceType, isCollection, name, r.Method, custom)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// Encode serialises a provider response as JSON.
func (c *JSONCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCP error envelope.
func (c *JSONCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	env := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  gcpStatusString(status),
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}

// detectResourceType returns the GCP resource-type marker from the path
// segments (after projects/{project}). cryptoKeys wins over keyRings since a
// cryptoKey path also contains "keyRings"; cryptoKeyVersions wins over
// cryptoKeys since a version path also contains "cryptoKeys".
func detectResourceType(segs []string) string {
	var hasKeyRings, hasCryptoKeys, hasVersions, hasServiceAccounts bool
	for _, s := range segs {
		switch s {
		case "topics":
			return "topics"
		case "subscriptions":
			return "subscriptions"
		case "secrets":
			return "secrets"
		case "keys":
			return "keys" // service account keys
		case "cryptoKeyVersions":
			hasVersions = true
		case "cryptoKeys":
			hasCryptoKeys = true
		case "keyRings":
			hasKeyRings = true
		case "serviceAccounts":
			hasServiceAccounts = true
		}
	}
	if hasVersions {
		return "cryptoKeyVersions"
	}
	if hasCryptoKeys {
		return "cryptoKeys"
	}
	if hasKeyRings {
		return "keyRings"
	}
	if hasServiceAccounts {
		return "serviceAccounts"
	}
	return ""
}

// deriveAction maps (resourceType, isCollection, name, method, custom method)
// to an action name.
func deriveAction(resourceType string, isCollection bool, name, method, custom string) string {
	if custom != "" {
		switch resourceType {
		case "topics":
			switch custom {
			case "publish":
				return "TopicPublish"
			case "getIamPolicy":
				return "TopicGetIamPolicy"
			case "setIamPolicy":
				return "TopicSetIamPolicy"
			case "testIamPermissions":
				return "TopicTestIamPermissions"
			}
		case "subscriptions":
			switch custom {
			case "pull":
				return "SubscriptionPull"
			case "acknowledge":
				return "SubscriptionAcknowledge"
			case "modifyAckDeadline":
				return "SubscriptionModifyAckDeadline"
			case "getIamPolicy":
				return "SubscriptionGetIamPolicy"
			case "setIamPolicy":
				return "SubscriptionSetIamPolicy"
			case "testIamPermissions":
				return "SubscriptionTestIamPermissions"
			}
		case "secrets":
			switch custom {
			case "addVersion":
				return "AddVersion"
			case "access":
				return "Access"
			case "destroy":
				return "DestroyVersion"
			case "disable":
				return "DisableVersion"
			case "enable":
				return "EnableVersion"
			case "getIamPolicy":
				return "SecretGetIamPolicy"
			case "setIamPolicy":
				return "SecretSetIamPolicy"
			case "testIamPermissions":
				return "SecretTestIamPermissions"
			}
		case "cryptoKeys":
			switch custom {
			case "encrypt":
				return "CryptoKeyEncrypt"
			case "decrypt":
				return "CryptoKeyDecrypt"
			case "updatePrimaryVersion":
				return "CryptoKeyUpdatePrimaryVersion"
			}
		case "cryptoKeyVersions":
			switch custom {
			case "destroy":
				return "CryptoKeyVersionDestroy"
			case "disable":
				return "CryptoKeyVersionDisable"
			case "enable":
				return "CryptoKeyVersionEnable"
			case "asymmetricSign":
				return "CryptoKeyVersionAsymmetricSign"
			case "asymmetricDecrypt":
				return "CryptoKeyVersionAsymmetricDecrypt"
			case "macSign":
				return "CryptoKeyVersionMacSign"
			case "macVerify":
				return "CryptoKeyVersionMacVerify"
			}
		case "serviceAccounts":
			switch custom {
			case "getIamPolicy":
				return "ServiceAccountGetIamPolicy"
			case "setIamPolicy":
				return "ServiceAccountSetIamPolicy"
			case "testIamPermissions":
				return "ServiceAccountTestIamPermissions"
			case "signBlob":
				return "ServiceAccountSignBlob"
			case "signJwt":
				return "ServiceAccountSignJwt"
			}
		}
	}

	switch resourceType {
	case "topics":
		switch {
		case method == http.MethodPut && !isCollection:
			return "TopicCreate"
		case isCollection && method == http.MethodGet:
			return "TopicList"
		case method == http.MethodGet:
			return "TopicGet"
		case method == http.MethodDelete:
			return "TopicDelete"
		}
	case "subscriptions":
		switch {
		case method == http.MethodPut && !isCollection:
			return "SubscriptionCreate"
		case isCollection && method == http.MethodGet:
			return "SubscriptionList"
		case method == http.MethodGet:
			return "SubscriptionGet"
		case method == http.MethodDelete:
			return "SubscriptionDelete"
		}
	case "secrets":
		switch {
		case isCollection && method == http.MethodPost:
			return "Create"
		case isCollection && method == http.MethodGet:
			return "List"
		case method == http.MethodPatch:
			return "Update"
		case method == http.MethodDelete:
			return "Delete"
		case method == http.MethodGet && strings.Contains(name, "/versions/"):
			return "GetVersion"
		case method == http.MethodGet:
			return "Get"
		}
	case "keyRings":
		switch {
		case isCollection && method == http.MethodPost:
			return "KeyRingCreate"
		case isCollection && method == http.MethodGet:
			return "KeyRingList"
		case method == http.MethodGet:
			return "KeyRingGet"
		}
	case "cryptoKeys":
		switch {
		case isCollection && method == http.MethodPost:
			return "CryptoKeyCreate"
		case isCollection && method == http.MethodGet:
			return "CryptoKeyList"
		case method == http.MethodGet:
			return "CryptoKeyGet"
		}
	case "cryptoKeyVersions":
		switch {
		case method == http.MethodGet && strings.HasSuffix(name, "/publicKey"):
			return "CryptoKeyVersionGetPublicKey"
		case isCollection && method == http.MethodPost:
			return "CryptoKeyVersionCreate"
		case isCollection && method == http.MethodGet:
			return "CryptoKeyVersionList"
		case method == http.MethodGet:
			return "CryptoKeyVersionGet"
		}
	case "serviceAccounts":
		switch {
		case isCollection && method == http.MethodPost:
			return "ServiceAccountCreate"
		case isCollection && method == http.MethodGet:
			return "ServiceAccountList"
		case method == http.MethodGet:
			return "ServiceAccountGet"
		case method == http.MethodDelete:
			return "ServiceAccountDelete"
		}
	case "keys":
		switch {
		case isCollection && method == http.MethodPost:
			return "ServiceAccountKeyCreate"
		case isCollection && method == http.MethodGet:
			return "ServiceAccountKeyList"
		case method == http.MethodGet:
			return "ServiceAccountKeyGet"
		case method == http.MethodDelete:
			return "ServiceAccountKeyDelete"
		}
	}
	return ""
}
