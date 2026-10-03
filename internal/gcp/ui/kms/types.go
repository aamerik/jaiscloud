// Package kmsui serves the Cloud KMS UI API. Handlers call the KMS provider
// directly (in-process) and reshape its Discovery-shaped response maps for the
// console. Scope is key rings, crypto keys and crypto-key versions plus IAM
// policy; encrypt/decrypt, asymmetric signing and getPublicKey are deferred (see
// the console-UI wave plan).
package kmsui

import "jaiscloud/internal/gcp/ui/uihelper"

// KeyRing is the UI summary of a KMS key ring.
type KeyRing struct {
	Name       string `json:"name"`
	KeyRingID  string `json:"keyRingId,omitempty"`
	Location   string `json:"location,omitempty"`
	CreateTime string `json:"createTime,omitempty"`
}

// ListKeyRingsResponse is the response for GET /locations/{location}/keyRings.
type ListKeyRingsResponse struct {
	KeyRings      []KeyRing `json:"keyRings"`
	Total         int       `json:"total"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

// CreateKeyRingRequest is the body for POST /locations/{location}/keyRings.
type CreateKeyRingRequest struct {
	KeyRingID string `json:"keyRingId"`
}

// CryptoKey is the UI summary of a KMS crypto key.
type CryptoKey struct {
	Name             string            `json:"name"`
	CryptoKeyID      string            `json:"cryptoKeyId,omitempty"`
	Purpose          string            `json:"purpose,omitempty"`
	Algorithm        string            `json:"algorithm,omitempty"`
	PrimaryState     string            `json:"primaryState,omitempty"`
	PrimaryVersion   string            `json:"primaryVersion,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	RotationPeriod   string            `json:"rotationPeriod,omitempty"`
	NextRotationTime string            `json:"nextRotationTime,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
}

// ListCryptoKeysResponse is the response for the cryptoKeys collection.
type ListCryptoKeysResponse struct {
	CryptoKeys    []CryptoKey `json:"cryptoKeys"`
	Total         int         `json:"total"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// CreateCryptoKeyRequest is the body for POST .../cryptoKeys. Only cryptoKeyId
// is required.
type CreateCryptoKeyRequest struct {
	CryptoKeyID    string            `json:"cryptoKeyId"`
	Purpose        string            `json:"purpose,omitempty"`
	Algorithm      string            `json:"algorithm,omitempty"`
	RotationPeriod string            `json:"rotationPeriod,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// CryptoKeyVersion is the UI summary of a crypto-key version.
type CryptoKeyVersion struct {
	Name             string `json:"name"`
	VersionID        string `json:"versionId,omitempty"`
	State            string `json:"state,omitempty"`
	Algorithm        string `json:"algorithm,omitempty"`
	CreateTime       string `json:"createTime,omitempty"`
	DestroyTime      string `json:"destroyTime,omitempty"`
	DestroyEventTime string `json:"destroyEventTime,omitempty"`
}

// ListCryptoKeyVersionsResponse is the response for the versions collection.
type ListCryptoKeyVersionsResponse struct {
	CryptoKeyVersions []CryptoKeyVersion `json:"cryptoKeyVersions"`
	Total             int                `json:"total"`
	NextPageToken     string             `json:"nextPageToken,omitempty"`
}

// SetVersionStateRequest is the body for POST .../versions/{version}/disable
// and /enable. KMS only allows ENABLED or DISABLED.
type SetVersionStateRequest struct {
	State string `json:"state"`
}

// SetPrimaryVersionRequest is the body for POST .../cryptoKeys/{key}/setPrimary.
type SetPrimaryVersionRequest struct {
	CryptoKeyVersionID string `json:"cryptoKeyVersionId"`
}

// IamBinding is one role-to-members binding in an IAM policy.
type IamBinding = uihelper.IamBinding

// IamPolicy mirrors google.iam.v1.Policy.
type IamPolicy = uihelper.IamPolicy

// SetIamPolicyRequest is the body for PUT .../iam.
type SetIamPolicyRequest struct {
	Policy IamPolicy `json:"policy"`
}

// TestIamPermissionsRequest is the body for .../iam/testIamPermissions.
type TestIamPermissionsRequest struct {
	Permissions []string `json:"permissions"`
}

// TestIamPermissionsResponse is the response for testIamPermissions.
type TestIamPermissionsResponse struct {
	Permissions []string `json:"permissions"`
}
