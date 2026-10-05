// Package ui is the cloud-neutral JaisCloud UI core: it serves the embedded
// React portal, the meta/services/accounts endpoints, the SSE event stream and
// the admin/scene static assets. Cloud-specific service APIs are contributed
// by a Registrar implementation (see internal/aws/ui for the AWS registrar).
package ui

// Tiers describe how faithfully a service is emulated.
const (
	// TierFull is real business logic (wire protocol + behavior).
	TierFull = "full"
	// TierMetadata is wire protocol + resource CRUD, but nothing executes.
	TierMetadata = "metadata"
	// TierStub returns plausible responses with limited operation coverage.
	TierStub = "stub"
)

// ServiceChild is a sub-page of a service (e.g. S3 → Buckets).
type ServiceChild struct {
	Label string `json:"label"`
	Path  string `json:"path"`
}

// ServiceDescriptor describes one service the UI can render. The navigation
// menu is built from these; a service appears only when its provider is wired.
type ServiceDescriptor struct {
	ID       string         `json:"id"`
	Label    string         `json:"label"`
	Category string         `json:"category"`
	RootPath string         `json:"rootPath"`
	Children []ServiceChild `json:"children"`
	Tier     string         `json:"tier"`
	Note     string         `json:"note,omitempty"`
}

// ServicesResponse is the payload for GET /api/ui/v1/services.
type ServicesResponse struct {
	Services []ServiceDescriptor `json:"services"`
}

// MetaResponse is the payload for GET /api/ui/v1/meta.
type MetaResponse struct {
	Cloud      string `json:"cloud"`
	Region     string `json:"region"`
	AccountId  string `json:"accountId"`
	Mode       string `json:"mode"` // "memory" | "postgres" | "ephemeral"
	Version    string `json:"version"`
	UIVersion  string `json:"uiVersion"`
	InstanceId string `json:"instanceId"`
	// BootID identifies this process start; it changes on every restart (unlike
	// the persisted InstanceId) so the browser can invalidate cached data.
	BootID string `json:"bootId"`
}

// AccountsResponse is the payload for GET /api/ui/v1/meta/accounts.
type AccountsResponse struct {
	Accounts []string `json:"accounts"`
}
