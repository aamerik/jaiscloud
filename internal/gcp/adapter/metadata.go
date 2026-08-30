package gcp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/clock"

	"github.com/go-chi/chi/v5"
)

// MetadataConfig configures the GCP metadata-server emulator.
type MetadataConfig struct {
	ProjectID      string
	ServiceAccount string
}

// RegisterMetadataRoutes mounts the GCP metadata-server emulator endpoints.
// ADC-based clients set GCE_METADATA_HOST to point here and receive a mocked
// project ID, service-account email, and access token without touching Google.
func RegisterMetadataRoutes(r chi.Router, cfg MetadataConfig) {
	r.Route("/computeMetadata/v1", func(rt chi.Router) {
		rt.Get("/project/project-id", func(w http.ResponseWriter, req *http.Request) {
			writeMetadataText(w, cfg.ProjectID)
		})
		rt.Get("/project/numeric-project-id", func(w http.ResponseWriter, req *http.Request) {
			writeMetadataText(w, "0")
		})
		rt.Get("/instance/service-accounts/default/email", func(w http.ResponseWriter, req *http.Request) {
			writeMetadataText(w, cfg.ServiceAccount)
		})
		rt.Get("/instance/service-accounts/default/token", func(w http.ResponseWriter, req *http.Request) {
			token := mockAccessToken(cfg.ServiceAccount, cfg.ProjectID)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": token,
				"expires_in":   3600,
				"token_type":   "Bearer",
			})
		})
		rt.Get("/instance/service-accounts/default/scopes", func(w http.ResponseWriter, req *http.Request) {
			writeMetadataText(w, "https://www.googleapis.com/auth/cloud-platform")
		})
	})
}

func writeMetadataText(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, s)
}

// mockAccessToken builds an unverified HS256 JWT carrying the service-account
// email and project, signed with a fixed dev secret. JaisCloud never verifies
// the signature — it only decodes the payload (internal/gcp/identity).
func mockAccessToken(sa, project string) string {
	const secret = "jaiscloud-dev"
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(
		`{"email":%q,"sub":%q,"project_id":%q,"exp":%d}`,
		sa, sa, project, clock.RealNow().Add(time.Hour).Unix(),
	)))
	signingInput := hdr + "." + payload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}
