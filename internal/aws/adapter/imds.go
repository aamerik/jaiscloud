package aws

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/clock"
)

const imdsStaticToken = "jaiscloud-imds-token"

// IMDSConfig carries the values served by the IMDS emulator.
type IMDSConfig struct {
	Region          string
	AccountID       string
	RoleName        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// RegisterIMDSRoutes mounts the AWS Instance Metadata Service emulator routes
// onto r. Call this from the gateway WithExtraRoutes option when IMDSEnabled.
func RegisterIMDSRoutes(r chi.Router, cfg IMDSConfig) {
	roleName := cfg.RoleName
	if roleName == "" {
		roleName = "jaiscloud-emulator-role"
	}

	// IMDSv2 token
	r.Put("/latest/api/token", func(w http.ResponseWriter, r *http.Request) {
		ttl := r.Header.Get("X-Aws-Ec2-Metadata-Token-Ttl-Seconds")
		if ttl == "" {
			ttl = "21600"
		}
		w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", ttl)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, imdsStaticToken)
	})

	// Placement
	r.Get("/latest/meta-data/placement/region", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, cfg.Region)
	})
	r.Get("/latest/meta-data/placement/availability-zone", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, cfg.Region+"a")
	})

	// Instance identity
	r.Get("/latest/dynamic/instance-identity/document", func(w http.ResponseWriter, _ *http.Request) {
		writeIMDSJSON(w, map[string]any{
			"region":                  cfg.Region,
			"accountId":               cfg.AccountID,
			"availabilityZone":        cfg.Region + "a",
			"instanceId":              "i-jaiscloud000000001",
			"instanceType":            "m5.xlarge",
			"privateIp":               "10.0.0.1",
			"architecture":            "x86_64",
			"imageId":                 "ami-00000000000000001",
			"pendingTime":             clock.Now().Format(time.RFC3339),
			"devpayProductCodes":      nil,
			"billingProducts":         nil,
			"marketplaceProductCodes": nil,
		})
	})

	// IAM credentials listing
	r.Get("/latest/meta-data/iam/security-credentials/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, roleName)
	})
	r.Get("/latest/meta-data/iam/security-credentials/"+roleName, func(w http.ResponseWriter, _ *http.Request) {
		akid := cfg.AccessKeyID
		if akid == "" {
			akid = "test"
		}
		sak := cfg.SecretAccessKey
		if sak == "" {
			sak = "test"
		}
		writeIMDSJSON(w, map[string]any{
			"Code":            "Success",
			"LastUpdated":     clock.Now().Format(time.RFC3339),
			"Type":            "AWS-HMAC",
			"AccessKeyId":     akid,
			"SecretAccessKey": sak,
			"Token":           cfg.SessionToken,
			"Expiration":      clock.Now().Add(6 * time.Hour).UTC().Format(time.RFC3339),
		})
	})

	// Instance id + region convenience shortcuts
	r.Get("/latest/meta-data/instance-id", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, "i-jaiscloud000000001")
	})
	r.Get("/latest/meta-data/region", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, cfg.Region)
	})

	// Root listing
	r.Get("/latest/meta-data/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, strings.Join([]string{
			"iam/",
			"instance-id",
			"placement/",
			"region",
		}, "\n"))
	})
}

// writeIMDSJSON encodes v as JSON. Uses json.Encoder so arbitrary string values
// (including those containing quotes) are safely encoded.
func writeIMDSJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("imds: json encode failed", "err", err)
	}
}
