package ui

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
)

func buildMetaHandler(adminHandler *admin.Handler, cfg *config.Config, version string, cloud string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		meta := adminHandler.Meta()

		mode := "memory"
		if cfg.Ephemeral {
			mode = "ephemeral"
		} else if cfg.DSN != "" {
			mode = "postgres"
		}

		writeJSON(w, MetaResponse{
			Cloud:      cloud,
			Region:     cfg.Region,
			AccountId:  cfg.AccountID,
			Mode:       mode,
			Version:    version,
			UIVersion:  "dev",
			InstanceId: meta.InstanceID,
		})
	}
}

func buildAccountsHandler(cfg *config.Config) http.HandlerFunc {
	accounts := append([]string{cfg.AccountID}, cfg.ExtraAccounts...)
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, AccountsResponse{Accounts: accounts})
	}
}

// buildServicesHandler reports the services the active registrar supports.
func buildServicesHandler(reg Registrar) http.HandlerFunc {
	services := reg.Services()
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, ServicesResponse{Services: services})
	}
}

// writeJSON encodes v as JSON with Content-Type application/json.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
