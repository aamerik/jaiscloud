//go:build gcp_differential

package gcpdifferential

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Target describes where a scenario run is sent. Record mode targets real GCP
// with an ADC bearer token; replay targets the local emulator with no
// credentials and no external network.
type Target struct {
	Name          string
	Project       string
	ProjectNumber string
	Suffix        string
	Names         ResourceNames
	Token         string
	HTTP          *http.Client
	URLFor        func(service, path string) string
}

// Run executes every scenario against the target in order, capturing a
// normalized Exchange for each. Saved variables feed later scenarios.
func (t *Target) Run(scenarios []Scenario) ([]Exchange, error) {
	if t.HTTP == nil {
		t.HTTP = &http.Client{Timeout: 60 * time.Second}
	}
	norm := NewNormalizer(t.Project, t.ProjectNumber, t.Suffix, t.Names)

	vars := map[string]string{}
	exs := make([]Exchange, 0, len(scenarios))
	for i, sc := range scenarios {
		path := expandVars(sc.Path, vars)
		reqBody := expandVars(sc.Body, vars)
		url := t.URLFor(sc.Service, path)

		var body io.Reader
		if reqBody != "" {
			body = bytes.NewReader([]byte(reqBody))
		}
		req, err := http.NewRequest(sc.Method, url, body)
		if err != nil {
			return exs, fmt.Errorf("%s %s: %w", sc.Method, path, err)
		}
		if reqBody != "" {
			ct := sc.ContentType
			if ct == "" {
				ct = "application/json"
			}
			req.Header.Set("Content-Type", ct)
		}
		if t.Token != "" {
			req.Header.Set("Authorization", "Bearer "+t.Token)
		}
		req.Header.Set("User-Agent", "jaiscloud-gcp-differential/1")

		resp, err := t.HTTP.Do(req)
		if err != nil {
			return exs, fmt.Errorf("%s %s: %w", sc.Method, path, err)
		}
		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return exs, fmt.Errorf("%s %s: read body: %w", sc.Method, path, readErr)
		}

		ex := Exchange{
			Index:   i,
			Service: sc.Service,
			Op:      sc.Op,
			Method:  sc.Method,
			Path:    norm.substitute(path),
			Status:  resp.StatusCode,
		}
		if len(bytes.TrimSpace(respBody)) > 0 {
			ex.Response = norm.Bytes(respBody)
		}
		if reqBody != "" {
			ex.Request = norm.Bytes([]byte(reqBody))
		}
		exs = append(exs, ex)

		if len(sc.Save) > 0 {
			var decoded any
			if err := json.Unmarshal(respBody, &decoded); err == nil {
				for name, jsonPath := range sc.Save {
					if val, ok := captureVar(decoded, jsonPath); ok {
						vars[name] = val
					}
				}
			}
		}
	}
	return exs, nil
}

// RealTarget builds a record target for real GCP. token must be a fresh ADC
// access token; it is held in memory only and never logged.
func RealTarget(project, projectNumber, suffix string, names ResourceNames, token string) *Target {
	return &Target{
		Name:          "real-gcp",
		Project:       project,
		ProjectNumber: projectNumber,
		Suffix:        suffix,
		Names:         names,
		Token:         token,
		HTTP:          &http.Client{Timeout: 60 * time.Second},
		URLFor: func(service, path string) string {
			base, ok := serviceBaseURL[service]
			if !ok {
				// Fall back to storage-style origin; every included service must
				// have an entry, so this is a programming error surfaced loudly.
				base = "https://" + service + ".googleapis.com"
			}
			return base + path
		},
	}
}

// EmulatorTarget builds a replay target for the local emulator. All services are
// served from one origin, so the per-scenario path (which already carries the
// service prefix) is appended unchanged.
func EmulatorTarget(endpoint, project, suffix string, names ResourceNames) *Target {
	return &Target{
		Name:          "emulator",
		Project:       project,
		ProjectNumber: "",
		Suffix:        suffix,
		Names:         names,
		HTTP:          &http.Client{Timeout: 60 * time.Second},
		URLFor: func(_, path string) string {
			return endpoint + path
		},
	}
}
