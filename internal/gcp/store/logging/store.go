// Package logging provides the Cloud Logging (v2) store. Log entries live in a
// dedicated jc_log_entries table (mirroring the Secret Manager / Functions
// store conventions: project-scoped, clock.Now for absent timestamps, and a
// snapshot pair for the admin export/import surface).
package logging

import (
	"context"
	"time"
)

// LogEntry is a stored Cloud Logging entry. LogName is the full resource name
// ("projects/{p}/logs/{l}"); Severity is the numeric google.logging.type
// LogSeverity value; ID is a monotonic sequence used to break ties when
// entries share a timestamp.
type LogEntry struct {
	ID             int64             `json:"id"`
	LogName        string            `json:"logName"`
	ResourceType   string            `json:"resourceType"`
	ResourceLabels map[string]string `json:"resourceLabels,omitempty"`
	Severity       int               `json:"severity"`
	PayloadType    string            `json:"payloadType,omitempty"` // "" or "text" or "json"
	TextPayload    string            `json:"textPayload,omitempty"`
	JsonPayload    map[string]any    `json:"jsonPayload,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
	InsertID       string            `json:"insertId,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// Store is the Cloud Logging store. Entries are project-scoped; queries return
// entries ordered by (timestamp, id) ascending.
type Store interface {
	Write(ctx context.Context, projectID string, e LogEntry) error
	// List returns every entry in the project, ordered by (timestamp, id).
	List(ctx context.Context, projectID string) ([]LogEntry, error)
	// ListLogs returns the distinct full log names under the project, sorted.
	ListLogs(ctx context.Context, projectID string) ([]string, error)
	// DeleteLog deletes every entry whose log name equals logName.
	DeleteLog(ctx context.Context, projectID, logName string) error
	Reset(ctx context.Context)
}
