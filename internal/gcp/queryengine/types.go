// Package queryengine is the transport-neutral SQL scratch engine that backs
// bigquery jobs.query/getQueryResults. It translates a bounded BigQuery
// Standard SQL subset to SQLite (pure-Go modernc.org/sqlite), hydrates the
// referenced tables into a per-query in-memory SQLite scratch database, runs
// the query and returns the ordered result set. SQLite is disposable scratch:
// it is never persisted and the caller's catalog remains the source of truth.
//
// The engine deliberately implements only the frozen v1 subset (see
// docs/gcp-bigquery-sql-engine.md). Any construct it cannot translate fails
// loud (UnsupportedError) rather than silently returning wrong rows.
package queryengine

import (
	"context"
	"errors"
)

// Field is one BigQuery table field. Type uses the BigQuery type names
// (STRING, INT64, FLOAT64, BOOL, BYTES, DATE, ...); Mode is NULLABLE,
// REQUIRED or REPEATED (empty is treated as NULLABLE).
type Field struct {
	Name string
	Type string
	Mode string
}

// Table is a source table resolved through a Catalog: its flat schema and all
// rows as JSON-decoded objects keyed by field name. Nested and REPEATED columns
// are not supported by v1.
type Table struct {
	Project string
	Dataset string
	Table   string
	Fields  []Field
	Rows    []map[string]any
}

// Catalog resolves a fully-qualified table to its schema and rows. The engine
// decorates a not-found table with the reference it came from; a Catalog should
// return ErrTableNotFound (wrapped is fine) for a missing table.
type Catalog interface {
	Table(ctx context.Context, project, dataset, table string) (Table, error)
}

// Request is one jobs.query/getQueryResults execution.
type Request struct {
	// Project is the project the query job runs in.
	Project string
	// Query is the BigQuery Standard SQL text.
	Query string
	// DefaultProject/DefaultDataset resolve unqualified and dataset-qualified
	// table references (the jobs.query defaultDataset). DefaultProject falls
	// back to Project when empty.
	DefaultProject string
	DefaultDataset string
}

// Result is the executed result set in output-column order.
type Result struct {
	Fields []Field
	// Rows holds one slice per output column, aligned with Fields. Scalar
	// values are normalized to their BigQuery representation (BOOL is a Go
	// bool, INT64 an int64, FLOAT64 a float64, BYTES a []byte, everything else
	// a string) so the transport can render the wire TableCell shape.
	Rows [][]any
}

// ErrUnsupported marks a construct the v1 subset does not implement (or a
// runtime execution failure); the caller must map it to an InvalidQuery error,
// never to a successful response.
var ErrUnsupported = errors.New("unsupported query")

// ErrTableNotFound marks a referenced table that does not exist in the catalog.
var ErrTableNotFound = errors.New("table not found")

// UnsupportedError is returned for a BigQuery construct the v1 subset does not
// translate, or for an execution failure. It is errors.Is(ErrUnsupported).
type UnsupportedError struct {
	Reason string
}

func (e *UnsupportedError) Error() string { return ErrUnsupported.Error() + ": " + e.Reason }

// Is lets errors.Is(err, ErrUnsupported) match.
func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

// TableNotFoundError is returned for a referenced table missing from the
// catalog. It is errors.Is(ErrTableNotFound).
type TableNotFoundError struct {
	Project string
	Dataset string
	Table   string
}

func (e *TableNotFoundError) Error() string {
	return ErrTableNotFound.Error() + ": table " + e.Project + "." + e.Dataset + "." + e.Table
}

// Is lets errors.Is(err, ErrTableNotFound) match.
func (e *TableNotFoundError) Is(target error) bool { return target == ErrTableNotFound }

func unsupported(reason string) error { return &UnsupportedError{Reason: reason} }
