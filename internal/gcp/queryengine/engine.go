package queryengine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (CGO_ENABLED=0)
)

// maxHydratedRows bounds the number of rows loaded into query scratch so a
// single query cannot exhaust the process. Exceeding it fails loud.
const maxHydratedRows = 1_000_000

// Execute translates and runs one BigQuery-subset query over the tables
// resolved through cat, returning the ordered result set.
func Execute(ctx context.Context, cat Catalog, req Request) (Result, error) {
	if strings.TrimSpace(req.Query) == "" {
		return Result{}, unsupported("query is empty")
	}
	if cat == nil {
		return Result{}, errors.New("queryengine: nil catalog")
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	// modernc.org/sqlite's ":memory:" database lives on a single connection;
	// pin the pool so hydration and execution share one scratch database.
	db.SetMaxOpenConns(1)

	h := &hydrator{
		ctx:    ctx,
		cat:    cat,
		db:     db,
		req:    req,
		byKey:  map[string]string{},
		byRef:  map[string]string{},
		fields: map[string]Field{},
	}
	translated, err := translate(req.Query, h.resolve)
	if err != nil {
		return Result{}, err
	}
	return h.run(translated)
}

type hydrator struct {
	ctx    context.Context
	cat    Catalog
	db     *sql.DB
	req    Request
	next   int
	byKey  map[string]string // "project\x00dataset\x00table" -> internal name
	byRef  map[string]string // raw reference -> internal name
	fields map[string]Field  // lower(column name) -> declared field (first wins)
	total  int
}

// resolve qualifies a table reference, hydrates it on first use and returns the
// internal SQLite name.
func (h *hydrator) resolve(ref string) (string, error) {
	if name, ok := h.byRef[ref]; ok {
		return name, nil
	}
	project, dataset, table, err := h.qualify(ref)
	if err != nil {
		return "", err
	}
	key := project + "\x00" + dataset + "\x00" + table
	if name, ok := h.byKey[key]; ok {
		h.byRef[ref] = name
		return name, nil
	}
	if err := h.hydrate(project, dataset, table); err != nil {
		return "", err
	}
	name := h.byKey[key]
	h.byRef[ref] = name
	return name, nil
}

func (h *hydrator) qualify(ref string) (project, dataset, table string, err error) {
	if strings.Contains(ref, "*") {
		return "", "", "", unsupported("wildcard tables are not supported")
	}
	if strings.Contains(strings.ToUpper(ref), "INFORMATION_SCHEMA") {
		return "", "", "", unsupported("INFORMATION_SCHEMA is not supported")
	}
	parts := strings.Split(ref, ".")
	switch len(parts) {
	case 1:
		project, dataset, table = h.defaultProject(), h.req.DefaultDataset, parts[0]
		if dataset == "" {
			return "", "", "", unsupported(fmt.Sprintf("table %q must be qualified with a dataset: no defaultDataset was set", ref))
		}
	case 2:
		project, dataset, table = h.defaultProject(), parts[0], parts[1]
	case 3:
		project, dataset, table = parts[0], parts[1], parts[2]
	default:
		return "", "", "", unsupported(fmt.Sprintf("table reference %q is not supported", ref))
	}
	if project == "" || dataset == "" || table == "" {
		return "", "", "", unsupported(fmt.Sprintf("table reference %q is not fully qualified", ref))
	}
	return project, dataset, table, nil
}

func (h *hydrator) defaultProject() string {
	if h.req.DefaultProject != "" {
		return h.req.DefaultProject
	}
	return h.req.Project
}

func (h *hydrator) hydrate(project, dataset, table string) error {
	tbl, err := h.cat.Table(h.ctx, project, dataset, table)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return &TableNotFoundError{Project: project, Dataset: dataset, Table: table}
		}
		return err
	}
	if len(tbl.Fields) == 0 {
		return unsupported(fmt.Sprintf("table %s.%s.%s has no schema", project, dataset, table))
	}
	for _, f := range tbl.Fields {
		if strings.EqualFold(f.Mode, "REPEATED") {
			return unsupported(fmt.Sprintf("REPEATED column %q is not supported", f.Name))
		}
		if isNestedType(f.Type) {
			return unsupported(fmt.Sprintf("%s column %q is not supported", strings.ToUpper(f.Type), f.Name))
		}
	}
	if h.total+len(tbl.Rows) > maxHydratedRows {
		return unsupported("query scans too many rows")
	}
	h.total += len(tbl.Rows)

	name := fmt.Sprintf("bq_t%d", h.next)
	h.next++

	cols := make([]string, len(tbl.Fields))
	for i, f := range tbl.Fields {
		cols[i] = quoteSQLiteIdent(f.Name) + " " + sqliteAffinity(f.Type)
	}
	if _, err := h.db.ExecContext(h.ctx, fmt.Sprintf("CREATE TABLE %s (%s)", quoteSQLiteIdent(name), strings.Join(cols, ", "))); err != nil {
		return err
	}

	ph := make([]string, len(tbl.Fields))
	for i := range ph {
		ph[i] = "?"
	}
	insert := fmt.Sprintf("INSERT INTO %s VALUES (%s)", quoteSQLiteIdent(name), strings.Join(ph, ","))
	for _, row := range tbl.Rows {
		vals := make([]any, len(tbl.Fields))
		for i, f := range tbl.Fields {
			vals[i] = toSQLite(f, row[f.Name])
		}
		if _, err := h.db.ExecContext(h.ctx, insert, vals...); err != nil {
			return err
		}
	}

	key := project + "\x00" + dataset + "\x00" + table
	h.byKey[key] = name
	for _, f := range tbl.Fields {
		if _, ok := h.fields[strings.ToLower(f.Name)]; !ok {
			h.fields[strings.ToLower(f.Name)] = f
		}
	}
	return nil
}

func (h *hydrator) run(translated string) (Result, error) {
	rows, err := h.db.QueryContext(h.ctx, translated)
	if err != nil {
		return Result{}, unsupported(err.Error())
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return Result{}, err
	}
	fields := make([]Field, len(cols))
	for i, c := range cols {
		name := c
		if !isSimpleIdent(c) {
			name = fmt.Sprintf("f%d_", i) // BigQuery's name for an expression column
		}
		if f, ok := h.fields[strings.ToLower(name)]; ok {
			fields[i] = Field{Name: name, Type: f.Type, Mode: modeOrDefault(f.Mode)}
		} else {
			fields[i] = Field{Name: name, Mode: "NULLABLE"}
		}
	}

	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Result{}, err
		}
		for i := range vals {
			vals[i] = normalizeValue(fields[i], vals[i])
			if fields[i].Type == "" {
				fields[i].Type = inferType(vals[i])
			}
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	for i := range fields {
		if fields[i].Type == "" {
			fields[i].Type = "STRING"
		}
	}
	return Result{Fields: fields, Rows: out}, nil
}

// --- type mapping ---

// sqliteAffinity maps a BigQuery type to the SQLite column affinity used to
// hydrate a scratch table.
func sqliteAffinity(t string) string {
	switch strings.ToUpper(t) {
	case "INT64", "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT",
		"BOOL", "BOOLEAN":
		return "INTEGER"
	case "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL",
		"REAL", "DOUBLE":
		return "REAL"
	case "BYTES":
		return "BLOB"
	default: // STRING, DATE, DATETIME, TIME, TIMESTAMP, ...
		return "TEXT"
	}
}

func isNestedType(t string) bool {
	switch strings.ToUpper(t) {
	case "RECORD", "STRUCT", "ARRAY", "GEOGRAPHY", "GEOG", "JSON":
		return true
	}
	return false
}

// toSQLite converts a JSON-decoded row value to the Go type accepted by the
// scratch table's affinity.
func toSQLite(f Field, v any) any {
	if v == nil {
		return nil
	}
	switch sqliteAffinity(f.Type) {
	case "INTEGER":
		switch x := v.(type) {
		case bool:
			if x {
				return int64(1)
			}
			return int64(0)
		case int64:
			return x
		case int:
			return int64(x)
		case float64:
			return int64(x)
		case json.Number:
			if n, err := x.Int64(); err == nil {
				return n
			}
			if fv, err := x.Float64(); err == nil {
				return int64(fv)
			}
		case string:
			if n, err := strconv.ParseInt(x, 10, 64); err == nil {
				return n
			}
		}
	case "REAL":
		switch x := v.(type) {
		case bool:
			if x {
				return float64(1)
			}
			return float64(0)
		case int64:
			return float64(x)
		case float64:
			return x
		case json.Number:
			if fv, err := x.Float64(); err == nil {
				return fv
			}
		case string:
			if fv, err := strconv.ParseFloat(x, 64); err == nil {
				return fv
			}
		}
	case "BLOB":
		switch x := v.(type) {
		case []byte:
			return x
		case string:
			return []byte(x)
		}
	default: // TEXT
		if s, ok := v.(string); ok {
			return s
		}
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprint(v)
	}
	return v
}

// normalizeValue converts a scanned SQLite value into BigQuery's scalar
// representation for the result field (BOOL as a Go bool so it renders
// "true"/"false", INT64 as int64, FLOAT64 as float64, BYTES as []byte).
func normalizeValue(f Field, v any) any {
	if v == nil || f.Type == "" {
		return v
	}
	switch strings.ToUpper(f.Type) {
	case "BOOL", "BOOLEAN":
		switch x := v.(type) {
		case int64:
			return x != 0
		case float64:
			return x != 0
		case bool:
			return x
		case []byte:
			return numericTrue(string(x))
		case string:
			return numericTrue(x)
		}
	case "INT64", "INTEGER", "INT":
		switch x := v.(type) {
		case int64:
			return x
		case float64:
			return int64(x)
		case bool:
			if x {
				return int64(1)
			}
			return int64(0)
		case []byte:
			if n, err := strconv.ParseInt(string(x), 10, 64); err == nil {
				return n
			}
		case string:
			if n, err := strconv.ParseInt(x, 10, 64); err == nil {
				return n
			}
		}
	case "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL",
		"REAL", "DOUBLE":
		switch x := v.(type) {
		case float64:
			return x
		case int64:
			return float64(x)
		case bool:
			if x {
				return float64(1)
			}
			return float64(0)
		case []byte:
			if fv, err := strconv.ParseFloat(string(x), 64); err == nil {
				return fv
			}
		case string:
			if fv, err := strconv.ParseFloat(x, 64); err == nil {
				return fv
			}
		}
	default: // STRING, DATE, DATETIME, TIME, TIMESTAMP, BYTES
		switch x := v.(type) {
		case string:
			return x
		case []byte:
			return x
		case int64:
			return strconv.FormatInt(x, 10)
		case float64:
			return strconv.FormatFloat(x, 'g', -1, 64)
		case bool:
			if x {
				return "true"
			}
			return "false"
		}
	}
	return v
}

func numericTrue(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1":
		return true
	}
	return false
}

func inferType(v any) string {
	switch v.(type) {
	case bool:
		return "BOOL"
	case int64:
		return "INT64"
	case float64:
		return "FLOAT64"
	case []byte:
		return "BYTES"
	default:
		return "STRING"
	}
}

func isSimpleIdent(s string) bool {
	if s == "" || !isIdentStart(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isIdentPart(s[i]) {
			return false
		}
	}
	return true
}

func modeOrDefault(m string) string {
	if m == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(m)
}
