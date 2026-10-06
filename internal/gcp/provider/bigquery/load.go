package bigquery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"
)

// SourceReader resolves the plaintext bytes of a GCS object named by a load
// job's gs:// source URI. It is implemented by the GCS provider (which owns
// object encryption/decryption) and injected by main.go; a nil reader disables
// load jobs (an explicit Unimplemented rather than a silent no-op), so the
// BigQuery provider never imports the storage provider.
type SourceReader interface {
	FetchObjectBytes(ctx context.Context, bucket, object string) ([]byte, error)
}

// Option configures a Provider.
type Option func(*Provider)

// WithSourceReader wires the GCS object reader that resolves gs:// load sources.
func WithSourceReader(r SourceReader) Option {
	return func(p *Provider) { p.sourceReader = r }
}

// Source formats the load subset understands. Anything else (Parquet, Avro,
// ORC, DATASTORE_BACKUP) fails loud — it is scheduled follow-up work (BQL3).
const (
	loadFormatNDJSON = "NEWLINE_DELIMITED_JSON"
	loadFormatCSV    = "CSV"
)

// maxLoadLineBytes caps a single NDJSON record; a longer line is a decode error
// rather than an unbounded allocation.
const maxLoadLineBytes = 16 << 20

// loadRecords is a decoded source: the rows that passed schema coercion and the
// records that did not (each with a human-readable reason). bad records are
// tolerated up to configuration.load.maxBadRecords.
type loadRecords struct {
	rows []map[string]any
	bad  []string
}

// runLoad evaluates a configuration.load job synchronously: it reads the single
// gs:// source from the emulated GCS, decodes it, coerces every row to the
// destination schema, and writes the result according to writeDisposition. It
// returns statistics.load, plus an optional status.errorResult for a data-level
// failure (too many bad records, WRITE_EMPTY on a non-empty table) which real
// BigQuery reports in the job result rather than as an HTTP error. A non-nil
// error is a request-level failure (unsupported option/format, malformed URI,
// missing schema) that fails jobs.insert loud.
//
// Nothing is written until every validation has passed, so a failed load never
// creates the destination table (real BigQuery's create/truncate/append is one
// atomic action on job completion).
func (p *Provider) runLoad(ctx context.Context, project string, load map[string]any, now time.Time) (stats map[string]any, jobErr map[string]any, err error) {
	if p.sourceReader == nil {
		return nil, nil, model.NewProviderError("Unimplemented", "load jobs are not enabled (no GCS source reader configured)", 501)
	}
	if err := validateLoadOptions(load); err != nil {
		return nil, nil, err
	}

	dest := mapValue(load, "destinationTable")
	datasetID := strValue(dest, "datasetId")
	tableID := strValue(dest, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, nil, invalidArgument("configuration.load.destinationTable.datasetId and tableId are required")
	}

	format := strings.ToUpper(strValue(load, "sourceFormat"))
	if format == "" {
		format = loadFormatCSV // Discovery default
	}
	if format != loadFormatNDJSON && format != loadFormatCSV {
		return nil, nil, model.NewProviderError("Unimplemented",
			"configuration.load.sourceFormat "+format+" is not supported (only NEWLINE_DELIMITED_JSON and CSV)", 501)
	}

	uris := stringSlice(load, "sourceUris")
	switch len(uris) {
	case 0:
		return nil, nil, invalidArgument("configuration.load.sourceUris is required")
	case 1:
	default:
		return nil, nil, model.NewProviderError("Unimplemented", "multiple sourceUris are not supported (one gs:// object per load)", 501)
	}
	bucket, object, perr := parseGSUri(uris[0])
	if perr != nil {
		return nil, nil, invalidArgument(perr.Error())
	}

	zeroStats := map[string]any{"inputFiles": "0", "inputFileBytes": "0", "outputRows": "0", "badRecords": "0"}
	data, ferr := p.sourceReader.FetchObjectBytes(ctx, bucket, object)
	if ferr != nil {
		// A missing object is a job-level notFound (real BigQuery accepts the
		// job then fails it); any other read failure is a real server error.
		if errors.Is(ferr, gcs.ErrNoSuchObject) {
			return zeroStats, notFound("Not found: URI " + uris[0]), nil
		}
		return nil, nil, model.NewProviderError("Internal", "failed to read source "+uris[0]+": "+ferr.Error(), 500)
	}

	loadSchema := mapValue(load, "schema")
	fields := schemaFieldsFromMap(loadSchema)

	t, terr := p.store.GetTable(ctx, project, datasetID, tableID)
	exists := true
	switch {
	case terr == nil:
		if len(fields) == 0 {
			fields = parseSchemaFields(t.Schema)
		}
	case errors.Is(terr, bqstore.ErrNoSuchTable):
		exists = false
		if len(fields) == 0 {
			return nil, nil, invalidArgument("configuration.load.schema is required to create the destination table (autodetect is not supported)")
		}
		if strings.EqualFold(strValue(load, "createDisposition"), "CREATE_NEVER") {
			// Real BigQuery: the table must already exist, else a 'notFound'
			// error is returned in the job result.
			return zeroStats, notFound(fmt.Sprintf("Not found: Table %s:%s.%s", project, datasetID, tableID)), nil
		}
	default:
		return nil, nil, mapErr(terr)
	}
	if len(fields) == 0 {
		return nil, nil, invalidArgument("destination table has no schema; provide configuration.load.schema (autodetect is not supported)")
	}

	disp := strings.ToUpper(strValue(load, "writeDisposition"))
	if disp == "" {
		disp = "WRITE_APPEND"
	}
	switch disp {
	case "WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY":
	default:
		return nil, nil, invalidArgument("configuration.load.writeDisposition " + disp + " is not supported")
	}

	recs, derr := decodeLoadRecords(format, load, fields, data)
	if derr != nil {
		return nil, nil, derr
	}
	maxBad, merr := intOption(load, "maxBadRecords")
	if merr != nil {
		return nil, nil, merr
	}

	stats = map[string]any{
		"inputFiles":     "1",
		"inputFileBytes": strconv.FormatInt(int64(len(data)), 10),
		"outputRows":     strconv.FormatInt(int64(len(recs.rows)), 10),
		"badRecords":     strconv.FormatInt(int64(len(recs.bad)), 10),
	}

	if len(recs.bad) > maxBad {
		return stats, map[string]any{
			"reason":  "invalid",
			"message": fmt.Sprintf("processing encountered too many bad records (%d > %d): %s", len(recs.bad), maxBad, recs.bad[0]),
		}, nil
	}
	if disp == "WRITE_EMPTY" && exists && t.NumRows > 0 {
		return stats, map[string]any{
			"reason":  "duplicate",
			"message": "Cannot use WRITE_EMPTY disposition with a non-empty table.",
		}, nil
	}

	// Validation passed: create the destination table (if needed) and write.
	if !exists {
		if _, derr := p.store.GetDataset(ctx, project, datasetID); derr != nil {
			return nil, nil, mapErr(derr)
		}
		nt := bqstore.Table{DatasetID: datasetID, TableID: tableID, CreateTime: now, UpdateTime: now}
		if raw, merr := json.Marshal(loadSchema); merr == nil {
			nt.Schema = raw
		}
		if cerr := p.store.CreateTable(ctx, project, datasetID, nt); cerr != nil {
			return nil, nil, mapErr(cerr)
		}
		t = nt
	}

	rows := make([]bqstore.Row, 0, len(recs.rows))
	for _, obj := range recs.rows {
		raw, merr := json.Marshal(obj)
		if merr != nil {
			return nil, nil, model.NewProviderError("Internal", "failed to encode loaded row", 500)
		}
		rows = append(rows, bqstore.Row{Data: raw})
	}
	var werr error
	if disp == "WRITE_TRUNCATE" {
		werr = p.store.ReplaceRows(ctx, project, datasetID, tableID, rows)
	} else if len(rows) > 0 {
		_, werr = p.store.InsertRows(ctx, project, datasetID, tableID, rows)
	}
	if werr != nil {
		// Roll back a table this call created so a failed write leaves no trace.
		if !exists {
			_ = p.store.DeleteTable(ctx, project, datasetID, tableID)
		}
		return nil, nil, mapErr(werr)
	}
	return stats, nil, nil
}

// notFound builds a status.errorResult for a job-level not-found failure.
func notFound(message string) map[string]any {
	return map[string]any{"reason": "notFound", "message": message}
}

// validateLoadOptions rejects load options the emulator does not model, so a
// client never gets a silently-simplified load. The options the emulator acts
// on are: sourceFormat (default CSV), sourceUris (one gs:// object),
// destinationTable, schema, writeDisposition, createDisposition,
// skipLeadingRows, fieldDelimiter, nullMarker, maxBadRecords and
// ignoreUnknownValues. The quote character is only supported at its default
// `"` (the Go standard library cannot express another); everything listed here
// fails loud.
func validateLoadOptions(load map[string]any) error {
	unsupported := func(msg string) error { return model.NewProviderError("Unimplemented", msg, 501) }
	if boolValue(load, "autodetect") {
		return unsupported("configuration.load.autodetect is not supported; provide an explicit schema")
	}
	if c := strings.ToUpper(strValue(load, "compression")); c != "" && c != "NONE" {
		return unsupported("configuration.load.compression " + c + " is not supported")
	}
	if boolValue(load, "allowJaggedRows") {
		return unsupported("configuration.load.allowJaggedRows is not supported")
	}
	if enc := strings.ToUpper(strValue(load, "encoding")); enc != "" && enc != "UTF-8" {
		return unsupported("configuration.load.encoding " + enc + " is not supported (UTF-8 only)")
	}
	if q, ok := load["quote"]; ok {
		if s, _ := q.(string); s != `"` {
			return unsupported(`configuration.load.quote is only supported as the default '"'`)
		}
	}
	if len(stringSlice(load, "schemaUpdateOptions")) > 0 {
		return unsupported("configuration.load.schemaUpdateOptions is not supported")
	}
	if mapValue(load, "timePartitioning") != nil || mapValue(load, "rangePartitioning") != nil ||
		mapValue(load, "clustering") != nil || mapValue(load, "hivePartitioningOptions") != nil {
		return unsupported("configuration.load partitioning/clustering is not supported")
	}
	for _, k := range []string{
		"dateFormat", "timeFormat", "datetimeFormat", "timestampFormat",
		"sourceColumnMatch", "referenceFileSchemaUri", "schemaInline",
	} {
		if strValue(load, k) != "" {
			return unsupported("configuration.load." + k + " is not supported")
		}
	}
	if len(stringSlice(load, "decimalTargetTypes")) > 0 || len(stringSlice(load, "nullMarkers")) > 0 {
		return unsupported("configuration.load decimalTargetTypes/nullMarkers is not supported")
	}
	if boolValue(load, "preserveAsciiControlCharacters") {
		return unsupported("configuration.load.preserveAsciiControlCharacters is not supported")
	}
	if ext := strings.ToUpper(strValue(load, "jsonExtension")); ext != "" && ext != "NONE" {
		return unsupported("configuration.load.jsonExtension " + ext + " is not supported")
	}
	return nil
}

// decodeLoadRecords decodes the source bytes into schema-coerced rows plus the
// list of records that failed. A non-nil error is a request-level failure (an
// unusable delimiter, an unreadable stream), never a per-record failure.
func decodeLoadRecords(format string, load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	switch format {
	case loadFormatNDJSON:
		return decodeNDJSON(load, fields, data)
	case loadFormatCSV:
		return decodeCSV(load, fields, data)
	}
	return nil, model.NewProviderError("Unimplemented", "unsupported sourceFormat "+format, 501)
}

// decodeNDJSON reads one JSON object per line. A malformed line, a missing
// REQUIRED field or an unknown field (unless ignoreUnknownValues) is a bad
// record. Numbers decode as json.Number so INTEGERs beyond 2^53 survive.
func decodeNDJSON(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	ignoreUnknown := boolValue(load, "ignoreUnknownValues")
	res := &loadRecords{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), maxLoadLineBytes)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(text))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			res.bad = append(res.bad, fmt.Sprintf("line %d: %v", line, err))
			continue
		}
		row, err := coerceRow(fields, obj, ignoreUnknown)
		if err != nil {
			res.bad = append(res.bad, fmt.Sprintf("line %d: %v", line, err))
			continue
		}
		res.rows = append(res.rows, row)
	}
	if err := sc.Err(); err != nil {
		return nil, invalidArgument("failed to read source: " + err.Error())
	}
	return res, nil
}

// decodeCSV reads comma-separated (or fieldDelimiter-separated) records mapped
// positionally onto the schema fields. Nested/repeated fields, jagged rows and
// a non-default quote are rejected — the standard library cannot express them.
func decodeCSV(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	if d := strValue(load, "fieldDelimiter"); d != "" {
		runes := []rune(d)
		if len(runes) != 1 {
			return nil, invalidArgument("configuration.load.fieldDelimiter must be a single character")
		}
		r.Comma = runes[0]
	}
	skip, err := intOption(load, "skipLeadingRows")
	if err != nil {
		return nil, err
	}
	for i := 0; i < skip; i++ {
		if _, err := r.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, invalidArgument("failed to read leading rows: " + err.Error())
		}
	}
	// The default null marker is the empty string; a custom one is honored by
	// csvRow (where an empty field then means "error or empty string", not NULL).
	marker, _ := load["nullMarker"].(string)

	res := &loadRecords{}
	line := skip
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		line++
		if err != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", line, err))
			continue
		}
		if len(rec) != len(fields) {
			res.bad = append(res.bad, fmt.Sprintf("row %d: expected %d columns, got %d", line, len(fields), len(rec)))
			continue
		}
		obj, err := csvRow(fields, rec, marker)
		if err != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", line, err))
			continue
		}
		res.rows = append(res.rows, obj)
	}
	return res, nil
}

// csvRow maps one CSV record positionally onto fields. The default null marker
// is the empty string (empty field => NULL). With a custom marker an empty
// field is an error for every type except STRING/BYTES (which take the empty
// value), matching the Discovery nullMarker description.
func csvRow(fields []schemaField, rec []string, nullMarker string) (map[string]any, error) {
	obj := make(map[string]any, len(fields))
	for i, f := range fields {
		if strings.EqualFold(f.Mode, "REPEATED") || isRecordType(f.Type) {
			return nil, fmt.Errorf("nested or repeated field %q cannot be loaded from CSV", f.Name)
		}
		raw := rec[i]
		if raw == nullMarker {
			if strings.EqualFold(f.Mode, "REQUIRED") {
				return nil, fmt.Errorf("missing required field %q", f.Name)
			}
			obj[f.Name] = nil
			continue
		}
		if raw == "" {
			if isStringColumn(f.Type) {
				obj[f.Name] = ""
				continue
			}
			return nil, fmt.Errorf("field %q: empty value is not allowed when a custom nullMarker is set", f.Name)
		}
		v, err := coerceScalar(f, raw)
		if err != nil {
			return nil, err
		}
		obj[f.Name] = v
	}
	return obj, nil
}

// isStringColumn reports whether a type takes an empty string as an empty value
// (rather than an error) under a custom nullMarker.
func isStringColumn(t string) bool {
	switch strings.ToUpper(t) {
	case "STRING", "BYTES":
		return true
	}
	return false
}

// coerceRow projects a decoded JSON object onto the schema (column names are
// case-insensitive, as in BigQuery): known fields are coerced to their declared
// type, absent optional fields are omitted, and unknown fields are rejected
// unless ignoreUnknown.
func coerceRow(fields []schemaField, raw map[string]any, ignoreUnknown bool) (map[string]any, error) {
	byLower := make(map[string]any, len(raw))
	for k, v := range raw {
		byLower[strings.ToLower(k)] = v
	}
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		v, ok := byLower[strings.ToLower(f.Name)]
		if !ok || v == nil {
			if strings.EqualFold(f.Mode, "REQUIRED") {
				return nil, fmt.Errorf("missing required field %q", f.Name)
			}
			continue
		}
		cv, err := coerceValue(f, v)
		if err != nil {
			return nil, err
		}
		out[f.Name] = cv
	}
	if !ignoreUnknown {
		known := make(map[string]bool, len(fields))
		for _, f := range fields {
			known[strings.ToLower(f.Name)] = true
		}
		for k := range raw {
			if !known[strings.ToLower(k)] {
				return nil, fmt.Errorf("no such field: %s", k)
			}
		}
	}
	return out, nil
}

// coerceValue coerces a decoded JSON value to the field's type, recursing into
// REPEATED elements and RECORD fields.
func coerceValue(f schemaField, v any) (any, error) {
	if strings.EqualFold(f.Mode, "REPEATED") {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("field %q is REPEATED but the value is not an array", f.Name)
		}
		elem := f
		elem.Mode = "NULLABLE"
		out := make([]any, 0, len(arr))
		for _, e := range arr {
			cv, err := coerceValue(elem, e)
			if err != nil {
				return nil, err
			}
			out = append(out, cv)
		}
		return out, nil
	}
	if isRecordType(f.Type) {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %q is a RECORD but the value is not an object", f.Name)
		}
		if len(f.Fields) == 0 {
			return obj, nil
		}
		return coerceRow(f.Fields, obj, false)
	}
	return coerceScalar(f, v)
}

// coerceScalar coerces a single scalar to the field's declared type. Strings
// (CSV fields, and lenient JSON) are parsed for numeric/boolean fields; JSON
// numbers are accepted for INTEGER/FLOAT; every other scalar renders to its
// canonical string for the string-like types. Non-finite floats are rejected —
// the JSON store cannot represent them.
func coerceScalar(f schemaField, v any) (any, error) {
	typ := strings.ToUpper(f.Type)
	if s, ok := v.(string); ok {
		trimmed := strings.TrimSpace(s)
		switch typ {
		case "INTEGER", "INT64":
			n, err := strconv.ParseInt(trimmed, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("field %q: %q is not an INTEGER", f.Name, s)
			}
			return n, nil
		case "FLOAT", "FLOAT64":
			fl, err := strconv.ParseFloat(trimmed, 64)
			if err != nil || math.IsNaN(fl) || math.IsInf(fl, 0) {
				return nil, fmt.Errorf("field %q: %q is not a finite FLOAT", f.Name, s)
			}
			return fl, nil
		case "BOOLEAN", "BOOL":
			switch strings.ToLower(trimmed) {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
			return nil, fmt.Errorf("field %q: %q is not a BOOLEAN", f.Name, s)
		default:
			return s, nil
		}
	}
	switch typ {
	case "INTEGER", "INT64":
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				return i, nil
			}
		}
		if fl, ok := v.(float64); ok && !math.IsNaN(fl) && !math.IsInf(fl, 0) && fl == math.Trunc(fl) {
			return int64(fl), nil
		}
		return nil, fmt.Errorf("field %q: value is not an INTEGER", f.Name)
	case "FLOAT", "FLOAT64":
		if n, ok := v.(json.Number); ok {
			if fl, err := n.Float64(); err == nil && !math.IsNaN(fl) && !math.IsInf(fl, 0) {
				return fl, nil
			}
			return nil, fmt.Errorf("field %q: value is not a finite FLOAT", f.Name)
		}
		if fl, ok := v.(float64); ok && !math.IsNaN(fl) && !math.IsInf(fl, 0) {
			return fl, nil
		}
		return nil, fmt.Errorf("field %q: value is not a finite FLOAT", f.Name)
	case "BOOLEAN", "BOOL":
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("field %q: value is not a BOOLEAN", f.Name)
	default:
		return scalarString(v), nil
	}
}

// scalarString renders a scalar as the string a string-like BigQuery field
// stores (numbers without an exponent where possible).
func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case json.Number:
		return x.String()
	default:
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}

// parseGSUri splits a single-object gs:// source URI. Wildcards and bucket-only
// URIs are rejected (globbing is scheduled follow-up work).
func parseGSUri(uri string) (bucket, object string, err error) {
	const scheme = "gs://"
	if !strings.HasPrefix(uri, scheme) {
		return "", "", fmt.Errorf("source URI %q must use the gs:// scheme", uri)
	}
	rest := uri[len(scheme):]
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", "", fmt.Errorf("source URI %q must be gs://bucket/object", uri)
	}
	bucket, object = rest[:i], rest[i+1:]
	if strings.ContainsAny(object, "*?[") {
		return "", "", fmt.Errorf("wildcard source URIs are not supported: %q", uri)
	}
	return bucket, object, nil
}

// schemaFieldsFromMap extracts the top-level fields from a TableSchema object
// (configuration.load.schema). A nil/unparseable schema yields nil.
func schemaFieldsFromMap(m map[string]any) []schemaField {
	if m == nil {
		return nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return parseSchemaFields(raw)
}

// stringSlice reads a []any of strings (sourceUris, schemaUpdateOptions),
// ignoring non-string entries.
func stringSlice(m map[string]any, key string) []string {
	raw, _ := m[key].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// intOption reads an integer option. The Discovery document types
// skipLeadingRows/maxBadRecords as int32 (a JSON number); a numeric string is
// also accepted leniently. Absent/empty is 0; negative or non-integer is
// InvalidArgument.
func intOption(m map[string]any, key string) (int, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}
	switch x := v.(type) {
	case string:
		if strings.TrimSpace(x) == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil || n < 0 {
			return 0, invalidArgument(key + " must be a non-negative integer")
		}
		return n, nil
	case float64:
		if x < 0 || x != math.Trunc(x) {
			return 0, invalidArgument(key + " must be a non-negative integer")
		}
		return int(x), nil
	case json.Number:
		n, err := x.Int64()
		if err != nil || n < 0 {
			return 0, invalidArgument(key + " must be a non-negative integer")
		}
		return int(n), nil
	}
	return 0, invalidArgument(key + " must be a non-negative integer")
}
