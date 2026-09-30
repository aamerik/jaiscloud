package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"jaiscloud/internal/gcp/queryengine"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

// invalidQuery is a BigQuery semantic/syntax error: HTTP 400 with
// ErrorProto.reason "invalidQuery" (the codec maps the "InvalidQuery" code to
// that reason). Used for constructs outside the emulator's SQL subset so the
// client never receives a successful-looking wrong result.
func invalidQuery(msg string) error {
	return model.NewProviderError("InvalidQuery", msg, 400)
}

// storeCatalog adapts the BigQuery ResourceStore to the query engine's Catalog:
// a table's extracted schema plus its streamed rows. Rows are decoded with
// json.Number so INT64 values keep full precision.
type storeCatalog struct {
	store bqstore.Store
}

func (c storeCatalog) Table(ctx context.Context, project, dataset, table string) (queryengine.Table, error) {
	t, err := c.store.GetTable(ctx, project, dataset, table)
	if err != nil {
		if errors.Is(err, bqstore.ErrNoSuchTable) {
			return queryengine.Table{}, queryengine.ErrTableNotFound
		}
		return queryengine.Table{}, err
	}
	fields := toQueryFields(parseSchemaFields(t.Schema))
	rows, err := c.store.ListRows(ctx, project, dataset, table)
	if err != nil {
		return queryengine.Table{}, err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		m := map[string]any{}
		if len(r.Data) > 0 {
			dec := json.NewDecoder(bytes.NewReader(r.Data))
			dec.UseNumber()
			if err := dec.Decode(&m); err != nil {
				return queryengine.Table{}, err
			}
		}
		out = append(out, m)
	}
	return queryengine.Table{Project: project, Dataset: dataset, Table: table, Fields: fields, Rows: out}, nil
}

func toQueryFields(fields []schemaField) []queryengine.Field {
	out := make([]queryengine.Field, len(fields))
	for i, f := range fields {
		out[i] = queryengine.Field{Name: f.Name, Type: f.Type, Mode: f.Mode}
	}
	return out
}

// runQuery executes a jobs.query request body against the store. The caller has
// already validated that body.query is present and useLegacySql is false.
func (p *Provider) runQuery(ctx context.Context, project string, body map[string]any) (queryengine.Result, error) {
	defProject, defDataset := "", ""
	if dd := mapValue(body, "defaultDataset"); dd != nil {
		defProject = strValue(dd, "projectId")
		defDataset = strValue(dd, "datasetId")
	}
	res, err := queryengine.Execute(ctx, storeCatalog{store: p.store}, queryengine.Request{
		Project:        project,
		Query:          strValue(body, "query"),
		DefaultProject: defProject,
		DefaultDataset: defDataset,
	})
	if err != nil {
		return queryengine.Result{}, mapQueryEngineErr(err)
	}
	return res, nil
}

func mapQueryEngineErr(err error) error {
	var nf *queryengine.TableNotFoundError
	if errors.As(err, &nf) {
		return model.NewProviderError("NotFound", fmt.Sprintf("Table %s.%s.%s not found", nf.Project, nf.Dataset, nf.Table), 404)
	}
	var ue *queryengine.UnsupportedError
	if errors.As(err, &ue) {
		return invalidQuery(ue.Reason)
	}
	return model.NewProviderError("Internal", err.Error(), 500)
}

// jobQueryBody extracts the query configuration stored on a job (from either
// jobs.query or jobs.insert). It returns nil when the job carries no query.
func jobQueryBody(j bqstore.Job) map[string]any {
	if len(j.Config) == 0 {
		return nil
	}
	var cfg map[string]any
	if json.Unmarshal(j.Config, &cfg) != nil {
		return nil
	}
	conf, _ := cfg["configuration"].(map[string]any)
	q, _ := conf["query"].(map[string]any)
	return q
}

// jobLocation returns the jobReference.location stored on a job, if any.
func jobLocation(j bqstore.Job) string {
	if len(j.Config) == 0 {
		return ""
	}
	var cfg map[string]any
	if json.Unmarshal(j.Config, &cfg) != nil {
		return ""
	}
	ref, _ := cfg["jobReference"].(map[string]any)
	return strValue(ref, "location")
}

// encodeQueryResult renders an executed result set as the Discovery
// jobs.query (bigquery#queryResponse) or getQueryResults
// (bigquery#getQueryResultsResponse) body.
func encodeQueryResult(kindSuffix, project, jobID, location string, res queryengine.Result) map[string]any {
	sfields := make([]schemaField, len(res.Fields))
	fields := make([]any, len(res.Fields))
	for i, f := range res.Fields {
		sf := schemaField{Name: f.Name, Type: wireFieldType(f.Type), Mode: modeOr(f.Mode)}
		sfields[i] = sf
		fields[i] = map[string]any{"name": sf.Name, "type": sf.Type, "mode": sf.Mode}
	}
	rows := make([]any, 0, len(res.Rows))
	for _, r := range res.Rows {
		cells := make([]any, len(sfields))
		for i := range sfields {
			cells[i] = tableCell(sfields[i], r[i])
		}
		rows = append(rows, map[string]any{"f": cells})
	}
	ref := map[string]any{"projectId": project, "jobId": jobID}
	if location != "" {
		ref["location"] = location
	}
	return map[string]any{
		"kind":         kindPrefix + kindSuffix,
		"jobComplete":  true,
		"jobReference": ref,
		"schema":       map[string]any{"fields": fields},
		"rows":         rows,
		"totalRows":    strconv.Itoa(len(res.Rows)),
	}
}

// wireFieldType maps an engine/BigQuery type to the name the Discovery
// TableFieldSchema.type uses on the wire.
func wireFieldType(t string) string {
	switch strings.ToUpper(t) {
	case "INT64", "INTEGER":
		return "INTEGER"
	case "FLOAT64", "FLOAT":
		return "FLOAT"
	case "BOOL", "BOOLEAN":
		return "BOOLEAN"
	case "STRUCT", "RECORD":
		return "RECORD"
	default:
		return strings.ToUpper(t)
	}
}

func modeOr(m string) string {
	if m == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(m)
}
