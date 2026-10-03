// Package bigqueryui serves the BigQuery UI API. Handlers call the BigQuery
// provider directly (in-process) rather than over the wire, and reuse its
// Discovery-shaped response maps so the console shows exactly what the API
// returns.
//
// Scope is the control plane: datasets, tables and jobs. A table page previews
// rows via tabledata.list; running SQL from the console is deferred (see the
// console-UI wave plan).
package bigqueryui

// Dataset is the UI summary of a BigQuery dataset (the datasets.list shape).
type Dataset struct {
	ID           string            `json:"id"`
	DatasetID    string            `json:"datasetId"`
	ProjectID    string            `json:"projectId,omitempty"`
	Location     string            `json:"location,omitempty"`
	FriendlyName string            `json:"friendlyName,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// ListDatasetsResponse is the response for GET /datasets.
type ListDatasetsResponse struct {
	Datasets      []Dataset `json:"datasets"`
	Total         int       `json:"total"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

// CreateDatasetRequest is the body for POST /datasets. Only datasetId is
// required; the rest mirror the writable fields of the Dataset resource.
type CreateDatasetRequest struct {
	DatasetID    string            `json:"datasetId"`
	Location     string            `json:"location,omitempty"`
	FriendlyName string            `json:"friendlyName,omitempty"`
	Description  string            `json:"description,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// Table is the UI summary of a BigQuery table (the tables.list shape).
type Table struct {
	ID           string `json:"id"`
	DatasetID    string `json:"datasetId"`
	TableID      string `json:"tableId"`
	Type         string `json:"type,omitempty"`
	FriendlyName string `json:"friendlyName,omitempty"`
	CreationTime string `json:"creationTime,omitempty"`
}

// CreateTableRequest is the body for POST /datasets/{dataset}/tables. Only
// tableId is required; schema is a TableSchema object (its `fields` array).
type CreateTableRequest struct {
	TableID      string         `json:"tableId"`
	Schema       map[string]any `json:"schema,omitempty"`
	FriendlyName string         `json:"friendlyName,omitempty"`
	Description  string         `json:"description,omitempty"`
}

// ListTablesResponse is the response for GET /datasets/{dataset}/tables.
type ListTablesResponse struct {
	Tables        []Table `json:"tables"`
	Total         int     `json:"total"`
	NextPageToken string  `json:"nextPageToken,omitempty"`
}

// Job is the UI summary of a BigQuery job (the jobs.list shape, flattened).
type Job struct {
	ID            string `json:"id"`
	JobID         string `json:"jobId"`
	State         string `json:"state,omitempty"`
	StatementType string `json:"statementType,omitempty"`
	Query         string `json:"query,omitempty"`
}

// ListJobsResponse is the response for GET /jobs.
type ListJobsResponse struct {
	Jobs          []Job  `json:"jobs"`
	Total         int    `json:"total"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}

// ListRowsResponse is the response for GET /datasets/{dataset}/tables/{table}/rows.
// Rows carry the provider's TableRow encoding verbatim so the preview matches
// the wire API.
type ListRowsResponse struct {
	Rows          []map[string]any `json:"rows"`
	TotalRows     string           `json:"totalRows"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}
