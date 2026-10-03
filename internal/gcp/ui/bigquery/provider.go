package bigqueryui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *bigquery.Provider used by the BigQuery UI
// handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// Datasets.
	ListDatasets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Tables.
	ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Rows (tabledata.list), read-only preview for the table page.
	ListRows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Jobs.
	ListJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CancelJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
