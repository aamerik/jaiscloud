import { api } from '../client'

const BASE = '/api/ui/v1/gcp/bigquery'

/** A BigQuery dataset summary (the datasets.list shape). */
export interface BigQueryDataset {
  id: string
  datasetId: string
  projectId?: string
  location?: string
  friendlyName?: string
  labels?: Record<string, string>
}

export interface ListDatasetsResponse {
  datasets: BigQueryDataset[]
  total: number
  nextPageToken?: string
}

export interface CreateDatasetRequest {
  datasetId: string
  location?: string
  friendlyName?: string
  description?: string
  labels?: Record<string, string>
}

/** A full bigquery#dataset wire object. */
export type BigQueryDatasetDetail = Record<string, unknown>

/** A BigQuery table summary (the tables.list shape). */
export interface BigQueryTable {
  id: string
  datasetId: string
  tableId: string
  type?: string
  friendlyName?: string
  creationTime?: string
}

export interface ListTablesResponse {
  tables: BigQueryTable[]
  total: number
  nextPageToken?: string
}

export interface CreateTableRequest {
  tableId: string
  /** A TableSchema object, i.e. `{ fields: [...] }`. */
  schema?: Record<string, unknown>
  friendlyName?: string
  description?: string
}

/** A full bigquery#table wire object. */
export type BigQueryTableDetail = Record<string, unknown>

export interface ListRowsResponse {
  rows: Record<string, unknown>[]
  totalRows: string
  nextPageToken?: string
}

/** A BigQuery job summary (the jobs.list shape, flattened). */
export interface BigQueryJob {
  id: string
  jobId: string
  state?: string
  statementType?: string
  query?: string
}

export interface ListJobsResponse {
  jobs: BigQueryJob[]
  total: number
  nextPageToken?: string
}

/** A full bigquery#job wire object. */
export type BigQueryJobDetail = Record<string, unknown>

// Datasets.
export const listDatasets = () => api.get<ListDatasetsResponse>(`${BASE}/datasets`)

export const createDataset = (body: CreateDatasetRequest) =>
  api.post<BigQueryDatasetDetail>(`${BASE}/datasets`, body)

export const getDataset = (dataset: string) =>
  api.get<BigQueryDatasetDetail>(`${BASE}/datasets/${encodeURIComponent(dataset)}`)

export const deleteDataset = (dataset: string) =>
  api.delete<void>(`${BASE}/datasets/${encodeURIComponent(dataset)}`)

// Tables.
export const listTables = (dataset: string) =>
  api.get<ListTablesResponse>(`${BASE}/datasets/${encodeURIComponent(dataset)}/tables`)

export const createTable = (dataset: string, body: CreateTableRequest) =>
  api.post<BigQueryTableDetail>(
    `${BASE}/datasets/${encodeURIComponent(dataset)}/tables`,
    body,
  )

export const getTable = (dataset: string, table: string) =>
  api.get<BigQueryTableDetail>(
    `${BASE}/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table)}`,
  )

export const deleteTable = (dataset: string, table: string) =>
  api.delete<void>(
    `${BASE}/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table)}`,
  )

export const listRows = (dataset: string, table: string) =>
  api.get<ListRowsResponse>(
    `${BASE}/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table)}/rows`,
  )

// Jobs.
export const listJobs = () => api.get<ListJobsResponse>(`${BASE}/jobs`)

export const getJob = (job: string) =>
  api.get<BigQueryJobDetail>(`${BASE}/jobs/${encodeURIComponent(job)}`)

export const deleteJob = (job: string) => api.delete<void>(`${BASE}/jobs/${encodeURIComponent(job)}`)

export const cancelJob = (job: string) =>
  api.post<BigQueryJobDetail>(`${BASE}/jobs/${encodeURIComponent(job)}/cancel`)
