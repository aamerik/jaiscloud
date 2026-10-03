import { api } from '../client'

const BASE = '/api/ui/v1/gcp/monitoring'

/** A Cloud Monitoring metric descriptor. */
export interface MetricDescriptor {
  name?: string
  type: string
  metricKind?: string
  valueType?: string
  unit?: string
  description?: string
  displayName?: string
  monitoredResourceTypes?: string[]
  labels?: { key: string; valueType?: string; description?: string }[]
}

export interface ListMetricDescriptorsResponse {
  metricDescriptors?: MetricDescriptor[]
  nextPageToken?: string
}

export interface TypedValue {
  boolValue?: boolean
  int64Value?: string
  doubleValue?: number
  stringValue?: string
  distributionValue?: Record<string, unknown>
}

export interface Point {
  interval?: { startTime?: string; endTime?: string }
  value?: TypedValue
}

/** A time series: points identified by metric + monitored resource. */
export interface TimeSeries {
  metric?: { type?: string; labels?: Record<string, string> }
  resource?: { type?: string; labels?: Record<string, string> }
  metricKind?: string
  valueType?: string
  unit?: string
  points?: Point[]
}

export interface ListTimeSeriesResponse {
  timeSeries?: TimeSeries[]
  nextPageToken?: string
}

export interface ListTimeSeriesParams {
  filter?: string
  startTime?: string
  endTime?: string
  pageSize?: number
  pageToken?: string
}

/** An alerting policy. */
export interface AlertPolicy {
  name: string
  displayName?: string
  documentation?: Record<string, unknown>
  conditions?: Record<string, unknown>[]
  combiner?: string
  enabled?: boolean
  notificationChannels?: string[]
  userLabels?: Record<string, string>
}

export interface ListAlertPoliciesResponse {
  alertPolicies?: AlertPolicy[]
  totalSize?: number
  nextPageToken?: string
}

export interface AlertPolicyRequest {
  displayName: string
  documentation?: Record<string, unknown>
  conditions?: Record<string, unknown>[]
  combiner?: string
  enabled?: boolean
  notificationChannels?: string[]
  userLabels?: Record<string, string>
}

/** A notification channel. */
export interface NotificationChannel {
  name: string
  type?: string
  displayName?: string
  description?: string
  labels?: Record<string, string>
  userLabels?: Record<string, string>
  enabled?: boolean
  verificationStatus?: string
}

export interface ListNotificationChannelsResponse {
  notificationChannels?: NotificationChannel[]
  totalSize?: number
  nextPageToken?: string
}

export interface NotificationChannelRequest {
  type: string
  displayName: string
  description?: string
  labels?: Record<string, string>
  userLabels?: Record<string, string>
  enabled?: boolean
}

export interface NotificationChannelDescriptor {
  name?: string
  type: string
  displayName?: string
  description?: string
  labels?: { key: string; valueType?: string; description?: string }[]
}

export interface ListNotificationChannelDescriptorsResponse {
  channelDescriptors?: NotificationChannelDescriptor[]
  nextPageToken?: string
}

// ─── metrics ──────────────────────────────────────────────────────────────────

export const listMetricDescriptors = (filter = '') =>
  api.get<ListMetricDescriptorsResponse>(`${BASE}/metricDescriptors`, { filter })

export const listTimeSeries = (params: ListTimeSeriesParams = {}) =>
  api.get<ListTimeSeriesResponse>(`${BASE}/timeSeries`, {
    filter: params.filter ?? '',
    startTime: params.startTime ?? '',
    endTime: params.endTime ?? '',
    pageSize: params.pageSize ?? '',
    pageToken: params.pageToken ?? '',
  })

// ─── alerting ─────────────────────────────────────────────────────────────────

const policyPath = (id: string) => `${BASE}/alertPolicies/${encodeURIComponent(id)}`

export const listAlertPolicies = () =>
  api.get<ListAlertPoliciesResponse>(`${BASE}/alertPolicies`)

export const createAlertPolicy = (body: AlertPolicyRequest) =>
  api.post<AlertPolicy>(`${BASE}/alertPolicies`, body)

export const updateAlertPolicy = (id: string, body: AlertPolicyRequest) =>
  api.patch<AlertPolicy>(policyPath(id), body)

export const deleteAlertPolicy = (id: string) => api.delete<void>(policyPath(id))

// ─── notification channels ────────────────────────────────────────────────────

const channelPath = (id: string) => `${BASE}/notificationChannels/${encodeURIComponent(id)}`

export const listNotificationChannels = () =>
  api.get<ListNotificationChannelsResponse>(`${BASE}/notificationChannels`)

export const createNotificationChannel = (body: NotificationChannelRequest) =>
  api.post<NotificationChannel>(`${BASE}/notificationChannels`, body)

export const updateNotificationChannel = (id: string, body: NotificationChannelRequest) =>
  api.patch<NotificationChannel>(channelPath(id), body)

export const deleteNotificationChannel = (id: string) =>
  api.delete<void>(channelPath(id))

export const listNotificationChannelDescriptors = () =>
  api.get<ListNotificationChannelDescriptorsResponse>(
    `${BASE}/notificationChannelDescriptors`,
  )
