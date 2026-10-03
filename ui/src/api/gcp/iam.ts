import { api } from '../client'

const BASE = '/api/ui/v1/gcp/iam'

/** An IAM service account (the serviceAccounts.list shape). */
export interface ServiceAccount {
  name: string
  email: string
  displayName?: string
  projectId?: string
  uniqueId?: string
  description?: string
  disabled?: boolean
  etag?: string
}

export interface ListServiceAccountsResponse {
  accounts: ServiceAccount[]
  total: number
  nextPageToken?: string
}

export interface CreateServiceAccountRequest {
  accountId: string
  displayName?: string
  description?: string
}

export interface UpdateServiceAccountRequest {
  displayName?: string
  description?: string
  etag?: string
}

/** A service-account key; privateKeyData is only present on create. */
export interface ServiceAccountKey {
  name: string
  keyId: string
  keyAlgorithm?: string
  keyOrigin?: string
  keyType?: string
  validAfterTime?: string
  disabled?: boolean
  disableTime?: string
  publicKeyData?: string
  privateKeyData?: string
}

export interface ListServiceAccountKeysResponse {
  keys: ServiceAccountKey[]
  total: number
  nextPageToken?: string
}

export interface IamBinding {
  role: string
  members: string[]
  condition?: Record<string, unknown>
}

export interface IamPolicy {
  bindings?: IamBinding[]
  etag?: string
  version?: number
}

// Service accounts.
export const listServiceAccounts = () =>
  api.get<ListServiceAccountsResponse>(`${BASE}/serviceAccounts`)

export const createServiceAccount = (body: CreateServiceAccountRequest) =>
  api.post<ServiceAccount>(`${BASE}/serviceAccounts`, body)

export const getServiceAccount = (email: string) =>
  api.get<ServiceAccount>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}`)

export const updateServiceAccount = (email: string, body: UpdateServiceAccountRequest) =>
  api.patch<ServiceAccount>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}`, body)

export const deleteServiceAccount = (email: string) =>
  api.delete<void>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}`)

export const disableServiceAccount = (email: string) =>
  api.post<ServiceAccount>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/disable`)

export const enableServiceAccount = (email: string) =>
  api.post<ServiceAccount>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/enable`)

// IAM policy.
export const getServiceAccountIam = (email: string) =>
  api.get<IamPolicy>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/iam`)

export const putServiceAccountIam = (email: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/iam`, { policy })

// Keys.
export const listServiceAccountKeys = (email: string) =>
  api.get<ListServiceAccountKeysResponse>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys`,
  )

export const createServiceAccountKey = (email: string) =>
  api.post<ServiceAccountKey>(`${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys`)

export const deleteServiceAccountKey = (email: string, key: string) =>
  api.delete<void>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys/${encodeURIComponent(key)}`,
  )

export const disableServiceAccountKey = (email: string, key: string) =>
  api.post<ServiceAccountKey>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys/${encodeURIComponent(key)}/disable`,
  )

export const enableServiceAccountKey = (email: string, key: string) =>
  api.post<ServiceAccountKey>(
    `${BASE}/serviceAccounts/${encodeURIComponent(email)}/keys/${encodeURIComponent(key)}/enable`,
  )
