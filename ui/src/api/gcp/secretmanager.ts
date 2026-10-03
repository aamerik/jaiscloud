import { api } from '../client'

const BASE = '/api/ui/v1/gcp/secretmanager'

/** A Secret Manager secret. */
export interface Secret {
  name: string
  secretId?: string
  createTime?: string
  etag?: string
  labels?: Record<string, string>
  annotations?: Record<string, string>
  rotationPeriod?: string
  nextRotationTime?: string
  kmsKeyName?: string
}

export interface ListSecretsResponse {
  secrets: Secret[]
  total: number
  nextPageToken?: string
}

export interface CreateSecretRequest {
  secretId: string
  labels?: Record<string, string>
  annotations?: Record<string, string>
  rotationPeriod?: string
  nextRotationTime?: string
  kmsKeyName?: string
}

export interface UpdateSecretRequest {
  labels?: Record<string, string>
  annotations?: Record<string, string>
  rotationPeriod?: string
  nextRotationTime?: string
  kmsKeyName?: string
}

export interface SecretVersion {
  name: string
  versionId?: string
  state?: string
  createTime?: string
  destroyTime?: string
  etag?: string
}

export interface ListSecretVersionsResponse {
  versions: SecretVersion[]
  total: number
  nextPageToken?: string
}

export interface AccessSecretVersionResponse {
  name: string
  data?: string
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

const secretPath = (secret: string) => `${BASE}/secrets/${encodeURIComponent(secret)}`

const versionPath = (secret: string, version: string) =>
  `${secretPath(secret)}/versions/${encodeURIComponent(version)}`

// Secrets.
export const listSecrets = () => api.get<ListSecretsResponse>(`${BASE}/secrets`)

export const createSecret = (body: CreateSecretRequest) =>
  api.post<Secret>(`${BASE}/secrets`, body)

export const getSecret = (secret: string) => api.get<Secret>(secretPath(secret))

export const updateSecret = (secret: string, body: UpdateSecretRequest) =>
  api.patch<Secret>(secretPath(secret), body)

export const deleteSecret = (secret: string) => api.delete<void>(secretPath(secret))

// IAM policy.
export const getSecretIam = (secret: string) =>
  api.get<IamPolicy>(`${secretPath(secret)}/iam`)

export const putSecretIam = (secret: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${secretPath(secret)}/iam`, { policy })

// Versions.
export const listSecretVersions = (secret: string) =>
  api.get<ListSecretVersionsResponse>(`${secretPath(secret)}/versions`)

export const addSecretVersion = (secret: string, payload: string) =>
  api.post<SecretVersion>(`${secretPath(secret)}/versions`, { payload })

export const getSecretVersion = (secret: string, version: string) =>
  api.get<SecretVersion>(versionPath(secret, version))

export const accessSecretVersion = (secret: string, version: string) =>
  api.get<AccessSecretVersionResponse>(`${versionPath(secret, version)}/access`)

export const destroySecretVersion = (secret: string, version: string) =>
  api.post<SecretVersion>(`${versionPath(secret, version)}/destroy`)

export const disableSecretVersion = (secret: string, version: string) =>
  api.post<SecretVersion>(`${versionPath(secret, version)}/disable`)

export const enableSecretVersion = (secret: string, version: string) =>
  api.post<SecretVersion>(`${versionPath(secret, version)}/enable`)
