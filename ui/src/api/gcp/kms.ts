import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/kms'

/** A KMS key ring. */
export interface KeyRing {
  name: string
  keyRingId?: string
  location?: string
  createTime?: string
}

export interface ListKeyRingsResponse {
  keyRings: KeyRing[]
  total: number
  nextPageToken?: string
}

export interface CryptoKey {
  name: string
  cryptoKeyId?: string
  purpose?: string
  algorithm?: string
  primaryState?: string
  primaryVersion?: string
  createTime?: string
  rotationPeriod?: string
  nextRotationTime?: string
  labels?: Record<string, string>
}

export interface ListCryptoKeysResponse {
  cryptoKeys: CryptoKey[]
  total: number
  nextPageToken?: string
}

export interface CreateCryptoKeyRequest {
  cryptoKeyId: string
  purpose?: string
  algorithm?: string
  rotationPeriod?: string
  labels?: Record<string, string>
}

export interface CryptoKeyVersion {
  name: string
  versionId?: string
  state?: string
  algorithm?: string
  createTime?: string
  destroyTime?: string
  destroyEventTime?: string
}

export interface ListCryptoKeyVersionsResponse {
  cryptoKeyVersions: CryptoKeyVersion[]
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

const locationPath = (location: string) =>
  `${BASE}/locations/${encodeURIComponent(location)}`

const keyRingPath = (location: string, keyRing: string) =>
  `${locationPath(location)}/keyRings/${encodeURIComponent(keyRing)}`

const cryptoKeyPath = (location: string, keyRing: string, key: string) =>
  `${keyRingPath(location, keyRing)}/cryptoKeys/${encodeURIComponent(key)}`

// Key rings.
/** List every key ring in a location; pass `pageToken` for a single raw page. */
export async function listKeyRings(
  location: string,
  params?: { pageToken?: string },
): Promise<ListKeyRingsResponse> {
  const path = `${locationPath(location)}/keyRings`
  if (params?.pageToken) {
    return api.get<ListKeyRingsResponse>(path, { pageToken: params.pageToken })
  }
  const keyRings = await fetchAllPages(
    (pageToken) =>
      api.get<ListKeyRingsResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.keyRings,
  )
  return { keyRings, total: keyRings.length }
}

export const createKeyRing = (location: string, keyRingId: string) =>
  api.post<KeyRing>(`${locationPath(location)}/keyRings`, { keyRingId })

export const getKeyRing = (location: string, keyRing: string) =>
  api.get<KeyRing>(keyRingPath(location, keyRing))

// Crypto keys.
/** List every crypto key in a key ring; pass `pageToken` for a single raw page. */
export async function listCryptoKeys(
  location: string,
  keyRing: string,
  params?: { pageToken?: string },
): Promise<ListCryptoKeysResponse> {
  const path = `${keyRingPath(location, keyRing)}/cryptoKeys`
  if (params?.pageToken) {
    return api.get<ListCryptoKeysResponse>(path, { pageToken: params.pageToken })
  }
  const cryptoKeys = await fetchAllPages(
    (pageToken) =>
      api.get<ListCryptoKeysResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.cryptoKeys,
  )
  return { cryptoKeys, total: cryptoKeys.length }
}

export const createCryptoKey = (
  location: string,
  keyRing: string,
  body: CreateCryptoKeyRequest,
) => api.post<CryptoKey>(`${keyRingPath(location, keyRing)}/cryptoKeys`, body)

export const getCryptoKey = (location: string, keyRing: string, key: string) =>
  api.get<CryptoKey>(cryptoKeyPath(location, keyRing, key))

export const setPrimaryVersion = (
  location: string,
  keyRing: string,
  key: string,
  versionId: string,
) =>
  api.post<CryptoKey>(`${cryptoKeyPath(location, keyRing, key)}/setPrimary`, {
    cryptoKeyVersionId: versionId,
  })

// Versions.
/** List every version of a crypto key; pass `pageToken` for a single raw page. */
export async function listCryptoKeyVersions(
  location: string,
  keyRing: string,
  key: string,
  params?: { pageToken?: string },
): Promise<ListCryptoKeyVersionsResponse> {
  const path = `${cryptoKeyPath(location, keyRing, key)}/versions`
  if (params?.pageToken) {
    return api.get<ListCryptoKeyVersionsResponse>(path, { pageToken: params.pageToken })
  }
  const cryptoKeyVersions = await fetchAllPages(
    (pageToken) =>
      api.get<ListCryptoKeyVersionsResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.cryptoKeyVersions,
  )
  return { cryptoKeyVersions, total: cryptoKeyVersions.length }
}

export const createCryptoKeyVersion = (location: string, keyRing: string, key: string) =>
  api.post<CryptoKeyVersion>(`${cryptoKeyPath(location, keyRing, key)}/versions`)

export const getCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.get<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}`,
  )

export const destroyCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.post<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}/destroy`,
  )

export const disableCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.post<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}/disable`,
  )

export const enableCryptoKeyVersion = (
  location: string,
  keyRing: string,
  key: string,
  version: string,
) =>
  api.post<CryptoKeyVersion>(
    `${cryptoKeyPath(location, keyRing, key)}/versions/${encodeURIComponent(version)}/enable`,
  )

// IAM policy.
export const getKeyRingIam = (location: string, keyRing: string) =>
  api.get<IamPolicy>(`${keyRingPath(location, keyRing)}/iam`)

export const putKeyRingIam = (location: string, keyRing: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${keyRingPath(location, keyRing)}/iam`, { policy })

export const getCryptoKeyIam = (location: string, keyRing: string, key: string) =>
  api.get<IamPolicy>(`${cryptoKeyPath(location, keyRing, key)}/iam`)

export const putCryptoKeyIam = (
  location: string,
  keyRing: string,
  key: string,
  policy: IamPolicy,
) => api.put<IamPolicy>(`${cryptoKeyPath(location, keyRing, key)}/iam`, { policy })
