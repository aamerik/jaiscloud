import { api } from '../client'

const BASE = '/api/ui/v1/gcp/storage'

export interface Bucket {
  name: string
  location?: string
  storageClass?: string
  timeCreated?: string
  updated?: string
  versioning: boolean
}

export interface ListBucketsResponse {
  items: Bucket[]
  total: number
}

export interface GCSObject {
  name: string
  size?: string
  contentType?: string
  storageClass?: string
  updated?: string
  md5Hash?: string
}

export interface ListObjectsResponse {
  items: GCSObject[]
  prefixes?: string[]
  nextPageToken?: string
}

export interface CreateBucketRequest {
  name: string
  location?: string
  storageClass?: string
}

export const listBuckets = () => api.get<ListBucketsResponse>(`${BASE}/buckets`)

export const createBucket = (body: CreateBucketRequest) => api.post<Bucket>(`${BASE}/buckets`, body)

export const deleteBucket = (name: string) =>
  api.delete<void>(`${BASE}/buckets/${encodeURIComponent(name)}`)

export const listObjects = (bucket: string, prefix?: string) =>
  api.get<ListObjectsResponse>(
    `${BASE}/buckets/${encodeURIComponent(bucket)}/objects`,
    prefix ? { prefix } : undefined,
  )
