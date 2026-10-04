import { api } from '../client'

const BASE = '/api/ui/v1/gcp/firestore'

/** A top-level Firestore collection. Collections are implicit (created on the
 * first document write) and have no configuration of their own. */
export interface Collection {
  id: string
}

export interface ListCollectionsResponse {
  collections: Collection[]
  total: number
  nextPageToken?: string
}

/** Firestore's typed field encoding, e.g. `{ stringValue: 'Ada' }`,
 * `{ integerValue: '42' }`, `{ doubleValue: 1.5 }`, `{ mapValue: { fields } }`.
 * The console edits this object verbatim so every value type round-trips. */
export type FirestoreFields = Record<string, unknown>

/** A Firestore document. */
export interface FirestoreDocument {
  id: string
  name: string
  collection: string
  fields: FirestoreFields
  createTime?: string
  updateTime?: string
}

export interface ListDocumentsResponse {
  documents: FirestoreDocument[]
  total: number
  nextPageToken?: string
}

export interface CreateDocumentRequest {
  documentId?: string
  fields: FirestoreFields
}

export interface UpdateDocumentRequest {
  fields: FirestoreFields
  /** Observed updateTime; enforced as an optimistic-concurrency precondition. */
  updateTime?: string
}

export const listCollections = () => api.get<ListCollectionsResponse>(`${BASE}/collections`)

/** Subcollection IDs of a document. `collection` may itself be a nested
 * collection path (e.g. `users/alice/orders`). */
export const listSubcollections = (collection: string, document: string) =>
  api.get<ListCollectionsResponse>(
    `${BASE}/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(document)}/collections`,
  )

/** `collection` is a collection path: a root id (`users`) or a nested
 * collection path (`users/alice/orders`); the '/' is escaped on the wire. */
export const listDocuments = (collection: string) =>
  api.get<ListDocumentsResponse>(
    `${BASE}/collections/${encodeURIComponent(collection)}/documents`,
  )

export const getDocument = (collection: string, document: string) =>
  api.get<FirestoreDocument>(
    `${BASE}/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(document)}`,
  )

export const createDocument = (collection: string, body: CreateDocumentRequest) =>
  api.post<FirestoreDocument>(
    `${BASE}/collections/${encodeURIComponent(collection)}/documents`,
    body,
  )

export const updateDocument = (collection: string, document: string, body: UpdateDocumentRequest) =>
  api.patch<FirestoreDocument>(
    `${BASE}/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(document)}`,
    body,
  )

export const deleteDocument = (collection: string, document: string) =>
  api.delete<void>(
    `${BASE}/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(document)}`,
  )
