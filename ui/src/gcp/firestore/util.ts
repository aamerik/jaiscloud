/** parseJsonObject parses editor text, requiring a JSON object at the top
 * level. It returns either the parsed object or a human-readable error. */
export function parseJsonObject(text: string): {
  value?: Record<string, unknown>
  error?: string
} {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { error: err instanceof Error ? err.message : 'invalid JSON' }
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { error: 'Document data must be a JSON object' }
  }
  return { value: parsed as Record<string, unknown> }
}

/** pathSegments splits a Firestore collection/document path into its non-empty
 * segments, tolerant of a stray leading/trailing '/'. */
export function pathSegments(path: string): string[] {
  return path.split('/').filter((segment) => segment.length > 0)
}

/** collectionId returns the last (leaf) segment of a collection path, which is
 * the id shown as the page title. */
export function collectionId(collectionPath: string): string {
  const segments = pathSegments(collectionPath)
  return segments[segments.length - 1] ?? collectionPath
}

/** validateCollectionPath returns an error message for an invalid collection
 * path, or null when it is valid. A collection path is a root id (`users`) or
 * an alternating nested path (`users/alice/orders`): non-empty segments, no
 * "." / "..", ending on a collection id (odd segment count). Mirrors the
 * backend `collectionParam` validation. */
export function validateCollectionPath(path: string): string | null {
  if (!path) return 'Collection ID is required'
  const segments = path.split('/')
  for (const segment of segments) {
    if (!segment) return 'Collection path has an empty segment'
    if (segment === '.' || segment === '..') {
      return 'Collection path must not contain "." or ".."'
    }
  }
  if (segments.length % 2 === 0) {
    return 'Collection path must end with a collection ID, e.g. users/alice/orders'
  }
  return null
}

/** isNestedCollection reports whether a collection path is itself under a
 * document (i.e. a subcollection) rather than a root collection. Mirrors the
 * backend rule: a collection path has an odd number of segments and a nested
 * one has more than one. */
export function isNestedCollection(collectionPath: string): boolean {
  const length = pathSegments(collectionPath).length
  return length > 1 && length % 2 === 1
}

/** parentDocument locates the document that owns a nested collection. It returns
 * the parent collection path and document id, or undefined for a root
 * collection. E.g. `users/alice/orders` -> `{ collection: 'users', document:
 * 'alice' }`. */
export function parentDocument(
  collectionPath: string,
): { collection: string; document: string } | undefined {
  const segments = pathSegments(collectionPath)
  if (segments.length < 3) return undefined
  return {
    collection: segments.slice(0, -2).join('/'),
    document: segments[segments.length - 2] as string,
  }
}
