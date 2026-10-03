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
