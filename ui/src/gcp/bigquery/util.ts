/** A table schema field, reduced to the columns the console previews. */
export interface SchemaField {
  name: string
  type: string
  mode?: string
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

/** parseJsonObject parses editor text, requiring a JSON object at the top
 * level. It returns either the parsed object or a human-readable error. */
export function parseJsonObject(text: string): {
  value?: Record<string, unknown>
  error?: string
} {
  if (text.trim() === '') return { value: {} }
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { error: err instanceof Error ? err.message : 'invalid JSON' }
  }
  if (!isRecord(parsed)) {
    return { error: 'Must be a JSON object' }
  }
  return { value: parsed }
}

/** formatMillis renders a BigQuery epoch-milliseconds timestamp, or em dash. */
export function formatMillis(value?: string): string {
  if (!value) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return value
  const d = new Date(n)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** schemaFields extracts the top-level fields of a table's schema. */
export function schemaFields(detail: Record<string, unknown> | undefined): SchemaField[] {
  const schema = detail?.schema
  if (!isRecord(schema) || !Array.isArray(schema.fields)) return []
  return schema.fields.filter(isRecord).map((f) => ({
    name: typeof f.name === 'string' ? f.name : '',
    type: typeof f.type === 'string' ? f.type : '',
    mode: typeof f.mode === 'string' ? f.mode : undefined,
  }))
}

/** cellValue renders one tabledata TableCell (`{ v: ... }`). Repeated and
 * record values are JSON-encoded; a missing v is null. */
export function cellValue(cell: unknown): string {
  if (!isRecord(cell)) return ''
  const v = cell.v
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}
