/**
 * Client-side pagination/filter helpers for `GcpDataTable`. Kept framework-free
 * so the table's paging logic is unit-testable without a DOM.
 */

/** Slice one page of rows; `page` is 0-based. A non-positive size returns all. */
export function paginate<T>(rows: T[], page: number, rowsPerPage: number): T[] {
  if (rowsPerPage <= 0) return rows
  const start = Math.max(0, page) * rowsPerPage
  return rows.slice(start, start + rowsPerPage)
}

/** Page count for `count` rows, never below 1 so an empty table has one page. */
export function pageCount(count: number, rowsPerPage: number): number {
  if (rowsPerPage <= 0) return 1
  return Math.max(1, Math.ceil(count / rowsPerPage))
}

/** Case-insensitive substring filter over text derived from each row. */
export function filterRows<T>(rows: T[], query: string, toText: (row: T) => string): T[] {
  const q = query.trim().toLowerCase()
  if (!q) return rows
  return rows.filter((row) => toText(row).toLowerCase().includes(q))
}
