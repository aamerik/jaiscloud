import { useState, type ReactNode } from 'react'
import {
  Alert,
  Box,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
} from '@mui/material'
import { pageCount, paginate } from './pagination'

export interface GcpColumn<T> {
  /** Stable column key. */
  key: string
  header: ReactNode
  render: (row: T) => ReactNode
  align?: 'left' | 'center' | 'right'
  /** Optional CSS width (number = px). */
  width?: number | string
}

export interface GcpDataTableProps<T> {
  columns: GcpColumn<T>[]
  rows: T[]
  getRowKey: (row: T) => string
  loading?: boolean
  /** Error message rendered above the table. */
  error?: string | null
  emptyMessage?: string
  /** Rows per page; 0 renders every row and hides the footer. Defaults to 10. */
  rowsPerPage?: number
  /** Click handler applied to each body row. */
  onRowClick?: (row: T) => void
  /** Skeleton placeholder rows shown while loading. Defaults to 5. */
  skeletonRows?: number
  'aria-label'?: string
}

/**
 * Shared dense data table for console list pages: typed columns, loading
 * skeleton, empty state, error banner and a client-side pagination footer.
 * Server-side `nextPageToken` following is intentionally out of scope (UI18).
 */
export function GcpDataTable<T>({
  columns,
  rows,
  getRowKey,
  loading = false,
  error = null,
  emptyMessage = 'No items.',
  rowsPerPage = 10,
  onRowClick,
  skeletonRows = 5,
  'aria-label': ariaLabel,
}: GcpDataTableProps<T>) {
  const [page, setPage] = useState(0)
  const [perPage, setPerPage] = useState(rowsPerPage)
  const paginated = perPage > 0
  const safePage = Math.min(page, pageCount(rows.length, perPage) - 1)
  const visible = paginated ? paginate(rows, safePage, perPage) : rows

  return (
    <Box>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small" aria-label={ariaLabel}>
          <TableHead>
            <TableRow>
              {columns.map((column) => (
                <TableCell key={column.key} align={column.align} sx={{ width: column.width }}>
                  {column.header}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {loading &&
              Array.from({ length: skeletonRows }).map((_unused, rowIndex) => (
                <TableRow key={`skeleton-${rowIndex}`}>
                  {columns.map((column) => (
                    <TableCell key={column.key} align={column.align}>
                      <Skeleton variant="text" />
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            {!loading && !error && rows.length === 0 && (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  align="center"
                  sx={{ py: 4, color: 'text.secondary' }}
                >
                  {emptyMessage}
                </TableCell>
              </TableRow>
            )}
            {!loading &&
              visible.map((row) => (
                <TableRow
                  key={getRowKey(row)}
                  hover
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                  sx={onRowClick ? { cursor: 'pointer' } : undefined}
                >
                  {columns.map((column) => (
                    <TableCell key={column.key} align={column.align}>
                      {column.render(row)}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      {paginated && (
        <TablePagination
          component="div"
          count={rows.length}
          page={safePage}
          onPageChange={(_event, next) => setPage(next)}
          rowsPerPage={perPage}
          onRowsPerPageChange={(event) => {
            setPerPage(Number(event.target.value))
            setPage(0)
          }}
          rowsPerPageOptions={[10, 25, 50]}
        />
      )}
    </Box>
  )
}
