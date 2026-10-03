import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  CircularProgress,
  IconButton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { getTable, listRows } from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { cellValue, formatMillis, schemaFields } from './util'

function str(detail: Record<string, unknown> | undefined, key: string): string {
  const v = detail?.[key]
  return typeof v === 'string' ? v : ''
}

function cellsOf(row: Record<string, unknown>): unknown[] {
  return Array.isArray(row.f) ? row.f : []
}

/** One BigQuery table: schema and a read-only row preview. */
export function TableDetailPage() {
  const { dataset = '', table = '' } = useParams()
  const { accountId } = useAccount()

  const detail = useQuery({
    queryKey: ['gcp', 'bigquery', 'table', dataset, table, accountId],
    queryFn: () => getTable(dataset, table),
    enabled: Boolean(dataset && table),
  })

  const rows = useQuery({
    queryKey: ['gcp', 'bigquery', 'rows', dataset, table, accountId],
    queryFn: () => listRows(dataset, table),
    enabled: Boolean(dataset && table),
  })

  const fields = schemaFields(detail.data)
  const firstRow = rows.data?.rows[0]
  const columns =
    fields.length > 0
      ? fields.map((f) => f.name)
      : firstRow
        ? cellsOf(firstRow).map((_, i) => `f${i}`)
        : []

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to={`/gcp/bigquery/datasets/${encodeURIComponent(dataset)}`}
          aria-label="Back to dataset"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <Typography variant="h5" sx={{ overflowWrap: 'anywhere' }}>
            {table}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Table in {dataset} · project {accountId || '—'}
          </Typography>
        </Box>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the table.</Alert>}
      {detail.data && (
        <Box sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2, p: 2, mb: 3 }}>
          <Stack spacing={0.5}>
            <Typography variant="body2" color="text.secondary">
              Type: {str(detail.data, 'type') || 'TABLE'}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Created: {formatMillis(str(detail.data, 'creationTime'))}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Rows: {str(detail.data, 'numRows') || '0'}
            </Typography>
          </Stack>
        </Box>
      )}

      <Typography variant="h6" sx={{ mb: 1 }}>
        Schema
      </Typography>
      {fields.length === 0 ? (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
          This table has no schema.
        </Typography>
      ) : (
        <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2, mb: 3 }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Column</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Mode</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {fields.map((f, i) => (
                <TableRow key={`${f.name}-${i}`}>
                  <TableCell>{f.name}</TableCell>
                  <TableCell>{f.type}</TableCell>
                  <TableCell>{f.mode || 'NULLABLE'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      <Typography variant="h6" sx={{ mb: 1 }}>
        Preview
      </Typography>
      {rows.isError && <Alert severity="error">Failed to load rows.</Alert>}
      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              {columns.length === 0 && <TableCell>Data</TableCell>}
              {columns.map((column) => (
                <TableCell key={column}>{column}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.isLoading && (
              <TableRow>
                <TableCell colSpan={Math.max(columns.length, 1)} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!rows.isLoading && (rows.data?.rows.length ?? 0) === 0 && (
              <TableRow>
                <TableCell
                  colSpan={Math.max(columns.length, 1)}
                  align="center"
                  sx={{ py: 4, color: 'text.secondary' }}
                >
                  No rows to preview.
                </TableCell>
              </TableRow>
            )}
            {rows.data?.rows.map((row, r) => (
              <TableRow key={r} hover>
                {columns.length === 0 ? (
                  <TableCell sx={{ fontFamily: 'monospace' }}>{JSON.stringify(row)}</TableCell>
                ) : (
                  columns.map((column, c) => (
                    <TableCell key={column} sx={{ fontFamily: 'monospace' }}>
                      {cellValue(cellsOf(row)[c])}
                    </TableCell>
                  ))
                )}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}
